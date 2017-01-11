# -*- coding: utf-8 -*-
from collections import defaultdict
import copy
import logging
import re
import threading
import time
import traceback

import elliptics

# import balancer
import helpers as h
from infrastructure import infrastructure
from jobs import Job
import keys
from load_manager import load_manager
from mastermind import helpers as mh
from mastermind.pool import skip_exceptions
from mastermind import MastermindClient
from mastermind_core.response import CachedGzipResponse
from mastermind_core import errors
from monitor_pool import monitor_pool
import timed_queue
import storage
from weight_manager import weight_manager

logger = logging.getLogger('mm.balancer')


class NodeInfoUpdaterBase(object):
    def __init__(self,
                 node,
                 job_finder,
                 namespaces_settings,
                 couple_record_finder,
                 prepare_namespaces_states,
                 prepare_flow_stats,
                 statistics):
        self._node = node
        self.statistics = statistics
        self.job_finder = job_finder
        self.namespaces_settings = namespaces_settings
        self.couple_record_finder = couple_record_finder
        self._namespaces_states = CachedGzipResponse()
        self._flow_stats = {}
        self._tq = timed_queue.TimedQueue()
        self._cluster_update_lock = threading.Lock()

        if prepare_namespaces_states and statistics is None:
            raise AssertionError('Statistics is required for namespaces states calculation')
        if prepare_flow_stats and statistics is None:
            raise AssertionError('Statistics is required for flow stats calculation')
        self._prepare_namespaces_states = prepare_namespaces_states
        self._prepare_flow_stats = prepare_flow_stats

    def _start_tq(self):
        self._tq.start()

    def _update_max_group(self):
        def _do_update_max_group(response, elapsed_time=None, end_time=None):
            if response.error.code:
                if response.error.code == error.ELLIPTICS_NOT_FOUND:
                    max_group = 0
                else:
                    raise RuntimeError(respose.error.message)
            else:
                max_group = int(response.data)

            cur_max_group = max((g.group_id for g in storage.groups))
            logger.info('Current max group in storage: {}'.format(cur_max_group))

            if cur_max_group > max_group:
                logger.info('Updating storage max group to {}'.format(max_group))
                self._node.meta_session.write_data(
                    keys.MASTERMIND_MAX_GROUP_KEY,
                    str(cur_max_group)).get()

        if not len(storage.groups):
            logger.warn('No groups found in storage, not updating max group')
            return

        try:
            session = self._node.meta_session.clone()
            session.set_exceptions_policy(elliptics.exceptions_policy.no_exceptions)
            result = session.read_data(keys.MASTERMIND_MAX_GROUP_KEY)

            try:
                h.process_elliptics_async_result(
                    result,
                    _do_update_max_group,
                    raise_on_error=False)
            except Exception as e:
                log.error('Failed to update max group number: {}'.format(e))
        except Exception as e:
            logger.error('Failed to read max group: {}'.format(e))

    @h.concurrent_handler
    def force_nodes_update(self, request):
        logger.info('Forcing nodes update')
        self._force_nodes_update()
        logger.info('Cluster was successfully updated')
        return True

    def _force_nodes_update(self, groups=None):
        raise NotImplemented('_force_nodes_update() must be implemented in derived class')

    @h.concurrent_handler
    def force_update_namespaces_states(self, request):
        start_ts = time.time()
        logger.info('Namespaces states forced updating: started')
        try:
            namespaces_settings = self.namespaces_settings.fetch()
            self._do_update_namespaces_states(namespaces_settings)
        except Exception as e:
            logger.exception('Namespaces states forced updating: failed')
            self._namespaces_states.set_exception(e)
        finally:
            logger.info('Namespaces states forced updating: finished, time: {0:.3f}'.format(
                time.time() - start_ts))

    def _update_namespaces_states(self, namespaces_settings, per_entity_stat):
        start_ts = time.time()
        logger.info('Namespaces states updating: started')
        try:
            self._do_update_namespaces_states(namespaces_settings, per_entity_stat)
        except Exception as e:
            logger.exception('Namespaces states updating: failed')
            self._namespaces_states.set_exception(e)
        finally:
            logger.info('Namespaces states updating: finished, time: {0:.3f}'.format(
                time.time() - start_ts))

    def _do_update_namespaces_states(self, namespaces_settings, per_entity_stat=None):
        def default():
            return {
                'settings': {},
                'couples': [],
                'weights': {},
                'statistics': {},
            }

        res = defaultdict(default)

        # settings
        for ns_settings in namespaces_settings:
            res[ns_settings.namespace]['settings'] = ns_settings.dump()

        # couples
        for couple in storage.replicas_groupsets:
            try:
                try:
                    ns = couple.namespace
                except ValueError:
                    continue
                # NOTE: copy is required to provide immutability of the cached objects
                # NOTE: deepcopy is not used because it is very expensive
                info = copy.copy(couple.info().serialize())
                info['hosts'] = couple.groupset_hosts()
                if couple.lrc822v1_groupset:
                    # NOTE: copy is required to provide immutability of the cached objects
                    # NOTE: deepcopy is not used because it is very expensive
                    info['groupsets'][storage.Group.TYPE_LRC_8_2_2_V1] = copy.copy(info['groupsets'][storage.Group.TYPE_LRC_8_2_2_V1])
                    info['groupsets'][storage.Group.TYPE_LRC_8_2_2_V1]['hosts'] = couple.lrc822v1_groupset.groupset_hosts()
                # couples
                res[ns.id]['couples'].append(info)
            except Exception:
                logger.exception(
                    'Failed to include couple {couple} in namespace states'.format(
                        couple=couple
                    )
                )
                continue

        # weights
        for ns_id in weight_manager.weights:
            res[ns_id]['weights'] = dict(
                (str(k), v) for k, v in weight_manager.weights[ns_id].iteritems()
            )
            logger.info('Namespace {}: weights are updated by weight manager'.format(
                ns_id
            ))

        # statistics
        for ns, stats in self.statistics.per_ns_statistics(per_entity_stat).iteritems():
            res[ns]['statistics'] = stats

        # removing internal namespaces that clients should not know about
        res.pop(storage.Group.CACHE_NAMESPACE, None)

        self._namespaces_states.set_result(dict(res))

    @h.concurrent_handler
    def force_update_flow_stats(self, request):
        start_ts = time.time()
        logger.info('Flow stats forced updating: started')
        try:
            self._do_update_flow_stats()
        finally:
            logger.info('Flow stats forced updating: finished, time: {0:.3f}'.format(
                time.time() - start_ts))

    def _update_flow_stats(self, per_entity_stat):
        start_ts = time.time()
        logger.info('Flow stats updating: started')
        try:
            self._do_update_flow_stats(per_entity_stat)
        finally:
            logger.info('Flow stats updating: finished, time: {0:.3f}'.format(
                time.time() - start_ts))

    def _do_update_flow_stats(self, per_entity_stat=None):
        try:
            self._flow_stats = self.statistics.calculate_flow_stats(per_entity_stat)
        except Exception as e:
            logger.exception('Flow stats updating: failed')
            self._flow_stats = e

    def stop(self):
        self._tq.shutdown()
