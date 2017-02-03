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
from mastermind_core.config import config
from mastermind_core.max_group import max_group_manager
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
                 statistics,
                 external_storage_meta):
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
        self.external_storage_meta = external_storage_meta
        self._prepare_flow_stats = prepare_flow_stats

    def _start_tq(self):
        self._tq.start()

    def _update_max_group(self):

        try:
            curr_max_group = max((g.group_id for g in storage.groups))
            logger.info('Current max group in storage: {}'.format(curr_max_group))
            max_group_manager.update_max_group_id(curr_max_group)
        except:
            logger.exception('Failed to update max group')
            pass

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
            self._do_update_cached_responses(namespaces_settings)
        except Exception as e:
            logger.exception('Namespaces states forced updating: failed')
            self._namespaces_states.set_exception(e)
        finally:
            logger.info('Namespaces states forced updating: finished, time: {0:.3f}'.format(
                time.time() - start_ts))

    def _do_update_cached_responses(self, namespaces_settings, per_entity_stat=None):

        cache_update_start_ts = time.time()
        logger.info('Cached responses updating: started')

        try:
            result_ts = time.time()

            start_ts = time.time()
            logger.info('Namespaces states updating: started')
            namespaces_states = self._calculate_namespaces_states(
                namespaces_settings,
                per_entity_stat=per_entity_stat,
            )
            logger.info('Namespaces states updating: result prepared, time: {:.3f}'.format(
                time.time() - start_ts
            ))

            if self.external_storage_meta:
                start_ts = time.time()
                logger.info('External storage mapping updating: started')
                external_storage_mapping = self.external_storage_meta.prepare_external_storage_mapping()
                logger.info('External storage mapping updating: result prepared, time: {:.3f}'.format(
                    time.time() - start_ts
                ))

        except Exception as e:
            logger.exception('Cached responses updating: failed')
            self._namespaces_states.set_exception(e)
            self.external_storage_meta.update_external_storage_mapping(e)
            return
        else:
            self._namespaces_states.set_result(
                namespaces_states,
                ts=result_ts,
            )
            logger.info('Namespaces states updating: finished')

            if self.external_storage_meta:
                self.external_storage_meta.update_external_storage_mapping(
                    external_storage_mapping,
                    result_ts=result_ts,
                )
                logger.info('External storage mapping updating: finished')
        finally:
            logger.info('Cached responses updating: finished, time: {:.3f}'.format(
                time.time() - cache_update_start_ts
            ))

    def _calculate_namespaces_states(self, namespaces_settings, per_entity_stat=None):

        def default():
            return {
                'settings': {},
                'couples': [],
                'weights': {},
                'statistics': {},
            }

        res = defaultdict(default)

        # couples
        for couple in storage.replicas_groupsets:
            try:
                try:
                    ns = couple.namespace
                except ValueError:
                    continue
                # NOTE: copy is required to provide immutability of the cached objects
                # NOTE: deepcopy is not used because it is very expensive
                info = copy.copy(couple.info_data())
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

        # settings
        for ns_settings in namespaces_settings:
            res[ns_settings.namespace]['settings'] = ns_settings.dump()

        # removing internal namespaces that clients should not know about
        res.pop(storage.Group.CACHE_NAMESPACE, None)

        for ns_id, ns_state in res.iteritems():
            logger.debug(
                'Namespace state: namespace: {ns}, couples: {couples_count}, weighted couples: '
                '{weighted_couples_count}'.format(
                    ns=ns_id,
                    couples_count=len(ns_state['couples']),
                    weighted_couples_count=sum(
                        (len(size_weights) for size_weights in ns_state['weights'].itervalues()),
                        0
                    ),
                )
            )

        return dict(res)

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
