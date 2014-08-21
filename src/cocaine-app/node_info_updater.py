# -*- coding: utf-8 -*-
from contextlib import contextmanager
import functools
import json
import logging
import sys
import threading
import time
import traceback

from cocaine.services import Service
import elliptics

import balancer
import balancelogicadapter as bla
from config import config
import errors
import keys
import timed_queue
import storage


logger = logging.getLogger('mm.balancer')

GROUPS_META_UPDATE_TASK_ID = 'update_symms_for_groups'
COUPLES_META_UPDATE_TASK_ID = 'update_meta_for_couples'


class NodeInfoUpdater(object):

    STORAGE_STATE_CACHE_KEY = 'mastermind_storage'
    STORAGE_STATE_VERSION = '1'
    DEFAULT_STORAGE_STATE_VALID_TIME = 600

    def __init__(self, node):
        logger.info("Created NodeInfoUpdater")
        self.__node = node
        self.__tq = timed_queue.TimedQueue()
        self.__tq.start()
        self.__session = elliptics.Session(self.__node)
        wait_timeout = config.get('elliptics', {}).get('wait_timeout', None) or config.get('wait_timeout', 5)
        self.__session.set_timeout(wait_timeout)
        self.__nodeUpdateTimestamps = (time.time(), time.time())

        self.loadNodes(delayed=False)

    def execute_tasks(self, delayed):
        try:

            # stat = getattr(self.__session, 'stat_log_count',
            #                self.__session.stat_log)
            # storage.update_statistics(stat())
            self.monitor_stats()

            if delayed:
                self.__tq.add_task_in(
                        GROUPS_META_UPDATE_TASK_ID,
                        config.get('symm_group_read_gap', 1),
                        self.update_symm_groups_async)
                self.__tq.add_task_in(
                        COUPLES_META_UPDATE_TASK_ID,
                        config.get('couple_read_gap', 1),
                        self.update_couples_meta_async)
            else:
                self.update_symm_groups_async()
                self.update_couples_meta_async()

        except Exception as e:
            logger.info('Failed to initialize node updater: %s\n%s' % (str(e), traceback.format_exc()))
            pass

    def loadNodes(self, delayed=True):
        logger.info('Start loading units')
        try:

            self.execute_tasks(delayed)

            try:
                max_group = self.__node.meta_session.read_data(
                    keys.MASTERMIND_MAX_GROUP_KEY).get()[0].data
            except:
                max_group = 0

            if not len(storage.groups):
                logger.warn('No groups found in storage')
                return

            curr_max_group = max((g.group_id for g in storage.groups))
            if curr_max_group > max_group:
                self.__node.meta_session.write_data(
                    keys.MASTERMIND_MAX_GROUP_KEY, str(curr_max_group)).get()

        except Exception as e:
            logger.error("Error while loading node stats: %s\n%s" % (str(e), traceback.format_exc()))
        finally:
            reload_period = config.get('nodes_reload_period', 60)
            self.__tq.add_task_in("load_nodes", reload_period, self.loadNodes)
            self.__nodeUpdateTimestamps = self.__nodeUpdateTimestamps[1:] + (time.time(),)
            bla.setConfigValue("dynamic_too_old_age", max(time.time() - self.__nodeUpdateTimestamps[0], reload_period * 3))

    def force_nodes_update(self, request):
        logger.info('Forcing nodes update')
        try:
            self.__tq.add_task_in('load_nodes', 0, self.loadNodes)
            logger.info('Task for nodes update was created successfully')
        except Exception:
            logger.info('Task for nodes update has already been created')
            self.__tq.hurry('load_nodes')
        return True

    def monitor_stats(self):
        hosts_id = {}
        for r in self.__session.routes.get_unique_routes():
            if not r.address in hosts_id:
                hosts_id[r.address] = r.id

        requests = []
        for address, eid in hosts_id.iteritems():
            session = self.__session.clone()
            session.set_direct_id(address)
            logger.debug('Request for monitor_stat of node {0}'.format(
                address))
            requests.append((session.monitor_stat(address), address))

        for result, address in requests:
            try:
                self._process_elliptics_async_result(result, self.update_statistics)
            except Exception as e:
                logger.error('Failed to request monitor_stat for node {0}: '
                    '{1}\n{2}'.format(address, e, traceback.format_exc()))
                continue

    @staticmethod
    def update_statistics(m_stat):

        node_addr = '{0}:{1}'.format(m_stat.address.host, m_stat.address.port)
        logger.info('Stats: {0}'.format(node_addr))

        stat = m_stat.statistics

        try:
            if not node_addr in storage.nodes:
                if not m_stat.address.host in storage.hosts:
                    logger.debug('Adding host {0}'.format(m_stat.address.host))
                    host = storage.hosts.add(m_stat.address.host)
                else:
                    host = storage.hosts[m_stat.address.host]

                node = storage.nodes.add(host, m_stat.address.port, m_stat.address.family)
            else:
                node = storage.nodes[node_addr]

            node.update_statistics(stat)

            for b_stat in stat['backends'].itervalues():
                backend_id = b_stat['backend_id']

                node_backend_addr = '{0}/{1}'.format(node_addr, backend_id)
                if not node_backend_addr in storage.node_backends:
                    node_backend = storage.node_backends.add(node, backend_id)
                else:
                    node_backend = storage.node_backends[node_backend_addr]

                gid = b_stat['config']['group']

                if gid == 0:
                    # skip zero group ids
                    continue

                if not gid in storage.groups:
                    logger.debug('Adding group {0}'.format(gid))
                    group = storage.groups.add(gid)
                else:
                    group = storage.groups[gid]

                if b_stat['status']['state'] == 0:
                    logger.info('Disabling node backend %s' % (str(node_backend)))
                    node_backend.disable()
                else:
                    logger.info('Updating statistics for node backend %s' % (str(node_backend)))
                    node_backend.enable()
                    node_backend.update_statistics(b_stat)

                    if not node_backend in group.node_backends:
                        logger.debug('Adding node backend %d -> %s' %
                                      (gid, node_backend))
                        group.add_node_backend(node_backend)

                logger.info('Updating status for group %d' % gid)
                group.update_status()

        except Exception as e:
            logger.error('Unable to process statictics for node {0}: '
                '{1}\n{2}'.format(node_addr, e, traceback.format_exc()))

    def update_symm_groups_async(self):

        _queue = set()
        def _process_group_metadata(response, group):

            meta = response.data

            group.parse_meta(meta)
            couple = group.meta['couple']
            logger.info('Read symmetric groups from group '
                '{0}: {1}'.format(group.group_id, couple))
            for gid in couple:
                if gid != group.group_id:
                    logger.info('Scheduling update '
                        'for group {0}'.format(gid))
                    _queue.add(gid)

            couple_str = ':'.join((str(gid) for gid in sorted(couple)))

            logger.debug('{0} in storage.couples: {1}'.format(
                couple_str, couple_str in storage.couples))

            if not couple_str in storage.couples:
                logger.info('Creating couple {0}'.format(couple_str))
                for gid in couple:
                    if not gid in storage.groups:
                        logger.info("Group {0} doesn't exist in "
                            "all_groups, add fake data with couple={1}".format(gid, couple))
                        storage.groups.add(gid)
                c = storage.couples.add([storage.groups[gid] for gid in couple])
                logger.info('Created couple {0} {1}'.format(c, repr(c)))
            return

        try:
            groups = storage.groups.keys()

            results = {}
            for group in groups:
                session = self.__session.clone()
                session.add_groups([group.group_id])

                logger.debug('Request to read {0} for group {1}'.format(
                     keys.SYMMETRIC_GROUPS_KEY.replace('\0', '\\0'), group.group_id))
                results[group.group_id] = session.read_data(keys.SYMMETRIC_GROUPS_KEY)

            while results:
                if _queue:
                    group_id = _queue.pop()
                    if group_id not in results:
                        continue
                    result = results.pop(group_id)
                else:
                    group_id, result = results.popitem()

                group = storage.groups[group_id]

                try:
                    self._process_elliptics_async_result(result, _process_group_metadata, group)
                except elliptics.NotFoundError as e:
                    logger.warn('Failed to read symmetric_groups '
                        'from group {0}: {1}'.format(group_id, e))
                    group.parse_meta(None)
                except Exception as e:
                    logger.error('Failed to read symmetric_groups '
                        'from group {0}: {1}\n{2}'.format(
                            group_id, e, traceback.format_exc()))
                    group.parse_meta(None)
                finally:
                    group.update_status_recursive()

        except Exception as e:
            logger.error('Critical error during symmetric group '
                                 'update, {0}: {1}'.format(str(e),
                                     traceback.format_exc()))

    def update_couples_meta_async(self):
        try:
            couples = storage.couples.keys()

            requests = []
            for couple in couples:
                session = self.__node.meta_session.clone()
                key = keys.MASTERMIND_COUPLE_META_KEY % str(couple)
                logger.debug('Request to read {0} for couple {1}'.format(
                     key, couple))
                requests.append((session.read_latest(key), couple))

            for result, couple in requests:
                try:
                    self._process_elliptics_async_result(result,
                        lambda r: couple.parse_meta(r.data))
                except elliptics.NotFoundError:
                    # no couple meta data, no need to worry
                    couple.parse_meta(None)
                except Exception as e:
                    logger.error('Failed to request couple meta key for '
                        'couple {0}: {1}\n{2}'.format(couple, e, traceback.format_exc()))
                    couple.parse_meta(None)
                finally:
                    couple.update_status()

        except Exception as e:
            logger.error('Critical error during couples metadata '
                                 'update, {0}: {1}'.format(str(e),
                                     traceback.format_exc()))

    @staticmethod
    def _process_elliptics_async_result(result, processor, *args, **kwargs):
        result.wait()
        if not len(result.get()):
            raise ValueError('empty response')
        entry = result.get()[0]
        if entry.error.code:
            raise Exception(entry.error.message)

        return processor(entry, *args, **kwargs)

    def stop(self):
        self.__tq.shutdown()
