# -*- coding: utf-8 -*-
from contextlib import contextmanager
import functools
import json
import logging
import re
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
import helpers as h
from jobs import Job, JobTypes
import keys
import timed_queue
import storage


logger = logging.getLogger('mm.balancer')

GROUPS_META_UPDATE_TASK_ID = 'groups_meta_update'
COUPLES_META_UPDATE_TASK_ID = 'couples_meta_update'


class NodeInfoUpdater(object):

    def __init__(self, node, job_finder):
        logger.info("Created NodeInfoUpdater")
        self.__node = node
        self.job_finder = job_finder
        self.__tq = timed_queue.TimedQueue()
        self.__session = elliptics.Session(self.__node)
        wait_timeout = config.get('elliptics', {}).get('wait_timeout', None) or config.get('wait_timeout', 5)
        self.__session.set_timeout(wait_timeout)
        self.__nodeUpdateTimestamps = (time.time(), time.time())

        self.__cluster_update_lock = threading.Lock()

    def start(self):
        self.node_statistics_update()
        self.update_symm_groups()

    def _start_tq(self):
        self.__tq.start()

    def node_statistics_update(self):
        try:
            with self.__cluster_update_lock:

                start_ts = time.time()
                logger.info('Cluster updating: node statistics collecting started')
                self.monitor_stats()

                try:
                    max_group = int(self.__node.meta_session.read_data(
                        keys.MASTERMIND_MAX_GROUP_KEY).get()[0].data)
                except Exception as e:
                    logger.error('Failed to read max group number: {0}'.format(e))
                    max_group = 0

                if not len(storage.groups):
                    logger.warn('No groups found in storage')
                    return

                curr_max_group = max((g.group_id for g in storage.groups))
                logger.info('Current max group in storage: {0}'.format(curr_max_group))
                if curr_max_group > max_group:
                    logger.info('Updating storage max group to {0}'.format(curr_max_group))
                    self.__node.meta_session.write_data(
                        keys.MASTERMIND_MAX_GROUP_KEY, str(curr_max_group)).get()

        except Exception as e:
            logger.error('Failed to fetch node statistics: {0}\n{1}'.format(e, traceback.format_exc()))
        finally:
            logger.info('Cluster updating: node statistics collecting finished, time: {0:.3f}'.format(time.time() - start_ts))
            reload_period = config.get('nodes_reload_period', 60)
            self.__tq.add_task_in('node_statistics_update', reload_period, self.node_statistics_update)
            self.__nodeUpdateTimestamps = self.__nodeUpdateTimestamps[1:] + (time.time(),)
            bla.setConfigValue("dynamic_too_old_age", max(time.time() - self.__nodeUpdateTimestamps[0], reload_period * 3))


    def update_symm_groups(self):
        try:
            with self.__cluster_update_lock:
                start_ts = time.time()
                logger.info('Cluster updating: updating group coupling info started')
                self.update_symm_groups_async()
        except Exception as e:
            logger.info('Failed to update groups: {0}\n{1}'.format(
                e, traceback.format_exc()))
        finally:
            logger.info('Cluster updating: updating group coupling info finished, time: {0:.3f}'.format(time.time() - start_ts))
            # TODO: change period
            reload_period = config.get('nodes_reload_period', 60)
            self.__tq.add_task_in(GROUPS_META_UPDATE_TASK_ID, reload_period, self.update_symm_groups)

    @h.concurrent_handler
    def force_nodes_update(self, request):
        logger.info('Forcing nodes update')
        self._force_nodes_update()
        logger.info('Cluster was successfully updated')
        return True

    def _force_nodes_update(self):
        try:
            with self.__cluster_update_lock:
                self.update_status(groups=None)
        except Exception as e:
            logger.info('Failed to update nodes status: {0}\n{1}'.format(e, traceback.format_exc()))
            raise

    MONITOR_STAT_CATEGORIES = (elliptics.monitor_stat_categories.procfs |
                               elliptics.monitor_stat_categories.backend |
                               elliptics.monitor_stat_categories.stats)

    def update_status(self, groups):
        self.monitor_stats(groups=groups)
        self.update_symm_groups_async(groups=groups)

    def monitor_stats(self, groups=None):

        if groups:
            hosts = set((nb.node.host.addr, nb.node.port, nb.node.family)
                        for g in groups for nb in g.node_backends)
            host_addrs = [elliptics.Address(*host) for host in hosts]
        else:
            logger.info('Before calculating routes')
            host_addrs = set(r.address for r in self.__session.routes.get_unique_routes())
            logger.info('Unique routes calculated')

        requests = []
        for address in host_addrs:
            session = self.__session.clone()
            session.set_direct_id(address)
            logger.debug('Request for monitor_stat of node {0}'.format(
                address))
            requests.append((session.monitor_stat(address,
                self.MONITOR_STAT_CATEGORIES), address))

        for result, address in requests:
            try:
                h.process_elliptics_async_result(result, self.update_statistics)
            except Exception as e:
                logger.error('Failed to request monitor_stat for node {0}: '
                    '{1}\n{2}'.format(address, e, traceback.format_exc()))
                continue

        nbs = (groups and [nb for g in groups for nb in g.node_backends]
                      or storage.node_backends.keys())
        for nb in nbs:
            nb.update_statistics_status()
            nb.update_status()

        fss = (groups and set(nb.fs for nb in nbs) or storage.fs.keys())
        for fs in fss:
            fs.update_status()

        for group in groups or storage.groups.keys():
            logger.info('Updating status for group {0}'.format(group.group_id))
            group.update_status()

    STAT_COMMIT_RE = re.compile('^eblob\.(\d+)\.disk.stat_commit.errors\.(.*)')

    @staticmethod
    def _parsed_stats(stats):
        parsed_stats = {}

        for key, vals in stats.iteritems():
            m = NodeInfoUpdater.STAT_COMMIT_RE.match(key)
            if m is None:
                continue

            try:
                backend_id, err = m.groups()
                backend_id = int(backend_id)
                if err.isdigit():
                    err = int(err)
            except ValueError:
                continue
            backend_stats = parsed_stats.setdefault(backend_id, {})
            sc_stats = backend_stats.setdefault('stat_commit', {})
            sc_errors = sc_stats.setdefault('errors', {})
            sc_errors[err] = vals['count']

        return parsed_stats

    @staticmethod
    def update_statistics(m_stat, elapsed_time=None, end_time=None):

        node_addr = '{0}:{1}'.format(m_stat.address.host, m_stat.address.port)
        logger.debug('Cluster updating: node {0} statistics time: {1}.{2:03d}'.format(
            node_addr, elapsed_time.tsec, int(round(elapsed_time.tnsec / (1000.0 * 1000.0)))))
        logger.info('Stats: {0}'.format(node_addr))

        collect_ts = end_time.tsec

        stat = m_stat.statistics

        try:
            host_addr = m_stat.address.host
            if host_addr not in storage.hosts:
                logger.debug('Adding host {0}'.format(host_addr))
                host = storage.hosts.add(host_addr)
            else:
                host = storage.hosts[host_addr]

            if node_addr not in storage.nodes:
                node = storage.nodes.add(host, m_stat.address.port, m_stat.address.family)
            else:
                node = storage.nodes[node_addr]

            try:
                node.update_statistics(stat, collect_ts)
            except KeyError as e:
                logger.warn('Bad procfs stat for node {0} ({1}): {2}'.format(node_addr, e, stat))
                pass

            backend_stats = NodeInfoUpdater._parsed_stats(stat['stats'])

            for b_stat in stat['backends'].itervalues():
                backend_id = b_stat['backend_id']
                b_stat['stats'] = backend_stats.get(backend_id, {})

                node_backend_addr = '{0}/{1}'.format(node_addr, backend_id)
                if node_backend_addr not in storage.node_backends:
                    node_backend = storage.node_backends.add(node, backend_id)
                else:
                    node_backend = storage.node_backends[node_backend_addr]

                nb_config = (b_stat['config']
                             if 'config' in b_stat else
                             b_stat['backend']['config'])

                gid = nb_config['group']

                if gid == 0:
                    # skip zero group ids
                    continue

                if b_stat['status']['state'] != 1:
                    logger.info('Node backend {0} is not enabled: state {1}'.format(
                        str(node_backend), b_stat['status']['state']))
                    node_backend.disable()
                    continue

                if gid not in storage.groups:
                    logger.debug('Adding group {0}'.format(gid))
                    group = storage.groups.add(gid)
                else:
                    group = storage.groups[gid]

                if 'vfs' not in b_stat['backend']:
                    logger.error(
                        'Failed to parse statistics for node backend {0}, '
                        'vfs key not found: {1}'.format(node_backend, b_stat))
                    continue

                fsid = b_stat['backend']['vfs']['fsid']
                fsid_key = '{addr}:{fsid}'.format(addr=host_addr, fsid=fsid)

                if fsid_key not in storage.fs:
                    logger.debug('Adding fs {0}'.format(fsid_key))
                    fs = storage.fs.add(host, fsid)
                else:
                    fs = storage.fs[fsid_key]

                if node_backend not in fs.node_backends:
                    fs.add_node_backend(node_backend)
                fs.update_statistics(b_stat['backend']['vfs'], collect_ts)

                node_backend.enable()
                node_backend.dstat_error_code = b_stat.get('backend', {}).get(
                    'dstat', {}).get('error', 0)
                if node_backend.dstat_error_code != 0:
                    logger.info('Node backend {0} dstat returned error code {1}'.format(
                        str(node_backend), b_stat['backend']['dstat']['error']))

                if node_backend.dstat_error_code == 0:
                    logger.info('Updating statistics for node backend %s' % (str(node_backend)))
                    if 'backend' not in b_stat:
                        logger.warn('No backend in b_stat: {0}'.format(b_stat))
                    elif 'dstat' not in b_stat['backend']:
                        logger.warn('No dstat in backend: {0}'.format(b_stat['backend']))
                    try:
                        node_backend.update_statistics(b_stat, collect_ts)
                    except KeyError as e:
                        logger.warn('Bad stat for node backend {0} ({1}): {2}'.format(
                            node_backend, e, b_stat))
                        pass

                if b_stat['status']['read_only'] or node_backend.stat_commit_errors > 0:
                    node_backend.make_read_only()
                else:
                    node_backend.make_writable()

                if node_backend.group is not group:
                    logger.debug('Adding node backend {0} to group {1}{2}'.format(
                        node_backend, group.group_id,
                        ' (moved from group {0})'.format(node_backend.group.group_id)
                        if node_backend.group else ''))
                    group.add_node_backend(node_backend)

        except Exception as e:
            logger.exception('Unable to process statictics for node {}'.format(node_addr))

    def update_symm_groups_async(self, groups=None):

        _queue = set()
        def _process_group_metadata(response, group, elapsed_time=None, end_time=None):
            logger.debug('Cluster updating: group {0} meta key read time: {1}.{2}'.format(
                    group.group_id, elapsed_time.tsec, elapsed_time.tnsec))
            meta = response.data

            group.parse_meta(meta)
            couple = group.meta.get('couple')
            if couple is None:
                logger.info('Read symmetric groups from group '
                    '{0} (no couple data): {1}'.format(group.group_id, meta))
                return

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

            if group.type == storage.Group.TYPE_DATA:
                if not couple_str in storage.couples:
                    logger.info('Creating couple {0}'.format(couple_str))
                    for gid in couple:
                        if not gid in storage.groups:
                            logger.info("Group {0} doesn't exist in "
                                "all_groups, add fake data with couple={1}".format(gid, couple))
                            storage.groups.add(gid)
                    c = storage.couples.add([storage.groups[gid] for gid in couple])
                    logger.info('Created couple {0} {1}'.format(c, repr(c)))
            elif group.type == storage.Group.TYPE_CACHE:
                if not couple_str in storage.cache_couples:
                    logger.info('Creating cache couple {0}'.format(couple_str))
                    c = storage.cache_couples.add([storage.groups[gid] for gid in couple])
                    logger.info('Created cache couple {0} {1}'.format(c, repr(c)))
            return

        try:
            check_groups = groups or storage.groups.keys()

            results = {}
            for group in check_groups:
                session = self.__session.clone()
                session.add_groups([group.group_id])

                logger.debug('Request to read {0} for group {1}'.format(
                    keys.SYMMETRIC_GROUPS_KEY.replace('\0', '\\0'), group.group_id))
                results[group.group_id] = session.read_data(keys.SYMMETRIC_GROUPS_KEY)

            jobs = {}
            if self.job_finder:
                try:
                    params = {'statuses': Job.ACTIVE_STATUSES}
                    if groups:
                        params['groups'] = [g.group_id for g in groups]
                    for job in self.job_finder.jobs(**params):
                        jobs[job.group] = job
                except Exception as e:
                    logger.exception('Failed to fetch pending jobs: {0}'.format(e))
                    pass

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
                    h.process_elliptics_async_result(result, _process_group_metadata, group)
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
                    try:
                        group.set_active_job(jobs.get(group.group_id))
                    except Exception as e:
                        logger.exception('Failed to set group active job: {}'.format(e))
                        pass
                    try:
                        group.update_status_recursive()
                    except Exception as e:
                        logger.error('Failed to update group {0} status: '
                            '{1}\n{2}'.format(group, e, traceback.format_exc()))
                        pass

        except Exception as e:
            logger.error('Critical error during symmetric group '
                                 'update, {0}: {1}'.format(str(e),
                                     traceback.format_exc()))

    def stop(self):
        self.__tq.shutdown()
