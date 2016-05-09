# -*- coding: utf-8 -*-
from collections import defaultdict
import logging
import re
import threading
import time
import traceback

import elliptics
import msgpack

# import balancer
from config import config
import helpers as h
from infrastructure import infrastructure
from jobs import Job
import keys
from load_manager import load_manager
from mastermind import helpers as mh
from mastermind.pool import skip_exceptions
from mastermind_core.response import CachedGzipResponse
from mastermind_core import errors
from monitor_pool import monitor_pool
import timed_queue
import storage
from weight_manager import weight_manager


logger = logging.getLogger('mm.balancer')

GROUPS_META_UPDATE_TASK_ID = 'groups_meta_update'
COUPLES_META_UPDATE_TASK_ID = 'couples_meta_update'


class NodeInfoUpdater(object):
    def __init__(self,
                 node,
                 job_finder,
                 couple_record_finder=None,
                 prepare_namespaces_states=False,
                 prepare_flow_stats=False,
                 statistics=None):
        logger.info("Created NodeInfoUpdater")
        self.__node = node
        self.statistics = statistics
        self.job_finder = job_finder
        self.couple_record_finder = couple_record_finder
        self._namespaces_states = CachedGzipResponse()
        self._flow_stats = {}
        self.__tq = timed_queue.TimedQueue()
        self.__session = elliptics.Session(self.__node)
        wait_timeout = config.get('elliptics', {}).get('wait_timeout') or config.get('wait_timeout', 5)
        self.__session.set_timeout(wait_timeout)
        self.__nodeUpdateTimestamps = (time.time(), time.time())

        self.__cluster_update_lock = threading.Lock()

        if prepare_namespaces_states and statistics is None:
            raise AssertionError('Statistics is required for namespaces states calculation')
        if prepare_flow_stats and statistics is None:
            raise AssertionError('Statistics is required for flow stats calculation')
        self._prepare_namespaces_states = prepare_namespaces_states
        self._prepare_flow_stats = prepare_flow_stats

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

    def update_symm_groups(self):
        try:
            with self.__cluster_update_lock:
                start_ts = time.time()
                logger.info('Cluster updating: updating group coupling info started')
                self.update_symm_groups_async()

            if self._prepare_namespaces_states:
                logger.info('Recalculating namespace states')
                self._update_namespaces_states()
            if self._prepare_flow_stats:
                logger.info('Recalculating flow stats')
                self._update_flow_stats()

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

    def _force_nodes_update(self, groups=None):
        try:
            with self.__cluster_update_lock:
                self.update_status(groups=groups)
        except Exception as e:
            logger.info('Failed to update nodes status: {0}\n{1}'.format(e, traceback.format_exc()))
            raise

    MONITOR_STAT_CATEGORIES = (elliptics.monitor_stat_categories.procfs |
                               elliptics.monitor_stat_categories.backend |
                               elliptics.monitor_stat_categories.io |
                               elliptics.monitor_stat_categories.stats |
                               elliptics.monitor_stat_categories.commands)

    def update_status(self, groups):
        self.monitor_stats(groups=groups)
        self.update_symm_groups_async(groups=groups)

    @staticmethod
    def log_monitor_stat_exc(e):
        logger.error('Malformed monitor stat response: {}'.format(e))

    @staticmethod
    def _do_get_monitor_stats(host_addrs):
        """Execute monitor stat requests via pool and return successful responses

        Sends requests to monitor pool and processes responses.
        Request can fail in several ways:
            - exception happened and http monitor stat response was not fetched;
            - http monitor stat response returned with bad status (not 2xx);
            - http monitor stat response contained invalid data (e.g.,
            malformed json);
            - monitor stat response did not contain host and/or port,
            so we cannot determine response's source.

        In case of any of these events we should log it and
        skip to the next monitor stat response.
        """
        results = monitor_pool.imap_unordered(
            None,
            ((ha.host, ha.port, ha.family) for ha in host_addrs)
        )
        logger.info('Waiting for monitor stats results')

        # TODO: set timeout!!!
        for packed_result in skip_exceptions(results,
                                             on_exc=NodeInfoUpdater.log_monitor_stat_exc):
            try:
                result = msgpack.unpackb(packed_result)
            except Exception:
                logger.exception('Malformed monitor stat result')
                continue

            try:
                node_addr = '{host}:{port}'.format(
                    host=result['host'],
                    port=result['port'],
                )
            except KeyError:
                logger.error('Malformed monitor stat result: host and port are required')
                continue

            try:
                node = storage.nodes[node_addr]
            except KeyError:
                logger.exception()
                continue

            try:
                if result['error']:
                    logger.info(
                        'Monitor stat {node}: request to {url} failed with error {error}'.format(
                            node=node,
                            url=result['url'],
                            error=result['error'],
                        )
                    )
                    continue
                elif result['code'] != 200:
                    logger.info(
                        'Monitor stat {node}: request to {url} failed with code {code}'.format(
                            node=node,
                            url=result['url'],
                            code=result['code'],
                        )
                    )
                    continue

                yield node, result
            except Exception:
                logger.exception(
                    'Failed to process monitor stat response for node {}'.format(node)
                )
                continue

    def monitor_stats(self, groups=None):
        if groups:
            hosts = set((nb.node.host.addr, nb.node.port, nb.node.family)
                        for g in groups for nb in g.node_backends)
            host_addrs = [elliptics.Address(*host) for host in hosts]
        else:
            logger.info('Before calculating routes')
            host_addrs = self.__session.routes.addresses()
            logger.info('Unique routes calculated')

        for ha in host_addrs:
            node_addr = '{host}:{port}'.format(
                host=ha.host,
                port=ha.port,
            )
            if ha.host not in storage.hosts:
                logger.debug('Adding host {}'.format(ha.host))
                host = storage.hosts.add(ha.host)
            else:
                host = storage.hosts[ha.host]
            if node_addr not in storage.nodes:
                logger.debug('Adding node {}'.format(node_addr))
                storage.nodes.add(host, ha.port, ha.family)

        responses_collected = 0
        for node, result in self._do_get_monitor_stats(host_addrs):
            responses_collected += 1
            self.update_statistics(
                node,
                result['content'],
                elapsed_time=result['request_time']
            )

        logger.info(
            'Number of hosts in route table: {}, responses collected {}'.format(
                len(host_addrs),
                responses_collected,
            )
        )

        # TODO: can we use iterkeys?
        nbs = (groups and
               [nb for g in groups for nb in g.node_backends] or
               storage.node_backends.keys())
        for nb in nbs:
            nb.update_statistics_status()
            nb.update_status()

        # TODO: can we use iterkeys?
        fss = (groups and set(nb.fs for nb in nbs) or storage.fs.keys())
        for fs in fss:
            fs.update_status()

        for group in groups or storage.groups.keys():
            logger.info('Updating status for group {0}'.format(group.group_id))
            group.update_status()

        if groups is None:
            storage.dc_host_view.update()
            load_manager.update(storage)

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
    def _process_backend_statistics(node,
                                    b_stat,
                                    backend_stats,
                                    collect_ts,
                                    processed_fss,
                                    processed_node_backends):

        backend_id = b_stat['backend_id']

        nb_config = (b_stat['config']
                     if 'config' in b_stat else
                     b_stat['backend']['config'])
        gid = nb_config['group']

        if gid == 0:
            # skip zero group ids
            return

        b_stat['stats'] = backend_stats.get(backend_id, {})

        update_group_history = False

        node_backend_addr = '{0}/{1}'.format(node, backend_id)
        if node_backend_addr not in storage.node_backends:
            node_backend = storage.node_backends.add(node, backend_id)
            update_group_history = True
        else:
            node_backend = storage.node_backends[node_backend_addr]

        if b_stat['status']['state'] != 1:
            logger.info('Node backend {0} is not enabled: state {1}'.format(
                str(node_backend), b_stat['status']['state']))
            node_backend.disable()
            return

        node_backend.enable()

        if gid not in storage.groups:
            logger.debug('Adding group {0}'.format(gid))
            group = storage.groups.add(gid)
        else:
            group = storage.groups[gid]

        fsid = b_stat['backend']['vfs']['fsid']
        fsid_key = '{host}:{fsid}'.format(host=node.host, fsid=fsid)

        if fsid_key not in storage.fs:
            logger.debug('Adding fs {0}'.format(fsid_key))
            fs = storage.fs.add(node.host, fsid)
        else:
            fs = storage.fs[fsid_key]

        if node_backend not in fs.node_backends:
            fs.add_node_backend(node_backend)

        if fs not in processed_fss:
            fs.update_statistics(b_stat['backend'], collect_ts)
            processed_fss.add(fs)

        logger.info('Updating statistics for node backend {}'.format(node_backend))
        prev_base_path = node_backend.base_path
        try:
            node_backend.update_statistics(b_stat, collect_ts)
        except KeyError as e:
            logger.warn('Bad stat for node backend {0} ({1}): {2}'.format(
                node_backend, e, b_stat))
            pass

        if node_backend.base_path != prev_base_path:
            update_group_history = True

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
            update_group_history = True

        # these backends' commands stat are used later to update accumulated
        # node commands stat
        processed_node_backends.append(node_backend)

        if update_group_history:
            logger.debug('Group {} history may be outdated, adding to update queue'.format(group))
            infrastructure.update_group_history(group)

    @staticmethod
    def update_statistics(node, stat, elapsed_time=None):

        logger.debug(
            'Cluster updating: node {0} statistics time: {1:03f}'.format(
                node, elapsed_time
            )
        )

        collect_ts = mh.elliptics_time_to_ts(stat['timestamp'])

        try:
            try:
                node.update_statistics(stat, collect_ts)
            except KeyError as e:
                logger.warn('Bad procfs stat for node {0} ({1}): {2}'.format(node, e, stat))
                pass

            fss = set()
            good_node_backends = []

            backend_stats = NodeInfoUpdater._parsed_stats(stat['stats'])

            for b_stat in stat['backends'].itervalues():
                try:
                    NodeInfoUpdater._process_backend_statistics(
                        node,
                        b_stat,
                        backend_stats,
                        collect_ts,
                        fss,
                        good_node_backends
                    )
                except Exception:
                    backend_id = b_stat['backend_id']
                    logger.exception(
                        'Failed to process backend {} stats on node {}'.format(backend_id, node)
                    )
                    continue

            logger.debug('Cluster updating: node {}, updating FS commands stats'.format(node))
            for fs in fss:
                fs.update_commands_stats()

            logger.debug('Cluster updating: node {}, updating node commands stats'.format(node))
            node.update_commands_stats(good_node_backends)

        except Exception as e:
            logger.exception('Unable to process statistics for node {}'.format(node))
        finally:
            logger.debug('Cluster updating: node {}, statistics processed'.format(node))

    def update_symm_groups_async(self, groups=None):

        _queue = set()

        def _get_data_groups(group):
            return group.meta['couple']

        def _get_lrc_groups(group):
            return group.meta['lrc']['groups']

        def _create_groupset_if_needed(groups, group_type, ns_id):

            for gid in groups:
                if gid not in storage.groups:
                    logger.info(
                        'Group {group} is not found, adding fake group '
                        'for groupset {groups}'.format(
                            group=gid,
                            groups=groups,
                        )
                    )
                    storage.groups.add(gid)

            groupset_str = ':'.join((str(gid) for gid in sorted(groups)))
            if groupset_str not in storage.groupsets:
                # TODO: somehow check that couple type matches group.type
                # for all groups in couple (not very easy when metakey read
                # fails)
                logger.info('Creating groupset {groups}, group type "{group_type}"'.format(
                    groups=groupset_str,
                    group_type=group_type,
                ))
                c = storage.groupsets.add(
                    groups=(storage.groups[gid] for gid in groups),
                    group_type=group_type,
                )

                for gid in groups:
                    infrastructure.update_group_history(storage.groups[gid])

                if ns_id not in storage.namespaces:
                    logger.info('Creating storage namespace {}'.format(ns_id))
                    ns = storage.namespaces.add(ns_id)
                else:
                    ns = storage.namespaces[ns_id]

                ns.add_couple(c)
            return storage.groupsets[groupset_str]

        def _process_group_metadata(response, group, elapsed_time=None, end_time=None):
            logger.debug('Cluster updating: group {0} meta key read time: {1}.{2}'.format(
                group.group_id, elapsed_time.tsec, elapsed_time.tnsec))

            if response.error.code:
                if response.error.code == errors.ELLIPTICS_NOT_FOUND:
                    # This group is some kind of uncoupled group, not an error
                    group.parse_meta(None)
                    logger.info(
                        'Group {group} has no metakey'.format(group=group)
                    )
                elif response.error.code in (
                    # Route list did not contain the group, expected error
                    errors.ELLIPTICS_GROUP_NOT_IN_ROUTE_LIST,
                    # Timeout in reading metakey from the group, expected error
                    errors.ELLIPTICS_TIMEOUT,
                ):
                    group.reset_meta()
                    logger.error(
                        'Error on updating metakey from group {group}: {error}'.format(
                            group=group,
                            error=response.error.message,
                        )
                    )
                else:
                    raise RuntimeError(response.error.mssage)

                return

            meta = response.data

            group.parse_meta(meta)

            if group.type == storage.Group.TYPE_UNCOUPLED_LRC_8_2_2_V1:
                return

            ns_id = group.meta.get('namespace')
            if ns_id is None:
                logger.error(
                    'Inconsistent meta read from group {group}, missing namespace: {meta}'.format(
                        group=group,
                        meta=group.meta,
                    )
                )
                return

            if group.type == storage.Group.TYPE_DATA:
                groups = _get_data_groups(group)
            elif group.type == storage.Group.TYPE_LRC_8_2_2_V1:
                groups = _get_lrc_groups(group)
            elif group.type == storage.Group.TYPE_CACHE:
                groups = _get_data_groups(group)
            else:
                raise RuntimeError(
                    'Group {group_id}, unexpected type to process: {type}'.format(
                        group_id=group.group_id,
                        type=group.type,
                    )
                )

            logger.info('Read symmetric groups from group {}: {}'.format(group.group_id, groups))

            for gid in groups:
                if gid != group.group_id:
                    logger.info('Scheduling update for group {}'.format(gid))
                    _queue.add(gid)

            groupset = _create_groupset_if_needed(groups, group.type, ns_id)

            if group.type == storage.Group.TYPE_LRC_8_2_2_V1:
                # TODO: this will become unnecessary when new "Couple" instance
                # is introduced
                data_groups = _get_data_groups(group)
                data_groupset = _create_groupset_if_needed(
                    data_groups,
                    storage.Group.TYPE_DATA,
                    ns_id
                )
                data_groupset.lrc822v1_groupset = groupset
                # TODO: this should point to a new "Couple" object
                groupset.couple = data_groupset
            return

        try:
            check_groups = groups or storage.groups.keys()

            results = {}
            for group in check_groups:
                session = self.__session.clone()
                session.set_exceptions_policy(elliptics.exceptions_policy.no_exceptions)
                session.set_filter(elliptics.filters.all_with_ack)
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
                        # TODO: this should definitely be done some other way
                        if hasattr(job, 'group'):
                            jobs[job.group] = job
                except Exception as e:
                    logger.exception('Failed to fetch pending jobs: {0}'.format(e))
                    pass

            while results:
                # TODO: Think on queue, it does not work well with lrc couples
                if _queue:
                    group_id = _queue.pop()
                    if group_id not in results:
                        continue
                    result = results.pop(group_id)
                else:
                    group_id, result = results.popitem()

                group = storage.groups[group_id]

                try:
                    h.process_elliptics_async_result(
                        result,
                        _process_group_metadata,
                        group,
                        raise_on_error=False,
                    )
                except Exception as e:
                    logger.exception(
                        'Critical error on updating metakey from group {}'.format(group_id)
                    )
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
                        logger.exception('Failed to update group {0} status: {1}'.format(group, e))
                        pass

            if groups is None:
                self.update_couple_settings()
                load_manager.update(storage)
                weight_manager.update(storage)

                infrastructure.schedule_history_update()

        except Exception as e:
            logger.exception('Critical error during symmetric group update')

    def update_couple_settings(self):
        if not self.couple_record_finder:
            # case for side worker that don't need access to couple settings
            return
        for cr in self.couple_record_finder.couple_records():
            if cr.couple_id not in storage.groups:
                logger.error('Couple record exists, but couple {couple} is not found'.format(
                    couple=cr.couple_id,
                ))
            group = storage.groups[cr.couple_id]
            if not group.couple:
                logger.error(
                    'Couple record exists and group {group} is found, '
                    'but does not participate in any couple'.format(
                        group=group.group_id,
                    )
                )
            couple = group.couple
            couple.settings = cr.settings

    @h.concurrent_handler
    def force_update_namespaces_states(self, request):
        start_ts = time.time()
        logger.info('Namespaces states forced updating: started')
        try:
            self._do_update_namespaces_states()
        except Exception as e:
            logger.exception('Namespaces states forced updating: failed')
            self._namespaces_states.set_exception(e)
        finally:
            logger.info('Namespaces states forced updating: finished, time: {0:.3f}'.format(
                time.time() - start_ts))

    def _update_namespaces_states(self):
        start_ts = time.time()
        logger.info('Namespaces states updating: started')
        try:
            self._do_update_namespaces_states()
        except Exception as e:
            logger.exception('Namespaces states updating: failed')
            self._namespaces_states.set_exception(e)
        finally:
            logger.info('Namespaces states updating: finished, time: {0:.3f}'.format(
                time.time() - start_ts))

    def _do_update_namespaces_states(self):
        def default():
            return {
                'settings': {},
                'couples': [],
                'weights': {},
                'statistics': {},
            }

        res = defaultdict(default)

        # settings
        ns_settings = infrastructure.ns_settings
        for ns, settings in ns_settings.items():
            res[ns]['settings'] = settings

        # couples
        # TODO: should we count lrc groupsets here?
        for couple in storage.replicas_groupsets:
            try:
                try:
                    ns = couple.namespace
                except ValueError:
                    continue
                info = couple.info().serialize()
                info['hosts'] = couple.couple_hosts()
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
        for ns, stats in self.statistics.per_ns_statistics().iteritems():
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

    def _update_flow_stats(self):
        start_ts = time.time()
        logger.info('Flow stats updating: started')
        try:
            self._do_update_flow_stats()
        finally:
            logger.info('Flow stats updating: finished, time: {0:.3f}'.format(
                time.time() - start_ts))

    def _do_update_flow_stats(self):
        try:
            self._flow_stats = self.statistics.calculate_flow_stats()
        except Exception as e:
            logger.exception('Flow stats updating: failed')
            self._flow_stats = e

    def stop(self):
        self.__tq.shutdown()
