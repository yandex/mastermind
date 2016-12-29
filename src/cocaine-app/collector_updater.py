# -*- coding: utf-8 -*-
from collections import defaultdict
import simplejson
import logging
import uuid

# import balancer
from mastermind_core.config import config
from infrastructure import infrastructure
from jobs import Job
from load_manager import load_manager
from mastermind import helpers as mh
from mastermind import MastermindClient
import storage
from weight_manager import weight_manager
from node_info_updater_base import NodeInfoUpdaterBase
from tornado.ioloop import IOLoop


logger = logging.getLogger('mm.balancer')

COLLECTOR_SERVICE_NAME = 'mastermind-collector'

GROUPSET_TYPE_REPLICAS = 'REPLICAS'
GROUPSET_TYPE_LRC = 'LRC'


def update_commands_stat(comm_stat, state):
    comm_stat.ell_disk_read_rate = state['ell_disk_read_rate']
    comm_stat.ell_disk_write_rate = state['ell_disk_write_rate']
    comm_stat.ell_net_read_rate = state['ell_net_read_rate']
    comm_stat.ell_net_write_rate = state['ell_net_write_rate']

def create_groupset_if_needed(gsid, groups, gstype, nsid, status=None, status_text=None):
    for gid in groups:
        if gid not in storage.groups:
            logger.info(
                'Group {group} is not found, adding fake group '
                'for groupset {groupset}'.format(
                    group=gid,
                    groupset=gsid,
                )
            )
            storage.groups.add(gid)

    if gsid not in storage.groupsets:
        logger.info('Creating groupset {groupset} of type {type}'.format(
            groupset=gsid,
            type=gstype,
        ))

        if gstype == GROUPSET_TYPE_REPLICAS:
            group_type = storage.Group.TYPE_DATA
        else:
            group_type = storage.Group.TYPE_LRC_8_2_2_V1

        groupset = storage.groupsets.add(
            groups=(storage.groups[gid] for gid in groups),
            group_type=group_type,
        )

        if status:
            groupset.status = status

        if status_text:
            groupset.status_text = status_text

        for gid in groups:
            infrastructure.update_group_history(storage.groups[gid])

        if nsid not in storage.namespaces:
            # In fact, the namespace must have already been created
            # TODO: warning?
            logger.info('Creating storage namespace {}'.format(nsid))
            ns = storage.namespaces.add(nsid)
        else:
            ns = storage.namespaces[nsid]

        ns.add_couple(groupset)

    return storage.groupsets[gsid]

class NodeInfoUpdater(NodeInfoUpdaterBase):
    # XXX add config option for collector worker name, address?
    def __init__(self,
                 node,
                 job_finder,
                 namespaces_settings,
                 couple_record_finder=None,
                 prepare_namespaces_states=False,
                 prepare_flow_stats=False,
                 statistics=None):
        super(NodeInfoUpdater, self).__init__(node=node,
                                              job_finder=job_finder,
                                              namespaces_settings=namespaces_settings,
                                              couple_record_finder=couple_record_finder,
                                              prepare_namespaces_states=prepare_namespaces_states,
                                              prepare_flow_stats=prepare_flow_stats,
                                              statistics=statistics)

        self._tq.add_task_in(task_id='init_ioloop', secs=0, function=self._init_ioloop)

        logger.info('Created collector.NodeInfoUpdater')

    def _init_ioloop(self):
        io_loop = IOLoop()
        io_loop.make_current()

    def start(self):
        logger.info('Starting')
        self.update()

    def new_str_traceid(self):
        return '%x' % (uuid.uuid1().int >> 64)

    def _force_collector_refresh(self, groups=None):
        collector_client = MastermindClient(COLLECTOR_SERVICE_NAME)

        if groups:
            request = {
                'item_types': [
                    'host',
                    'node',
                    'backend',
                    'fs',
                    'group',
                    'couple',
                    'namespace',
                ],
                'filter': {
                    'groups': []
                }
            }

            filter_groups = request['filter']['groups']
            for g in groups:
                filter_groups.append(g.group_id)

            response = collector_client.request('refresh', simplejson.dumps(request))
        else:
            response = collector_client.request('force_update', '')

        # Response is human-readable.
        logger.info('Collector refresh completed: {}'.format(response))

    def _force_nodes_update(self, groups=None):
        with self._cluster_update_lock:
            try:
                response = _force_collector_refresh(groups)
                logger.info('Collector refresh completed: {}'.format(response))
            except Exception as e:
                logger.error('Force collector refresh failed: {}'.format(e))
                raise e

    def monitor_stats(self, groups=None):
        self._force_nodes_update(groups=groups)
        self.update_status(groups=groups)

    def update_symm_groups_async(self):
        try:
            self._force_nodes_update()
        except Exception:
            return

        self.update()

    def update_status(self, groups):
        self._force_nodes_update(groups)
        self.update(groups=groups)

    def update(self, groups=None):
        logger = logging.getLogger('mm.balancer.update')
        traceid = self.new_str_traceid()
        logger.info('Fetching update from collector, traceid = {}'.format(traceid))

        with self._cluster_update_lock:
            try:
                request = {
                    'item_types': [
                        'host',
                        'node',
                        'backend',
                        'fs',
                        'group',
                        'couple',
                        'namespace',
                    ]
                }

                if groups:
                    filter_groups = request.setdefault('filter', {}).setdefault('groups', [])
                    for g in groups:
                        filter_groups.append(g.group_id)

                logger.info('Sending request to collector')

                collector_client = MastermindClient(COLLECTOR_SERVICE_NAME, traceid=traceid)
                response = collector_client.request('get_snapshot', simplejson.dumps(request))
            except Exception as e:
                logger.error('Failed to fetch snapshot from collector: {}'.format(e))
                self._schedule_next_round()
                return

            logger.info('Applying update')

            try:
                logger.info('Parsing json')
                snapshot = simplejson.loads(response)
                logger.info('Processing hosts')
                self._process_hosts(snapshot['hosts'])
                logger.info('Processing nodes')
                self._process_nodes(snapshot['nodes'])
                logger.info('Processing filesystems')
                self._process_filesystems(snapshot['filesystems'])
                logger.info('Processing backends')
                self._process_backends(snapshot['backends'])
                logger.info('Processing groups')
                self._process_groups(snapshot['groups'])
                logger.info('Processing jobs(groups)')
                self._process_jobs(groups)
                logger.info('Processing namespaces')
                self._process_namespaces(snapshot['namespaces'])
                logger.info('Processing couples')
                self._process_couples(snapshot['couples'])
                logger.info('Processing complete')
            except Exception as e:
                logger.error('Failed to process update from collector: {}'.format(e))
                self._schedule_next_round()
                return

            logger.info('self._update_max_group')
            self._update_max_group()
            logger.info('self._update_max_group complete')

            try:
                if groups is None:
                    logger.info('self.namespaces_settings.fetch()')
                    namespaces_settings = self.namespaces_settings.fetch()
                    logger.info('storage.dc_host_view.update()')
                    storage.dc_host_view.update()
                    logger.info('load_manager.update(storage)')
                    load_manager.update(storage)
                    logger.info('weight_manager.update(storage, namespaces_settings)')
                    weight_manager.update(storage, namespaces_settings)
                    logger.info('infrastructure.schedule_history_update()')
                    infrastructure.schedule_history_update()

                    if self._prepare_namespaces_states:
                        logger.info('Recalculating namespaces states')
                        self._update_namespaces_states(namespaces_settings)
                    if self._prepare_flow_stats:
                        logger.info('Recalculating flow stats')
                        self._update_flow_stats()

            except Exception as e:
                logger.error('Failed to update stuff: {}'.format(e))
            finally:
                self._schedule_next_round()
            logger.info('NodeInfoUpdater.update() complete')

    def _process_hosts(self, host_states):
        for host_state in host_states:
            try:
                host_id = host_state['id']
            except KeyError:
                logger.error('Malformed response from collector, failed to get host id');
                continue
            try:
                self._process_host(host_id, host_state)
            except Exception as e:
                logger.error('Failed to process host {host_id} state '
                             'from collector: {e}'.format(
                                host_id=host_id,
                                e=e,
                ))
                continue

    def _process_host(self, host_id, host_state):
        if host_id not in storage.hosts:
            logger.info('Creating host {}'.format(host_id))
            host = storage.hosts.add(host_id)
        else:
            host = storage.hosts[host_id]

        host.update(host_state)

    def _process_nodes(self, node_states):
        for node_state in node_states:
            try:
                node_id = node_state['id']
            except KeyError:
                logger.error('Malformed response from collector, failed to parse node id')
                continue
            try:
                self._process_node(node_id, node_state)
            except Exception as e:
                logger.error('Failed to process node {node_id} state from '
                             'collector: {e}'.format(
                            node_id=node_id,
                            e=e
                ))
                continue

    def _process_node(self, node_id, node_state):
        host_id = node_state['host_id']

        host = storage.hosts[host_id]

        if node_id not in storage.nodes:
            logger.info('Creating node {}'.format(node_id))
            node = storage.nodes.add(host, node_state['port'], node_state['family'])
            node.stat = storage.NodeStat()
        else:
            node = storage.nodes[node_id]

        stat = node.stat

        stat.ts = mh.elliptics_time_to_ts(node_state['timestamp'])
        stat.load_average = node_state['load_average']
        stat.tx_rate = node_state['tx_rate']
        stat.rx_rate = node_state['rx_rate']

        update_commands_stat(stat.commands_stat, node_state['commands_stat'])

    def _process_filesystems(self, filesystem_states):
        for filesystem_state in filesystem_states:
            try:
                fs_id = filesystem_state['id']
            except KeyError:
                logger.error('Malformed response from collector, failed '
                             'to parse filesystem id')
                continue
            try:
                self._process_filesystem(fs_id, filesystem_state)
            except Exception as e:
                logger.error(
                    'Failed to process filesystem {fs_id} state from collector: {e}'.format(
                        fs_id=fs_id,
                        e=e,
                    )
                )
                continue

    def _process_filesystem(self, fs_id, filesystem_state):
        if fs_id not in storage.fs:
            host = storage.hosts[filesystem_state['host_id']]
            logger.info('Creating filesystem {} on host {}'.format(fs_id, host))
            fs = storage.fs.add(host, filesystem_state['fsid'])
            fs.stat = storage.FsStat()
        else:
            fs = storage.fs[fs_id]

        fs.status = filesystem_state['status']
        fs.status_text = filesystem_state['status_text']

        stat = fs.stat
        stat.ts = mh.elliptics_time_to_ts(filesystem_state['timestamp'])  # Why is it elliptics-like? Does it have to do anything with elliptics?
        stat.total_space = filesystem_state['total_space']
        stat.free_space = filesystem_state['free_space']

        stat.disk_util = filesystem_state['disk_util']
        stat.disk_util_read = filesystem_state['disk_util_read']
        stat.disk_util_write = filesystem_state['disk_util_write']

        # XXX this is not the same as in commands_stats
        stat.disk_read_rate = filesystem_state['disk_read_rate']
        stat.disk_write_rate = filesystem_state['disk_write_rate']

        update_commands_stat(stat.commands_stat, filesystem_state['commands_stat'])

    def _process_backends(self, backend_states):
        for backend_state in backend_states:
            try:
                # TODO: backend id now includes family, check backend id
                # generation everywhere
                backend_id = backend_state['id']
            except KeyError:
                logger.error('Malformed response from collector, failed to parse backend id')
            try:
                self._process_backend(backend_id, backend_state)
            except Exception as e:
                logger.error(
                    'Failed to process backend {backend_id} state from collector: {e}'.format(
                        backend_id=backend_id,
                        e=e,
                    )
                )
                continue

    def _process_backend(self, backend_id, backend_state):
        node = storage.nodes[backend_state['node_id']]

        if backend_id not in storage.node_backends:
            logger.info('Creating node backend {}'.format(backend_id))
            node_backend = storage.node_backends.add(node, backend_state['backend_id'])
            node_backend.stat = storage.NodeBackendStat(node.stat)
        else:
            node_backend = storage.node_backends[backend_id]

        fs = storage.fs[backend_state['fs_id']]

        if node_backend not in fs.node_backends:
            fs.add_node_backend(node_backend)

        if node_backend.base_path != backend_state['base_path']:
            gid = backend_state['group']
            if gid in storage.groups:
                logger.debug('Group {} history may be outdated, adding to '
                             'update queue'.format(gid))
                group = storage.groups[gid]
                infrastructure.update_group_history(group)

        node_backend.base_path = backend_state['base_path']
        node_backend.read_only = backend_state['read_only']
        node_backend.status = backend_state['status']
        node_backend.status_text = backend_state['status_text']

        stat = node_backend.stat
        stat.ts = mh.elliptics_time_to_ts(backend_state['timestamp'])

        stat.free_space = backend_state['free_space']
        stat.total_space = backend_state['total_space']
        stat.used_space = backend_state['used_space']

        stat.vfs_free_space = backend_state['vfs_free_space']
        stat.vfs_total_space = backend_state['vfs_total_space']
        stat.vfs_used_space = backend_state['vfs_used_space']

        update_commands_stat(stat.commands_stat, backend_state['commands_stat'])

        stat.fragmentation = backend_state['fragmentation']

        stat.files = backend_state['records']
        stat.files_removed = backend_state['records_removed']
        stat.files_removed_size = backend_state['records_removed_size']

        stat.defrag_state = backend_state['defrag_state']
        stat.want_defrag = backend_state['want_defrag']

        stat.blob_size = backend_state['blob_size']
        stat.blob_size_limit = backend_state['blob_size_limit']
        stat.max_blob_base_size = backend_state['max_blob_base_size']

        stat.io_blocking_size = backend_state['io_blocking_size']
        stat.io_nonblocking_size = backend_state['io_nonblocking_size']

        stat.stat_commit_errors = backend_state['stat_commit_rofs_errors_diff']

    def _process_groups(self, group_states):
        for group_state in group_states:
            try:
                gid = group_state['id']
            except KeyError:
                logger.error('Malformed response from collector, failed to parse group id')
                continue
            try:
                self._process_group(gid, group_state)
            except Exception as e:
                logger.error('Failed to process group {gid} '
                             'state from collector: {e}'.format(
                                gid=gid,
                                e=e,
                ))
                continue

    def _process_group(self, gid, group_state):
        if gid not in storage.groups:
            logger.debug('Adding group {0}'.format(gid))
            backends = [storage.node_backends[nbid] for nbid in group_state['backends']]
            group = storage.groups.add(gid, backends)
            infrastructure.update_group_history(group)
        else:
            group = storage.groups[gid]

            new_backends = [storage.node_backends[nbid]
                for nbid in group_state['backends']
                    if nbid not in group.node_backends]

            removed_backends = [nb for nb in group.node_backends
                    if str(nb) not in group_state['backends']]

            for nb in new_backends:
                group.add_node_backend(nb)

            for nb in removed_backends:
                group.remove_node_backend(nb)

            if new_backends or removed_backends:
                infrastructure.update_group_history(group)

        group.status = group_state['status']
        group.status_text = group_state['status_text']
        group.meta = group_state['metadata']
        group._type = group_state['type']

    def _process_jobs(self, groups=None):
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
                    elif hasattr(job, 'couple'):
                        jobs[job.couple] = job
            except Exception as e:
                logger.exception('Failed to fetch pending jobs: {0}'.format(e))
                pass

        if not groups:
            groups = storage.groups.keys()

        for group in groups:
            active_job = jobs.get(group.group_id) or jobs.get(group.couple) or None
            group.set_active_job(active_job)

    def _process_namespaces(self, namespace_states):
        for namespace_state in namespace_states:
            try:
                nsid = namespace_state['id']
            except KeyError:
                logger.error('Malformed response from collector, failed to parse namespace id')
                continue
            try:
                self._process_namespace(nsid, namespace_state)
            except Exception as e:
                logger.error('Failed to process namespace {nsid} state '
                             'from collector: {e}'.format(
                    nsid=nsid,
                    e=e,
                ))
                continue

    def _process_namespace(self, nsid, namespace_state):
        if nsid not in storage.namespaces:
            ns = storage.namespaces.add(nsid)
        else:
            ns = storage.namespaces[nsid]

        if 'settings' in namespace_state:
            ns.settings = namespace_state['settings']

    def _process_couples(self, couple_states):
        for couple_state in couple_states:
            try:
                couple_id = couple_state['id']
            except KeyError:
                logger.error('Malformed response from collector, failed to parse couple id')
                continue
            try:
                self._process_couple(couple_id, couple_state)
            except Exception as e:
                logger.error('Failed to process couple {couple_id} state '
                             'from collector: {e}'.format(
                    couple_id=couple_id,
                    e=e
                ))
                continue

    def _process_couple(self, couple_id, couple_state):
        if not couple_state['groupsets']:
            # It is possible because Couple objects are never removed.
            logger.debug('Couple {} has empty groupsets'.format(couple_id))

            if (couple_id in storage.groupsets):
                groupset = storage.groupsets[couple_id]
                groupset.groups = []
                groupset.status = couple_state['status'] # XXX
                groupset.status_text = couple_state['status_text']

            return

        replicas_groupset_states = [state
            for state in couple_state['groupsets']
                if state['type'] == GROUPSET_TYPE_REPLICAS]

        if len(replicas_groupset_states) > 1:
            raise RuntimeError('Couple has {} replicas groupsets'.format(
                len(replicas_groupset_states)
            ))

        lrc_groupset_states = [state
            for state in couple_state['groupsets']
                if state['type'] == GROUPSET_TYPE_LRC]

        if len(lrc_groupset_states) > 1:
            raise RuntimeError('Couple has {} LRC groupsets'.format(
                len(lrc_groupsets_states)
            ))

        if replicas_groupset_states:
            gs_state = replicas_groupset_states[0]
            replicas_gs = create_groupset_if_needed(
                gsid=gs_state['id'],
                groups=gs_state['groups'],
                gstype=gs_state['type'],
                nsid=couple_state['namespace'],
                status=gs_state['status'],
                status_text=gs_state['status_text'],
            )
        else:
            group_ids = map(int, couple_state['id'].split(':'))
            replicas_gs = create_groupset_if_needed(
                gsid=couple_state['id'],
                groups=group_ids,
                gstype=GROUPSET_TYPE_REPLICAS,
                nsid=couple_state['namespace'],
            )

        if lrc_groupset_states:
            gs_state = lrc_groupset_states[0]
            lrc_gs = create_groupset_if_needed(
                gsid=gs_state['id'],
                groups=gs_state['groups'],
                gstype=gs_state['type'],
                nsid=couple_state['namespace'],
                status=gs_state['status'],
                status_text=gs_state['status_text'],
            )
            lrc_gs.couple = replicas_gs
            replicas_gs.lrc822v1_groupset = lrc_gs

        couple = replicas_gs

        couple.status = couple_state['status']
        couple.status_text = couple_state['status_text']

        if 'settings' in couple_state:
            couple.settings = couple_state['settings']

    def _schedule_next_round(self):
        logger.info('Scheduling next update round')
        reload_period = config.get('nodes_reload_period', 60)
        self._tq.add_task_in('update', reload_period, self.update)
