import copy
import logging
import os.path

from error import JobBrokenError
from errors import CacheUpstreamError
from infrastructure import infrastructure
from infrastructure_cache import cache
from job import Job, RESTORE_CFG
from job_types import JobTypes
from tasks import NodeStopTask, RsyncBackendTask, MinionCmdTask, HistoryRemoveNodeTask
import storage


logger = logging.getLogger('mm.jobs')


class MoveJob(Job):

    # used to write group id
    GROUP_FILE_PATH = RESTORE_CFG.get('group_file', None)

    # used to mark source node that content has been moved away from it
    GROUP_FILE_DIR_MOVE_DST_RENAME = RESTORE_CFG.get('group_file_dir_move_dst_rename', None)
    MERGE_GROUP_FILE_MARKER_PATH = RESTORE_CFG.get('merge_group_file_marker', None)
    MERGE_GROUP_FILE_DIR_MOVE_SRC_RENAME = RESTORE_CFG.get(
        'merge_group_file_dir_move_src_rename', None)

    PARAMS = ('group', 'uncoupled_group', 'uncoupled_group_fsid', 'merged_groups',
              'resources',
              'src_host', 'src_port', 'src_backend_id', 'src_family', 'src_base_path',
              'dst_host', 'dst_port', 'dst_backend_id', 'dst_family', 'dst_base_path')

    def __init__(self, **kwargs):
        super(MoveJob, self).__init__(**kwargs)
        self.type = JobTypes.TYPE_MOVE_JOB

    @classmethod
    def new(cls, *args, **kwargs):
        job = super(MoveJob, cls).new(*args, **kwargs)
        try:
            unc_group = storage.groups[kwargs['uncoupled_group']]
            job.uncoupled_group_fsid = str(unc_group.node_backends[0].fs.fsid)
        except Exception:
            job.release_locks()
            raise
        return job

    def _set_resources(self):
        resources = {
            Job.RESOURCE_HOST_IN: [],
            Job.RESOURCE_HOST_OUT: [],
            Job.RESOURCE_FS: [],
        }

        resources[Job.RESOURCE_HOST_IN].append(self.dst_host)
        resources[Job.RESOURCE_HOST_OUT].append(self.src_host)
        for gid in [self.uncoupled_group, self.group] + self.merged_groups:
            g = storage.groups[gid]
            resources[Job.RESOURCE_FS].append(
                (g.node_backends[0].node.host.addr, str(g.node_backends[0].fs.fsid)))
        self.resources = resources

    @property
    def src_node_backend(self):
        return self.node_backend(self.src_host, self.src_port, self.src_backend_id)

    @property
    def dst_node_backend(self):
        return self.node_backend(self.dst_host, self.dst_port, self.dst_backend_id)

    def on_start(self):
        group = storage.groups[self.group]
        self.check_node_backends(group)

        if storage.FORBIDDEN_DC_SHARING_AMONG_GROUPS:
            uncoupled_group = storage.groups[self.uncoupled_group]
            self.check_node_backends(uncoupled_group)
            try:
                ug_dc = uncoupled_group.node_backends[0].node.host.dc
            except CacheUpstreamError:
                raise RuntimeError('Failed to get dc for host {}'.format(
                    uncoupled_group.node_backends[0].node.host))

            for g in group.couple:
                if g.group_id == group.group_id:
                    continue
                dcs = set()
                for nb in g.node_backends:
                    try:
                        dcs.add(nb.node.host.dc)
                    except CacheUpstreamError:
                        raise RuntimeError('Failed to get dc for host {}'.format(
                            nb.node.host))

                if ug_dc in dcs:
                    raise JobBrokenError(
                        'Cannot move group {0} to uncoupled group '
                        '{1}, because group {2} is already in dc {3}'.format(
                            self.group, self.uncoupled_group, g.group_id, ug_dc))

        src_backend = group.node_backends[0]
        if src_backend.status != storage.Status.OK:
            raise JobBrokenError('Group {0} node backend {1} status is {2}, should be {3}'.format(
                group.group_id, src_backend, src_backend.status, storage.Status.OK))

    def human_dump(self):
        data = super(MoveJob, self).human_dump()
        data['src_hostname'] = cache.get_hostname_by_addr(data['src_host'], strict=False)
        data['dst_hostname'] = cache.get_hostname_by_addr(data['dst_host'], strict=False)
        return data

    def marker_format(self, marker):
        hostnames = []
        for host in (self.src_host, self.dst_host):
            try:
                hostnames.append(cache.get_hostname_by_addr(host))
            except CacheUpstreamError:
                raise RuntimeError('Failed to resolve host {0}'.format(host))
        src_hostname, dst_hostname = hostnames
        return marker.format(
            group_id=str(self.group),
            src_host=self.src_host,
            src_hostname=src_hostname,
            src_backend_id=self.src_backend_id,
            src_port=str(self.src_port),
            src_base_path=self.src_base_path,
            dst_host=self.dst_host,
            dst_hostname=dst_hostname,
            dst_port=str(self.dst_port),
            dst_base_path=self.dst_base_path,
            dst_backend_id=self.dst_backend_id)

    def create_tasks(self):

        for group_id in self.merged_groups or []:
            merged_group = storage.groups[group_id]
            merged_nb = merged_group.node_backends[0]

            merged_group_file = (os.path.join(merged_nb.base_path,
                                              self.GROUP_FILE_PATH)
                                 if self.GROUP_FILE_PATH else
                                 '')

            merged_path = ''
            if self.MERGE_GROUP_FILE_DIR_MOVE_SRC_RENAME and merged_group_file:
                merged_path = os.path.join(
                    merged_nb.base_path, self.MERGE_GROUP_FILE_DIR_MOVE_SRC_RENAME)

            node_backend_str = self.node_backend(merged_nb.node.host.addr,
                                                 merged_nb.node.port,
                                                 merged_nb.backend_id)

            merged_group_file_marker = (os.path.join(merged_nb.base_path,
                                                     self.MERGE_GROUP_FILE_MARKER_PATH)
                                        if self.MERGE_GROUP_FILE_MARKER_PATH else
                                        '')

            shutdown_cmd = infrastructure._disable_node_backend_cmd(
                merged_nb.node.host.addr,
                merged_nb.node.port,
                merged_nb.node.family,
                merged_nb.backend_id)

            params = {'node_backend': node_backend_str.encode('utf-8'),
                      'group': str(group_id),
                      'merged_to': str(self.uncoupled_group),
                      'remove_group_file': merged_group_file}

            if merged_group_file_marker:
                params['group_file_marker'] = merged_group_file_marker.format(
                    dst_group_id=self.uncoupled_group,
                    dst_backend_id=merged_nb.backend_id)

            if merged_path:
                params['move_src'] = os.path.dirname(merged_group_file)
                params['move_dst'] = merged_path

            task = NodeStopTask.new(self,
                                    group=group_id,
                                    uncoupled=True,
                                    host=merged_nb.node.host.addr,
                                    cmd=shutdown_cmd,
                                    params=params)

            self.tasks.append(task)

            reconfigure_cmd = infrastructure._reconfigure_node_cmd(
                merged_nb.node.host.addr,
                merged_nb.node.port,
                merged_nb.node.family)

            task = MinionCmdTask.new(self,
                                     host=merged_nb.node.host.addr,
                                     cmd=reconfigure_cmd,
                                     params={'node_backend': node_backend_str.encode('utf-8')})

            self.tasks.append(task)

            task = HistoryRemoveNodeTask.new(self,
                                             group=group_id,
                                             host=merged_nb.node.host.addr,
                                             port=merged_nb.node.port,
                                             backend_id=merged_nb.backend_id)
            self.tasks.append(task)

        shutdown_cmd = infrastructure._disable_node_backend_cmd(
            self.dst_host, self.dst_port, self.dst_family, self.dst_backend_id)

        group_file = (os.path.join(self.dst_base_path,
                                   self.GROUP_FILE_PATH)
                      if self.GROUP_FILE_PATH else
                      '')

        params = {'node_backend': self.dst_node_backend.encode('utf-8'),
                  'group': str(self.uncoupled_group),
                  'success_codes': [self.DNET_CLIENT_ALREADY_IN_PROGRESS]}

        remove_path = ''

        if self.GROUP_FILE_DIR_MOVE_DST_RENAME and group_file:
            params['move_src'] = os.path.join(os.path.dirname(group_file))
            remove_path = os.path.join(
                self.dst_base_path, self.GROUP_FILE_DIR_MOVE_DST_RENAME)
            params['move_dst'] = remove_path

        task = NodeStopTask.new(self,
                                group=self.uncoupled_group,
                                uncoupled=True,
                                host=self.dst_host,
                                cmd=shutdown_cmd,
                                params=params)
        self.tasks.append(task)

        make_readonly_cmd = infrastructure._make_readonly_node_backend_cmd(
            self.src_host, self.src_port, self.src_family, self.src_backend_id)

        mark_backend = self.make_path(
            self.BACKEND_DOWN_MARKER, base_path=self.src_base_path).format(
                backend_id=self.src_backend_id)

        task = MinionCmdTask.new(self,
                                 host=self.src_host,
                                 cmd=make_readonly_cmd,
                                 params={'node_backend': self.src_node_backend.encode('utf-8'),
                                         'mark_backend': mark_backend,
                                         'success_codes': [self.DNET_CLIENT_ALREADY_IN_PROGRESS]})

        self.tasks.append(task)

        move_cmd = infrastructure.move_group_cmd(
            src_host=self.src_host,
            src_path=self.src_base_path,
            src_family=self.src_family,
            dst_path=self.dst_base_path)
        group_file = (os.path.join(self.dst_base_path, self.GROUP_FILE_PATH)
                      if self.GROUP_FILE_PATH else
                      '')

        ids_file = (os.path.join(self.dst_base_path, self.IDS_FILE_PATH)
                    if self.IDS_FILE_PATH else
                    '')

        params = {'group': str(self.group),
                  'group_file': group_file,
                  'ids': ids_file}

        if remove_path:
            params['remove_path'] = remove_path

        task = RsyncBackendTask.new(self,
                                    host=self.dst_host,
                                    src_host=self.src_host,
                                    group=self.group,
                                    cmd=move_cmd,
                                    params=params)
        self.tasks.append(task)

        additional_files = RESTORE_CFG.get('move_additional_files', [])
        for src_file_tpl, dst_file_path in additional_files:
            rsync_cmd = infrastructure.move_group_cmd(
                src_host=self.src_host,
                src_path=self.src_base_path,
                src_family=self.src_family,
                dst_path=os.path.join(self.dst_base_path, dst_file_path),
                file_tpl=src_file_tpl)

            params = {'group': str(self.group)}

            task = MinionCmdTask.new(self,
                                     host=self.dst_host,
                                     group=self.group,
                                     cmd=rsync_cmd,
                                     params=params)
            self.tasks.append(task)

        shutdown_cmd = infrastructure._disable_node_backend_cmd(
            self.src_host, self.src_port, self.src_family, self.src_backend_id)

        group_file = (os.path.join(self.src_base_path,
                                   self.GROUP_FILE_PATH)
                      if self.GROUP_FILE_PATH else
                      '')

        group_file_marker = (os.path.join(self.src_base_path,
                                          self.GROUP_FILE_MARKER_PATH)
                             if self.GROUP_FILE_MARKER_PATH else
                             '')

        params = {'node_backend': self.src_node_backend.encode('utf-8'),
                  'group': str(self.group),
                  'group_file_marker': self.marker_format(group_file_marker),
                  'remove_group_file': group_file,
                  'success_codes': [self.DNET_CLIENT_ALREADY_IN_PROGRESS]}

        if self.GROUP_FILE_DIR_MOVE_SRC_RENAME and group_file:
            params['move_src'] = os.path.join(os.path.dirname(group_file))
            params['move_dst'] = os.path.join(
                self.src_base_path, self.GROUP_FILE_DIR_MOVE_SRC_RENAME)

        task = NodeStopTask.new(self,
                                group=self.group,
                                host=self.src_host,
                                cmd=shutdown_cmd,
                                params=params)
        self.tasks.append(task)

        reconfigure_cmd = infrastructure._reconfigure_node_cmd(
            self.src_host, self.src_port, self.src_family)

        task = MinionCmdTask.new(self,
                                 host=self.src_host,
                                 cmd=reconfigure_cmd,
                                 params={'node_backend': self.src_node_backend.encode('utf-8')})

        self.tasks.append(task)

        reconfigure_cmd = infrastructure._reconfigure_node_cmd(
            self.dst_host, self.dst_port, self.dst_family)

        task = MinionCmdTask.new(self,
                                 host=self.dst_host,
                                 cmd=reconfigure_cmd,
                                 params={'node_backend': self.dst_node_backend.encode('utf-8')})

        self.tasks.append(task)

        task = HistoryRemoveNodeTask.new(self,
                                         group=self.group,
                                         host=self.src_host,
                                         port=self.src_port,
                                         backend_id=self.src_backend_id)
        self.tasks.append(task)

        task = HistoryRemoveNodeTask.new(self,
                                         group=self.uncoupled_group,
                                         host=self.dst_host,
                                         port=self.dst_port,
                                         backend_id=self.dst_backend_id)
        self.tasks.append(task)

        start_cmd = infrastructure._enable_node_backend_cmd(
            self.dst_host, self.dst_port, self.dst_family, self.dst_backend_id)
        task = MinionCmdTask.new(self,
                                 host=self.dst_host,
                                 cmd=start_cmd,
                                 params={'node_backend': self.dst_node_backend.encode('utf-8')})
        self.tasks.append(task)

    @property
    def _involved_groups(self):
        group_ids = set([self.group])
        group = storage.groups[self.group]
        if group.couple:
            group_ids.update([g.group_id for g in group.coupled_groups])
        group_ids.add(self.uncoupled_group)
        if self.merged_groups:
            group_ids.update(self.merged_groups)
        return group_ids

    @property
    def _involved_couples(self):
        couples = []
        group = storage.groups[self.group]
        if group.couple:
            couples.append(str(group.couple))
        return couples

    def _group_marks(self):
        group = storage.groups[self.group]
        updated_meta = copy.deepcopy(group.meta)
        # valid convertion from tuple to dict
        # TODO: move this to storage.Group.meta
        updated_meta['version'] = 2
        updated_meta['service'] = {
            'status': storage.Status.MIGRATING,
            'job_id': self.id
        }

        yield group.group_id, updated_meta

    def _group_unmarks(self):
        if self.group not in storage.groups:
            raise RuntimeError('Group {0} is not found'.format(self.group))
        group = storage.groups[self.group]
        if not group.meta:
            raise StopIteration
        updated_meta = copy.deepcopy(group.meta)
        updated_meta.pop('service', None)

        yield group.group_id, updated_meta
