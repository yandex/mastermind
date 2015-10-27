import logging
import os.path

from config import config
from error import JobBrokenError
from errors import CacheUpstreamError
from infrastructure import infrastructure
from infrastructure_cache import cache
from job import Job
from job_types import JobTypes
from tasks import (NodeBackendDefragTask, CoupleDefragStateCheckTask,
                   RsyncBackendTask, MinionCmdTask, NodeStopTask, HistoryRemoveNodeTask)
import storage


logger = logging.getLogger('mm.jobs')


class RestoreGroupJob(Job):

    # used to write group id
    GROUP_FILE_PATH = config.get('restore', {}).get('group_file', None)
    MERGE_GROUP_FILE_MARKER_PATH = config.get('restore', {}).get('merge_group_file_marker', None)
    GROUP_FILE_DIR_MOVE_DST_RENAME = config.get('restore', {}).get('group_file_dir_move_dst_rename', None)
    MERGE_GROUP_FILE_DIR_MOVE_SRC_RENAME = config.get('restore', {}).get('merge_group_file_dir_move_src_rename', None)

    PARAMS = ('group', 'src_group', 'uncoupled_group', 'uncoupled_group_fsid',
              'merged_groups', 'resources')

    def __init__(self, **kwargs):
        super(RestoreGroupJob, self).__init__(**kwargs)
        self.type = JobTypes.TYPE_RESTORE_GROUP_JOB

    @classmethod
    def new(cls, *args, **kwargs):
        job = super(RestoreGroupJob, cls).new(*args, **kwargs)
        try:
            if 'uncoupled_group' in kwargs:
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

        def add_fs(group):
            resources[Job.RESOURCE_FS].append(
                (group.node_backends[0].node.host.addr, str(group.node_backends[0].fs.fsid)))

        src_group = storage.groups[self.src_group]
        self.check_node_backends(src_group)
        resources[Job.RESOURCE_HOST_OUT].append(src_group.node_backends[0].node.host.addr)
        add_fs(src_group)

        if self.uncoupled_group:
            unc_group = storage.groups[self.uncoupled_group]
            self.check_node_backends(unc_group)
            resources[Job.RESOURCE_HOST_IN].append(
                unc_group.node_backends[0].node.host.addr)
            for gid in [self.uncoupled_group] + self.merged_groups:
                add_fs(storage.groups[gid])
        else:
            group = storage.groups[self.group]
            if group.node_backends:
                resources[Job.RESOURCE_HOST_IN].append(
                    group.node_backends[0].node.host.addr)
                add_fs(group)
            else:
                old_group_node_backends_set = infrastructure.get_group_history(
                    group.group_id).nodes[-1].set
                if old_group_node_backends_set:
                    host = cache.get_ip_address_by_host(old_group_node_backends_set[0].hostname)
                    resources[Job.RESOURCE_HOST_IN].append(host)

        self.resources = resources

    def create_tasks(self):

        group = storage.groups[self.group]
        src_group = storage.groups[self.src_group]

        self.check_node_backends(src_group)

        old_group_node_backends_set = infrastructure.get_group_history(group.group_id).nodes[-1].set
        if old_group_node_backends_set:
            old_node_backend = old_group_node_backends_set[0]
            old_host = cache.get_ip_address_by_host(old_node_backend.hostname)
            old_port = old_node_backend.port
            old_family = old_node_backend.family
            old_backend_id = old_node_backend.backend_id
            old_base_path = old_node_backend.path

        if self.uncoupled_group:
            uncoupled_group = storage.groups[self.uncoupled_group]
            self.check_node_backends(uncoupled_group)

            dst_host = uncoupled_group.node_backends[0].node.host.addr
            dst_port = uncoupled_group.node_backends[0].node.port
            dst_family = uncoupled_group.node_backends[0].node.family
            dst_base_path = uncoupled_group.node_backends[0].base_path
            dst_backend_id = uncoupled_group.node_backends[0].backend_id
        else:
            if len(old_group_node_backends_set) != 1:
                raise JobBrokenError(
                    'History of group {0} lists {1} node backends, 1 expected'.format(
                        self.group, len(old_group_node_backends_set)
                    )
                )

            # gettings dst_* from group history
            dst_host, dst_port, dst_backend_id = old_host, old_port, old_backend_id
            # TODO: Fix hardcoded family value
            dst_base_path, dst_family = old_base_path, old_family

        dst_node_backend = self.node_backend(dst_host, dst_port, dst_backend_id)

        group_file = (os.path.join(dst_base_path, self.GROUP_FILE_PATH)
                      if self.GROUP_FILE_PATH else
                      '')

        remove_path = ''

        if self.GROUP_FILE_DIR_MOVE_DST_RENAME and group_file:
            remove_path = os.path.join(
                    dst_base_path, self.GROUP_FILE_DIR_MOVE_DST_RENAME)

        if self.uncoupled_group:
            for group_id in self.merged_groups:
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
                        dst_backend_id=dst_backend_id)

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


            # destination backend should be stopped
            shutdown_cmd = infrastructure._disable_node_backend_cmd(
                dst_host, dst_port, dst_family, dst_backend_id)

            params = {'node_backend': dst_node_backend.encode('utf-8'),
                      'group': str(self.uncoupled_group)}

            if remove_path:
                params['move_src'] = os.path.join(os.path.dirname(group_file))
                params['move_dst'] = remove_path

            task = NodeStopTask.new(self,
                        group=self.uncoupled_group,
                        uncoupled=True,
                        host=dst_host,
                        cmd=shutdown_cmd,
                        params=params)

            self.tasks.append(task)

        src_backend = src_group.node_backends[0]
        restore_backend = group.node_backends and group.node_backends[0]

        make_readonly = (src_backend is not restore_backend and
                         src_backend.status == storage.Status.OK)

        mark_src_backend = self.make_path(
            self.BACKEND_DOWN_MARKER, base_path=src_backend.base_path).format(
                backend_id=src_backend.backend_id)

        if make_readonly:
            make_readonly_cmd = infrastructure._make_readonly_node_backend_cmd(
                src_backend.node.host.addr, src_backend.node.port,
                src_backend.node.family, src_backend.backend_id)

            task = MinionCmdTask.new(self,
                host=src_backend.node.host.addr,
                cmd=make_readonly_cmd,
                params={'node_backend': self.node_backend(
                    src_backend.node.host.addr, src_backend.node.port,
                    src_backend.backend_id).encode('utf-8'),
                        'mark_backend': mark_src_backend})

            self.tasks.append(task)

        move_cmd = infrastructure.move_group_cmd(
            src_host=src_group.node_backends[0].node.host.addr,
            src_path=src_group.node_backends[0].base_path,
            src_family=src_group.node_backends[0].node.family,
            dst_path=dst_base_path)

        ids_file = (os.path.join(dst_base_path, self.IDS_FILE_PATH)
                    if self.IDS_FILE_PATH else
                    '')

        params = {'group': str(self.group),
                  'group_file': group_file,
                  'ids': ids_file}

        if remove_path and self.uncoupled_group:
            params['remove_path'] = remove_path

        check_node_backend = None
        if old_group_node_backends_set:
            check_node_backend = self.node_backend(old_host, old_port, old_backend_id)

        task = RsyncBackendTask.new(
            self,
            host=dst_host,
            src_host=src_group.node_backends[0].node.host.addr,
            group=self.group,
            cmd=move_cmd,
            node_backend=check_node_backend,
            params=params
        )
        self.tasks.append(task)

        additional_files = config.get('restore', {}).get('restore_additional_files', [])
        for src_file_tpl, dst_file_path in additional_files:
            rsync_cmd = infrastructure.move_group_cmd(
                src_host=src_group.node_backends[0].node.host.addr,
                src_path=src_group.node_backends[0].base_path,
                src_family=src_group.node_backends[0].node.family,
                dst_path=os.path.join(dst_base_path, dst_file_path),
                file_tpl=src_file_tpl)

            params = {'group': str(self.group)}

            task = MinionCmdTask.new(self,
                                     host=dst_host,
                                     group=self.group,
                                     cmd=rsync_cmd,
                                     params=params)
            self.tasks.append(task)

        if (restore_backend and restore_backend.status in
            storage.NodeBackend.ACTIVE_STATUSES):

            nb = group.node_backends[0]
            shutdown_cmd = infrastructure._disable_node_backend_cmd(
                nb.node.host.addr, nb.node.port, nb.node.family, nb.backend_id)

            group_file = (os.path.join(nb.base_path,
                                       self.GROUP_FILE_PATH)
                          if self.GROUP_FILE_PATH else
                          '')

            group_file_marker = (os.path.join(nb.base_path,
                                              self.GROUP_FILE_MARKER_PATH)
                                 if self.GROUP_FILE_MARKER_PATH else
                                 '')

            hostnames = []
            for host in (nb.node.host.addr, dst_host):
                try:
                    hostnames.append(cache.get_hostname_by_addr(host))
                except CacheUpstreamError:
                    raise RuntimeError('Failed to resolve host {0}'.format(host))
            src_hostname, dst_hostname = hostnames

            group_file_marker_fmt = group_file_marker.format(
                group_id=str(self.group),
                src_host=nb.node.host.addr,
                src_hostname=src_hostname,
                src_backend_id=nb.backend_id,
                src_port=str(nb.node.port),
                src_base_path=nb.base_path,
                dst_host=dst_host,
                dst_hostname=dst_hostname,
                dst_port=str(dst_port),
                dst_base_path=dst_base_path,
                dst_backend_id=dst_backend_id)
            params = {'node_backend': str(nb).encode('utf-8'),
                      'group': str(self.group),
                      'group_file_marker': group_file_marker_fmt,
                      'remove_group_file': group_file}

            if self.GROUP_FILE_DIR_MOVE_SRC_RENAME and group_file:
                params['move_src'] = os.path.join(os.path.dirname(group_file))
                params['move_dst'] = os.path.join(
                    nb.base_path, self.GROUP_FILE_DIR_MOVE_SRC_RENAME)

            task = NodeStopTask.new(self,
                                    group=self.group,
                                    host=nb.node.host.addr,
                                    cmd=shutdown_cmd,
                                    params=params)
            self.tasks.append(task)


        reconfigure_cmd = infrastructure._reconfigure_node_cmd(
            dst_host, dst_port, dst_family)

        task = MinionCmdTask.new(self,
            host=dst_host,
            cmd=reconfigure_cmd,
            params={'node_backend': dst_node_backend.encode('utf-8')})

        self.tasks.append(task)

        if self.uncoupled_group:

            if old_group_node_backends_set:
                task = HistoryRemoveNodeTask.new(self,
                                                 group=self.group,
                                                 host=old_host,
                                                 port=old_port,
                                                 backend_id=old_backend_id)
                self.tasks.append(task)

            task = HistoryRemoveNodeTask.new(self,
                                 group=self.uncoupled_group,
                                 host=dst_host,
                                 port=dst_port,
                                 backend_id=dst_backend_id)
            self.tasks.append(task)


        start_cmd = infrastructure._enable_node_backend_cmd(
            dst_host, dst_port, dst_family, dst_backend_id)
        task = MinionCmdTask.new(self,
                                 host=dst_host,
                                 cmd=start_cmd,
                                 params={'node_backend': dst_node_backend.encode('utf-8')})
        self.tasks.append(task)

        if make_readonly:
            make_writable_cmd = infrastructure._make_writable_node_backend_cmd(
                src_backend.node.host.addr, src_backend.node.port,
                src_backend.node.family, src_backend.backend_id)

            task = MinionCmdTask.new(self,
                host=src_backend.node.host.addr,
                cmd=make_writable_cmd,
                params={'node_backend': self.node_backend(
                    src_backend.node.host.addr, src_backend.node.port,
                    src_backend.backend_id).encode('utf-8'),
                        'unmark_backend': mark_src_backend,
                        'success_codes': [self.DNET_CLIENT_ALREADY_IN_PROGRESS]})

            self.tasks.append(task)

            # This start command is executed for the case when elliptics
            # was restarted between make_readonly and make_writable commands
            start_cmd = infrastructure._enable_node_backend_cmd(
                src_backend.node.host.addr,
                src_backend.node.port,
                src_backend.node.family,
                src_backend.backend_id)

            task = MinionCmdTask.new(self,
                host=src_backend.node.host.addr,
                cmd=start_cmd,
                params={'node_backend': str(src_backend).encode('utf-8'),
                        'success_codes': [self.DNET_CLIENT_ALREADY_IN_PROGRESS]})
            self.tasks.append(task)

    @property
    def _involved_groups(self):
        group_ids = set([self.group])
        group = storage.groups[self.group]
        if group.couple:
            group_ids.update([g.group_id for g in group.coupled_groups])
        if self.uncoupled_group:
            group_ids.add(self.uncoupled_group)
        if self.merged_groups:
            group_ids.update(self.merged_groups)
        if self.src_group:
            group_ids.add(self.src_group)
        return group_ids

    @property
    def _involved_couples(self):
        couples = []
        group = storage.groups[self.group]
        if group.couple:
            couples.append(str(group.couple))
        return couples
