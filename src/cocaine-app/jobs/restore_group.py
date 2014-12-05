import logging
import os.path

from config import config
from error import JobBrokenError
from infrastructure import infrastructure, BASE_PORT
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

    PARAMS = ('group', 'src_group', 'uncoupled_group', 'merged_groups')

    def __init__(self, **kwargs):
        super(RestoreGroupJob, self).__init__(**kwargs)
        self.type = JobTypes.TYPE_RESTORE_GROUP_JOB

    def check_node_backends(self, group):
        if len(group.node_backends) != 1:
            raise JobBrokenError('Group {0} cannot be used for job, '
                'it has {1} node backends, 1 expected'.format(
                    group.group_id, len(group.node_backends)))

    def create_tasks(self):

        group = storage.groups[self.group]
        src_group = storage.groups[self.src_group]

        self.check_node_backends(src_group)

        old_group_state = infrastructure.get_group_history(group.group_id)['nodes'][-1]['set']
        if len(old_group_state) != 1:
            raise JobBrokenError('History of group {0} lists {1} '
                'node backends, 1 expected'.format(self.group, len(old_group_state)))

        if len(old_group_state[0]) == 3:
            # convert old version port to new backend id
            old_host, old_version_port, old_base_path = old_group_state[0][:3]
            old_backend_id = old_version_port - BASE_PORT
            old_port = BASE_PORT + 1
        else:
            old_host, old_port, old_backend_id, old_base_path = old_group_state[0][:4]

        if self.uncoupled_group:
            uncoupled_group = storage.groups[self.uncoupled_group]
            self.check_node_backends(uncoupled_group)

            dst_host = uncoupled_group.node_backends[0].node.host.addr
            dst_port = uncoupled_group.node_backends[0].node.port
            dst_family = uncoupled_group.node_backends[0].node.family
            dst_base_path = uncoupled_group.node_backends[0].base_path
            dst_backend_id = uncoupled_group.node_backends[0].backend_id
        else:
            # gettings dst_* from group history
            dst_host, dst_port, dst_backend_id = old_host, old_port, old_backend_id
            # TODO: Fix hardcoded family value
            dst_base_path, dst_family = old_base_path, 2

        dst_node_backend = self.node_backend(dst_host, dst_port, dst_backend_id)

        group_file = (os.path.join(dst_base_path,
                          self.GROUP_FILE_PATH)
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


                shutdown_cmd = infrastructure.disable_node_backend_cmd([
                    merged_nb.node.host.addr,
                    merged_nb.node.port,
                    merged_nb.node.family,
                    merged_nb.backend_id])

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

                reconfigure_cmd = infrastructure.reconfigure_node_cmd(
                    [merged_nb.node.host.addr,
                     merged_nb.node.port,
                     merged_nb.node.family])

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
            shutdown_cmd = infrastructure.disable_node_backend_cmd([
                dst_host, dst_port, dst_family, dst_backend_id])

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

        move_cmd = infrastructure.move_group_cmd(
            src_host=src_group.node_backends[0].node.host.addr,
            src_path=src_group.node_backends[0].base_path,
            dst_path=dst_base_path)

        params = {'group': str(self.group),
                  'group_file': group_file}

        if remove_path:
            params['remove_path'] = remove_path

        task = RsyncBackendTask.new(self,
                                    host=dst_host,
                                    group=self.group,
                                    cmd=move_cmd,
                                    node_backend=self.node_backend(old_host, old_port, old_backend_id),
                                    params=params)
        self.tasks.append(task)

        additional_files = config.get('restore', {}).get('restore_additional_files', [])
        for src_file_tpl, dst_file_path in additional_files:
            rsync_cmd = infrastructure.move_group_cmd(
                src_host=src_group.node_backends[0].node.host.addr,
                src_path=src_group.node_backends[0].base_path,
                dst_path=os.path.join(dst_base_path, dst_file_path),
                file_tpl=src_file_tpl)

            params = {'group': str(self.group)}

            task = MinionCmdTask.new(self,
                                     host=dst_host,
                                     group=self.group,
                                     cmd=rsync_cmd,
                                     params=params)
            self.tasks.append(task)

        reconfigure_cmd = infrastructure.reconfigure_node_cmd(
            [dst_host, dst_port, dst_family])

        task = MinionCmdTask.new(self,
            host=dst_host,
            cmd=reconfigure_cmd,
            params={'node_backend': dst_node_backend.encode('utf-8')})

        self.tasks.append(task)

        if self.uncoupled_group:

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


        start_cmd = infrastructure.enable_node_backend_cmd([
            dst_host, dst_port, dst_family, dst_backend_id])
        task = MinionCmdTask.new(self,
                                 host=dst_host,
                                 cmd=start_cmd,
                                 params={'node_backend': dst_node_backend.encode('utf-8')})
        self.tasks.append(task)

    @property
    def _locks(self):
        if not self.group in storage.groups:
            raise JobBrokenError('Group {0} is not found'.format(self.group))

        group = storage.groups[self.group]

        if not group.couple:
            raise JobBrokenError('Group {0} does not participate in any couple'.format(self.group))

        groups = ['{0}{1}'.format(self.GROUP_LOCK_PREFIX, g.group_id) for g in group.couple.groups]
        if self.uncoupled_group:
            groups.append('{0}{1}'.format(self.GROUP_LOCK_PREFIX, self.uncoupled_group))

        if self.merged_groups:
            groups.extend(['{0}{1}'.format(self.GROUP_LOCK_PREFIX, mg) for mg in self.merged_groups])

        return (groups +
                ['{0}{1}'.format(self.COUPLE_LOCK_PREFIX, str(group.couple))])

