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
    GROUP_FILE_DIR_MOVE_DST_RENAME = config.get('restore', {}).get('group_file_dir_move_dst_rename', None)

    PARAMS = ('group', 'src_group', 'uncoupled_group')

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

        return (groups +
                ['{0}{1}'.format(self.COUPLE_LOCK_PREFIX, str(group.couple))])

