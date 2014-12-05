import logging
import os.path

from config import config
from infrastructure import infrastructure
from job import Job
from job_types import JobTypes
from tasks import NodeStopTask, MinionCmdTask, HistoryRemoveNodeTask
import storage
from sync.error import (
    LockError,
    LockFailedError,
    LockAlreadyAcquiredError,
    InconsistentLockError,
    API_ERROR_CODE
)


logger = logging.getLogger('mm.jobs')


class MoveJob(Job):

    # used to write group id
    GROUP_FILE_PATH = config.get('restore', {}).get('group_file', None)

    # used to mark source node that content has been moved away from it
    GROUP_FILE_MARKER_PATH = config.get('restore', {}).get('group_file_marker', None)
    GROUP_FILE_DIR_MOVE_SRC_RENAME = config.get('restore', {}).get('group_file_dir_move_src_rename', None)
    GROUP_FILE_DIR_MOVE_DST_RENAME = config.get('restore', {}).get('group_file_dir_move_dst_rename', None)

    PARAMS = ('group', 'uncoupled_group',
              'src_host', 'src_port', 'src_backend_id', 'src_family', 'src_base_path',
              'dst_host', 'dst_port', 'dst_backend_id', 'dst_family', 'dst_base_path')

    def __init__(self, **kwargs):
        super(MoveJob, self).__init__(**kwargs)
        self.type = JobTypes.TYPE_MOVE_JOB

    @property
    def src_node_backend(self):
        return self.node_backend(self.src_host, self.src_port, self.src_backend_id)

    @property
    def dst_node_backend(self):
        return self.node_backend(self.dst_host, self.dst_port, self.dst_backend_id)

    def human_dump(self):
        data = super(MoveJob, self).human_dump()
        data['src_hostname'] = infrastructure.get_hostname_by_addr(data['src_host'])
        data['dst_hostname'] = infrastructure.get_hostname_by_addr(data['dst_host'])
        return data

    def marker_format(self, marker):
        return marker.format(
            group_id=str(self.group),
            src_host=self.src_host,
            src_hostname=infrastructure.get_hostname_by_addr(self.src_host),
            src_backend_id=self.src_backend_id,
            src_port=str(self.src_port),
            src_base_path=self.src_base_path,
            dst_host=self.dst_host,
            dst_hostname=infrastructure.get_hostname_by_addr(self.dst_host),
            dst_port=str(self.dst_port),
            dst_base_path=self.dst_base_path,
            dst_backend_id=self.dst_backend_id)

    def create_tasks(self):

        shutdown_cmd = infrastructure.disable_node_backend_cmd([
            self.dst_host, self.dst_port, self.dst_family, self.dst_backend_id])

        group_file = (os.path.join(self.dst_base_path,
                                   self.GROUP_FILE_PATH)
                      if self.GROUP_FILE_PATH else
                      '')

        params = {'node_backend': self.dst_node_backend.encode('utf-8'),
                  'group': str(self.uncoupled_group)}

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

        shutdown_cmd = infrastructure.disable_node_backend_cmd([
            self.src_host, self.src_port, self.src_family, self.src_backend_id])

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
                  'remove_group_file': group_file}

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

        move_cmd = infrastructure.move_group_cmd(
            src_host=self.src_host,
            src_path=self.src_base_path,
            dst_path=self.dst_base_path)
        group_file = (os.path.join(self.dst_base_path, self.GROUP_FILE_PATH)
                      if self.GROUP_FILE_PATH else
                      '')

        params = {'group': str(self.group),
                  'group_file': group_file}

        if remove_path:
            params['remove_path'] = remove_path

        # TODO: think about changing MinionCmdTask to RsyncBackendTask
        task = MinionCmdTask.new(self,
                                 host=self.dst_host,
                                 cmd=move_cmd,
                                 params=params)
        self.tasks.append(task)

        additional_files = config.get('restore', {}).get('move_additional_files', [])
        for src_file_tpl, dst_file_path in additional_files:
            rsync_cmd = infrastructure.move_group_cmd(
                src_host=self.src_host,
                src_path=self.src_base_path,
                dst_path=os.path.join(self.dst_base_path, dst_file_path),
                file_tpl=src_file_tpl)

            params = {'group': str(self.group)}

            task = MinionCmdTask.new(self,
                                     host=self.dst_host,
                                     group=self.group,
                                     cmd=rsync_cmd,
                                     params=params)
            self.tasks.append(task)

        reconfigure_cmd = infrastructure.reconfigure_node_cmd(
            [self.src_host, self.src_port, self.src_family])

        task = MinionCmdTask.new(self,
                                 host=self.src_host,
                                 cmd=reconfigure_cmd,
                                 params={'node_backend': self.src_node_backend.encode('utf-8')})

        self.tasks.append(task)

        reconfigure_cmd = infrastructure.reconfigure_node_cmd(
            [self.dst_host, self.dst_port, self.dst_family])

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

        start_cmd = infrastructure.enable_node_backend_cmd([
            self.dst_host, self.dst_port, self.dst_family, self.dst_backend_id])
        task = MinionCmdTask.new(self,
                                 host=self.dst_host,
                                 cmd=start_cmd,
                                 params={'node_backend': self.dst_node_backend.encode('utf-8')})
        self.tasks.append(task)

    @property
    def _locks(self):
        couple_keys = []
        couple = self.group in storage.groups and storage.groups[self.group].couple or None
        if couple:
            couple_keys.append('{0}{1}'.format(self.COUPLE_LOCK_PREFIX, str(couple)))
        return (['{0}{1}'.format(self.GROUP_LOCK_PREFIX, group)
                 for group in (self.group, self.uncoupled_group)] +
                couple_keys)
