import logging
import os.path

from error import JobBrokenError
from infrastructure import infrastructure
from job import Job
from job_types import JobTypes
from tasks import NodeStopTask, MinionCmdTask, HistoryRemoveNodeTask
import storage


logger = logging.getLogger('mm.jobs')


class BackendCleanupJob(Job):

    GROUP_FILE_PATH = Job.BACKEND_COMMANDS_CFG.get('group_file')
    BACKEND_CLEANUP_GROUP_FILE_DIR_RENAME = Job.BACKEND_COMMANDS_CFG.get(
        'backend_cleanup_group_file_dir_rename'
    )

    PARAMS = ('group', 'couple', 'resources')

    def __init__(self, **kwargs):
        super(BackendCleanupJob, self).__init__(**kwargs)
        self.type = JobTypes.TYPE_BACKEND_CLEANUP_JOB

    def _set_resources(self):
        self.resources = {}

    def create_tasks(self, processor):
        group = storage.groups[self.group]

        if len(group.node_backends) == 1:
            node_backend = group.node_backends[0]
        else:
            raise JobBrokenError(
                'Group {} has {} node backends, currently '
                'only groups with 1 node backend can be used'.format(
                    group.group_id, len(group.node_backends)))

        tasks = []

        tasks.append(
            self._stop_node_backend_task(group, node_backend)
        )

        tasks.append(
            self._reconfigure_node_task(node_backend)
        )

        tasks.append(
            self._remove_node_backend_from_history(group, node_backend)
        )

        self.tasks = tasks

    def _stop_node_backend_task(self, group, node_backend):

        shutdown_cmd = infrastructure._remove_node_backend_cmd(
            host=node_backend.node.host.addr,
            port=node_backend.node.port,
            family=node_backend.node.family,
            backend_id=node_backend.backend_id,
        )

        params = {
            'node_backend': self.node_backend(
                host=node_backend.node.host.addr,
                port=node_backend.node.port,
                family=node_backend.node.family,
                backend_id=node_backend.backend_id,
            ),
            'group': str(group.group_id),
            'success_codes': [self.DNET_CLIENT_ALREADY_IN_PROGRESS],
        }

        group_file = (os.path.join(node_backend.base_path,
                                   self.GROUP_FILE_PATH)
                      if self.GROUP_FILE_PATH else
                      '')
        if self.BACKEND_CLEANUP_GROUP_FILE_DIR_RENAME and group_file:
            params['move_src'] = os.path.join(os.path.dirname(group_file))
            params['move_dst'] = os.path.join(
                node_backend.base_path,
                self.BACKEND_CLEANUP_GROUP_FILE_DIR_RENAME
            )

        return NodeStopTask.new(
            self,
            group=group.group_id,
            uncoupled=True,
            host=node_backend.node.host.addr,
            cmd=shutdown_cmd,
            params=params
        )

    def _reconfigure_node_task(self, node_backend):

        reconfigure_cmd = infrastructure._reconfigure_node_cmd(
            node_backend.node.host.addr,
            node_backend.node.port,
            node_backend.node.family
        )

        return MinionCmdTask.new(
            self,
            host=node_backend.node.host.addr,
            cmd=reconfigure_cmd,
            params={
                'node_backend': self.node_backend(
                    host=node_backend.node.host.addr,
                    port=node_backend.node.port,
                    family=node_backend.node.family,
                    backend_id=node_backend.backend_id,
                ),
            },
        )

    def _remove_node_backend_from_history(self, group, node_backend):
        return HistoryRemoveNodeTask.new(
            self,
            group=group.group_id,
            host=node_backend.node.host.addr,
            port=node_backend.node.port,
            family=node_backend.node.family,
            backend_id=node_backend.backend_id,
        )

    @property
    def _involved_groups(self):
        group_ids = set([self.group])
        if self.group in storage.groups:
            group = storage.groups[self.group]
            if group.couple:
                group_ids.update(g.group_id for g in group.coupled_groups)
        return group_ids

    @property
    def _involved_couples(self):
        if not self.couple:
            return []
        return [self.couple]

    @property
    def involved_uncoupled_groups(self):
        if self.group in storage.groups:
            group = storage.groups[self.group]
            if group.type == storage.Group.TYPE_UNCOUPLED:
                return [self.group]
        return []
