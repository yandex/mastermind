import logging

from error import JobBrokenError
from infrastructure import infrastructure
from job import Job
from job_types import JobTypes
from tasks import NodeStopTask
import storage


logger = logging.getLogger('mm.jobs')


class BackendManagerJob(Job):

    PARAMS = ('group', 'couple', 'cmd_type', 'resources', 'mark_backend', 'unmark_backend')

    CMD_TYPE_DISABLE = 'disable'
    CMD_TYPE_MAKE_WRITABLE = 'make_writable'
    CMD_TYPE_MAKE_READONLY = 'make_readonly'

    def __init__(self, **kwargs):
        super(BackendManagerJob, self).__init__(**kwargs)
        self.type = JobTypes.TYPE_BACKEND_MANAGER_JOB

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

        if self.cmd_type == self.CMD_TYPE_DISABLE:
            cmd = infrastructure._disable_node_backend_cmd(
                node_backend.node.host.addr,
                node_backend.node.port,
                node_backend.node.family,
                node_backend.backend_id,
            )
        elif self.cmd_type == self.CMD_TYPE_MAKE_WRITABLE:
            cmd = infrastructure._make_writable_node_backend_cmd(
                node_backend.node.host.addr,
                node_backend.node.port,
                node_backend.node.family,
                node_backend.backend_id,
            )
        elif self.cmd_type == self.CMD_TYPE_MAKE_READONLY:
            cmd = infrastructure._make_readonly_node_backend_cmd(
                node_backend.node.host.addr,
                node_backend.node.port,
                node_backend.node.family,
                node_backend.backend_id,
            )
        else:
            raise JobBrokenError('Unknown cmd type: {}'.format(self.cmd_type))

        mark_backend_path = self.make_path(
            self.BACKEND_DOWN_MARKER, base_path=node_backend.base_path).format(
                backend_id=node_backend.backend_id)

        task_params = {
            'node_backend': self.node_backend(
                host=node_backend.node.host.addr,
                port=node_backend.node.port,
                family=node_backend.node.family,
                backend_id=node_backend.backend_id,
            ),
            'group': str(group.group_id),
            'success_codes': [self.DNET_CLIENT_ALREADY_IN_PROGRESS],
        }

        if self.mark_backend:
            task_params['mark_backend'] = mark_backend_path
        elif self.unmark_backend:
            task_params['unmark_backend'] = mark_backend_path

        task = NodeStopTask.new(
            self,
            group=group.group_id,
            uncoupled=True,
            host=node_backend.node.host.addr,
            cmd=cmd,
            params=task_params
        )

        self.tasks.append(task)

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
        if self.group in storage:
            group = storage.groups[self.group]
            if group.type == storage.Group.TYPE_UNCOUPLED:
                return [self.group]
        return []
