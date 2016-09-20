import logging
import storage

from jobs import JobBrokenError, TaskTypes
from minion_cmd import MinionCmdTask


logger = logging.getLogger('mm.jobs')


class NodeStopTask(MinionCmdTask):

    PARAMS = MinionCmdTask.PARAMS + ('uncoupled',)

    def __init__(self, job):
        super(NodeStopTask, self).__init__(job)
        self.type = TaskTypes.TYPE_NODE_STOP_TASK

    def execute(self, processor):

        if self.group:
            # checking if task still applicable
            logger.info(
                'Job {job_id}, task {task_id}: checking group {group} and host {host} '
                'consistency'.format(
                    job_id=self.parent_job.id,
                    task_id=self.id,
                    group=self.group,
                    host=self.host
                )
            )

            if self.group not in storage.groups:
                raise JobBrokenError(
                    'Task {task_id}: group {group} is not found'.format(
                        task_id=self.id,
                        group=self.group
                    )
                )

            group = storage.groups[self.group]

            nb_addresses = set(nb.node.host.addr for nb in group.node_backends)
            nb_addresses.discard(self.host)

            if nb_addresses:
                raise JobBrokenError(
                    'Task {task_id}: group {group} has unexpected node backends: '
                    '{node_backends}, expected one backend on host {host}'.format(
                        task_id=self.id,
                        group=self.group,
                        node_backends=list(nb_addresses),
                        host=self.host
                    )
                )

            valid_statuses = (storage.Status.OK, storage.Status.RO, storage.Status.STALLED)
            if group.node_backends and group.node_backends[0].status not in valid_statuses:
                raise JobBrokenError(
                    'Task {task_id}: node of group {group} has '
                    'status {status}, should be one of {valid_statuses}'.format(
                        task_id=self.id,
                        group=self.group,
                        status=group.node_backends[0].status,
                        valid_statuses=valid_statuses
                    )
                )

        super(NodeStopTask, self).execute(processor)
