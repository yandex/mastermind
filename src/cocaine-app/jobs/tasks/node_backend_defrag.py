import logging

from jobs import JobBrokenError, RetryError, TaskTypes
from minion_cmd import MinionCmdTask
import storage


logger = logging.getLogger('mm.jobs')


class NodeBackendDefragTask(MinionCmdTask):

    PARAMS = MinionCmdTask.PARAMS + ('node_backend', 'group')

    def __init__(self, job):
        super(NodeBackendDefragTask, self).__init__(job)
        self.type = TaskTypes.TYPE_NODE_BACKEND_DEFRAG_TASK

    def execute(self, processor):
        # checking if task still applicable
        logger.info('Job {0}, task {1}: checking group {2} and node backend {3} '
                    'consistency'.format(
                        self.parent_job.id, self.id, self.group, self.node_backend))

        if self.group not in storage.groups:
            raise JobBrokenError('Group {0} is not found'.format(self.group))
        if self.node_backend not in storage.node_backends:
            raise JobBrokenError('Node backend {0} is not found'.format(self.node_backend))

        group = storage.groups[self.group]
        node_backend = storage.node_backends[self.node_backend]

        if group.couple is None:
            raise JobBrokenError('Task {0}: group {1} does not belong '
                                 'to any couple'.format(self, self.group))

        if group.couple.status not in storage.GOOD_STATUSES:
            raise RetryError(10, JobBrokenError('Task {}: group {} couple status is {}'.format(
                self, self.group, group.couple.status)))

        if node_backend not in group.node_backends:
            raise JobBrokenError('Task {0}: node backend {1} does not belong to '
                                 'group {2}'.format(self, self.node_backend, self.group))

        super(NodeBackendDefragTask, self).execute(processor)
