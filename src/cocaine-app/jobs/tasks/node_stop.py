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

    def execute(self, minions):

        if self.group:
            # checking if task still applicable
            logger.info('Job {0}, task {1}: checking group {2} and host {3} '
                'consistency'.format(self.parent_job.id, self.id, self.group, self.host))

            if not self.group in storage.groups:
                raise JobBrokenError('Task {0}: group {0} is not found'.format(self.id, self.group))

            group = storage.groups[self.group]
            if len(group.node_backends) != 1 or group.node_backends[0].node.host.addr != self.host:
                raise JobBrokenError('Task {0}: group {1} has more than '
                    'one node backend: {2}, expected host {3}'.format(self.id, self.group,
                        [str(nb) for nb in group.node_backends], self.host))

            if group.node_backends[0].status != storage.Status.OK:
                raise JobBrokenError('Task {0}: node of group {1} has '
                    'status {2}, should be {3}'.format(self.id, self.group,
                        group.node_backends[0].status, storage.Status.OK))

            if self.uncoupled:
                if group.couple:
                    raise JobBrokenError('Task {0}: group {1} happens to be '
                        'already coupled'.format(self.id, self.group))
                if group.node_backends[0].stat.files + group.node_backends[0].stat.files_removed > 0:
                    raise JobBrokenError('Task {0}: group {1} has non-zero '
                        'number of keys (including removed)'.format(self.id, self.group))
            else:
                if not group.couple:
                    raise JobBrokenError('Task {0}: group {1} is not '
                        'coupled'.format(self.id, self.group))
                if group.couple.status not in storage.GOOD_STATUSES:
                    raise JobBrokenError('Task {0}: group {1} couple {2} '
                        'status is {3}'.format(self.id, self.group, str(group.couple), group.couple.status))

        super(NodeStopTask, self).execute(minions)
