import logging
import storage

from jobs import JobBrokenError, TaskTypes
from minion_cmd import MinionCmdTask


logger = logging.getLogger('mm.jobs')


class RecoverGroupDcTask(MinionCmdTask):

    PARAMS = MinionCmdTask.PARAMS + ('couple',)

    def __init__(self, job):
        super(RecoverGroupDcTask, self).__init__(job)
        self.type = TaskTypes.TYPE_RECOVER_DC_GROUP_TASK

    @classmethod
    def new(cls, job, **kwargs):
        task = super(RecoverGroupDcTask, cls).new(job, **kwargs)
        task.check(task.group)
        task.couple = storage.groups[task.group].couple.as_tuple()
        return task

    def check(self, group_id):
        if not group_id in storage.groups:
            raise JobBrokenError('Group {0} is not found'.format(group_id))

        group = storage.groups[group_id]

        if group.status != storage.Status.COUPLED:
            raise JobBrokenError('Task {0}: group {1} has status {2}, '
                'should be {3}'.format(self, self.group,
                                       group.status, storage.Status.COUPLED))

    def execute(self, processor):

        # checking if task still applicable
        logger.info('Job {0}, task {1}: checking group {2} and couple {3} '
            'consistency'.format(self.parent_job.id, self.id, self.group, self.couple))

        self.check(self.group)
        group = storage.groups[self.group]

        if set(self.couple) != set(group.couple.as_tuple()):
            raise JobBrokenError('Task {0}: group {1} has changed couple to {2}, '
                'expected {3}'.format(self, self.group,
                                       group.couple, self.couple))

        super(RecoverGroupDcTask, self).execute(processor)
