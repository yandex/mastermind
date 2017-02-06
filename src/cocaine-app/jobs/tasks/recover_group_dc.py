import logging
import storage

from jobs import JobBrokenError, TaskTypes
from minion_cmd import MinionCmdTask


logger = logging.getLogger('mm.jobs')


class RecoverGroupDcTask(MinionCmdTask):

    TASK_TIMEOUT = 24 * 60 * 60  # 1 day

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

    STATUSES_TO_SKIP = (
        -2,   # No such file or directory (key is not found)
        -77,  # File descriptor in bad state (concurrent key write error when CAS is used)
    )

    def failed(self, processor):
        if self.minion_cmd is None:
            return True

        success = (
            self.minion_cmd['exit_code'] == 0 or
            self.minion_cmd.get('command_code') in self.params.get('success_codes', [])
        )
        if success:
            return False

        commands_statuses = self.minion_cmd.get('commands_statuses')
        if commands_statuses:
            logger.info(
                'Job {job_id}, task {task_id}: checking recovery commands statuses: '
                '{commands_statuses}'.format(
                    job_id=self.parent_job.id,
                    task_id=self.id,
                    commands_statuses=commands_statuses,
                )
            )
            statuses = {
                int(status): count
                for command, status_count in commands_statuses.iteritems()
                for status, count in status_count.iteritems()
            }

            for status, count in statuses.iteritems():
                if status not in self.STATUSES_TO_SKIP and count > 0:
                    logger.error(
                        'Job {job_id}, task {task_id}: commands statuses contain status that '
                        'cannot be skipped: {status} (count {count})'.format(
                            job_id=self.parent_job.id,
                            task_id=self.id,
                            status=status,
                            count=count,
                        )
                    )
                    return True

            return False

        return True

