import logging
import storage

from jobs import JobBrokenError, TaskTypes
from minion_cmd import MinionCmdTask


logger = logging.getLogger('mm.jobs')


class LrcRecoveryTask(MinionCmdTask):

    TASK_TIMEOUT = 24 * 60 * 60  # 1 day

    PARAMS = MinionCmdTask.PARAMS + ('couple',)

    def __init__(self, job):
        super(LrcRecoveryTask, self).__init__(job)
        self.type = TaskTypes.TYPE_LRC_RECOVERY_TASK

    @classmethod
    def new(cls, job, **kwargs):
        task = super(LrcRecoveryTask, cls).new(job, **kwargs)
        task.check(task.group)
        task.couple = storage.groups[task.group].couple.as_tuple()
        return task

    def check(self, group_id):
        if group_id not in storage.groups:
            raise JobBrokenError('Group {0} is not found'.format(group_id))

    def failed(self, processor):
        if self.minion_cmd is None:
            return True

        success = (
            self.minion_cmd['exit_code'] == 0 or
            self.minion_cmd.get('command_code') in self.params.get('success_codes', [])
        )
        if success:
            return False

        return True
