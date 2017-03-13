import logging

from jobs import TaskTypes, RetryError
from minion_cmd import MinionCmdTask


logger = logging.getLogger('mm.jobs')


class CheckFileSystemTask(MinionCmdTask):
    PARAMS = MinionCmdTask.PARAMS

    def __init__(self, job):
        super(CheckFileSystemTask, self).__init__(job)
        self.cmd = TaskTypes.TYPE_CHECK_FILE_SYSTEM_TASK
        self.type = TaskTypes.TYPE_CHECK_FILE_SYSTEM_TASK

    def _execute(self, processor):
        try:
            minion_response = processor.minions_monitor.minion_base_cmd(
                self.host,
                'check_file_system',
                self.params
            )
        except RuntimeError as e:
            raise RetryError(self.attempts, e)
        self._set_minion_task_parameters(minion_response)
