import logging

from jobs import TaskTypes, RetryError
from minion_cmd import MinionCmdTask


logger = logging.getLogger('mm.jobs')


class RemovePathTask(MinionCmdTask):
    PARAMS = MinionCmdTask.PARAMS

    def __init__(self, job):
        super(RemovePathTask, self).__init__(job)
        self.cmd = TaskTypes.TYPE_REMOVE_PATH
        self.type = TaskTypes.TYPE_REMOVE_PATH

    def execute(self, processor):
        try:
            minion_response = processor.minions_monitor.minion_base_cmd(
                self.host,
                'remove_path',
                self.params
            )
        except RuntimeError as e:
            raise RetryError(self.attempts, e)
        self._set_minion_task_parameters(minion_response)
