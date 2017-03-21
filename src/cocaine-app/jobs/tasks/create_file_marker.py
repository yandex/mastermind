import logging

from jobs import TaskTypes, RetryError
from minion_cmd import MinionCmdTask


logger = logging.getLogger('mm.jobs')


class CreateFileMarkerTask(MinionCmdTask):
    PARAMS = MinionCmdTask.PARAMS

    def __init__(self, job):
        super(CreateFileMarkerTask, self).__init__(job)
        self.cmd = TaskTypes.TYPE_CREATE_FILE_MARKER
        self.type = TaskTypes.TYPE_CREATE_FILE_MARKER

    def _execute(self, processor):
        try:
            minion_response = processor.minions_monitor.minion_base_cmd(
                self.host,
                'create_file_marker',
                self.params
            )
        except RuntimeError as e:
            raise RetryError(self.attempts, e)
        self._set_minion_task_parameters(minion_response)
