import logging

import inventory
from jobs import JobBrokenError, TaskTypes, RetryError
from minion_cmd import MinionCmdTask
import storage


logger = logging.getLogger('mm.jobs')


class CreateGroupTask(MinionCmdTask):
    """
    Minion task to create storage group

    Creates custom file structure on a file system
    for a node to find new group and run its backend.
    """

    PARAMS = MinionCmdTask.PARAMS

    def __init__(self, job):
        super(CreateGroupTask, self).__init__(job)
        self.cmd = TaskTypes.TYPE_CREATE_GROUP
        self.type = TaskTypes.TYPE_CREATE_GROUP

    def _execute(self, processor):
        try:
            minion_response = processor.minions_monitor.create_group(
                self.host,
                self.params,
                files=inventory.get_new_group_files(
                    group_id=self.group,
                    total_space=self.params['total_space'],
                )
            )
        except RuntimeError as e:
            raise RetryError(self.attempts, e)
        self._set_minion_task_parameters(minion_response)
