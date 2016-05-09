import logging

from jobs import JobBrokenError, TaskTypes, RetryError
from minion_cmd import MinionCmdTask
import storage


logger = logging.getLogger('mm.jobs')


class RemoveGroupTask(MinionCmdTask):
    """
    Minion task to remove storage group

    Current implementation just renames the backend base path
    so that automatic configuration could skip backend when
    the node is being started.
    """

    PARAMS = MinionCmdTask.PARAMS

    def __init__(self, job):
        super(RemoveGroupTask, self).__init__(job)
        self.cmd = TaskTypes.TYPE_REMOVE_GROUP
        self.type = TaskTypes.TYPE_REMOVE_GROUP

    def execute(self, processor):
        if self.group not in storage.groups:
            raise JobBrokenError(
                'Group {group_id} is not found in storage'.format(
                    group_id=self.group,
                )
            )
        try:
            minion_response = processor.minions.remove_group(
                self.host,
                self.params
            )
        except RuntimeError as e:
            raise RetryError(self.attempts, e)
        self._set_minion_task_parameters(minion_response)
