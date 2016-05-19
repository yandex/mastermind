import logging
import time

from jobs import TaskTypes
import storage
from task import Task


logger = logging.getLogger('mm.jobs')


class WaitGroupsetStateTask(Task):

    PARAMS = ('groupset', 'groupset_status')
    TASK_TIMEOUT = 30 * 60  # 30 minutes

    def __init__(self, job):
        super(WaitGroupsetStateTask, self).__init__(job)
        self.type = TaskTypes.TYPE_WAIT_GROUPSET_STATE

    def update_status(self):
        # infrastructure state is updated by itself via task queue
        pass

    def execute(self):
        pass

    def finished(self, processor):
        is_timeout = time.time() - self.start_ts > self.TASK_TIMEOUT
        if is_timeout:
            return True

        return self.__state_matched()

    def failed(self, processor):
        """Return True if task failed.

        NOTE: this check should be evaluated only if 'finished' check returned True.
        """
        return not self.__state_matched()

    def __state_matched(self):
        if not self.__groupset_detected():
            return False

        if self.groupset_status and not self.__status_matched():
            return False

        return True

    def __groupset_detected(self):
        return self.groupset in storage.groupsets

    def __status_matched(self):
        return storage.groupsets[self.groupset].status == self.groupset_status

    def __str__(self):
        return (
            'WaitGroupsetStateTask[id: {id}]<groupset {groupset}, status {status}>'.format(
                id=self.id,
                groupset=self.groupset,
                status=self.groupset_status,
            )
        )
