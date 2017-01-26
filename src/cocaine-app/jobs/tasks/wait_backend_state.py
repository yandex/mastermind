import logging
import time

from jobs import TaskTypes
import storage
from task import Task


logger = logging.getLogger('mm.jobs')


class WaitBackendStateTask(Task):

    PARAMS = (
        'backend',
        'backend_statuses',
        'missing',
        'sleep_period'
    )
    TASK_TIMEOUT = 30 * 60  # 30 minutes

    def __init__(self, job):
        super(WaitBackendStateTask, self).__init__(job)
        self.type = TaskTypes.TYPE_WAIT_BACKEND_STATE

    def _update_status(self, processor):
        # infrastructure state is updated by itself via task queue
        pass

    def _terminate(self, processor):
        # cannot terminate task, since this task works only synchronously
        # early cleanup phase breaks nothing
        pass

    def _execute(self, processor):
        pass

    def finished(self, processor):
        if self.sleep_period:
            if time.time() - self.start_ts < self.sleep_period:
                return False

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
        if not self.__backend_detected():
            if self.missing:
                return True
            else:
                return False

        if self.backend_statuses and self.__status_matched():
            return True

        return False

    def __backend_detected(self):
        return self.backend in storage.node_backends

    def __status_matched(self):
        return storage.node_backends[self.backend].status in self.backend_statuses

    def __str__(self):
        return (
            'WaitBackendStateTask[id: {id}]<backend {backend}, statuses {statuses}>'.format(
                id=self.id,
                backend=self.backend,
                statuses=self.backend_statuses,
            )
        )
