import logging
import time

import msgpack

import helpers
from jobs import TaskTypes
import keys
from task import Task


logger = logging.getLogger('mm.jobs')


class WriteExternalStorageMappingTask(Task):

    PARAMS = ('external_storage', 'external_storage_options', 'couples', 'namespace')
    TASK_TIMEOUT = 600

    def __init__(self, job):
        super(WriteExternalStorageMappingTask, self).__init__(job)
        self.type = TaskTypes.TYPE_WRITE_EXTERNAL_STORAGE_MAPPING
        self._written = False

    def update_status(self):
        # state update is not required.
        pass

    def execute(self):
        # this task execution does not rely on common task workflow
        # of executing a command and waiting till it's finished,
        # rather it tries to execute action on 'finished' check
        # till it completes without an error.
        pass

    def finished(self, processor):
        self._written = self._try_write_external_storage_mapping(processor.external_storage_meta)
        return (
            self._written or
            time.time() - self.start_ts > self.TASK_TIMEOUT
        )

    def _try_write_external_storage_mapping(self, external_storage_meta):
        return external_storage_meta.set_mapping(
            external_storage=self.external_storage,
            external_storage_options=self.external_storage_options,
            couples=self.couples,
            namespace=self.namespace,
        )

    def failed(self, processor):
        return (time.time() - self.start_ts > self.TASK_TIMEOUT and
                not self._written)

    def __str__(self):
        return 'WriteExternalStorageMappingTask[id: {task_id}]<couples {couples}>'.format(
            task_id=self.id,
            couples=self.couples,
        )
