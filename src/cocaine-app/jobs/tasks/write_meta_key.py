import logging
import time

import msgpack

import helpers
from jobs import TaskTypes
import keys
from task import Task


logger = logging.getLogger('mm.jobs')


class WriteMetaKeyTask(Task):

    PARAMS = ('group', 'metakey')
    TASK_TIMEOUT = 600

    def __init__(self, job):
        super(WriteMetaKeyTask, self).__init__(job)
        self.type = TaskTypes.TYPE_WRITE_META_KEY
        self._meta_key_written = False

    def _update_status(self, processor):
        # state update is not required.
        pass

    def _terminate(self, processor):
        # cannot terminate task, since this task works only synchronously
        # early cleanup phase breaks nothing
        pass

    def _execute(self, processor):
        # this task execution does not rely on common task workflow
        # of executing a command and waiting till it's finished,
        # rather it tries to execute action on 'finished' check
        # till it completes without an error.
        pass

    def finished(self, processor):
        self._meta_key_written = self._try_write_meta_key(processor.session)
        return (
            self._meta_key_written or
            time.time() - self.start_ts > self.TASK_TIMEOUT
        )

    def _try_write_meta_key(self, session):
        s = session.clone()
        s.add_groups([self.group])
        _, failed_groups = helpers.write_retry(
            s,
            keys.SYMMETRIC_GROUPS_KEY,
            msgpack.packb(self.metakey),
            retries=1,
        )
        if failed_groups:
            logger.error(
                'Job {job_id}, task {task_id}: failed to write metakey to group {group}'.format(
                    job_id=self.parent_job.id,
                    task_id=self.id,
                    group=self.group,
                )
            )
        else:
            logger.debug(
                'Job {job_id}, task {task_id}: metakey is successfully written '
                'to group {group}'.format(
                    job_id=self.parent_job.id,
                    task_id=self.id,
                    group=self.group,
                )
            )

        return not failed_groups

    def failed(self, processor):
        return (time.time() - self.start_ts > self.TASK_TIMEOUT and
                not self._meta_key_written)

    def __str__(self):
        return 'WriteMetaKeyTask[id: {task_id}]<group {group}>'.format(
            task_id=self.id,
            group=self.group,
        )
