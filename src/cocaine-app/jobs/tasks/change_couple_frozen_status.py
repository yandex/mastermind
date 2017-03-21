import logging
import time

import msgpack

import helpers
from jobs import TaskTypes
import keys
import storage
from task import Task


logger = logging.getLogger('mm.jobs')


class ChangeCoupleFrozenStatusTask(Task):

    PARAMS = ('couple', 'frozen')
    TASK_TIMEOUT = 10 * 60  # 10 minutes

    def __init__(self, job):
        super(ChangeCoupleFrozenStatusTask, self).__init__(job)
        self.type = TaskTypes.TYPE_CHANGE_COUPLE_FROZEN_STATUS
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
        # TODO: '_meta_key_written' value is not stored and therefore is valid
        # only during one job processor execution cycle. Maybe it should be
        # stored along with other task parameters in db?
        self._meta_key_written = self._try_write_meta_key(processor.session)
        return (
            self._meta_key_written or
            time.time() - self.start_ts > self.TASK_TIMEOUT
        )

    def _try_write_meta_key(self, session):
        couple = storage.couples[self.couple]

        settings = couple.groupset_settings
        settings['frozen'] = self.frozen
        metakey = couple.compose_group_meta(couple, settings)

        s = session.clone()
        s.add_groups([g.group_id for g in couple.groups])
        _, failed_groups = helpers.write_retry(
            s,
            keys.SYMMETRIC_GROUPS_KEY,
            msgpack.packb(metakey),
            retries=1,  # retries will be performed by jobs processor itself
        )
        if failed_groups:
            logger.error(
                'Job {job_id}, task {task_id}: failed to write metakey to groups {groups}'.format(
                    job_id=self.parent_job.id,
                    task_id=self.id,
                    groups=failed_groups,
                )
            )
        else:
            logger.debug(
                'Job {job_id}, task {task_id}: metakey is successfully written '
                'to couple {couple}'.format(
                    job_id=self.parent_job.id,
                    task_id=self.id,
                    couple=couple,
                )
            )

        return not failed_groups

    def failed(self, processor):
        """Return True if task failed.

        NOTE: this check should be evaluated only if 'finished' check returned True.
        """
        return not self._meta_key_written

    def __str__(self):
        return 'ChangeCoupleFrozenStatusTask[id: {task_id}]<couple {couple}, frozen {frozen}>'.format(
            task_id=self.id,
            couple=self.couple,
            frozen=self.frozen,
        )
