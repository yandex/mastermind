import logging
import time

from jobs import TaskTypes
import storage
from task import Task


logger = logging.getLogger('mm.jobs')


class ChangeCoupleSettingsTask(Task):

    PARAMS = ('couple', 'settings', 'update')
    TASK_TIMEOUT = 10 * 60  # 10 minutes

    def __init__(self, job):
        super(ChangeCoupleSettingsTask, self).__init__(job)
        self.type = TaskTypes.TYPE_CHANGE_COUPLE_SETTINGS
        self._changed = False

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
        # TODO: '_changed' value is not stored and therefore is valid
        # only during one job processor execution cycle. Maybe it should be
        # stored along with other task parameters in db?
        self._changed = self._try_change_settings(processor.couple_record_finder)
        return (
            self._changed or
            time.time() - self.start_ts > self.TASK_TIMEOUT
        )

    def _try_change_settings(self, couple_record_finder):
        couple = storage.couples[self.couple]

        try:
            couple_record = couple_record_finder.couple_record(couple)
            couple_record.set_settings(
                settings=self.settings,
                update=self.update,
            )
            couple_record.save()
        except Exception:
            logger.exception(
                'Job {job_id}, task {task_id}: failed to change couple settings'.format(
                    job_id=self.parent_job.id,
                    task_id=self.id,
                )
            )
            return False

        return bool(couple_record)

    def failed(self, processor):
        """Return True if task failed.

        NOTE: this check should be evaluated only if 'finished' check returned True.
        """
        return not self._changed

    def __str__(self):
        return (
            'ChangeCoupleSettingsTask[id: {task_id}]<couple {couple}, settings {settings}, '
            'update {update}>'.format(
                task_id=self.id,
                couple=self.couple,
                settings=self.settings,
                update=self.update,
            )
        )
