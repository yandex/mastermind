import functools
import logging
import time
import uuid

from mastermind_core.config import config
from jobs.error import RetryError
from run_history import RunHistoryRecord

logger = logging.getLogger('mm.jobs')

JOB_CONFIG = config.get('jobs', {})


def set_status_to_failed_on_error(error_description):
    def wrapper(function):
        @functools.wraps(function)
        def wrapped_function(self, *args, **kwargs):
            try:
                return function(self, *args, **kwargs)
            except RetryError as e:
                # TODO make retry logic here, not FAILED!
                logger.error('Job {}, task {}: retry error: {}'.format(self.parent_job.id, self.id, e))
                self.set_status(Task.STATUS_FAILED, error=e)
                raise
            except Exception as e:
                logger.exception('Job {}, task {}: {}'.format(
                    self.parent_job.id,
                    self.id,
                    error_description,
                ))
                self.set_status(Task.STATUS_FAILED, error=e)
                raise

        return wrapped_function

    return wrapper


class Task(object):

    STATUS_QUEUED = 'queued'
    STATUS_EXECUTING = 'executing'
    STATUS_FAILED = 'failed'
    STATUS_SKIPPED = 'skipped'
    STATUS_COMPLETED = 'completed'
    ALL_STATUSES = (STATUS_QUEUED, STATUS_EXECUTING, STATUS_FAILED, STATUS_SKIPPED, STATUS_COMPLETED)

    PREVIOUS_STATUSES = {
        STATUS_QUEUED: (STATUS_FAILED,),
        STATUS_EXECUTING: (STATUS_QUEUED,),
        STATUS_COMPLETED: (STATUS_EXECUTING,),
        STATUS_FAILED: (STATUS_EXECUTING, STATUS_FAILED),
        STATUS_SKIPPED: (STATUS_FAILED,),
    }
    FINISHED_STATUSES = (STATUS_SKIPPED, STATUS_COMPLETED)

    def __init__(self, job):
        self._status = Task.STATUS_QUEUED
        self.id = uuid.uuid4().hex
        self.type = None
        self.start_ts = None
        self.finish_ts = None
        self.attempts = 0
        self.error_msg = []
        self.run_history = []
        self.parent_job = job

    def _on_exec_start(self, processor):
        """
        Called every time task changes status from 'queued' to 'executing'
        Should not be called directly, use _wrapped_on_exec_start(processor) instead
        """
        pass

    @set_status_to_failed_on_error("failed to execute task start handler")
    def _wrapped_on_exec_start(self, processor):
        self._on_exec_start(processor)

    # self.status is an indicator either task is successfully finished or not
    def _on_exec_stop(self, processor):
        """
        Called every time task changes status from 'executing' to anything else
        Should not be called directly, use _wrapped_on_exec_stop(processor) instead
        """
        pass

    @set_status_to_failed_on_error("failed to execute task stop handler")
    def _wrapped_on_exec_stop(self, processor):
        self._on_exec_stop(processor)

    def _execute(self, processor):
        """
        Should not be called directly, use _start_executing(self, processor) instead
        """
        raise NotImplementedError("Children class should override this function")

    def _terminate(self, processor):
        """
        Should not be called directly, use stop(self, processor) instead.

        If you want to implement this function as:
            def _terminate(self, processor):
                pass
        Then make sure that cleanup phase, which will be after ._terminate(_, _) successfully finished
        do not break logic of the current execution (you can always raise an exception to prevent this).
        """
        raise NotImplementedError("Children class should override this function")

    @set_status_to_failed_on_error("failed to stop task")
    def stop(self, processor):
        self._terminate(processor)
        # if termination is failed, _wrapped_on_exec_stop is not safe to call
        self.set_status(Task.STATUS_FAILED, "Task is stopped")
        self._wrapped_on_exec_stop(processor)

    def _update_status(self, processor):
        raise NotImplementedError("Children class should override this function")

    def finished(self, processor):
        raise NotImplementedError("Children class should override this function")

    def failed(self, processor):
        raise NotImplementedError("Children class should override this function")

    def _start_executing(self, processor):
        self.start_ts, self.finish_ts = time.time(), None
        self._execute(processor)

    @classmethod
    def new(cls, job, **kwargs):
        task = cls(job)
        for param in cls.PARAMS:
            setattr(task, param, kwargs.get(param))
        return task

    @classmethod
    def from_data(cls, data, job):
        task = cls(job)
        task.load(data)
        return task

    def load(self, data):
        # TODO: remove 'or' part
        self.id = data['id'] or uuid.uuid4().hex
        self._status = data['status']
        self.type = data['type']
        self.start_ts = data['start_ts']
        self.finish_ts = data['finish_ts']
        self.error_msg = data['error_msg']
        self.attempts = data.get('attempts', 0)

        self.run_history = [
            RunHistoryRecord(run_history_record_data)
            for run_history_record_data in data.get('run_history', [])
        ]

        for param in self.PARAMS:
            val = data.get(param)
            if isinstance(val, unicode):
                val = val.encode('utf-8')
            setattr(self, param, val)

    def dump(self):
        res = {
            'status': self._status,
            'id': self.id,
            'type': self.type,
            'start_ts': self.start_ts,
            'finish_ts': self.finish_ts,
            'error_msg': self.error_msg,
            'attempts': self.attempts,
            'run_history': [
                run_history_record.dump()
                for run_history_record in self.run_history
            ],
        }
        res.update({
            k: getattr(self, k)
            for k in self.PARAMS
        })
        return res

    def human_dump(self):
        return self.dump()

    def __str__(self):
        raise RuntimeError('__str__ method should be implemented in derived class')

    def _make_new_history_record(self):
        return RunHistoryRecord({
            RunHistoryRecord.START_TS: None,
            RunHistoryRecord.FINISH_TS: None,
            RunHistoryRecord.STATUS: None,
            RunHistoryRecord.ARTIFACTS: {},
            RunHistoryRecord.ERROR_MSG: None,
            RunHistoryRecord.DELAYED_TILL_TS: None,
        })

    def _add_history_record(self):
        record = self._make_new_history_record()
        record.start_ts = int(time.time())
        self.run_history.append(record)
        return record

    @property
    def last_run_history_record(self):
        return self.run_history[-1]

    def on_run_history_update(self, error=None):
        if not self.run_history:
            return
        last_record = self.last_run_history_record
        last_record.finish_ts = int(time.time())
        if self._status == Task.STATUS_FAILED or error:
            last_record.status = 'error'
            if error:
                last_record.error_msg = str(error)
            last_record.delayed_till_ts = self.next_retry_ts
        else:
            last_record.status = 'success'

    def ready_for_retry(self, processor):
        if not self.run_history:
            return False
        last_record = self.last_run_history_record
        if last_record.delayed_till_ts and time.time() > last_record.delayed_till_ts:
            return True
        return False

    @property
    def next_retry_ts(self):
        """ Timestamp of the next attempt of task retry after an error.

        Task types that are subject to automatic retries should implement
        this property.

        'None' is interpreted as no automatic retry attempts.
        """
        return None

    @property
    def status(self):
        return self._status

    def set_status(self, status, error=None):
        if status not in Task.ALL_STATUSES:
            raise ValueError(
                "Attempt to change task status, unknown value: {}. Accepted statuses: {}".format(
                    status,
                    Task.ALL_STATUSES,
                )
            )

        if self._status not in Task.PREVIOUS_STATUSES[status]:
            error_msg = "(local error: {})".format(error) if error else ""
            raise ValueError(
                'Job {job_id}, task {task_id}: attempt to change task status to {new}, '
                'current status is {current}, but expected one of {expected}.{error_msg}'.format(
                    job_id=self.parent_job.id,
                    task_id=self.id,
                    new=status,
                    current=self.status,
                    expected=Task.PREVIOUS_STATUSES[status],
                    error_msg=error_msg,
                )
            )

        self._status = status

        if status in (Task.STATUS_FAILED, Task.STATUS_COMPLETED):
            self.on_run_history_update(error=error)
            return

        if status == Task.STATUS_EXECUTING:
            self._add_history_record()
            return

        if status == Task.STATUS_QUEUED:
            return

    def _start_task(self, processor):
        logger.info('Job {}, executing new task {}'.format(self.parent_job.id, self))

        self.set_status(Task.STATUS_EXECUTING)

        self._wrapped_on_exec_start(processor)
        logger.info('Job {}, task {}: preparation completed'.format(self.parent_job.id, self.id))

        try:
            self.attempts += 1
            self._start_executing(processor)
            logger.info('Job {}, task {}: execution successfully started'.format(
                self.parent_job.id,
                self.id
            ))
        except RetryError as e:
            logger.error('Job {}, task {}: retry error: {}'.format(self.parent_job.id, self.id, e))
            self.set_status(Task.STATUS_FAILED, error=e)
            self._wrapped_on_exec_stop(processor)
            if self.attempts < JOB_CONFIG.get('minions', {}).get('execute_attempts', 3):
                self.set_status(Task.STATUS_QUEUED)
                return
            raise
        except Exception as e:
            logger.exception('Job {}, task {}: failed to execute task'.format(self.parent_job.id, self.id))
            self.set_status(Task.STATUS_FAILED, error=e)
            self._wrapped_on_exec_stop(processor)
            raise

    def update(self, processor):
        assert self.status == Task.STATUS_EXECUTING

        logger.info('Job {}, task {}: status update'.format(self.parent_job.id, self.id))

        try:
            self._update_status(processor)
        except Exception as e:
            logger.exception('Job {}, task {}: failed to update status'.format(self.parent_job.id, self.id))
            self.set_status(Task.STATUS_FAILED, error=e)
            self._wrapped_on_exec_stop(processor)
            raise

        try:
            if not self.finished(processor):
                logger.debug('Job {}, task {}: is not finished'.format(self.parent_job.id, self.id))
                return
            task_is_failed = self.failed(processor)

        except Exception as e:
            logger.exception('Job {}, task {}: failed to check status'.format(self.parent_job.id, self.id))
            self.set_status(Task.STATUS_FAILED, error=e)
            self._wrapped_on_exec_stop(processor)
            raise

        if task_is_failed:
            self.set_status(Task.STATUS_FAILED)

        self._wrapped_on_exec_stop(processor)

        if not task_is_failed:
            self.set_status(Task.STATUS_COMPLETED)

        logger.debug('Job {}, task {}: is finished, status {}'.format(self.parent_job.id, self.id, self.status))
