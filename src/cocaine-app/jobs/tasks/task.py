import time
import uuid

from run_history import RunHistoryRecord


class Task(object):

    STATUS_QUEUED = 'queued'
    STATUS_EXECUTING = 'executing'
    STATUS_FAILED = 'failed'
    STATUS_SKIPPED = 'skipped'
    STATUS_COMPLETED = 'completed'

    def __init__(self, job):
        self.status = self.STATUS_QUEUED
        self.id = uuid.uuid4().hex
        self.type = None
        self.start_ts = None
        self.finish_ts = None
        self.attempts = 0
        self.error_msg = []
        self.run_history = []
        self.parent_job = job

    def on_exec_start(self, processor):
        """
        Called every time task changes status from 'queued' to 'executing'
        """
        pass

    def on_exec_stop(self, processor):
        """
        Called every time task changes status from 'executing' to anything else
        """
        pass

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
        self.status = data['status']
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
            'status': self.status,
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

    def make_new_history_record(self):
        return RunHistoryRecord({
            RunHistoryRecord.START_TS: None,
            RunHistoryRecord.FINISH_TS: None,
            RunHistoryRecord.STATUS: None,
            RunHistoryRecord.ARTIFACTS: {},
            RunHistoryRecord.ERROR_MSG: None,
            RunHistoryRecord.ATTEMPTS: 0,
            RunHistoryRecord.DELAYED_TILL_TS: None,
        })

    def add_history_record(self):
        record = self.make_new_history_record()
        record.start_ts = int(time.time())
        self.run_history.append(record)
        return record

    @property
    def last_run_history_record(self):
        return self.run_history[-1]

    def on_run_history_update(self, error=None):
        last_record = self.last_run_history_record
        last_record.finish_ts = int(time.time())
        if self.status == Task.STATUS_FAILED or error:
            last_record.status = 'error'
            if error:
                last_record.error_msg = str(error)
            last_record.delayed_till_ts = self.next_retry_ts
        else:
            last_record.status = 'success'

    def ready_for_retry(self, processor):
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
