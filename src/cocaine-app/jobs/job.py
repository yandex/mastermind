from contextlib import contextmanager
import logging
import msgpack
import threading
import time
import uuid

from db.mongo import MongoObject
from db.mongo.job import JobView
import helpers as h
import keys
from sync import sync_manager
from sync.error import (
    LockError,
    LockFailedError,
    InconsistentLockError,
    LockAlreadyAcquiredError,
    API_ERROR_CODE
)
from tasks import TaskFactory

import pymongo


logger = logging.getLogger('mm.jobs')


class Job(MongoObject):

    MODEL = JobView

    STATUS_NOT_APPROVED = 'not_approved'
    STATUS_NEW = 'new'
    STATUS_EXECUTING = 'executing'
    STATUS_PENDING = 'pending'
    STATUS_BROKEN = 'broken'
    STATUS_COMPLETED = 'completed'
    STATUS_CANCELLED = 'cancelled'

    GROUP_LOCK_PREFIX = 'group/'
    COUPLE_LOCK_PREFIX = 'couple/'

    COMMON_PARAMS = ('need_approving',)

    def __init__(self, need_approving=False):
        self.id = uuid.uuid4().hex
        self.status = (self.STATUS_NOT_APPROVED
                       if need_approving else
                       self.STATUS_NEW)
        self._create_ts = None
        self._start_ts = None
        self._update_ts = None
        self._finish_ts = None
        self.type = None
        self.tasks = []
        self.__tasklist_lock = threading.Lock()
        self.error_msg = []

    @contextmanager
    def tasks_lock(self):
        with self.__tasklist_lock:
            yield

    @classmethod
    def new(cls, session, **kwargs):
        super(Job, cls).new(session, **kwargs)

        cparams = {}
        for cparam in cls.COMMON_PARAMS:
            if cparam in kwargs:
                cparams[cparam] = kwargs[cparam]
        job = cls(**cparams)
        for param in cls.PARAMS:
            setattr(job, param, kwargs.get(param, None))
        ts = time.time()
        job.create_ts = ts
        job.update_ts = ts
        job._dirty = True
        try:
            job.perform_locks()
        except LockFailedError as e:
            logger.error('Job {0}: failed to perform required locks: {1}'.format(job.id, e))
            raise
        except LockError:
            logger.error('Job {0}: failed to perform required locks'.format(job.id))
            raise

        try:
            job.mark_groups(session)
        except RuntimeError:
            logger.error('Job {0}: failed to mark required groups'.format(job.id))
            job.release_locks()
            raise

        return job

    @classmethod
    def from_data(cls, data):
        job = cls()
        job.load(data)
        return job

    def load(self, data):
        self.id = data['id'].encode('utf-8')
        self.status = data['status']
        self.create_ts = data.get('create_ts') or data['start_ts']
        self.start_ts = data['start_ts']
        self.finish_ts = data['finish_ts']
        self.update_ts = data.get('update_ts') or self.finish_ts or self.start_ts
        self.type = data['type']
        self.error_msg = data.get('error_msg', [])

        with self.__tasklist_lock:
            self.tasks = [TaskFactory.make_task(task_data, self) for task_data in data['tasks']]

        for param in self.PARAMS:
            val = data.get(param, None)
            if isinstance(val, unicode):
                val = val.encode('utf-8')
            setattr(self, param, val)

        return self

    @property
    def create_ts(self):
        return self._create_ts

    @create_ts.setter
    def create_ts(self, value):
        if value is None:
            self._create_ts = None
        else:
            self._create_ts = int(value)

    @property
    def start_ts(self):
        return self._start_ts

    @start_ts.setter
    def start_ts(self, value):
        if value is None:
            self._start_ts = None
        else:
            self._start_ts = int(value)

    @property
    def update_ts(self):
        return self._update_ts

    @update_ts.setter
    def update_ts(self, value):
        if value is None:
            self._update_ts = None
        else:
            self._update_ts = int(value)

    @property
    def finish_ts(self):
        return self._finish_ts

    @finish_ts.setter
    def finish_ts(self, value):
        if value is None:
            self._finish_ts = None
        else:
            self._finish_ts = int(value)

    def _dump(self):
        data = {'id': self.id,
                'status': self.status,
                'create_ts': self.create_ts,
                'start_ts': self.start_ts,
                'update_ts': self.update_ts,
                'finish_ts': self.finish_ts,
                'type': self.type,
                'error_msg': self.error_msg}

        data.update(dict([(k, getattr(self, k)) for k in self.PARAMS]))
        return data

    def dump(self):
        data = self._dump()
        data['tasks'] = [task.dump() for task in self.tasks]
        return data

    def human_dump(self):
        data = self._dump()
        data['tasks'] = [task.human_dump() for task in self.tasks]
        return data

    def node_backend(self, host, port, backend_id):
        return '{0}:{1}/{2}'.format(host, port, backend_id)

    def create_tasks(self):
        raise RuntimeError('Job creation should be implemented '
            'in derived class')

    def perform_locks(self):
        try:
            sync_manager.persistent_locks_acquire(self._locks, self.id)
        except LockAlreadyAcquiredError as e:
            if e.holder_id != self.id:
                logger.error('Job {0}: {1} is already '
                    'being processed by job {2}'.format(self.id, e.lock_id, e.holder_id))

                last_error = self.error_msg and self.error_msg[-1] or None
                if last_error and (last_error.get('code') != API_ERROR_CODE.LOCK_ALREADY_ACQUIRED or
                                   last_error.get('holder_id') != e.holder_id):
                    self.add_error(e)

                raise
            else:
                logger.warn('Job {0}: lock for group {1} has already '
                    'been acquired, skipping'.format(self.id, self.group))


    def mark_groups(self, session):
        try:
            for group_id, updated_meta in self._group_marks():
                s = session.clone()
                s.set_groups([group_id])
                packed = msgpack.packb(updated_meta)
                _, failed_group = h.write_retry(
                    s, keys.SYMMETRIC_GROUPS_KEY, packed)
                if failed_group:
                    raise RuntimeError('Failed to mark group {0}'.format(
                        group_id))
        except Exception as e:
            logger.error('Job {0}: {1}'.format(self.id, e))
            raise

    def _group_marks(self):
        """
        Optional method for subclasses to mark groups.
        Returns iterable of (group, updated_meta)
        """
        return []

    def unmark_groups(self, session):
        try:
            for group, updated_meta in self._group_unmarks():
                s = session.clone()
                s.set_groups([group])
                packed = msgpack.packb(updated_meta)
                _, failed_group = h.write_retry(
                    s, keys.SYMMETRIC_GROUPS_KEY, packed)
                if failed_group:
                    raise RuntimeError('Failed to unmark group {1}'.format(
                        self.id, group))
        except Exception as e:
            logger.error('Job {0}: {1}'.format(self.id, e))
            raise

    def _group_unmarks(self):
        """
        Optional method for subclasses to mark groups.
        Returns iterable of (group, updated_meta)
        """
        return []

    def release_locks(self):
        try:
            sync_manager.persistent_locks_release(self._locks, self.id)
        except InconsistentLockError as e:
            logger.error('Job {0}: lock is already acquired by another '
                'job {1}'.format(self.id, e.holder_id))
            pass

    @property
    def _locks(self):
        raise NotImplemented('Locks are listed in derived classes')

    def complete(self, session):
        self.unmark_groups(session)
        ts = time.time()
        if not self.start_ts:
            self.start_ts = ts
        self.update_ts = ts
        self.finish_ts = ts
        self.release_locks()

    def add_error(self, e):
        error_msg = e.dump()
        error_msg['ts'] = time.time()
        self.error_msg.append(error_msg)

    def add_error_msg(self, msg):
        self.error_msg.append({'ts': time.time(), 'msg': msg})


    def on_start(self):
        """
        Performs checkings before starting the job.
        Should through JobBrokenError if job should not be started.
        """
        pass

    @staticmethod
    def list(collection, **kwargs):
        params = {}
        for k, v in kwargs.iteritems():
            if v is None:
                continue
            params.update(condition(k, v))

        return collection.find(params).sort('create_ts', pymongo.DESCENDING)


def condition(field_name, field_val):
    if isinstance(field_val, (list, tuple)):
        return {field_name: {'$in': list(field_val)}}
    else:
        return {field_name: field_val}
