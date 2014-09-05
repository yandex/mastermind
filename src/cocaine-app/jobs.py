from contextlib import contextmanager
import json
import logging
import os.path
import random
import threading
import time
import traceback
import uuid

from config import config
import indexes
from infrastructure import infrastructure, port_to_dir
import keys
import storage
import timed_queue
from sync import sync_manager
from sync.error import (
    LockError,
    LockFailedError,
    LockAlreadyAcquiredError,
    InconsistentLockError,
)


logger = logging.getLogger('mm.jobs')


class Job(object):

    STATUS_NOT_APPROVED = 'not_approved'
    STATUS_NEW = 'new'
    STATUS_EXECUTING = 'executing'
    STATUS_PENDING = 'pending'
    STATUS_BROKEN = 'broken'
    STATUS_COMPLETED = 'completed'
    STATUS_CANCELLED = 'cancelled'

    GROUP_LOCK_PREFIX = 'group/'

    COMMON_PARAMS = ('need_approving',)

    def __init__(self, need_approving=False):
        self.id = uuid.uuid4().hex
        self.status = (self.STATUS_NOT_APPROVED
                       if need_approving else
                       self.STATUS_NEW)
        self.start_ts = None
        self.finish_ts = None
        self.type = None
        self.tasks = []
        self.__tasklist_lock = threading.Lock()
        self.error_msg = []

    @contextmanager
    def tasks_lock(self):
        with self.__tasklist_lock:
            yield

    @classmethod
    def new(cls, **kwargs):
        cparams = {}
        for cparam in cls.COMMON_PARAMS:
            if cparam in kwargs:
                cparams[cparam] = kwargs[cparam]
        job = cls(**cparams)
        for param in cls.PARAMS:
            setattr(job, param, kwargs.get(param, None))
        return job

    @classmethod
    def from_data(cls, data):
        job = cls()
        job.load(data)
        return job

    def load(self, data):
        self.id = data['id'].encode('utf-8')
        self.status = data['status']
        self.start_ts = data['start_ts']
        self.finish_ts = data['finish_ts']
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

    def _dump(self):
        data = {'id': self.id,
                'status': self.status,
                'start_ts': self.start_ts,
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

    def create_tasks(self):
        raise RuntimeError('Job creation should be implemented '
            'in derived class')

    def perform_locks(self):
        pass

    def release_locks(self):
        pass

    def complete(self):
        self.finish_ts = time.time()
        self.release_locks()

    def add_error(self, e):
        error_msg = e.dump()
        error_msg['ts'] = time.time()
        self.error_msg.append(error_msg)

    def add_error_msg(self, msg):
        self.error_msg.append({'ts': time.time(), 'msg': msg})


class MoveJob(Job):

    # used to write group id
    GROUP_FILE_PATH = config.get('restore', {}).get('group_file', None)

    # used to mark source node that content has been moved away from it
    GROUP_FILE_MARKER_PATH = config.get('restore', {}).get('group_file_marker', None)
    GROUP_FILE_DIR_MOVE_DST = config.get('restore', {}).get('group_file_dir_move_dst', None)

    PARAMS = ('group', 'uncoupled_group',
              'src_host', 'src_port', 'src_backend_id', 'src_family', 'src_base_path',
              'dst_host', 'dst_port', 'dst_backend_id', 'dst_family', 'dst_base_path')

    def __init__(self, **kwargs):
        super(MoveJob, self).__init__(**kwargs)
        self.type = JobFactory.TYPE_MOVE_JOB

    @property
    def src_node_backend(self):
        return '{0}:{1}/{2}'.format(self.src_host, self.src_port, self.src_backend_id).encode('utf-8')

    @property
    def dst_node_backend(self):
        return '{0}:{1}/{2}'.format(self.dst_host, self.dst_port, self.dst_backend_id).encode('utf-8')

    def human_dump(self):
        data = super(MoveJob, self).human_dump()
        data['src_hostname'] = infrastructure.get_hostname_by_addr(data['src_host'])
        data['dst_hostname'] = infrastructure.get_hostname_by_addr(data['dst_host'])
        return data

    def marker_format(self, marker):
        return marker.format(
            group_id=str(self.group),
            src_host=self.src_host,
            src_hostname=infrastructure.get_hostname_by_addr(self.src_host),
            src_backend_id=self.src_backend_id,
            src_port=str(self.src_port),
            src_base_path=self.src_base_path,
            dst_host=self.dst_host,
            dst_hostname=infrastructure.get_hostname_by_addr(self.dst_host),
            dst_port=str(self.dst_port),
            dst_base_path=self.dst_base_path,
            dst_backend_id=self.dst_backend_id)

    def create_tasks(self):

        shutdown_cmd = infrastructure.disable_node_backend_cmd([
            self.dst_host, self.dst_port, self.dst_family, self.dst_backend_id])
        task = NodeStopTask.new(self,
                                group=self.uncoupled_group,
                                uncoupled=True,
                                host=self.dst_host,
                                cmd=shutdown_cmd,
                                params={'node_backend': self.dst_node_backend,
                                        'group': str(self.group)})
        self.tasks.append(task)

        shutdown_cmd = infrastructure.disable_node_backend_cmd([
            self.src_host, self.src_port, self.src_family, self.src_backend_id])

        group_file_marker = (os.path.join(self.src_base_path,
                                          self.GROUP_FILE_MARKER_PATH)
                             if self.GROUP_FILE_MARKER_PATH else
                             '')
        group_file = (os.path.join(self.src_base_path,
                                   self.GROUP_FILE_PATH)
                      if self.GROUP_FILE_PATH else
                      '')

        params = {'node_backend': self.src_node_backend,
                  'group': str(self.group),
                  'group_file_marker': self.marker_format(group_file_marker),
                  'remove_group_file': group_file}

        if self.GROUP_FILE_DIR_MOVE_DST and group_file:
            params['move_src'] = os.path.join(os.path.dirname(group_file))
            params['move_dst'] = os.path.join(
                self.src_base_path, self.GROUP_FILE_DIR_MOVE_DST)

        task = NodeStopTask.new(self,
                                group=self.group,
                                host=self.src_host,
                                cmd=shutdown_cmd,
                                params=params)
        self.tasks.append(task)

        move_cmd = infrastructure.move_group_cmd(
            src_host=self.src_host,
            src_path=self.src_base_path,
            dst_path=self.dst_base_path)
        group_file = (os.path.join(self.dst_base_path, self.GROUP_FILE_PATH)
                      if self.GROUP_FILE_PATH else
                      '')

        task = MinionCmdTask.new(self,
                                 host=self.dst_host,
                                 cmd=move_cmd,
                                 params={'group': str(self.group),
                                         'group_file': group_file})
        self.tasks.append(task)

        reconfigure_cmd = infrastructure.reconfigure_node_cmd(
            [self.src_host, self.src_port, self.src_family])

        task = MinionCmdTask.new(self,
                                 host=self.src_host,
                                 cmd=reconfigure_cmd,
                                 params={'node_backend': self.src_node_backend})

        self.tasks.append(task)

        reconfigure_cmd = infrastructure.reconfigure_node_cmd(
            [self.dst_host, self.dst_port, self.dst_family])

        task = MinionCmdTask.new(self,
                                 host=self.dst_host,
                                 cmd=reconfigure_cmd,
                                 params={'node_backend': self.dst_node_backend})

        self.tasks.append(task)

        start_cmd = infrastructure.enable_node_backend_cmd([
            self.dst_host, self.dst_port, self.dst_family, self.dst_backend_id])
        task = MinionCmdTask.new(self,
                                 host=self.dst_host,
                                 cmd=start_cmd,
                                 params={'node_backend': self.dst_node_backend})
        self.tasks.append(task)

        task = HistoryRemoveNodeTask.new(self,
                                         group=self.group,
                                         host=self.src_host,
                                         port=self.src_port,
                                         backend_id=self.src_backend_id)
        self.tasks.append(task)

    def perform_locks(self):
        locks = ['{0}{1}'.format(self.GROUP_LOCK_PREFIX, group)
                 for group in (self.group, self.uncoupled_group)]
        try:
            sync_manager.persistent_locks_acquire(locks, self.id)
        except LockAlreadyAcquiredError as e:
            logger.error('Job {0}: some of the groups is already '
                'being processed by job {1}'.format(self.id, e.holder_id))
            self.add_error(e)
            raise

    def release_locks(self):
        locks = ['{0}{1}'.format(self.GROUP_LOCK_PREFIX, group)
                 for group in (self.group, self.uncoupled_group)]
        try:
            sync_manager.persistent_locks_release(locks, self.id)
        except InconsistentLockError as e:
            logger.error('Job {0}: lock for some job groups is already '
                'acquired by another job {1}'.format(self.id, e.holder_id))
            pass


class RecoverDcJob(Job):

    PARAMS = ('group', 'host', 'port', 'family', 'backend_id')

    def __init__(self, **kwargs):
        super(RecoverDcJob, self).__init__(**kwargs)
        self.type = JobFactory.TYPE_RECOVER_DC_JOB

    @property
    def node_backend(self):
        return '{0}:{1}/{2}'.format(self.host, self.port, self.backend_id).encode('utf-8')

    def human_dump(self):
        data = super(RecoverDcJob, self).human_dump()
        data['hostname'] = infrastructure.get_hostname_by_addr(data['host'])
        return data

    def create_tasks(self):
        recover_cmd = infrastructure.recover_group_cmd([self.group])
        task = RecoverGroupDcTask.new(self,
                                group=self.group,
                                host=self.host,
                                cmd=recover_cmd,
                                params={'node_backend': self.node_backend,
                                        'group': str(self.group)})
        self.tasks.append(task)

    def perform_locks(self):
        try:
            sync_manager.persistent_locks_acquire(
                ['{0}{1}'.format(self.GROUP_LOCK_PREFIX, self.group)], self.id)
        except LockAlreadyAcquiredError as e:
            logger.error('Job {0}: group {1} is already '
                'being processed by job {2}'.format(self.id, self.group, e.holder_id))
            self.add_error(e)
            raise

    def release_locks(self):
        try:
            sync_manager.persistent_locks_release(
                ['{0}{1}'.format(self.GROUP_LOCK_PREFIX, self.group)], self.id)
        except InconsistentLockError as e:
            logger.error('Job {0}: lock for group {1} is already acquired by another '
                'job {2}'.format(self.id, self.group, e.holder_id))
            pass


class JobBrokenError(Exception):
    pass


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
        self.error_msg = []
        self.parent_job = job

    @classmethod
    def new(cls, job, **kwargs):
        task = cls(job)
        for param in cls.PARAMS:
            setattr(task, param, kwargs.get(param, None))
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

        for param in self.PARAMS:
            val = data.get(param, None)
            if isinstance(val, unicode):
                val = val.encode('utf-8')
            setattr(self, param, val)

    def dump(self):
        res = {'status': self.status,
               'id': self.id,
               'type': self.type,
               'start_ts': self.start_ts,
               'finish_ts': self.finish_ts,
               'error_msg': self.error_msg}
        res.update(dict([(k, getattr(self, k)) for k in self.PARAMS]))
        return res

    def human_dump(self):
        return self.dump()

    def __str__(self):
        raise RuntimeError('__str__ method should be implemented in '
            'derived class')


class MinionCmdTask(Task):

    PARAMS = ('group', 'host', 'cmd', 'params', 'minion_cmd_id')
    TASK_TIMEOUT = 600

    def __init__(self, job):
        super(MinionCmdTask, self).__init__(job)
        self.minion_cmd = None
        self.minion_cmd_id = None
        self.type = TaskFactory.TYPE_MINION_CMD

    def update_status(self, minions):
        try:
            self.minion_cmd = minions.get_command([self.minion_cmd_id])
            logger.debug('Job {0}, task {1}, minion command status was updated: {2}'.format(
                self.parent_job.id, self.id, self.minion_cmd))
        except ValueError:
            logger.warn('Job {0}, task {1}, minion command status {2} is not fetched '
                'from minions'.format(self.parent_job.id, self.id, self.minion_cmd_id))
            pass

    def execute(self, minions):
        minion_response = minions.execute_cmd([self.host,
            self.cmd, self.params])
        self.minion_cmd = minion_response.values()[0]
        logger.info('Job {0}, task {1}, minions task execution: {2}'.format(
            self.parent_job.id, self.id, self.minion_cmd))
        self.minion_cmd_id = self.minion_cmd['uid']

    def human_dump(self):
        data = super(MinionCmdTask, self).human_dump()
        data['hostname'] = infrastructure.get_hostname_by_addr(data['host'])
        return data

    @property
    def finished(self):
        return ((self.minion_cmd is None and
                 time.time() - self.start_ts > self.TASK_TIMEOUT) or
                (self.minion_cmd and self.minion_cmd['progress'] == 1.0))

    @property
    def failed(self):
        return self.minion_cmd is None or self.minion_cmd['exit_code'] != 0

    def __str__(self):
        return 'MinionCmdTask[id: {0}]<{1}>'.format(self.id, self.cmd)


class NodeStopTask(MinionCmdTask):

    PARAMS = MinionCmdTask.PARAMS + ('uncoupled',)

    def __init__(self, job):
        super(NodeStopTask, self).__init__(job)
        self.type = TaskFactory.TYPE_NODE_STOP_TASK

    def execute(self, minions):

        if self.group:
            # checking if task still applicable
            logger.info('Job {0}, task {1}: checking group {2} and host {3} '
                'consistency'.format(self.parent_job.id, self.id, self.group, self.host))

            if not self.group in storage.groups:
                raise JobBrokenError('Group {0} is not found'.format(self.group))

            group = storage.groups[self.group]
            if len(group.node_backends) != 1 or group.node_backends[0].node.host.addr != self.host:
                raise JobBrokenError('Task {0}: group {1} has more than '
                    'one node backend: {2}, expected host {3}'.format(self, self.group,
                        [str(nb) for nb in group.node_backends], self.host))

            if group.node_backends[0].status != storage.Status.OK:
                raise JobBrokenError('Task {0}: node of group {1} has '
                    'status {2}, should be {3}'.format(self, self.group,
                        self.node_backends[0].status, storage.Status.OK))

            if self.uncoupled:
                if group.couple:
                    raise JobBrokenError('Task {0}: group {1} happens to be '
                        'already coupled'.format(self, self.group))
                if group.node_backends[0].stat.files + group.node_backends[0].stat.files_removed > 0:
                    raise JobBrokenError('Task {0}: group {1} has non-zero '
                        'number of keys (including removed)')

        super(NodeStopTask, self).execute(minions)


class RecoverGroupDcTask(MinionCmdTask):

    PARAMS = MinionCmdTask.PARAMS + ('couple',)

    def __init__(self, job):
        super(RecoverGroupDcTask, self).__init__(job)
        self.type = TaskFactory.TYPE_RECOVER_DC_GROUP_TASK

    @classmethod
    def new(cls, job, **kwargs):
        task = super(RecoverGroupDcTask, cls).new(job, **kwargs)
        task.check(task.group)
        task.couple = storage.groups[task.group].couple.as_tuple()
        return task

    def check(self, group_id):
        if not group_id in storage.groups:
            raise JobBrokenError('Group {0} is not found'.format(group_id))

        group = storage.groups[group_id]

        if group.status != storage.Status.COUPLED:
            raise JobBrokenError('Task {0}: group {1} has status {2}, '
                'should be {3}'.format(self, self.group,
                                       group.status, storage.Status.COUPLED))

    def execute(self, minions):

        # checking if task still applicable
        logger.info('Job {0}, task {1}: checking group {2} and couple {3} '
            'consistency'.format(self.parent_job.id, self.id, self.group, self.couple))

        self.check(self.group)
        group = storage.groups[self.group]

        if set(self.couple) != set(group.couple.as_tuple()):
            raise JobBrokenError('Task {0}: group {1} has changed couple to {2}, '
                'expected {3}'.format(self, self.group,
                                       group.couple, self.couple))

        super(RecoverGroupDcTask, self).execute(minions)


class HistoryRemoveNodeTask(Task):

    PARAMS = ('group', 'host', 'port', 'backend_id')
    TASK_TIMEOUT = 600

    def __init__(self, job):
        super(HistoryRemoveNodeTask, self).__init__(job)
        self.type = TaskFactory.TYPE_HISTORY_REMOVE_NODE

    def update_status(self):
        # infrastructure state is updated by itself via task queue
        pass

    def execute(self):
        self.id = uuid.uuid4().hex
        group = storage.groups[self.group]
        try:
            infrastructure.detach_node(group, self.host, self.port, self.backend_id,
                infrastructure.HISTORY_RECORD_JOB)
        except ValueError as e:
            # TODO: Think about changing ValueError to some dedicated exception
            # to differentiate between event when there is no such node in group
            # and an actual ValueError being raised
            logger.error('Job {0}, task {1}: failed to execute {2}: {3}'.format(
                self.parent_job.id, self.id, str(self), e))
            pass

        nb_str = '{0}:{1}/{2}'.format(self.host, self.port, self.backend_id).encode('utf-8')
        node_backend = nb_str in storage.node_backends and storage.node_backends[nb_str] or None
        if node_backend and node_backend in group.node_backends:
            logger.info('Job {0}, task {1}: removing node backend {2} '
                'from group {3} node backends'.format(
                    self.parent_job.id, self.id, node_backend, group))
            group.remove_node_backend(node_backend)
            group.update_status_recursive()
            logger.info('Job {0}, task {1}: removed node backend {2} '
                'from group {3} node backends'.format(
                    self.parent_job.id, self.id, node_backend, group))

    def human_dump(self):
        data = super(HistoryRemoveNodeTask, self).human_dump()
        data['hostname'] = infrastructure.get_hostname_by_addr(data['host'])
        return data

    @property
    def finished(self):
        return (not self.__node_in_group() or
                time.time() - self.start_ts > self.TASK_TIMEOUT)

    @property
    def failed(self):
        return (time.time() - self.start_ts > self.TASK_TIMEOUT and
                self.__node_in_group())

    def __node_in_group(self):
        group = storage.groups[self.group]
        node_backend = '{0}:{1}/{2}'.format(self.host, self.port, self.backend_id).encode('utf-8')
        logger.debug('Job {0}, task {1}: checking node backend {2} '
            'with group {3} node backends: {4}'.format(
                self.parent_job.id, self.id, node_backend, group, group.node_backends))
        nb_in_group = group.has_node_backend(node_backend)

        nb_in_history = infrastructure.node_backend_in_last_history_state(
            group.group_id, self.host, self.port, self.backend_id)
        logger.debug('Job {0}, task {1}: checking node backend {2} '
            'in group {3} history set: {4}'.format(
                self.parent_job.id, self.id, node_backend, group.group_id, nb_in_history))

        if nb_in_group:
            logger.info('Job {0}, task {1}: node backend {2} is still '
                'in group {3}'.format(self.parent_job.id, self.id, node_backend, group))
        if nb_in_history:
            logger.info('Job {0}, task {1}: node backend {2} is still '
                'in group\'s {3} history'.format(
                    self.parent_job.id, self.id, node_backend, group))

        return nb_in_group or nb_in_history

    def __str__(self):
        return 'HistoryRemoveNodeTask[id: {0}]<remove {1}:{2}/{3} from group {4}>'.format(
            self.id, self.host, self.port, self.backend_id, self.group)


class JobFactory(object):

    TYPE_MOVE_JOB = 'move_job'
    TYPE_RECOVER_DC_JOB = 'recover_dc_job'

    @classmethod
    def make_job(cls, data):
        job_type = data.get('type', None)
        if job_type == cls.TYPE_MOVE_JOB:
            return MoveJob.from_data(data)
        elif job_type == cls.TYPE_RECOVER_DC_JOB:
            return RecoverDcJob.from_data(data)
        raise ValueError('Unknown job type {0}'.format(job_type))


class TaskFactory(object):

    TYPE_MINION_CMD = 'minion_cmd'
    TYPE_NODE_STOP_TASK = 'node_stop_task'
    TYPE_RECOVER_DC_GROUP_TASK = 'recover_dc_group_task'
    TYPE_HISTORY_REMOVE_NODE = 'history_remove_node'

    @classmethod
    def make_task(cls, data, job):
        task_type = data.get('type', None)
        if task_type == cls.TYPE_NODE_STOP_TASK:
            return NodeStopTask.from_data(data, job)
        if task_type == cls.TYPE_MINION_CMD:
            return MinionCmdTask.from_data(data, job)
        if task_type == cls.TYPE_HISTORY_REMOVE_NODE:
            return HistoryRemoveNodeTask.from_data(data, job)
        if task_type == cls.TYPE_RECOVER_DC_GROUP_TASK:
            return RecoverGroupDcTask.from_data(data, job)
        raise ValueError('Unknown task type {0}'.format(task_type))


class JobProcessor(object):

    JOBS_EXECUTE = 'jobs_execute'
    JOBS_UPDATE = 'jobs_update'
    JOBS_LOCK = 'jobs'

    MAX_EXECUTING_JOBS = config.get('jobs', {}).get('max_executing_jobs', 3)

    def __init__(self, meta_session, minions):
        logger.info('Starting JobProcessor')
        self.meta_session = meta_session
        self.minions = minions

        self.jobs = {}
        self.jobs_index = indexes.SecondaryIndex(keys.MM_JOBS_IDX,
            keys.MM_JOBS_KEY_TPL, self.meta_session)

        self.__tq = timed_queue.TimedQueue()
        self.__tq.start()

        self.__tq.add_task_in(self.JOBS_UPDATE,
            4, self._update_jobs)
        self.__tq.add_task_in(self.JOBS_EXECUTE,
            5, self._execute_jobs)

    def _load_job(self, job_rawdata):
        job_data = json.loads(job_rawdata)
        if not job_data['id'] in self.jobs:
            job = self.jobs[job_data['id']] = JobFactory.make_job(job_data)
            logger.info('Job {0}: loaded from job index'.format(job.id))
        else:
            # TODO: Think about other ways of updating job
            job = self.jobs[job_data['id']].load(job_data)
        return job

    def _update_jobs(self):
        try:
            self._do_update_jobs()
        except Exception as e:
            logger.error('Failed to update jobs: {0}\n{1}'.format(
                e, traceback.format_exc()))
        finally:
            self.__tq.add_task_in(self.JOBS_UPDATE,
                config.get('jobs', {}).get('update_period', 50),
                self._update_jobs)


    def _do_update_jobs(self):
        [self._load_job(job) for job in self.jobs_index]

    def _execute_jobs(self):

        logger.info('Jobs execution started')
        try:
            logger.debug('Lock acquiring')
            with sync_manager.lock(self.JOBS_LOCK):
                logger.debug('Lock acquired')
                # TODO: check! # fetch jobs - read_latest!!!
                self._do_update_jobs()

                (new_jobs, executing_jobs) = ([], [])
                for job in self.jobs.itervalues():
                    if job.status == Job.STATUS_EXECUTING:
                        executing_jobs.append(job)
                    elif job.status == Job.STATUS_NEW:
                        new_jobs.append(job)

                # check number of running jobs
                free_slots = max(0, self.MAX_EXECUTING_JOBS - len(executing_jobs))
                ready_jobs = executing_jobs + new_jobs[:free_slots]

                logger.debug('{0} jobs to process'.format(len(ready_jobs)))

                for job in ready_jobs:
                    try:
                        with job.tasks_lock():
                            self.__process_job(job)
                    except Exception as e:
                        logger.error('Failed to process job {0}: '
                            '{1}\n{2}'.format(job.id, e, traceback.format_exc()))
                        continue
                    self.jobs_index[job.id] = self.__dump_job(job)

        except Exception as e:
            logger.error('Failed to process existing jobs: {0}\n{1}'.format(
                e, traceback.format_exc()))
        finally:
            logger.info('Jobs execution finished')
            self.__tq.add_task_in(self.JOBS_EXECUTE,
                config.get('jobs', {}).get('execute_period', 60),
                self._execute_jobs)

    def __process_job(self, job):

        logger.debug('Job {0}, processing started: {1}'.format(job.id, job.dump()))

        if all([task.status == Task.STATUS_QUEUED for task in job.tasks]):
            logger.info('Job {0}: setting job start time'.format(job.id))
            job.start_ts = time.time()

        for task in job.tasks:
            if task.status == Task.STATUS_EXECUTING:

                logger.info('Job {0}, task {1} status update'.format(
                    job.id, task.id))
                try:
                    self.__update_task_status(task)
                except Exception as e:
                    logger.error('Job {0}, task {1}: failed to update status: '
                        '{2}\n{3}'.format(job.id, task, e, traceback.format_exc()))
                    task.error_msg.append(str(e))
                    task.status = Task.STATUS_FAILED
                    job.status = Job.STATUS_PENDING
                    job.finish_ts = time.time()
                    break

                if not task.finished:
                    logger.debug('Job {0}, task {1} is not finished'.format(
                        job.id, task.id))
                    break

                task.finish_ts = time.time()

                task.status = (Task.STATUS_FAILED
                               if task.failed else
                               Task.STATUS_COMPLETED)

                logger.debug('Job {0}, task {1} is finished, status {2}'.format(
                    job.id, task.id, task.status))

                if task.status == Task.STATUS_FAILED:
                    job.status = Job.STATUS_PENDING
                    job.finish_ts = time.time()
                    break
                else:
                    continue
                pass
            elif task.status == Task.STATUS_QUEUED:
                try:
                    logger.info('Job {0}, executing new task {1}'.format(job.id, task))
                    self.__execute_task(task)
                    logger.info('Job {0}, task {1} execution was successfully requested'.format(
                        job.id, task.id))
                    task.status = Task.STATUS_EXECUTING
                    job.status = Job.STATUS_EXECUTING
                except JobBrokenError as e:
                    logger.error('Job {0}, task {1}: cannot execute task, '
                        'not applicable for current storage state: {2}'.format(
                            job.id, task, e))
                    task.status = Task.STATUS_FAILED
                    job.status = Job.STATUS_BROKEN
                    job.add_error_msg(str(e))
                    job.finish_ts = time.time()
                except Exception as e:
                    logger.error('Job {0}, task {1}: failed to execute: {2}\n{3}'.format(
                        job.id, task, e, traceback.format_exc()))
                    task.status = Task.STATUS_FAILED
                    job.status = Job.STATUS_PENDING
                    job.finish_ts = time.time()
                break

        if all([task.status in (Task.STATUS_COMPLETED, Task.STATUS_SKIPPED)
                for task in job.tasks]):
            logger.info('Job {0}, tasks processing is finished'.format(job.id))
            job.status = Job.STATUS_COMPLETED
            job.complete()

    def __update_task_status(self, task):
        if isinstance(task, MinionCmdTask):
            task.update_status(self.minions)
        elif isinstance(task, HistoryRemoveNodeTask):
            task.update_status()
        else:
            raise ValueError('Status of task with type "{0}" cannot be '
                'updated'.format(type(task)))

    def __execute_task(self, task):
        if not task.start_ts:
            task.start_ts = time.time()
        if isinstance(task, MinionCmdTask):
            task.execute(self.minions)
        elif isinstance(task, HistoryRemoveNodeTask):
            task.execute()
        else:
            raise ValueError('Task with type "{0}" cannot be '
                'executed'.format(type(task)))

    def __dump_job(self, job):
        return json.dumps(job.dump())

    def __load_job(self, data):
        return json.loads(data)

    def create_job(self, request):
        try:
            try:
                job_type = request[0]
            except IndexError:
                raise ValueError('Job type is required')

            if job_type not in (JobFactory.TYPE_MOVE_JOB, JobFactory.TYPE_RECOVER_DC_JOB):
                raise ValueError('Invalid job type: {0}'.format(job_type))

            try:
                params = request[1]
            except IndexError:
                params = {}

            # Forcing manual approval of newly created job
            params['need_approving'] = True

            if job_type == JobFactory.TYPE_MOVE_JOB:
                JobType = MoveJob
            elif job_type == JobFactory.TYPE_RECOVER_DC_JOB:
                JobType = RecoverDcJob
            job = JobType.new(**params)
            job.create_tasks()

            with sync_manager.lock(self.JOBS_LOCK):
                logger.info('Job {0} created: {1}'.format(job.id, job.dump()))
                self.jobs_index[job.id] = self.__dump_job(job)

            self.jobs[job.id] = job
        except Exception as e:
            logger.error('Failed to create job: {0}\n{1}'.format(e,
                traceback.format_exc()))
            raise

        return job.dump()

    def get_job_list(self, request):
        try:
            options = request[0]
        except TypeError:
            options = {}

        def job_filter(j):
            if options.get('job_type', None) and j.type != options['job_type']:
                return False
            return True

        res = [job.human_dump() for job in sorted(self.jobs.itervalues(),
                   key=lambda j: (j.finish_ts, j.start_ts))
               if job_filter(job)]
        return res

    # def clear_jobs(self, request):
    #     try:
    #         for raw_job in self.jobs_index:
    #             job = self.__load_job(raw_job)
    #             del self.jobs_index[job['id'].encode('utf-8')]
    #     except Exception as e:
    #         logger.error('Failed to clear all jobs: {0}\n{1}'.format(e,
    #             traceback.format_exc()))
    #         raise

    def cancel_job(self, request):
        job_id = None
        try:
            try:
                job_id = request[0]
            except IndexError as e:
                raise ValueError('Job id is required')

            job = self.jobs[job_id]

            logger.debug('Lock acquiring')
            with sync_manager.lock(self.JOBS_LOCK), job.tasks_lock():
                logger.debug('Lock acquired')

                if job.status not in (Job.STATUS_PENDING,
                    Job.STATUS_NOT_APPROVED, Job.STATUS_BROKEN):
                    raise ValueError('Job {0}: status is "{1}", should have been '
                        '"{2}|{3}"'.format(job.id, job.status,
                            Job.STATUS_PENDING, Job.STATUS_NOT_APPROVED))

                job.status = Job.STATUS_CANCELLED
                job.complete()
                self.jobs_index[job.id] = self.__dump_job(job)

                logger.info('Job {0}: status set to {1}'.format(job.id, job.status))

        except Exception as e:
            logger.error('Failed to cancel job {0}: {1}\n{2}'.format(
                job_id, e, traceback.format_exc()))
            raise

        return job.dump()

    def approve_job(self, request):
        job_id = None
        try:
            try:
                job_id = request[0]
            except IndexError as e:
                raise ValueError('Job id is required')

            job = self.jobs[job_id]

            logger.debug('Lock acquiring')
            with sync_manager.lock(self.JOBS_LOCK), job.tasks_lock():
                logger.debug('Lock acquired')

                if job.status != Job.STATUS_NOT_APPROVED:
                    raise ValueError('Job {0}: status is "{1}", should have been '
                        '"{2}"'.format(job.id, job.status, Job.STATUS_NOT_APPROVED))

                try:
                    job.perform_locks()
                except LockError:
                    logger.error('Job {0}: failed to lock target groups'.format(job.id))
                else:
                    job.status = Job.STATUS_NEW
                    logger.info('Job {0}: status set to {1}'.format(job.id, job.status))

                self.jobs_index[job.id] = self.__dump_job(job)


        except Exception as e:
            logger.error('Failed to approve job {0}: {1}\n{2}'.format(
                job_id, e, traceback.format_exc()))
            raise

        return job.dump()

    def retry_failed_job_task(self, request):
        job_id = None
        try:
            try:
                job_id, task_id = request[:2]
            except ValueError as e:
                raise ValueError('Job id and task id are required')

            job = self.__change_failed_task_status(job_id, task_id, Task.STATUS_QUEUED)

        except Exception as e:
            logger.error('Failed to retry job task, job {0}, task {1}: '
                '{2}\n{3}'.format(job_id, task_id, e, traceback.format_exc()))
            raise

        return job.dump()

    def skip_failed_job_task(self, request):
        job_id = None
        try:
            try:
                job_id, task_id = request[:2]
            except ValueError as e:
                raise ValueError('Job id and task id are required')

            job = self.__change_failed_task_status(job_id, task_id, Task.STATUS_SKIPPED)

        except Exception as e:
            logger.error('Failed to skip job task, job {0}, task {1}: '
                '{2}\n{3}'.format(job_id, task_id, e, traceback.format_exc()))
            raise

        return job.dump()

    def __change_failed_task_status(self, job_id, task_id, status):
        if not job_id in self.jobs:
            raise ValueError('Job {0}: job is not found'.format(job_id))
        job = self.jobs[job_id]

        if job.status not in (Job.STATUS_PENDING, Job.STATUS_BROKEN):
            raise ValueError('Job {0}: status is "{1}", should have been '
                '{2}|{3}'.format(job.id, job.status, Job.STATUS_PENDING, Job.STATUS_BROKEN))

        logger.debug('Lock acquiring')
        with sync_manager.lock(self.JOBS_LOCK), job.tasks_lock():
            logger.debug('Lock acquired')

            task = None
            for t in job.tasks:
                if t.id == task_id:
                    task = t
                    break
            else:
                raise ValueError('Job {0} does not contain task '
                    'with id {1}'.format(job_id, task_id))

            if task.status != Task.STATUS_FAILED:
                raise ValueError('Job {0}: task {1} has status {2}, should '
                    'have been failed'.format(job.id, task.id, task.status))

            task.status = status
            job.status = Job.STATUS_EXECUTING
            self.jobs_index[job.id] = self.__dump_job(job)
            logger.info('Job {0}: task {1} status was reset to {2}, '
                'job status was reset to {3}'.format(
                    job.id, task.id, task.status, job.status))

        return job
