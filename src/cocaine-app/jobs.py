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
from infrastructure import infrastructure
import keys
import storage
import timed_queue
from sync import sync_manager


logger = logging.getLogger('mm.jobs')


class Job(object):

    STATUS_NOT_APPROVED = 'not_approved'
    STATUS_NEW = 'new'
    STATUS_EXECUTING = 'executing'
    STATUS_PENDING = 'pending'
    STATUS_COMPLETED = 'completed'
    STATUS_CANCELLED = 'cancelled'

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
        # self.fixes = []
        self.__tasklist_lock = threading.Lock()

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

        # self.fixes = [FixFactory.make_fix(fix_data) for fix_data in data.get('fixes', [])]

        with self.__tasklist_lock:
            self.tasks = [TaskFactory.make_task(task_data) for task_data in data['tasks']]

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
                # 'fixes': [fix.dump() for fix in self.fixes],
                }

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


class MoveJob(Job):

    # used to write group id
    GROUP_FILE_PATH = config.get('restore', {}).get('group_file', None)

    # used to mark source node that content has been moved away from it
    GROUP_FILE_MARKER_PATH = config.get('restore', {}).get('group_file_marker', None)

    PARAMS = ('group', 'src_host', 'src_port', 'dst_host', 'dst_port')

    def __init__(self, **kwargs):
        super(MoveJob, self).__init__(**kwargs)
        self.type = JobFactory.TYPE_MOVE_JOB

    @property
    def src_node(self):
        return '{0}:{1}'.format(self.src_host, self.src_port).encode('utf-8')

    @property
    def dst_node(self):
        return '{0}:{1}'.format(self.dst_host, self.dst_port).encode('utf-8')

    def human_dump(self):
        data = super(MoveJob, self).human_dump()
        data['src_hostname'] = infrastructure.get_hostname_by_addr(data['src_host'])
        data['dst_hostname'] = infrastructure.get_hostname_by_addr(data['dst_host'])
        return data

    def create_tasks(self):
        shutdown_cmd = infrastructure.shutdown_node_cmd([self.src_host, self.src_port])

        group_file_marker = (os.path.join(infrastructure.node_path(port=self.src_port),
                                          self.GROUP_FILE_MARKER_PATH)
                             if self.GROUP_FILE_MARKER_PATH else
                             '')

        task = MinionCmdTask.new(host=self.src_host,
                                 cmd=shutdown_cmd,
                                 params={'node': self.src_node,
                                         'group': str(self.group),
                                         'group_file_marker': group_file_marker})
        self.tasks.append(task)

        shutdown_cmd = infrastructure.shutdown_node_cmd([self.dst_host, self.dst_port])
        task = MinionCmdTask.new(host=self.dst_host,
                                 cmd=shutdown_cmd,
                                 params={'node': self.dst_node,
                                         'group': str(self.group)})
        self.tasks.append(task)

        move_cmd = infrastructure.move_group_cmd(
            src_host=self.src_host,
            src_port=self.src_port,
            dst_port=self.dst_port)
        group_file = (os.path.join(infrastructure.node_path(port=self.dst_port),
                                   self.GROUP_FILE_PATH)
                      if self.GROUP_FILE_PATH else
                      '')

        task = MinionCmdTask.new(host=self.dst_host,
                                 cmd=move_cmd,
                                 params={'group': str(self.group),
                                         'group_file': group_file})
        self.tasks.append(task)

        start_cmd = infrastructure.start_node_cmd([self.dst_host, self.dst_port])
        task = MinionCmdTask.new(host=self.dst_host,
                                 cmd=start_cmd,
                                 params={'node': self.dst_node})
        self.tasks.append(task)

        task = HistoryRemoveNodeTask.new(group=self.group,
                                         host=self.src_host,
                                         port=self.src_port)
        self.tasks.append(task)


class Task(object):

    STATUS_QUEUED = 'queued'
    STATUS_EXECUTING = 'executing'
    STATUS_FAILED = 'failed'
    STATUS_SKIPPED = 'skipped'
    STATUS_COMPLETED = 'completed'

    def __init__(self):
        self.status = self.STATUS_QUEUED
        self.id = None
        self.type = None
        self.start_ts = None
        self.finish_ts = None
        self.error_msg = []

    @classmethod
    def new(cls, **kwargs):
        task = cls()
        for param in cls.PARAMS:
            setattr(task, param, kwargs.get(param, None))
        return task

    @classmethod
    def from_data(cls, data):
        task = cls()
        task.load(data)
        return task

    def load(self, data):
        self.id = data['id']
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

    PARAMS = ('host', 'cmd', 'params', 'minion_cmd_id')
    TASK_TIMEOUT = 600

    def __init__(self):
        super(MinionCmdTask, self).__init__()
        self.minion_cmd = None
        self.minion_cmd_id = None
        self.type = TaskFactory.TYPE_MINION_CMD

    def update_status(self, minions):
        try:
            self.minion_cmd = minions.get_command([self.minion_cmd_id])
        except ValueError:
            # mastermind have not yet received minion cmd status
            pass

    def execute(self, minions):
        self.id = uuid.uuid4().hex
        minion_response = minions.execute_cmd([self.host,
            self.cmd, self.params])
        self.minion_cmd = minion_response.values()[0]
        logger.info('Minions task execution: {0}'.format(self.minion_cmd))
        self.minion_cmd_id = self.minion_cmd['uid']

    def human_dump(self):
        data = super(MinionCmdTask, self).human_dump()
        data['hostname'] = infrastructure.get_hostname_by_addr(data['host'])
        return data

    @property
    def finished(self):
        assert self.minion_cmd, "Task status should be fetched from minion"
        return ((self.minion_cmd is None and
                 time.time() - self.start_ts > self.TASK_TIMEOUT) or
                self.minion_cmd['progress'] == 1.0)

    @property
    def failed(self):
        assert self.minion_cmd, "Task status should be fetched from minion"
        return self.minion_cmd is None or self.minion_cmd['exit_code'] != 0

    def __str__(self):
        return 'MinionCmdTask<{0}>'.format(self.cmd)


class HistoryRemoveNodeTask(Task):

    PARAMS = ('group', 'host', 'port')
    TASK_TIMEOUT = 600

    def __init__(self):
        super(HistoryRemoveNodeTask, self).__init__()
        self.type = TaskFactory.TYPE_HISTORY_REMOVE_NODE

    def update_status(self):
        # infrastructure state is updated by itself via task queue
        pass

    def execute(self):
        self.id = uuid.uuid4().hex
        group = storage.groups[self.group]
        try:
            infrastructure.detach_node(group, self.host, self.port)
        except ValueError as e:
            # TODO: Think about changing ValueError to some dedicated exception
            # to differentiate between event when there is no such node in group
            # and an actual ValueError being raised
            logger.error('Failed to execute {0}: {1}'.format(str(self), e))
            pass

        node_str = '{0}:{1}'.format(self.host, self.port).encode('utf-8')
        node = node_str in storage.nodes and storage.nodes[node_str] or None
        if node and node in group.nodes:
            logger.info('Removing node {0} from group {1} nodes'.format(node, group))
            group.remove_node(node)
            group.update_status_recursive()
            logger.info('Removed node {0} from group {1} nodes'.format(node, group))

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
        node = '{0}:{1}'.format(self.host, self.port).encode('utf-8')
        logger.debug('Checking node {0} with group {1} nodes: {2}'.format(
            node, group, group.nodes))
        node_in_group = group.has_node(node)

        node_in_history = infrastructure.node_in_last_history_state(
            group.group_id, self.host, self.port)
        logger.debug('Checking node {0} in group {1} history set: {2}'.format(
            node, group.group_id, node_in_history))

        if node_in_group:
            logger.info('Node {0} is still in group {1}'.format(node, group))
        if node_in_history:
            logger.info('Node {0} is still in group\'s {1} history'.format(node, group))

        return node_in_group or node_in_history

    def __str__(self):
        return 'HistoryRemoveNodeTask<remove {0}:{1} from group {2}>'.format(
            self.host, self.port, self.group)


class JobFactory(object):

    TYPE_MOVE_JOB = 'move_job'

    @classmethod
    def make_job(cls, data):
        job_type = data.get('type', None)
        if job_type == cls.TYPE_MOVE_JOB:
            return MoveJob.from_data(data)
        raise ValueError('Unknown job type {0}'.format(job_type))


class TaskFactory(object):

    TYPE_MINION_CMD = 'minion_cmd'
    TYPE_HISTORY_REMOVE_NODE = 'history_remove_node'

    @classmethod
    def make_task(cls, data):
        task_type = data.get('type', None)
        if task_type == cls.TYPE_MINION_CMD:
            return MinionCmdTask.from_data(data)
        if task_type == cls.TYPE_HISTORY_REMOVE_NODE:
            return HistoryRemoveNodeTask.from_data(data)
        raise ValueError('Unknown task type {0}'.format(task_type))


# class FixFactory(object):

#     TYPE_GROUP_NODE_SWAP = 'group_node_swap'

#     @classmethod
#     def make_fix(cls, data):
#         fix_type, params = data
#         if fix_type == cls.TYPE_GROUP_NODE_SWAP:
#             return GroupNodeSwapFix.from_data(data)
#         raise ValueError('Unknown fix type {0}'.format(fix_type))


# class Fix(object):

#     GROUP_NODE_SWAP = 'group_node_swap'

#     def __init__(self, params):
#         self.params = params

#     def apply(self):
#         raise AttributeError('Apply method should be implemented '
#             'in derived class')

#     def node_addr(self, host, port):
#         return '{0}:{1}'.format(host, port)

#     def dump(self):
#         return (self.fix_type, self.params)

#     @classmethod
#     def from_data(cls, data):
#         fix = cls(data[1])
#         return fix


# class GroupNodeSwapFix(Fix):
#     def apply(self):
#         for group_id, src_host, src_port, dst_host, dst_port in self.params:
#             group = storage.groups[group_id]

#             src_node_addr = self.node_addr(src_host, src_port)
#             if src_node_addr in storage.nodes:
#                 src_node = storage.nodes[src_node_addr]
#                 if group.has_node(src_node):
#                     group.remove_node(src_node)

#             dst_node_addr = self.node_addr(dst_host, dst_port)
#             if dst_node_addr in storage.nodes:
#                 dst_node = storage.nodes[dst_node_addr]
#                 if not group.has_node(dst_node):
#                     group.add_node(dst_node)

#             try:
#                 infrastructure.detach_node(group_id, src_host, src_port)
#             except ValueError as e:
#                 # Node seems to have already been removed
#                 pass


class JobProcessor(object):

    JOBS_EXECUTE = 'jobs_execute'
    JOBS_UPDATE = 'jobs_update'
    JOBS_LOCK = 'jobs'

    MAX_EXECUTING_JOBS = config.get('jobs', {}).get('max_executing_jobs', 2)

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
        logger.debug('job data: {0}'.format(job_data))
        if not job_data['id'] in self.jobs:
            job = self.jobs[job_data['id']] = JobFactory.make_job(job_data)
        else:
            # TODO: Think about other ways of updating job
            job = self.jobs[job_data['id']].load(job_data)
        logger.info('Job loaded from job index: {0}'.format(job))
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
            # fetch zookeeper mutex
            logger.debug('Lock acquiring')
            with sync_manager.lock(self.JOBS_LOCK):
                logger.debug('Lock acquired')
                # TODO: check! # fetch jobs - read_latest!!!
                self._do_update_jobs()

                # jobs = [self._load_job(job) for job in self.jobs_index]
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
                    logger.info('Processing job {0}'.format(job.id))
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
            self.__tq.add_task_in(self.JOBS_EXECUTE,
                config.get('jobs', {}).get('execute_period', 60),
                self._execute_jobs)

    def __process_job(self, job):

        logger.debug('Processing job {0}: {1}'.format(job.id, job.dump()))

        if all([task.status == Task.STATUS_QUEUED for task in job.tasks]):
            logger.info('Setting job {0} start time'.format(job.id))
            job.start_ts = time.time()

        for task in job.tasks:
            if task.status == Task.STATUS_EXECUTING:

                try:
                    self.__update_task_status(task)
                except Exception as e:
                    logger.error('Failed to update task status: '
                        '{0}\n{1}'.format(e, traceback.format_exc()))
                    task.error_msg.append(str(e))
                    task.status = Task.STATUS_FAILED
                    job.status = Job.STATUS_PENDING
                    job.finish_ts = time.time()
                    break

                if not task.finished:
                    logger.debug('Task {0} not finished'.format(task))
                    break

                logger.debug('Task {0} is finished'.format(task))

                task.finish_ts = time.time()

                task.status = (Task.STATUS_FAILED
                               if task.failed else
                               Task.STATUS_COMPLETED)

                logger.debug('Task {0} status: {1}'.format(task, task.status))

                if task.status == Task.STATUS_FAILED:
                    job.status = Job.STATUS_PENDING
                    job.finish_ts = time.time()
                    break
                else:
                    continue
                pass
            elif task.status == Task.STATUS_QUEUED:
                try:
                    logger.info('Executing new task "{0}"'.format(task))
                    self.__execute_task(task)
                    logger.info('Task {0} execution was requested'.format(task))
                    task.status = Task.STATUS_EXECUTING
                    job.status = Job.STATUS_EXECUTING
                except Exception as e:
                    logger.error('Failed to execute task: {0}\n{1}'.format(e,
                        traceback.format_exc()))
                    task.status = Task.STATUS_FAILED
                    job.status = Job.STATUS_PENDING
                    job.finish_ts = time.time()
                break

        if all([task.status in (Task.STATUS_COMPLETED, Task.STATUS_SKIPPED)
                for task in job.tasks]):
            job.status = Job.STATUS_COMPLETED
            job.finish_ts = time.time()

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

            if job_type not in (JobFactory.TYPE_MOVE_JOB,):
                raise ValueError('Invalid job type: {0}'.format(job_type))

            try:
                params = request[1]
            except IndexError:
                params = {}

            # Forcing manual approval of newly created job
            params['need_approving'] = True

            if job_type == JobFactory.TYPE_MOVE_JOB:
                JobType = MoveJob
            job = JobType.new(**params)
            job.create_tasks()

            with sync_manager.lock(self.JOBS_LOCK):
                logger.info('created job: {0}'.format(job.dump()))
                self.jobs_index[job.id] = self.__dump_job(job)

            self.jobs[job.id] = job
        except Exception as e:
            logger.error('Failed to create job: {0}\n{1}'.format(e,
                traceback.format_exc()))
            raise

        return job.dump()

    def get_job_list(self, request):
        return [job.human_dump() for job in sorted(self.jobs.itervalues(),
            key=lambda j: (j.finish_ts, j.start_ts))]

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

                if job.status not in (Job.STATUS_PENDING, Job.STATUS_NOT_APPROVED):
                    raise ValueError('Job {0}: status is "{1}", should have been '
                        '"{2}|{3}"'.format(job.id, job.status, Job.STATUS_PENDING, Job.STATUS_NOT_APPROVED))

                job.status = Job.STATUS_CANCELLED
                self.jobs_index[job.id] = self.__dump_job(job)

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

                job.status = Job.STATUS_NEW
                self.jobs_index[job.id] = self.__dump_job(job)

        except Exception as e:
            logger.error('Failed to cancel job {0}: {1}\n{2}'.format(
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

            self.__change_failed_task_status(job_id, task_id, Task.STATUS_QUEUED)

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

            self.__change_failed_task_status(job_id, task_id, Task.STATUS_SKIPPED)

        except Exception as e:
            logger.error('Failed to skip job task, job {0}, task {1}: '
                '{2}\n{3}'.format(job_id, task_id, e, traceback.format_exc()))
            raise

        return job.dump()

    def __change_failed_task_status(self, job_id, task_id, status):
        if not job_id in self.jobs:
            raise ValueError('Job {0}: job is not found'.format(job_id))
        job = self.jobs[job_id]

        if job.status != Job.STATUS_PENDING:
            raise ValueError('Job {0}: status is "{1}", should have been '
                '"{2}"'.format(job.id, job.status, Job.STATUS_PENDING))

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
            logger.info('Job {0}: task {1} was skipped'.format(job.id, task.id))
