import datetime
import json
import logging
import time
import traceback

import elliptics

from config import config
from db.mongo.pool import Collection
import errors
from error import JobBrokenError, RetryError
import helpers as h
from job_types import JobTypes, TaskTypes
from job import Job
from couple_defrag import CoupleDefragJob
from move import MoveJob
from recover_dc import RecoverDcJob
from job_factory import JobFactory
from restore_group import RestoreGroupJob
import indexes
import keys
from tasks import Task, MinionCmdTask
import timed_queue
import storage
from sync import sync_manager
from sync.error import (
    LockError,
    LockFailedError,
    LockAlreadyAcquiredError,
    InconsistentLockError,
    API_ERROR_CODE
)
from timer import periodic_timer


logger = logging.getLogger('mm.jobs')

JOB_CONFIG = config.get('jobs', {})


class JobProcessor(object):

    JOBS_EXECUTE = 'jobs_execute'
    JOBS_UPDATE = 'jobs_update'
    JOBS_LOCK = 'jobs'

    JOB_MANUAL_TIMEOUT = 20

    def __init__(self, node, db, niu, minions):
        logger.info('Starting JobProcessor')
        self.session = elliptics.Session(node)
        wait_timeout = config.get('elliptics', {}).get('wait_timeout', None) or config.get('wait_timeout', 5)
        self.session.set_timeout(wait_timeout)
        self.meta_session = node.meta_session
        self.minions = minions
        self.node_info_updater = niu
        self.planner = None

        self.__tq = timed_queue.TimedQueue()

        if config['metadata'].get('jobs', {}).get('db'):
            self.jobs_timer = periodic_timer(seconds=JOB_CONFIG.get('execute_period', 60))
            self.collection = Collection(db[config['metadata']['jobs']['db']], 'jobs')
            self.downtimes = Collection(db[config['metadata']['jobs']['db']], 'downtimes')
            self.__tq.add_task_at(self.JOBS_EXECUTE,
                self.jobs_timer.next(), self._execute_jobs)

    def _start_tq(self):
        self.__tq.start()

    def _get_processing_jobs_hosts(self):
        hosts = []
        for job in self.jobs(statuses=Job.STATUS_EXECUTING):
            for task in job.tasks:
                if task.status == task.STATUS_EXECUTING and isinstance(task, MinionCmdTask):
                    if not task.host:
                        continue
                    hosts.append(task.host)
        return hosts

    def _execute_jobs(self):

        logger.info('Jobs execution started')
        try:
            logger.debug('Lock acquiring')

            with sync_manager.lock(self.JOBS_LOCK, blocking=False):
                logger.debug('Lock acquired')

                new_jobs, executing_jobs = [], []
                type_jobs_count = {}

                for job in self.jobs(statuses=Job.STATUS_EXECUTING):
                    type_jobs_count.setdefault(job.type, 0)
                    type_jobs_count[job.type] += 1
                    executing_jobs.append(job)
                for job in self.jobs(statuses=Job.STATUS_NEW):
                    jobs_count = type_jobs_count.setdefault(job.type, 0)
                    if jobs_count >= JOB_CONFIG.get(job.type, {}).get('max_executing_jobs', 3):
                        continue
                    type_jobs_count[job.type] += 1
                    new_jobs.append(job)

                new_jobs.sort(key=lambda j: j.create_ts)
                ready_jobs = executing_jobs + new_jobs
                logger.debug('Ready jobs: {0}'.format(len(ready_jobs)))

                for job in ready_jobs:
                    try:
                        with job.tasks_lock():
                            self.__process_job(job)
                        job.save()
                    except LockError:
                        pass
                    except Exception as e:
                        logger.error('Failed to process job {0}: '
                            '{1}\n{2}'.format(job.id, e, traceback.format_exc()))
                        continue

        except LockFailedError as e:
            pass
        except Exception as e:
            logger.error('Failed to process existing jobs: {0}\n{1}'.format(
                e, traceback.format_exc()))
        finally:
            logger.info('Jobs execution finished')
            self.__tq.add_task_at(self.JOBS_EXECUTE,
                self.jobs_timer.next(),
                self._execute_jobs)

    def tag(self, job):
        ts = job.create_ts or job.start_ts
        if not ts:
            logger.error('Bad job: {0}'.format(job.human_dump()))
        return self.tag_dt(datetime.datetime.fromtimestamp(ts or time.time()))

    def tag_dt(self, dt):
        return dt.strftime('%Y-%m')

    def __process_job(self, job):

        logger.debug('Job {0}, processing started: {1}'.format(job.id, job.dump()))

        if job.status == Job.STATUS_NEW:
            logger.info('Job {0}: setting job start time'.format(job.id))
            job.start_ts = time.time()
            job._dirty = True
            try:
                job.on_start()
            except Exception as e:
                logger.error('Job {0}: failed to start job: {1}\n{2}'.format(
                    job.id, e, traceback.format_exc()))
                if isinstance(e, JobBrokenError):
                    job.status = Job.STATUS_BROKEN
                else:
                    job.status = Job.STATUS_PENDING
                job.add_error_msg(str(e))
                ts = time.time()
                job.update_ts = ts
                job.finish_ts = ts
                return

        for task in job.tasks:
            if task.status == Task.STATUS_EXECUTING:

                logger.info('Job {0}, task {1} status update'.format(
                    job.id, task.id))
                try:
                    self.__update_task_status(task)
                except Exception as e:
                    logger.error('Job {0}, task {1}: failed to update status: '
                        '{2}\n{3}'.format(job.id, task.id, e, traceback.format_exc()))
                    job.add_error_msg(str(e))
                    task.status = Task.STATUS_FAILED
                    job.status = Job.STATUS_PENDING
                    ts = time.time()
                    job.update_ts = ts
                    job.finish_ts = ts
                    job._dirty = True
                    break

                if not task.finished:
                    logger.debug('Job {0}, task {1} is not finished'.format(
                        job.id, task.id))
                    break

                ts = time.time()
                job.update_ts = ts
                job.finish_ts = ts
                job._dirty = True

                task.status = (Task.STATUS_FAILED
                               if task.failed else
                               Task.STATUS_COMPLETED)

                try:
                    task.on_exec_stop(self)
                except Exception as e:
                    logger.error('Job {0}, task {1}: failed to execute task '
                        'stop handler: {2}\n{3}'.format(
                            job.id, task.id, e, traceback.format_exc()))
                    raise

                logger.debug('Job {0}, task {1} is finished, status {2}'.format(
                    job.id, task.id, task.status))

                if task.status == Task.STATUS_FAILED:
                    job.status = Job.STATUS_PENDING
                    ts = time.time()
                    job.update_ts = ts
                    job.finish_ts = ts
                    break
                else:
                    continue

            elif task.status == Task.STATUS_QUEUED:
                logger.info('Job {0}, executing new task {1}'.format(job.id, task))

                try:
                    task.on_exec_start(self)
                    logger.info('Job {0}, task {1} preparation completed'.format(job.id, task.id))
                except Exception as e:
                    logger.error('Job {0}, task {1}: failed to execute task '
                        'start handler: {2}\n{3}'.format(
                            job.id, task.id, e, traceback.format_exc()))
                    raise

                try:
                    task.attempts += 1
                    self.__execute_task(task)
                    logger.info('Job {0}, task {1} execution was successfully requested'.format(
                        job.id, task.id))
                except Exception as e:
                    try:
                        task.on_exec_stop(self)
                    except Exception as e:
                        logger.error('Job {0}, task {1}: failed to execute task '
                            'stop handler: {2}\n{3}'.format(
                                job.id, task.id, e, traceback.format_exc()))
                        raise

                    if isinstance(e, RetryError):
                        if task.attempts < JOB_CONFIG.get('minions', {}).get('execute_attempts', 3):
                            job._dirty = True
                            break
                        else:
                            job.add_error_msg(str(e))

                    task.status = Task.STATUS_FAILED
                    if isinstance(e, JobBrokenError):
                        logger.error('Job {0}, task {1}: cannot execute task, '
                            'not applicable for current storage state: {2}'.format(
                                job.id, task.id, e))
                        job.status = Job.STATUS_BROKEN
                    else:
                        logger.error('Job {0}, task {1}: failed to execute: {2}\n{3}'.format(
                            job.id, task.id, e, traceback.format_exc()))
                        job.status = Job.STATUS_PENDING
                    job.add_error_msg(str(e))
                    ts = time.time()
                    job.update_ts = ts
                    job.finish_ts = ts
                    job._dirty = True
                    break

                job.update_ts = time.time()
                task.status = Task.STATUS_EXECUTING
                job.status = Job.STATUS_EXECUTING
                job._dirty = True
                break

        if all([task.status in (Task.STATUS_COMPLETED, Task.STATUS_SKIPPED)
                for task in job.tasks]):
            logger.info('Job {0}, tasks processing is finished'.format(job.id))
            try:
                job.status = Job.STATUS_COMPLETED
                job.complete(self)
                job._dirty = True
            except RuntimeError as e:
                logger.error('Job {0}, failed to complete job: {1}'.format(job.id, e))
                raise

    def __update_task_status(self, task):
        if isinstance(task, MinionCmdTask):
            task.update_status(self)
        else:
            task.update_status()

    def __execute_task(self, task):
        task.start_ts, task.finish_ts = time.time(), None
        if isinstance(task, MinionCmdTask):
            task.execute(self)
        else:
            task.execute()

    def __dump_job(self, job):
        return json.dumps(job.dump())

    @h.concurrent_handler
    def create_job(self, request):
        try:
            try:
                job_type = request[0]
            except IndexError:
                raise ValueError('Job type is required')

            if job_type not in (JobTypes.TYPE_MOVE_JOB, JobTypes.TYPE_RECOVER_DC_JOB,
                JobTypes.TYPE_COUPLE_DEFRAG_JOB, JobTypes.TYPE_RESTORE_GROUP_JOB):
                raise ValueError('Invalid job type: {0}'.format(job_type))

            try:
                params = request[1]
            except IndexError:
                params = {}

            try:
                force = request[2]
            except IndexError:
                force = False

            with sync_manager.lock(self.JOBS_LOCK, timeout=self.JOB_MANUAL_TIMEOUT):
                job = self._create_job(job_type, params, force=force)

        except LockFailedError as e:
            raise
        except Exception as e:
            logger.error('Failed to create job: {0}\n{1}'.format(e,
                traceback.format_exc()))
            raise

        return job.dump()

    def _create_job(self, job_type, params, force=False):
        # Forcing manual approval of newly created job
        params.setdefault('need_approving', True)

        if job_type == JobTypes.TYPE_MOVE_JOB:
            JobType = MoveJob
        elif job_type == JobTypes.TYPE_RECOVER_DC_JOB:
            JobType = RecoverDcJob
        elif job_type == JobTypes.TYPE_COUPLE_DEFRAG_JOB:
            JobType = CoupleDefragJob
        elif job_type == JobTypes.TYPE_RESTORE_GROUP_JOB:
            JobType = RestoreGroupJob

        try:
            job = JobType.new(self.session, **params)
        except LockAlreadyAcquiredError as e:
            if not force:
                raise

            job_ids = e.holders_ids

            # check job types priority
            STOP_ALLOWED_TYPES = (JobTypes.TYPE_RECOVER_DC_JOB,
                                  JobTypes.TYPE_COUPLE_DEFRAG_JOB)

            if job_type not in (JobTypes.TYPE_RESTORE_GROUP_JOB, JobTypes.TYPE_MOVE_JOB):
                raise

            jobs = self.jobs(ids=job_ids)
            for job in jobs:
                if job.type not in STOP_ALLOWED_TYPES:
                    raise RuntimeError('Cannot stop job {0}, type is {1}'.format(job.id, job.type))

            logger.info('Stopping jobs: {0}'.format(job_ids))
            self._stop_jobs(jobs)

            logger.info('Retrying job creation')
            job = JobType.new(self.session, **params)

        job.collection = self.collection

        inv_group_ids = job._involved_groups
        try:
            logger.info('Job {0}: updating groups {1} status'.format(job.id, inv_group_ids))
            inv_groups = [storage.groups[ig] for ig in inv_group_ids]
            self.node_info_updater.monitor_stats(groups=inv_groups)
            self.node_info_updater.update_symm_groups_async(groups=inv_groups)
        except Exception as e:
            logger.info('Job {0}: failed to update groups status: {1}\n{2}'.format(
                job.id, e, traceback.format_exc()))
            pass

        try:
            job.create_tasks()

            logger.info('Job {0} created: {1}'.format(job.id, job.dump()))

            job.save()
        except Exception:
            job.release_locks()
            raise

        return job

    @h.concurrent_handler
    def stop_jobs(self, request):
        jobs = []
        try:
            try:
                job_uids = request[0]
            except IndexError:
                raise ValueError('Job uids is required')

            logger.debug('Lock acquiring')
            with sync_manager.lock(self.JOBS_LOCK, timeout=self.JOB_MANUAL_TIMEOUT):
                logger.debug('Lock acquired')

                jobs = self.jobs(ids=job_uids)
                self._stop_jobs(jobs)

        except LockFailedError as e:
            raise
        except Exception as e:
            logger.error('Failed to stop jobs: {0}\n{1}'.format(e,
                traceback.format_exc()))
            raise

        return [job.dump() for job in jobs]

    def _stop_jobs(self, jobs):

        executing_jobs = []

        for job in jobs:
            if job.status in Job.ACTIVE_STATUSES:
                if job.status != Job.STATUS_EXECUTING:
                    self._cancel_job(job)
                else:
                    executing_jobs.append(job)

        for job in executing_jobs:
            for task in job.tasks:
                if task.status == Task.STATUS_EXECUTING:
                    # Move task stop handling to task itself?
                    if isinstance(task, MinionCmdTask):
                        self.minions._terminate_cmd(task.host, task.minion_cmd_id)

                    try:
                        task.on_exec_stop(self)
                    except Exception as e:
                        logger.error('Job {0}, task {1}: failed to execute task '
                            'stop handler: {2}\n{3}'.format(
                                job.id, task.id, e, traceback.format_exc()))
                        raise

                    task.status = Task.STATUS_FAILED

                    break

            self._cancel_job(job)

        for job in jobs:
            job.save()

        return jobs

    @h.concurrent_handler
    def get_job_list(self, request):
        try:
            options = request[0]
        except (TypeError, IndexError):
            options = {}

        jobs_list = Job.list(self.collection,
            status=options['statuses'],
            type=options['job_type'])
        total_jobs = jobs_list.count()

        if options.get('limit'):
            limit = int(options['limit'])
            offset = int(options.get('offset', 0))

            jobs_list = jobs_list[offset:offset + limit]

        res = [JobFactory.make_job(j).human_dump() for j in jobs_list]
        return {'jobs': res,
                'total': total_jobs}

    def __get_job(self, job_id):
        jobs_list = Job.list(self.collection,
            id=job_id).limit(1)
        if not jobs_list:
            raise ValueError('Job {0} is not found'.format(job_id))
        job = JobFactory.make_job(jobs_list[0])
        job.collection = self.collection
        return job

    @h.concurrent_handler
    def get_job_status(self, request):
        try:
            job_id = request[0]
        except (TypeError, IndexError):
            raise ValueError('Job id is required')

        return self.__get_job(job_id).human_dump()

    @h.concurrent_handler
    def get_jobs_status(self, request):
        try:
            job_ids = request[0]
        except (TypeError, IndexError):
            raise ValueError('Job ids are required')

        return [j.human_dump() for j in self.jobs(ids=job_ids)]

    @h.concurrent_handler
    def cancel_job(self, request):
        job_id = None
        try:
            try:
                job_id = request[0]
            except IndexError as e:
                raise ValueError('Job id is required')

            job = self.__get_job(job_id)

            with job.tasks_lock():

                if job.status not in (Job.STATUS_PENDING,
                    Job.STATUS_NOT_APPROVED, Job.STATUS_BROKEN):
                    raise ValueError('Job {0}: status is "{1}", should have been '
                        '"{2}|{3}|{4}"'.format(job.id, job.status,
                            Job.STATUS_PENDING, Job.STATUS_NOT_APPROVED, Job.STATUS_BROKEN))

                self._cancel_job(job)

                job.save()

                logger.info('Job {0}: status set to {1}'.format(job.id, job.status))

        except Exception as e:
            logger.error('Failed to cancel job {0}: {1}\n{2}'.format(
                job_id, e, traceback.format_exc()))
            raise

        return job.dump()

    def _cancel_job(self, job):
        if job.status not in Job.ACTIVE_STATUSES:
            raise ValueError('Job {0}: job is not active, its status '
                'is {1}'.format(job.id, job.status))

        job.status = Job.STATUS_CANCELLED
        job.complete(self)
        job._dirty = True

    @h.concurrent_handler
    def approve_job(self, request):
        job_id = None
        try:
            try:
                job_id = request[0]
            except IndexError as e:
                raise ValueError('Job id is required')

            job = self.__get_job(job_id)

            with job.tasks_lock():

                if job.status != Job.STATUS_NOT_APPROVED:
                    raise ValueError('Job {0}: status is "{1}", should have been '
                        '"{2}"'.format(job.id, job.status, Job.STATUS_NOT_APPROVED))

                job.status = Job.STATUS_NEW
                job.update_ts = time.time()
                job._dirty = True

                job.save()

        except Exception as e:
            logger.error('Failed to approve job {0}: {1}\n{2}'.format(
                job_id, e, traceback.format_exc()))
            raise

        return job.dump()

    @h.concurrent_handler
    def retry_failed_job_task(self, request):
        job_id = None
        try:
            try:
                job_id, task_id = request[:2]
            except ValueError as e:
                raise ValueError('Job id and task id are required')

            job = self.__change_failed_task_status(job_id, task_id, Task.STATUS_QUEUED)

        except LockFailedError as e:
            raise
        except Exception as e:
            logger.error('Failed to retry job task, job {0}, task {1}: '
                '{2}\n{3}'.format(job_id, task_id, e, traceback.format_exc()))
            raise

        return job.dump()

    @h.concurrent_handler
    def skip_failed_job_task(self, request):
        job_id = None
        try:
            try:
                job_id, task_id = request[:2]
            except ValueError as e:
                raise ValueError('Job id and task id are required')

            job = self.__change_failed_task_status(job_id, task_id, Task.STATUS_SKIPPED)

        except LockFailedError as e:
            raise
        except Exception as e:
            logger.error('Failed to skip job task, job {0}, task {1}: '
                '{2}\n{3}'.format(job_id, task_id, e, traceback.format_exc()))
            raise

        return job.dump()

    def __change_failed_task_status(self, job_id, task_id, status):

        logger.debug('Lock acquiring')
        with sync_manager.lock(self.JOBS_LOCK, timeout=self.JOB_MANUAL_TIMEOUT):
            logger.debug('Lock acquired')

            job = self.__get_job(job_id)
            with job.tasks_lock():
                if job.status not in (Job.STATUS_PENDING, Job.STATUS_BROKEN):
                    raise ValueError('Job {0}: status is "{1}", should have been '
                        '{2}|{3}'.format(job.id, job.status, Job.STATUS_PENDING, Job.STATUS_BROKEN))

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
                task.attempts = 0
                job.status = Job.STATUS_EXECUTING
                job.update_ts = time.time()
                job._dirty = True
                job.save()
                logger.info('Job {0}: task {1} status was reset to {2}, '
                    'job status was reset to {3}'.format(
                        job.id, task.id, task.status, job.status))

        return job

    def jobs_count(self, types=None, statuses=None):
        return Job.list(self.collection,
            status=statuses,
            type=types).count()

    def jobs(self, types=None, statuses=None, ids=None):
        jobs = [JobFactory.make_job(j) for j in Job.list(self.collection,
                                                         status=statuses,
                                                         type=types,
                                                         id=ids)]
        for j in jobs:
            j.collection = self.collection
            j._dirty = False
        return jobs

    def get_uncoupled_groups_in_service(self):
        jobs = self.jobs(
            types=(JobTypes.TYPE_MOVE_JOB, JobTypes.TYPE_RESTORE_GROUP_JOB),
            statuses=(Job.STATUS_NOT_APPROVED,
                      Job.STATUS_NEW,
                      Job.STATUS_EXECUTING,
                      Job.STATUS_PENDING,
                      Job.STATUS_BROKEN))

        uncoupled_groups = []
        for job in jobs:
            if job.uncoupled_group:
                uncoupled_groups.append(job.uncoupled_group)
            if job.merged_groups:
                uncoupled_groups.extend(job.merged_groups)

        return uncoupled_groups
