import datetime
import json
import logging
import time
import traceback

import elliptics

from config import config
import errors
from error import JobBrokenError
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
from sync import sync_manager
from sync.error import (
    LockError,
    LockFailedError,
    LockAlreadyAcquiredError,
    InconsistentLockError,
    API_ERROR_CODE
)


logger = logging.getLogger('mm.jobs')


class JobProcessor(object):

    JOBS_EXECUTE = 'jobs_execute'
    JOBS_UPDATE = 'jobs_update'
    JOBS_LOCK = 'jobs'

    INDEX_BATCH_SIZE = 1000

    def __init__(self, node, minions):
        logger.info('Starting JobProcessor')
        self.session = elliptics.Session(node)
        wait_timeout = config.get('elliptics', {}).get('wait_timeout', None) or config.get('wait_timeout', 5)
        self.session.set_timeout(wait_timeout)
        self.meta_session = node.meta_session
        self.minions = minions

        self.jobs = {}
        self.jobs_index = indexes.TagSecondaryIndex(
            keys.MM_JOBS_IDX,
            keys.MM_JOBS_IDX_TPL,
            keys.MM_JOBS_KEY_TPL,
            self.meta_session,
            logger=logger,
            batch_size=self.INDEX_BATCH_SIZE,
            namespace='jobs')

        self.__tq = timed_queue.TimedQueue()
        self.__tq.start()

        self.job_tags = self._active_tags()

        self._update_jobs()
        self.__tq.add_task_in(self.JOBS_EXECUTE,
            5, self._execute_jobs)

    def _active_tags(self):
        tags = []
        dt = datetime.datetime.now().replace(day=1)
        for month in range(2):
            tags.append(self.tag_dt(dt))
            dt = dt - datetime.timedelta(days=1)
            dt = dt.replace(day=1)
        return tags

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
        for tag in self.job_tags:
            logger.debug('updating jobs with tag {0}'.format(tag))
            for job in self.jobs_index.tagged(tag):
                self._load_job(job)

    def _execute_jobs(self):

        logger.info('Jobs execution started')
        try:
            if not self.minions.ready:
                raise errors.NotReadyError

            logger.debug('Lock acquiring')

            with sync_manager.lock(self.JOBS_LOCK, blocking=False):
                logger.debug('Lock acquired')
                # TODO: check! # fetch jobs - read_latest!!!
                self._do_update_jobs()

                new_jobs, executing_jobs = [], []
                type_jobs_count = {}
                for job in filter(lambda j: j.status == Job.STATUS_EXECUTING, self.jobs.itervalues()):
                    type_jobs_count.setdefault(job.type, 0)
                    type_jobs_count[job.type] += 1
                    executing_jobs.append(job)
                for job in filter(lambda j: j.status == Job.STATUS_NEW, self.jobs.itervalues()):
                    jobs_count = type_jobs_count.setdefault(job.type, 0)
                    if jobs_count >= config.get('jobs', {}).get(job.type, {}).get('max_executing_jobs', 3):
                        continue
                    type_jobs_count[job.type] += 1
                    new_jobs.append(job)

                new_jobs.sort(key=lambda j: j.create_ts)
                ready_jobs = executing_jobs + new_jobs
                logger.debug('Ready jobs: {0}'.format(len(ready_jobs)))

                executing_count = 0
                for job in ready_jobs:
                    try:
                        with job.tasks_lock():
                            self.__process_job(job)
                    except LockError:
                        pass
                    except Exception as e:
                        logger.error('Failed to process job {0}: '
                            '{1}\n{2}'.format(job.id, e, traceback.format_exc()))
                        continue
                    else:
                        executing_count += 1
                    self.jobs_index[job.id] = self.__dump_job(job)

        except LockFailedError as e:
            pass
        except errors.NotReadyError as e:
            logger.warn('Failed to process jobs: minions state is not fetched')
        except Exception as e:
            logger.error('Failed to process existing jobs: {0}\n{1}'.format(
                e, traceback.format_exc()))
        finally:
            logger.info('Jobs execution finished')
            self.__tq.add_task_in(self.JOBS_EXECUTE,
                config.get('jobs', {}).get('execute_period', 60),
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
            try:
                job.on_start()
            except JobBrokenError as e:
                logger.error('Job {0}: cannot start job: {1}'.format(
                        job.id, e))
                job.status = Job.STATUS_BROKEN
                job.add_error_msg(str(e))
                job.finish_ts = time.time()
                return
            except Exception as e:
                logger.error('Job {0}: failed to start job: {1}\n{2}'.format(
                    job.id, e, traceback.format_exc()))
                job.status = Job.STATUS_PENDING
                job.finish_ts = time.time()
                return

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
            try:
                job.complete(self.session)
            except RuntimeError as e:
                logger.error('Job {0}, failed to complete job: {1}'.format(job.id, e))
            else:
                job.status = Job.STATUS_COMPLETED

    def __update_task_status(self, task):
        if isinstance(task, MinionCmdTask):
            task.update_status(self.minions)
        else:
            task.update_status()

    def __execute_task(self, task):
        task.start_ts, task.finish_ts = time.time(), None
        if isinstance(task, MinionCmdTask):
            task.execute(self.minions)
        else:
            task.execute()

    def __dump_job(self, job):
        return json.dumps(job.dump())

    def __load_job(self, data):
        return json.loads(data)

    JOB_MANUAL_TIMEOUT = 20

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

            with sync_manager.lock(self.JOBS_LOCK, timeout=self.JOB_MANUAL_TIMEOUT):
                job = self._create_job(job_type, params)

        except LockFailedError as e:
            raise
        except Exception as e:
            logger.error('Failed to create job: {0}\n{1}'.format(e,
                traceback.format_exc()))
            raise

        return job.dump()

    def _create_job(self, job_type, params):
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
        job = JobType.new(self.session, **params)

        try:
            job.create_tasks()

            logger.info('Job {0} created: {1}'.format(job.id, job.dump()))
            self.jobs_index[job.id] = self.__dump_job(job)
            self.jobs_index.set_tag(job.id, self.tag(job))

            self.jobs[job.id] = job
        except Exception:
            job.release_locks()
            raise

        return job

    def get_job_list(self, request):
        try:
            options = request[0]
        except (TypeError, IndexError):
            options = {}

        def job_filter(j):
            if options.get('job_type', None) and j.type != options['job_type']:
                return False
            if options.get('tag', None) and self.tag(j) != options['tag']:
                return False
            if options.get('statuses', None) and j.status not in options['statuses']:
                return False
            return True

        jobs = self.jobs.itervalues()
        if options.get('tag', None) and options['tag'] not in self.job_tags:
            logger.info('Requested job list not from active tags: {0}'.format(options['tag']))
            jobs = [self._load_job(job) for job in self.jobs_index.tagged(options['tag'])]

        jobs = sorted(filter(job_filter, jobs), key=lambda j: (j.create_ts, j.start_ts, j.finish_ts))
        total_jobs = len(jobs)

        if options.get('limit'):
            limit = int(options['limit'])
            offset = int(options.get('offset', 0))

            jobs = jobs[offset:offset + limit]

        res = [job.human_dump() for job in jobs]
        return {'jobs': res,
                'total': total_jobs}

    def get_job_status(self, request):
        try:
            job_id = request[0]
        except (TypeError, IndexError):
            raise ValueError('Job id is required')

        if not job_id in self.jobs:
            raise ValueError('Job {0} is not found'.format(job_id))

        return self.jobs[job_id].dump()

    def get_jobs_status(self, request):
        try:
            job_ids = request[0]
        except (TypeError, IndexError):
            raise ValueError('Job ids are required')

        statuses = []
        for job_id in job_ids:
            if not job_id in self.jobs:
                logger.error('Job {0} is not found'.format(job_id))
                continue
            statuses.append(self.jobs[job_id].human_dump())

        return statuses

    def cancel_job(self, request):
        job_id = None
        try:
            try:
                job_id = request[0]
            except IndexError as e:
                raise ValueError('Job id is required')

            if not job_id in self.jobs:
                raise ValueError('Job id {0} is not found'.format(job_id))

            job = self.jobs[job_id]

            logger.debug('Lock acquiring')
            with job.tasks_lock():
                logger.debug('Lock acquired')

                if job.status not in (Job.STATUS_PENDING,
                    Job.STATUS_NOT_APPROVED, Job.STATUS_BROKEN):
                    raise ValueError('Job {0}: status is "{1}", should have been '
                        '"{2}|{3}|{4}"'.format(job.id, job.status,
                            Job.STATUS_PENDING, Job.STATUS_NOT_APPROVED, Job.STATUS_BROKEN))

                job.status = Job.STATUS_CANCELLED
                job.complete(self.session)
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
            with job.tasks_lock():
                logger.debug('Lock acquired')

                if job.status != Job.STATUS_NOT_APPROVED:
                    raise ValueError('Job {0}: status is "{1}", should have been '
                        '"{2}"'.format(job.id, job.status, Job.STATUS_NOT_APPROVED))

                job.status = Job.STATUS_NEW

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

        except LockFailedError as e:
            raise
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

        except LockFailedError as e:
            raise
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
        with sync_manager.lock(self.JOBS_LOCK, timeout=self.JOB_MANUAL_TIMEOUT), job.tasks_lock():
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

    def get_uncoupled_groups_in_service(self):
        uncoupled_groups = []

        try:
            self._do_update_jobs()
        except Exception as e:
            logger.error('Failed to update jobs: {0}\n{1}'.format(
                e, traceback.format_exc()))

        for job in self.jobs.values():
            if job.status in (Job.STATUS_COMPLETED, Job.STATUS_CANCELLED):
                continue
            if job.type not in (JobTypes.TYPE_MOVE_JOB, JobTypes.TYPE_RESTORE_GROUP_JOB):
                continue
            if job.uncoupled_group:
                uncoupled_groups.append(job.uncoupled_group)
            if job.type == JobTypes.TYPE_RESTORE_GROUP_JOB and job.merged_groups:
                uncoupled_groups.extend(merged_groups)

        return uncoupled_groups
