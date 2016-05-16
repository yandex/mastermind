from collections import defaultdict
import itertools
import logging
import time
import traceback

import elliptics

from config import config
from db.mongo.pool import Collection
from error import JobBrokenError, RetryError
import helpers as h
from job_types import JobTypes, TaskTypes
from job import Job
from couple_defrag import CoupleDefragJob
import lrc_builder
from move import MoveJob
from recover_dc import RecoverDcJob
from make_lrc_groups import MakeLrcGroupsJob
from job_factory import JobFactory
from restore_group import RestoreGroupJob
from tasks import Task, MinionCmdTask
import timed_queue
import storage
from sync import sync_manager
from sync.error import (
    LockError,
    LockFailedError,
    LockAlreadyAcquiredError,
)
from timer import periodic_timer


logger = logging.getLogger('mm.jobs')

JOB_CONFIG = config.get('jobs', {})


class JobProcessor(object):

    JOBS_EXECUTE = 'jobs_execute'
    JOBS_UPDATE = 'jobs_update'
    JOBS_LOCK = 'jobs'

    JOB_MANUAL_TIMEOUT = 20

    JOB_PRIORITIES = {
        JobTypes.TYPE_RESTORE_GROUP_JOB: 25,
        JobTypes.TYPE_MOVE_JOB: 20,
        JobTypes.TYPE_RECOVER_DC_JOB: 15,
        JobTypes.TYPE_COUPLE_DEFRAG_JOB: 10,
        JobTypes.TYPE_MAKE_LRC_GROUPS_JOB: 5,
    }

    # job types that should be processed by processor,
    # jobs with other types will be skipped
    SUPPORTED_JOBS = set([
        JobTypes.TYPE_RESTORE_GROUP_JOB,
        JobTypes.TYPE_MOVE_JOB,
        JobTypes.TYPE_RECOVER_DC_JOB,
        JobTypes.TYPE_COUPLE_DEFRAG_JOB,
        JobTypes.TYPE_MAKE_LRC_GROUPS_JOB,
    ])

    def __init__(self, job_finder, node, db, niu, minions):
        logger.info('Starting JobProcessor')
        self.job_finder = job_finder
        self.session = elliptics.Session(node)
        wait_timeout = config.get('elliptics', {}).get('wait_timeout') or \
            config.get('wait_timeout', 5)
        self.session.set_timeout(wait_timeout)
        self.meta_session = node.meta_session
        self.minions = minions
        self.node_info_updater = niu
        self.planner = None

        self.__tq = timed_queue.TimedQueue()

        self.jobs_timer = periodic_timer(seconds=JOB_CONFIG.get('execute_period', 60))
        self.downtimes = Collection(db[config['metadata']['jobs']['db']], 'downtimes')
        self.__tq.add_task_at(
            self.JOBS_EXECUTE,
            self.jobs_timer.next(),
            self._execute_jobs)

    def _start_tq(self):
        self.__tq.start()

    @staticmethod
    def _unfold_resources(d):
        if d is None:
            raise StopIteration
        for res_type, res_val in itertools.chain(*[itertools.product([k], v)
                                                   for k, v in d.iteritems()]):
            if isinstance(res_val, list):
                res_val = tuple(res_val)
            yield res_type, res_val

    def _ready_jobs(self):

        active_jobs = self.job_finder.jobs(statuses=Job.ACTIVE_STATUSES)

        ready_jobs = []
        new_jobs = []

        def default_res_counter():
            return {
                Job.RESOURCE_FS: {},
                Job.RESOURCE_HOST_IN: {},
                Job.RESOURCE_HOST_OUT: {},
            }

        resources = defaultdict(default_res_counter)

        # global job counter by type, accounts only executing jobs
        type_jobs_count = {}

        # counting current resource usage
        for job in active_jobs:
            if job.status == Job.STATUS_NOT_APPROVED:
                continue
            if job.status == Job.STATUS_NEW:
                if job.type in self.SUPPORTED_JOBS:
                    new_jobs.append(job)
            else:
                type_res = resources[job.type]
                for res_type, res_val in self._unfold_resources(job.resources):
                    type_res[res_type].setdefault(res_val, 0)
                    type_res[res_type][res_val] += 1

                if job.status == Job.STATUS_EXECUTING:
                    ready_jobs.append(job)
                    type_jobs_count.setdefault(job.type, 0)
                    type_jobs_count[job.type] += 1

        logger.debug('Resources usage: {}; by type {}'.format(dict(resources), type_jobs_count))

        # selecting jobs to start processing
        for job in new_jobs:

            check_types = [t for t, p in self.JOB_PRIORITIES.iteritems()
                           if p > self.JOB_PRIORITIES[job.type]]
            logger.debug('Job {}: checking higher priority job types: {}'.format(
                job.id, check_types))

            no_slots = False

            for res_type, res_val in self._unfold_resources(job.resources):

                for job_type in check_types:
                    if resources[job_type][res_type].get(res_val, 0) > 0:
                        # job of higher priority is using the resource
                        logger.debug(
                            'Job {}: will be skipped, resource {} / {} '
                            'is occupuied by higher priority job of type {}'.format(
                                job.id, res_type, res_val, job_type))
                        no_slots = True
                        break

                if no_slots:
                    break

                cur_usage = resources[job.type][res_type].get(res_val, 0)
                max_usage = JOB_CONFIG.get(job.type, {}).get(
                    'resources_limits', {}).get(res_type, float('inf'))
                if cur_usage >= max_usage:
                    logger.debug(
                        'Job {job_id}: will be skipped, resource '
                        '{used_resource_type} / {used_resource} '
                        'counter {used_resource_counter} >= {used_resource_max} '
                        'is occupied by jobs of same or less priority'.format(
                            job_id=job.id,
                            used_resource_type=res_type,
                            used_resource=res_val,
                            used_resource_counter=cur_usage,
                            used_resource_max=max_usage,
                        )
                    )

                    no_slots = True

                if no_slots:
                    break

            if no_slots:
                continue

            type_cur_usage = type_jobs_count.get(job.type, 0)
            type_max_usage = JOB_CONFIG.get(job.type, {}).get('max_executing_jobs', 50)
            if type_cur_usage >= type_max_usage:
                logger.debug(
                    'Job {}: will be skipped, job type {} counter {} >= {}'.format(
                        job.id, job.type, type_cur_usage, type_max_usage))
                continue

            # account job resources and add job to queue
            for res_type, res_val in self._unfold_resources(job.resources):
                logger.debug(
                    'Job {}: will use resource {} / {} counter {} / {}'.format(
                        job.id, res_type, res_val, cur_usage, max_usage))
                resources[job.type][res_type].setdefault(res_val, 0)
                resources[job.type][res_type][res_val] += 1

            type_jobs_count.setdefault(job.type, 0)
            type_jobs_count[job.type] += 1

            ready_jobs.append(job)

        logger.debug('Ready jobs: {0}'.format(len(ready_jobs)))

        return ready_jobs

    def _execute_jobs(self):

        logger.info('Jobs execution started')
        try:
            logger.debug('Lock acquiring')
            with sync_manager.lock(self.JOBS_LOCK, blocking=False):
                logger.debug('Lock acquired')

                ready_jobs = self._ready_jobs()

                for job in ready_jobs:
                    try:
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

                if not task.finished(self):
                    logger.debug('Job {0}, task {1} is not finished'.format(
                        job.id, task.id))
                    break

                ts = time.time()
                job.update_ts = ts
                job.finish_ts = ts
                job._dirty = True

                task.status = (Task.STATUS_FAILED
                               if task.failed(self) else
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
                        logger.error('Job {}, task {}: retry error: {}'.format(
                            job.id, task.id, e))
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

            elif task.status == Task.STATUS_FAILED:
                break

        finished_statuses = (Task.STATUS_COMPLETED, Task.STATUS_SKIPPED)

        if all(task.status in finished_statuses for task in job.tasks):
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

    @h.concurrent_handler
    def create_job(self, request):
        try:
            try:
                job_type = request[0]
            except IndexError:
                raise ValueError('Job type is required')

            if job_type not in JobTypes.AVAILABLE_TYPES:
                raise ValueError('Invalid job type: {0}'.format(job_type))

            try:
                params = request[1]
            except IndexError:
                params = {}

            try:
                force = request[2]
            except IndexError:
                force = False

            job = self._create_job(job_type, params, force=force)

        except LockFailedError:
            raise
        except Exception:
            logger.exception('Failed to create job')
            raise

        return job.dump()

    def _create_job(self, job_type, params, force=False):
        # Forcing manual approval of newly created job
        params.setdefault('need_approving', True)

        JobType = JobFactory.make_job_type(job_type)

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

            jobs = self.job_finder.jobs(ids=job_ids)
            for existing_job in jobs:
                if self.JOB_PRIORITIES[existing_job.type] >= self.JOB_PRIORITIES[job_type]:
                    raise RuntimeError('Cannot stop job {0}, type is {1} '
                                       'and has equal or higher priority'.format(
                                           existing_job.id, existing_job.type))

                if existing_job.status in (Job.STATUS_NOT_APPROVED, Job.STATUS_NEW):
                    continue
                elif existing_job.type not in STOP_ALLOWED_TYPES:
                    raise RuntimeError('Cannot stop job {0}, type is {1}'.format(
                        existing_job.id, existing_job.type))

            logger.info('Stopping jobs: {0}'.format(job_ids))
            logger.debug('Lock acquiring')
            with sync_manager.lock(self.JOBS_LOCK, timeout=self.JOB_MANUAL_TIMEOUT):
                logger.debug('Lock acquired')
                self._stop_jobs(jobs)

            logger.info('Retrying job creation')
            job = JobType.new(self.session, **params)

        job.collection = self.job_finder.collection

        inv_group_ids = job._involved_groups
        try:
            logger.info('Job {0}: updating groups {1} status'.format(job.id, inv_group_ids))
            self.node_info_updater.update_status(
                groups=[
                    storage.groups[ig]
                    for ig in inv_group_ids
                    if ig in storage.groups
                ]
            )
        except Exception as e:
            logger.info('Job {0}: failed to update groups status: {1}\n{2}'.format(
                job.id, e, traceback.format_exc()))
            pass

        try:
            job.create_tasks()
            job.save()
            logger.info('Job {0} created: {1}'.format(job.id, job.dump()))
        except Exception:
            job.release_locks()
            job.unmark_groups(self.session)
            raise

        if 'group' in params:
            group_id = params['group']
            if group_id in storage.groups:
                group = storage.groups[group_id]
                group.set_active_job(job)
                group.update_status_recursive()

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

                jobs = self.job_finder.jobs(ids=job_uids)
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
                        try:
                            self.minions._terminate_cmd(task.host, task.minion_cmd_id)
                        except Exception as e:
                            logger.error('Job {0}, task {1}: failed to stop '
                                'minion task: {2}\n{3}'.format(
                                    job.id, task.id, e, traceback.format_exc()))
                            raise

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
    def cancel_job(self, request):
        job_id = None
        try:
            try:
                job_id = request[0]
            except IndexError as e:
                raise ValueError('Job id is required')

            job = self.job_finder._get_job(job_id)

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

            job = self.job_finder._get_job(job_id)

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

        except Exception as e:
            logger.error('Failed to skip job task, job {0}, task {1}: '
                '{2}\n{3}'.format(job_id, task_id, e, traceback.format_exc()))
            raise

        return job.dump()

    def __change_failed_task_status(self, job_id, task_id, status):

        job = self.job_finder._get_job(job_id)

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

    @h.concurrent_handler
    def restart_failed_to_start_job(self, request):
        job_id = None
        try:
            try:
                job_id = request[:1]
            except ValueError:
                raise ValueError('Job id is required')

            job = self.__change_not_started_job_status(job_id, Job.STATUS_NEW)

        except Exception:
            logger.exception('Job {}: failed to restart job'.format(
                job_id))
            raise

        return job.dump()

    def __change_not_started_job_status(self, job_id, status):
        job = self.job_finder._get_job(job_id)

        if job.status not in (Job.STATUS_PENDING, Job.STATUS_BROKEN):
            raise ValueError('Job {0}: status is "{1}", should have been {2}|{3}'.format(
                job.id, job.status, Job.STATUS_PENDING, Job.STATUS_BROKEN))

        for t in job.tasks:
            if t.status != Task.STATUS_QUEUED:
                raise ValueError('Job {}: task {} has status {}, expected {}'.format(
                    job.id, t.id, t.status, Task.STATUS_QUEUED))

        job.status = status
        job.update_ts = time.time()
        job._dirty = True
        job.save()
        logger.info('Job {}: job status was reset to {}'.format(
            job.id, job.status))
        return job

    @h.concurrent_handler
    def build_lrc_groups(self, request):
        if 'scheme' not in request:
            raise ValueError('Parameter "scheme" is required to build LRC groups')
        scheme = storage.Lrc.make_scheme(request['scheme'])

        count = request.get('count', 1)
        mandatory_dcs = request.get('mandatory_dcs', [])

        builder = scheme.builder(self)
        return builder.build(
            count=count,
            mandatory_dcs=mandatory_dcs,
        )


class JobFinder(object):

    def __init__(self, db):
        self.collection = Collection(db[config['metadata']['jobs']['db']], 'jobs')

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

        res = []
        for j in jobs_list:
            try:
                res.append(JobFactory.make_job(j).human_dump())
            except Exception:
                job_id = j.get('id', '[unknown id]')
                logger.exception('Failed to dump job {}'.format(job_id))
                continue
        return {'jobs': res,
                'total': total_jobs}

    @h.concurrent_handler
    def get_job_status(self, request):
        try:
            job_id = request[0]
        except (TypeError, IndexError):
            raise ValueError('Job id is required')

        return self._get_job(job_id).human_dump()

    @h.concurrent_handler
    def get_jobs_status(self, request):
        try:
            job_ids = request[0]
        except (TypeError, IndexError):
            raise ValueError('Job ids are required')

        return [j.human_dump() for j in self.jobs(ids=job_ids)]

    def _get_job(self, job_id):
        jobs_list = Job.list(self.collection,
            id=job_id).limit(1)
        if not jobs_list:
            raise ValueError('Job {0} is not found'.format(job_id))
        job = JobFactory.make_job(jobs_list[0])
        job.collection = self.collection
        return job

    def jobs_count(self, types=None, statuses=None):
        return Job.list(self.collection,
            status=statuses,
            type=types).count()

    def jobs(self, types=None, statuses=None, ids=None, groups=None):
        jobs = []
        job_list = Job.list(
            self.collection,
            status=statuses,
            type=types,
            group=groups,
            id=ids,
        )
        for j in job_list:
            try:
                job = JobFactory.make_job(j)
            except Exception:
                job_id = j.get('id', '[unknown id]')
                logger.exception('Failed to dump job {}'.format(job_id))
                continue
            job.collection = self.collection
            job._dirty = False
            jobs.append(job)
        return jobs

    def get_uncoupled_groups_in_service(self):
        # TODO: this list of types should be dynamical,
        # each job type should have a property to determine
        # if it uses uncoupled groups.
        jobs = self.jobs(
            types=(
                JobTypes.TYPE_MOVE_JOB,
                JobTypes.TYPE_RESTORE_GROUP_JOB,
                JobTypes.TYPE_MAKE_LRC_GROUPS_JOB,
            ),
            statuses=(Job.STATUS_NOT_APPROVED,
                      Job.STATUS_NEW,
                      Job.STATUS_EXECUTING,
                      Job.STATUS_PENDING,
                      Job.STATUS_BROKEN))

        uncoupled_groups = []
        for job in jobs:
            uncoupled_groups.extend(job.involved_uncoupled_groups)

        return uncoupled_groups
