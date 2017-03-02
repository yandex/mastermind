from collections import defaultdict
import itertools
import logging
import time
import traceback

import elliptics

from error import JobBrokenError, RetryError
import helpers as h
from job_types import JobTypes, TaskTypes
from job import Job
from couple_defrag import CoupleDefragJob
import lrc_builder
from mastermind_core.config import config
from mastermind_core.db.mongo.pool import Collection
from move import MoveJob
from recover_dc import RecoverDcJob
from make_lrc_groups import MakeLrcGroupsJob
from add_lrc_groupset import AddLrcGroupsetJob
from job_factory import JobFactory
from restore_group import RestoreGroupJob
from restore_lrc_group import RestoreLrcGroupJob
from backend_cleanup import BackendCleanupJob
from backend_manager import BackendManagerJob
from convert_to_lrc_groupset import ConvertToLrcGroupsetJob
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
        JobTypes.TYPE_RESTORE_LRC_GROUP_JOB: 25,
        JobTypes.TYPE_MOVE_JOB: 20,
        JobTypes.TYPE_TTL_CLEANUP_JOB: 19,
        JobTypes.TYPE_BACKEND_MANAGER_JOB: 18,
        JobTypes.TYPE_ADD_LRC_GROUPSET_JOB: 17,
        JobTypes.TYPE_CONVERT_TO_LRC_GROUPSET_JOB: 17,
        JobTypes.TYPE_RECOVER_DC_JOB: 15,
        JobTypes.TYPE_COUPLE_DEFRAG_JOB: 10,
        JobTypes.TYPE_MAKE_LRC_GROUPS_JOB: 5,
        JobTypes.TYPE_MAKE_LRC_RESERVED_GROUPS_JOB: 5,
        # Priority of this jobs is zero
        # because they're not using resources
        JobTypes.TYPE_BACKEND_CLEANUP_JOB: 0,
        JobTypes.TYPE_RESTORE_UNCOUPLED_LRC_GROUP_JOB: 0,
    }

    # job types that should be processed by processor,
    # jobs with other types will be skipped
    SUPPORTED_JOBS = set([
        JobTypes.TYPE_RESTORE_GROUP_JOB,
        JobTypes.TYPE_RESTORE_LRC_GROUP_JOB,
        JobTypes.TYPE_RESTORE_UNCOUPLED_LRC_GROUP_JOB,
        JobTypes.TYPE_MOVE_JOB,
        JobTypes.TYPE_TTL_CLEANUP_JOB,
        JobTypes.TYPE_RECOVER_DC_JOB,
        JobTypes.TYPE_COUPLE_DEFRAG_JOB,
        JobTypes.TYPE_MAKE_LRC_GROUPS_JOB,
        JobTypes.TYPE_MAKE_LRC_RESERVED_GROUPS_JOB,
        JobTypes.TYPE_ADD_LRC_GROUPSET_JOB,
        JobTypes.TYPE_CONVERT_TO_LRC_GROUPSET_JOB,
        JobTypes.TYPE_BACKEND_CLEANUP_JOB,
        JobTypes.TYPE_BACKEND_MANAGER_JOB,
    ])

    def __init__(self, job_finder, node, db, niu, minions_monitor, external_storage_meta, couple_record_finder):
        logger.info('Starting JobProcessor')
        self.job_finder = job_finder
        self.session = elliptics.Session(node)
        wait_timeout = config.get('elliptics', {}).get('wait_timeout') or \
            config.get('wait_timeout', 5)
        self.session.set_timeout(wait_timeout)
        self.minions_monitor = minions_monitor
        self.node_info_updater = niu
        self.planner = None
        self.external_storage_meta = external_storage_meta
        self.couple_record_finder = couple_record_finder

        self.__tq = None

        self.jobs_timer = periodic_timer(seconds=JOB_CONFIG.get('execute_period', 60))
        self.downtimes = Collection(db[config['metadata']['jobs']['db']], 'downtimes')

        if config.get('jobs', {}).get('enabled', True):
            self.__tq = timed_queue.TimedQueue()
            self.__tq.add_task_at(
                task_id=self.JOBS_EXECUTE,
                at=self.jobs_timer.next(),
                function=self._execute_jobs,
            )
        else:
            logger.warn("Job processor is disabled (config option ['jobs']['enabled'])")
            pass

    def _start_tq(self):
        if self.__tq:
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

    def _retry_jobs(self):

        pending_jobs = self.job_finder.jobs(statuses=Job.STATUS_PENDING, sort=False)
        pending_jobs.sort(key=lambda j: j.create_ts)

        for job in pending_jobs:
            try:
                self._retry_job(job)
            except Exception:
                logger.exception('Job {}: failed to retry'.format(job.id))
                continue

    def _retry_job(self, job):
        for task in job.tasks:
            if task.status in (Task.STATUS_COMPLETED, Task.STATUS_COMPLETED):
                continue
            if task.status != Task.STATUS_FAILED:
                # unexpected status, skip job processing
                break

            assert task.status == Task.STATUS_FAILED

            if task.ready_for_retry(self):
                logger.info(
                    'Job {}, task {}: ready for retry, setting status to "queued"'.format(
                        job.id,
                        task.id
                    )
                )
                self.__change_failed_task_status(job.id, task.id, Task.STATUS_QUEUED)

            break

    def _ready_jobs(self):

        active_statuses = list(Job.ACTIVE_STATUSES)
        active_statuses.remove(Job.STATUS_NOT_APPROVED)

        # TEMP: do not account broken/pending jobs' resources
        active_statuses.remove(Job.STATUS_PENDING)
        active_statuses.remove(Job.STATUS_BROKEN)

        active_jobs = self.job_finder.jobs(statuses=active_statuses, sort=False)
        active_jobs.sort(key=lambda j: j.create_ts)

        # in the case of smart scheduler, it plans jobs in accordance with resource consumption
        # while scheduler is not the only source of jobs, jobs created manually are difficult to predict and
        # we suppose that there are not much of them
        # so for the smart-scheduler case we could assume that all active jobs could be executed
        if config.get('scheduler', {}).get('enabled', False):
            return active_jobs

        ready_jobs = []
        new_jobs = []

        def default_res_counter():
            return {
                Job.RESOURCE_FS: {},
                Job.RESOURCE_HOST_IN: {},
                Job.RESOURCE_HOST_OUT: {},
                Job.RESOURCE_CPU: {},
            }

        resources = defaultdict(default_res_counter)

        # global job counter by type, accounts only executing jobs
        type_jobs_count = {}

        # counting current resource usage
        for job in active_jobs:
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

            check_types = [
                t
                for t, p in self.JOB_PRIORITIES.iteritems()
                if p >= self.JOB_PRIORITIES[job.type]
            ]
            logger.debug(
                'Job {}: checking job type resources according to priorities: {}'.format(
                    job.id,
                    check_types
                )
            )

            no_slots = False

            for res_type, res_val in self._unfold_resources(job.resources):

                max_res_limit = JOB_CONFIG.get(job.type, {}).get('resources_limits', {}).get(
                    res_type, float('inf')
                )

                used_res_count = sum(
                    resources[job_type][res_type].get(res_val, 0)
                    for job_type in check_types
                )

                if used_res_count >= max_res_limit:
                    # job of higher or same priority is using the resource
                    logger.debug(
                        'Job {job_id}: will be skipped, resource {res_type} / {res_val} is '
                        'occupuied by higher or same priority jobs (used {used_res_count} >= '
                        '{max_res_limit})'.format(
                            job_id=job.id,
                            res_type=res_type,
                            res_val=res_val,
                            used_res_count=used_res_count,
                            max_res_limit=max_res_limit,
                        )
                    )
                    no_slots = True
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

                cur_usage = resources[job.type][res_type].get(res_val, 0)
                max_usage = JOB_CONFIG.get(job.type, {}).get(
                    'resources_limits', {}).get(res_type, float('inf'))

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

                self._retry_jobs()

                ready_jobs = self._ready_jobs()

                for job in ready_jobs:
                    try:
                        self.__process_job(job)
                        job.save()
                    except LockError:
                        pass
                    except Exception as e:
                        logger.exception('Failed to process job {}'.format(job.id))
                        continue

        except LockFailedError as e:
            pass
        except Exception as e:
            logger.error('Failed to process existing jobs: {0}\n{1}'.format(
                e, traceback.format_exc()))
        finally:
            logger.info('Jobs execution finished')
            self.__tq.add_task_at(
                self.JOBS_EXECUTE,
                self.jobs_timer.next(),
                self._execute_jobs
            )

    def __process_job(self, job):

        logger.debug('Job {}, processing started: {}'.format(job.id, job.dump()))

        if job.status == Job.STATUS_NEW:
            try:
                job.start_ts = int(time.time())
                job.on_start()
                job.status = Job.STATUS_EXECUTING
            except JobBrokenError as e:
                logger.error('Job {}: failed to start job: {}'.format(job.id, e))
                job.status = Job.STATUS_BROKEN
                job.on_execution_interrupted(error_msg=str(e))
                return
            except Exception as e:
                logger.exception('Job {}: failed to start job'.format(job.id))
                job.status = Job.STATUS_PENDING
                job.on_execution_interrupted(error_msg=str(e))
                return
            finally:
                job._dirty = True
                job.update_ts = time.time()

        for task in job.tasks:

            if task.status == Task.STATUS_SKIPPED:
                continue

            if task.status == Task.STATUS_COMPLETED:
                continue

            if task.status == Task.STATUS_QUEUED:
                # NOTE: task can change status to 'executing' on this step, then
                # it is safe to continue task execution
                try:
                    self.__start_task(job, task)
                except JobBrokenError as e:
                    job.status = Job.STATUS_BROKEN
                    job.on_execution_interrupted(error_msg=str(e))
                    logger.error(
                        'Job {}, task {}: cannot execute task, not applicable for current storage '
                        'state: {}'.format(
                            job.id,
                            task.id,
                            e
                        )
                    )
                    break
                except Exception as e:
                    job.status = job.STATUS_PENDING
                    job.on_execution_interrupted(error_msg=str(e))
                    logger.exception('Job {}, task {}: failed to execute'.format(job.id, task.id))
                    break
                finally:
                    # TODO: move update_ts and _dirty update
                    job._dirty = True
                    job.update_ts = time.time()

                job.status = Job.STATUS_EXECUTING

            if task.status == Task.STATUS_EXECUTING:
                try:
                    self.__update_task(job, task)
                except Exception as e:
                    job.status = Job.STATUS_PENDING
                    job.on_execution_interrupted(error_msg=str(e))
                    break

                if task.status == Task.STATUS_EXECUTING:
                    # task is still executing, continue with the next job
                    break
                else:
                    # NOTE: this condition serves as optimization to lower mongo update query
                    # frequency for the job
                    # TODO: move update_ts and _dirty update (?)
                    job._dirty = True
                    job.update_ts = time.time()

            if task.status == Task.STATUS_FAILED:
                job.status = Job.STATUS_PENDING
                job.on_execution_interrupted()
                break

        finished_statuses = (Task.STATUS_COMPLETED, Task.STATUS_SKIPPED)

        if all(task.status in finished_statuses for task in job.tasks):
            logger.info('Job {}, tasks processing is finished'.format(job.id))
            try:
                job.status = Job.STATUS_COMPLETED
                job.complete(self)
            except RuntimeError as e:
                logger.error('Job {}, failed to complete job: {}'.format(job.id, e))
                raise

    def __start_task(self, job, task):
        logger.info('Job {}, executing new task {}'.format(job.id, task))

        task.add_history_record()

        try:
            task.on_exec_start(self)
            logger.info('Job {}, task {} preparation completed'.format(job.id, task.id))
        except Exception as e:
            logger.exception('Job {}, task {}: failed to execute task start handler'.format(
                job.id,
                task.id
            ))
            task.status = Task.STATUS_FAILED
            task.on_run_history_update(error=e)
            raise

        try:
            task.attempts += 1
            task.last_run_history_record.attempts = task.last_run_history_record.attempts + 1
            self.__execute_task(task)
            logger.info('Job {}, task {} execution successfully started'.format(
                job.id,
                task.id
            ))
        except Exception as e:
            try:
                task.on_exec_stop(self)
            except Exception:
                logger.exception('Job {}, task {}: failed to execute task stop handler'.format(
                    job.id,
                    task.id
                ))
                task.status = Task.STATUS_FAILED
                task.on_run_history_update(error=e)
                raise

            if isinstance(e, RetryError):
                logger.error('Job {}, task {}: retry error: {}'.format(job.id, task.id, e))
                if task.attempts < JOB_CONFIG.get('minions', {}).get('execute_attempts', 3):
                    # NOTE: no status change, will be retried
                    return

            task.status = Task.STATUS_FAILED
            task.on_run_history_update(error=e)
            raise

        task.status = Task.STATUS_EXECUTING

    def __update_task(self, job, task):
        logger.info('Job {}, task {} status update'.format(job.id, task.id))

        try:
            self.__update_task_status(task)
        except Exception as e:
            logger.exception('Job {}, task {}: failed to update status'.format(job.id, task.id))
            task.status = Task.STATUS_FAILED

            # TODO: should we call on_exec_stop here?

            task.on_run_history_update(error=e)
            raise

        try:
            if not task.finished(self):
                logger.debug('Job {}, task {} is not finished'.format(job.id, task.id))
                return

            task.status = (Task.STATUS_FAILED
                           if task.failed(self) else
                           Task.STATUS_COMPLETED)
        except Exception as e:
            logger.exception('Job {}, task {}: failed to check status'.format(job.id, task.id))
            task.status = Task.STATUS_FAILED

            # TODO: should we call on_exec_stop here?

            task.on_run_history_update(error=e)
            raise

        try:
            task.on_exec_stop(self)
        except Exception as e:
            logger.exception('Job {}, task {}: failed to execute task stop handler'.format(
                job.id,
                task.id
            ))
            task.status = Task.STATUS_FAILED
            raise

        task.on_run_history_update()
        logger.debug('Job {}, task {} is finished, status {}'.format(job.id, task.id, task.status))

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
            job = JobType.new(self, self.session, **params)
        except LockAlreadyAcquiredError as e:
            if not force:
                raise

            job_ids = e.holders_ids

            # check job types priority
            STOP_ALLOWED_TYPES = (JobTypes.TYPE_RECOVER_DC_JOB,
                                  JobTypes.TYPE_COUPLE_DEFRAG_JOB,
                                  JobTypes.TYPE_TTL_CLEANUP_JOB
                                  )

            if job_type not in (JobTypes.TYPE_RESTORE_GROUP_JOB, JobTypes.TYPE_MOVE_JOB, JobTypes.TYPE_BACKEND_MANAGER_JOB):
                raise

            jobs = self.job_finder.jobs(ids=job_ids, sort=False)
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

            # TODO: Lock is preferable here to prevent concurrent job processing, but it is not
            # acquired anymore for performance reasons. Think on acquiring per-job lock when
            # processing a job.

            # logger.debug('Lock acquiring')
            # with sync_manager.lock(self.JOBS_LOCK, timeout=self.JOB_MANUAL_TIMEOUT):
            #     logger.debug('Lock acquired')
            #     self._stop_jobs(jobs)
            self.stop_jobs_list(jobs)

            logger.info('Retrying job creation')
            job = JobType.new(self, self.session, **params)

        job.collection = self.job_finder.collection

        try:
            job.create_tasks(self)
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

                jobs = self.job_finder.jobs(ids=job_uids, sort=False)
                self.stop_jobs_list(jobs)

        except LockFailedError as e:
            raise
        except Exception as e:
            logger.error('Failed to stop jobs: {0}\n{1}'.format(e,
                traceback.format_exc()))
            raise

        return [job.dump() for job in jobs]

    def stop_jobs_list(self, jobs):

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
                            self.minions_monitor._terminate_cmd(task.host, task.minion_cmd_id)
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
        job.save()

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

    def _check_groupset(self, group_ids, type):
        if type == storage.GROUPSET_LRC:
            for group_id in group_ids:
                group = storage.groups[group_id]
                if group.type != storage.Group.TYPE_UNCOUPLED_LRC_8_2_2_V1:
                    raise ValueError(
                        'Group {group} has unexpected type {group_type}, expected '
                        '{expected_type}'.format(
                            group=group,
                            group_type=group.type,
                            expected_type=storage.Group.TYPE_UNCOUPLED_LRC_8_2_2_V1,
                        )
                    )
                if group.status != storage.Status.COUPLED:
                    raise ValueError(
                        'Group {group} has unexpected status {group_status}, expected '
                        '{expected_status}'.format(
                            group=group,
                            group_status=group.status,
                            expected_status=storage.Status.COUPLED,
                        )
                    )
                if tuple(group.meta['lrc_groups']) != tuple(group_ids):
                    raise ValueError(
                        'Group {group} cannot be used as a part of groupset {groupset}, '
                        'linked groups are {linked_groups}'.format(
                            group=group,
                            groupset=group_ids,
                            linked_groups=group.meta['lrc_groups'],
                        )
                    )

    def _select_groups_for_groupset(self, type, mandatory_dcs, skip_groups=None):
        '''Select appropriate groups for a new groupset of selected 'type' for 'couple'.

        Parameters:
            couple - a couple to select groupset for;
            type - new groupset's type;
            mandatory_dcs -  if supplied, selected groups will be located in these dcs
                according to groupset type distribution rules. See certain type
                implementations for further details;
            skip_groups - a list of groups to skip when selecting new groups;
        '''
        if type == storage.GROUPSET_LRC:
            return storage.Lrc.select_groups_for_groupset(
                mandatory_dcs=mandatory_dcs,
                skip_groups=skip_groups,
            )
        else:
            raise ValueError('Unsupported groupset type: {}'.format(type))

    def _compose_groupset_metakey(self, groupset_type, groups, couple, settings):
        """Construct metakey for a new groupset that does not exist at the moment.

        This is a helper function that creates a dummy groupset object to construct
        metakey and then destroys it.
        """

        groupset = groupset_type(groups)
        metakey = groupset.compose_group_meta(
            couple=couple,
            settings=settings,
        )
        groupset.destroy()
        return metakey

    @h.concurrent_handler
    def add_groupset_to_couple(self, request):
        if 'couple' not in request:
            raise ValueError('Request should contain "couple" field')
        couple = storage.groupsets.get_couple(request['couple'])

        if 'type' not in request:
            raise ValueError('Request should contain "type" field')

        if 'settings' not in request:
            raise ValueError('Request should contain "settings" field')

        if 'groupset' not in request:
            groupset = self._select_groups_for_groupset(
                type=request['type'],
                mandatory_dcs=request.get('mandatory_dcs', []),
            )
        else:
            groupset = [int(group_id) for group_id in request['groupset'].split(':')]

        logger.info('Selected groups for groupset: {}'.format(groupset))

        self._check_groupset(groupset, type=request['type'])

        settings = request['settings']
        Groupset = storage.Groupsets.make_groupset_type(
            type=request['type'],
            settings=settings,
        )
        Groupset.check_settings(settings)

        if request['type'] == storage.GROUPSET_LRC:
            job_type = JobTypes.TYPE_ADD_LRC_GROUPSET_JOB
        else:
            raise ValueError('Unsupported groupset type: {}'.format(request['type']))

        job = self._create_job(
            job_type=job_type,
            params={
                'metakey': self._compose_groupset_metakey(
                    groupset_type=Groupset,
                    groups=[storage.groups[g] for g in groupset],
                    couple=couple,
                    settings=settings,
                ),
                'groups': list(groupset),
                'couple': str(couple),
                'part_size': settings['part_size'],
                'scheme': settings['scheme'],
            },
        )

        return job.dump()


class JobFinder(object):

    def __init__(self, db):
        self.collection = Collection(db[config['metadata']['jobs']['db']], 'jobs')

    @h.concurrent_handler
    def get_job_list(self, request):
        try:
            options = request[0]
        except (TypeError, IndexError):
            options = {}

        statuses = options.get('statuses', None)
        job_type = options.get('job_type', None)
        groups = options.get('groups', None)
        limit = options.get('limit', None)
        offset = int(options.get('offset', 0))
        ids = options.get('ids', None)

        jobs_list = Job.list(
            self.collection,
            status=statuses,
            type=job_type,
            group=groups,
            id=ids,
        )
        total_jobs = jobs_list.count()

        if limit is not None:
            jobs_list = jobs_list[offset:offset + int(limit)]

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

        return [j.human_dump() for j in self.jobs(ids=job_ids, sort=False)]

    def _get_job(self, job_id):
        jobs_list = Job.list(self.collection,
            id=job_id).limit(1)
        if not jobs_list:
            raise ValueError('Job {0} is not found'.format(job_id))
        job = JobFactory.make_job(jobs_list[0])
        job.collection = self.collection
        return job

    def jobs_count(self, types=None, statuses=None, **kwargs):
        return Job.list(
            self.collection,
            status=statuses,
            type=types,
            **kwargs
        ).count()

    def jobs(self, types=None, statuses=None, ids=None, groups=None, sort=True):
        jobs = []
        if sort:
            job_listing = Job.list
        else:
            # TODO: replace temporary solution when 'list' method gets
            # refactored
            job_listing = Job.list_no_sort

        job_list = job_listing(
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
            statuses=(Job.STATUS_NOT_APPROVED,
                      Job.STATUS_NEW,
                      Job.STATUS_EXECUTING,
                      Job.STATUS_PENDING,
                      Job.STATUS_BROKEN),
            sort=False,
        )

        uncoupled_groups = []
        for job in jobs:
            uncoupled_groups.extend(job.involved_uncoupled_groups)

        return uncoupled_groups
