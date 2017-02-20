from copy import copy, deepcopy
import heapq
from logging import getLogger
import storage
import time
from collections import defaultdict, Counter
import itertools

import pymongo

import jobs
from jobs.job import Job
from jobs.job_factory import JobFactory
from mastermind_core.config import config
from mastermind_core.db.mongo.pool import Collection
from sync import sync_manager
from sync.error import LockFailedError, LockAlreadyAcquiredError
from timer import periodic_timer
from history import GroupHistoryFinder
import timed_queue

logger = getLogger('mm.sched')


class Planner(object):

    RESOURCE_GROUP = "group"

    def __init__(self, db, job_processor):

        self.params = config.get('scheduler', {})

        logger.info('Scheduler initializing')

        self.job_processor = job_processor
        self.__tq = timed_queue.TimedQueue()

        # store a list of starters in order to manage them and make a better schedule in the future
        self.starters = []

        if config['metadata'].get('scheduler', {}).get('db'):
            self.collection = Collection(db[config['metadata']['scheduler']['db']], 'scheduler')

        self.res = defaultdict(list)
        self.history_data = {}

        # store job limits instead of regular updating it from the config
        self.res_limits = defaultdict(dict)
        for res_type, job_type in itertools.product(jobs.Job.RESOURCE_TYPES, jobs.JobTypes.AVAILABLE_TYPES):
            self.res_limits[job_type][res_type] = config.get('jobs', {}).get(job_type, {}).get(
                            'resources_limits', {}).get(res_type, 1)
        logger.info("Obtained res limits are {}".format(self.res_limits))

    def _start_tq(self):
        self.__tq.start()

    def register_periodic_func(self, starter_func, period_default, starter_name=None, lock_name=None):
        """
        Guarantee that starter_func would be run within specified period under a zookeeper lock
        :param starter_func - a function to be run
        :param period_default - the default value of period to run the function with
            if the "{starter_name}_period" is not specified under {starter_name} sub-section with sched config section
        :param starter_name - a logical name of starter. Name of starter subsection within sched section in config.
                            Defaults to starter_func.__name__
        :param lock_name - a name of lock in zookeeper that protects starter from simultaneous execution).
                            Defaults to "scheduler/{starter_name}
        """

        starter_name = starter_name or starter_func.__name__
        lock_name = lock_name or "scheduler/{}".format(starter_name)
        period_param = "{}_period".format(starter_name)
        period_val = self.params.get(starter_name, {}).get(period_param, period_default)

        logger.info("Registering periodic starter func {} to be raised on {} period".format(starter_name, period_val))

        if not self.params.get(starter_name, {}).get('enabled', False):
            logger.info("Starter {} is disabled".format(starter_name))
            return

        timer = periodic_timer(seconds=period_val)

        def _starter_periodic_func():
            try:
                logger.info('Starting {}'.format(starter_name))

                with sync_manager.lock(lock_name, blocking=False):
                    starter_func()

            except LockFailedError:
                logger.info('Another {} is already running under {} lock'.format(starter_name, lock_name))
            except:
                logger.exception('Failed to {}'.format(starter_name))
            finally:
                logger.info('{} finished'.format(starter_name))
                self.__tq.add_task_at(
                    starter_name,
                    timer.next(),
                    _starter_periodic_func)

        self.__tq.add_task_at(
            starter_name,
            timer.next(),
            _starter_periodic_func
        )

        starter = {
            "starter_func": starter_func,
            "starter_name": starter_name,
            "starter_lock": lock_name,
            "starter_timer": timer,
        }

        self.starters.append(starter)

        logger.info("Registered starter function")

    def update_resource_stat(self):

        # update jobs list
        active_jobs = self.job_processor.job_finder.jobs(
            statuses=jobs.Job.ACTIVE_STATUSES,
            sort=False,
        )

        # update job count per each type
        self.job_count = Counter()

        # update host stat
        self.res = defaultdict(list)

        for job in active_jobs:
            self.job_count[job.type] += 1
            res_demand = self.convert_resource_representation(job.resources, job._involved_groups, job.type)
            self.add_to_resource_stat(res_demand, job.id)

    def convert_resource_representation(self, resources, groups, job_type):
        """
        The function takes old resources (equivalent to job.resources), "resources_limits" for each job type from config
        and produces a new resource representation
        :param resources:  Dict of res_type where each value is a list of node addr or tuples (node_addr, fs)
        :param groups: List of groups ids
        :param job_type: job_type
        :param errors: list of strings
        :return: a dict where keys are tuples (res_type, group) or (res_type, node_addr) or (res_type, node_addr, fs_id)
                and values are consumption level
        """
        res_demand = {}

        for g in groups:
            res_demand[(self.RESOURCE_GROUP, g)] = 100  # Assume that group is always 100% utilized

        for res_type, res_vals in resources.iteritems():
            for res_val in res_vals:
                if res_type in (Job.RESOURCE_CPU, Job.RESOURCE_HOST_IN, Job.RESOURCE_HOST_OUT):
                    # Percent of resource consumed. 1..100 (zero is not valid since otherwise the resource is not consumed)
                    consumption = 100 / max(self.res_limits[job_type][res_type], 1)
                    res_demand[(res_type, res_val)] = consumption
                elif res_type == Job.RESOURCE_FS:
                    # Assume that FS is always 100% utilized
                    res_demand[(Job.RESOURCE_FS, res_val[0], res_val[1])] = 100

        return res_demand

    def add_to_resource_stat(self, res_demand, job_id):
        """
        Add to saved resource representation resource consumption for the specified job. We could skip this and call
        update_resource_stat after each iteration but this is rather expensive. So while the representation won't be
        actual, let's keep it
        :param res_demand: a dict where keys are tuples (res_type, node_addr\group_id, [fs_id])
                            and values are consumption level
        :param job_id: resource owner
        """
        for r, d in res_demand.iteritems():
            self.res[r].append((d, job_id))

    def cancel_crossing_jobs(self, job_type, sched_params, demand):
        """
        Try to cancel jobs that are concurrent with a job we are trying to create (if there are some)
        :param job_type: the type of the job we are trying to create
        :param sched_params: scheduling params of the job we are trying to create
        :param demand: required resources (see convert_resource_representation)
        :return: True if cancellation was successful, False otherwise
        """
        force = sched_params.get('force', False)

        # XXX: these jobs should not be hard-coded. This should be present in config
        STOP_ALLOWED_TYPES = (jobs.JobTypes.TYPE_RECOVER_DC_JOB,
                              jobs.JobTypes.TYPE_COUPLE_DEFRAG_JOB,
                              jobs.JobTypes.TYPE_TTL_CLEANUP_JOB
                              )

        # get a list of jobs crossing with the planned one based on resource demand
        job_ids = []
        for r in demand:
            job_ids.extend([v[1] for v in self.res[r]])

        if len(job_ids) == 0:
            logger.info("No crossing jobs {}".format(demand))
            return True

        existing_jobs = self.job_processor.job_finder.jobs(ids=job_ids, sort=False)
        cancellable_jobs_ids = []
        completed_jobs_ids = []
        for job in existing_jobs:

            if self.job_processor.JOB_PRIORITIES[job.type] >= self.job_processor.JOB_PRIORITIES[job_type] and not force:
                logger.info('Cannot stop job {} type {} since it has >= priority and no force'.format(job.id, job.type))
                continue

            # The time has passed since self.res was built. existing_jobs have more actual statuses
            if job.status in (jobs.Job.STATUS_CANCELLED, jobs.Job.STATUS_COMPLETED):
                # Rebuilding the entire table is complicated. Getting involed_groups or resources is not safe
                # Thus clean only those of resources we are crossing here. The rest would be "fixed" in next crossings
                completed_jobs_ids.append(job.id)
                continue

            if job.type not in STOP_ALLOWED_TYPES:
                if job.status in (jobs.Job.STATUS_NOT_APPROVED, jobs.Job.STATUS_NEW):
                    # These jobs could be cancelled, but there is a potential race between making decision
                    #  and actual job status. Since we are prohibited to cancel running jobs of not STOP_ALLOWED_TYPES
                    # skip cancellation for a while. That would be fixed as soon as job queries would be introduced
                    logger.info('Job was not running {} of type {}'.format(job.id, job.type))

                logger.info('Cannot stop job {} of type {}'.format(job.id, job.type))
                continue

            cancellable_jobs_ids.append(job.id)

        if len(cancellable_jobs_ids) == 0 and len(completed_jobs_ids) == 0:
            logger.info("No jobs to cancel while {} are blocking".format(len(job_ids)))
            return False

        logger.info("Analyzing demand {} while cancellable_jobs are {}".format(demand, cancellable_jobs_ids))

        # Analyze whether stopping of cancelable_jobs would be enough
        for res in demand:
            res_demand = demand[res]  # how much of resource of this type and id, the job wants to consume
            consumption_if_cancel = 0  # how much resource would be still consumed when we'd cancel all cancellable jobs

            for j in self.res[res]:
                if j[1] in completed_jobs_ids:
                    self.res[res].remove(j)
                    continue
                if j[1] not in cancellable_jobs_ids:
                    consumption_if_cancel += j[0]

            if res_demand > (100 - consumption_if_cancel):
                logger.info("No sense to cancel cause of res={} (demand={} < usage_if_cancel={}, jobs are {})".format(
                    res, res_demand, consumption_if_cancel, self.res[res]))
                return False

        # XXX: Now we cancel all jobs while we may be satisfied with less amount. Fix that

        try:
            self.job_processor.stop_jobs_list(filter(lambda x: x.id in cancellable_jobs_ids, existing_jobs))
        except:
            logger.exception("Failed to cancel jobs {}".format(cancellable_jobs_ids))
            return False

        logger.info("Successfully cancelled {}".format(cancellable_jobs_ids))
        return True

    # TODO: rename process_jobs into create_jobs
    def process_jobs(self, job_type, jobs_param_list, sched_params):
        """
        Scheduler creates jobs with specified params of specified type.
        :param job_type: job type
        :param jobs_param_list: a list of dictionaries with params for jobs created.
                                A number of job created <= len(jobs_param_list)
        :param sched_params: in a common case sched params are stored in config in "scheduler/{starter}" section.
                            But in certain cases the starter may want to redefine some parameters.
                            For example, one may want to increase priority or decrease max_deadline_time
        :return: a number of generated jobs
        """

        self.update_resource_stat()

        max_jobs = sched_params.get('max_executing_jobs')
        if max_jobs:
            job_count = self.job_count[job_type]
            if job_count >= max_jobs:
                logger.info('Found {} unfinished jobs (>= {}) of {} type'.format(job_count, max_jobs, job_type))
                return 0
            max_jobs -= job_count

        created_job = 0

        # common param for all types of jobs
        need_approving = not sched_params.get('autoapprove', False)

        job_class = JobFactory.make_job_type(job_type)

        if not hasattr(job_class, 'report_resources'):
            logger.error("Couldn't schedule the job {}. Add static report_resources function".format(job_type))
            return 0

        job_report_resources = job_class.report_resources

        for job_param in jobs_param_list:

            # Get resource demand and verify whether run is possible
            old_res = job_report_resources(job_param)
            res = self.convert_resource_representation(old_res['resources'], old_res['groups'], job_type)

            if not self.cancel_crossing_jobs(job_type, sched_params, res):
                continue

            job_param['need_approving'] = need_approving

            try:
                job = self.job_processor._create_job(
                        job_type=job_type,
                        params=job_param,
                        force=False)

            except LockAlreadyAcquiredError as e:
                logger.error("Failed to create a new job {} since couple/group are already locked {}".format(job_type, e))
                # update resource stat, something has changed in the cluster or there is an error in the scheduler
                self.update_resource_stat()
                continue

            except:
                logger.exception("Creating job {} with params {} has excepted".format(job_type, job_param))
                continue

            created_job += 1
            if max_jobs and created_job >= max_jobs:
                return created_job

            self.add_to_resource_stat(res, job.id)

        # do not update self.job_count, since they would be rewritten on next update & they don't influence calculations

        # TODO: return a list of created jobs
        return created_job

    def get_history(self):

        if len(self.history_data) != len(storage.couples):
            self.sync_history()
        return self.history_data

    def sync_history(self):

        cursor = self.collection.find()

        logger.info('Sync recover data is required: {} records/{} couples, cursor {}'.format(
                len(self.history_data), len(storage.couples), cursor.count()))

        recover_data_couples = set()
        history = {}

        for data_record in cursor:
            couple_str = data_record['couple']
            history[couple_str] = {'recover_ts': data_record.get('recover_ts')}
            recover_data_couples.add(couple_str)

        ts = int(time.time())

        # XXX: rework, it is too expensive
        storage_couples = set(str(c) for c in storage.couples.keys())
        add_couples = list(storage_couples - recover_data_couples)
        remove_couples = list(recover_data_couples - storage_couples)

        logger.info('Couples to add {}, couple to remove {}'.format(add_couples, remove_couples))

        offset = 0
        OP_SIZE = 200
        while offset < len(add_couples):
            bulk_op = self.collection.initialize_unordered_bulk_op()
            bulk_add_couples = add_couples[offset:offset + OP_SIZE]
            for couple in bulk_add_couples:
                bulk_op.insert({'couple': couple, 'recover_ts': ts})
            res = bulk_op.execute()
            if res['nInserted'] != len(bulk_add_couples):
                raise ValueError('Failed to add couples recover data: {}/{} ({})'.format(
                    res['nInserted'], len(bulk_add_couples), res))
            offset += res['nInserted']

        offset = 0
        while offset < len(remove_couples):
            bulk_op = self.collection.initialize_unordered_bulk_op()
            bulk_remove_couples = remove_couples[offset:offset + OP_SIZE]
            bulk_op.find({'couple': {'$in': bulk_remove_couples}}).remove()
            res = bulk_op.execute()
            if res['nRemoved'] != len(bulk_remove_couples):
                raise ValueError('failed to remove couples recover data: {0}/{1} ({2})'.format(
                    res['nRemoved'], len(bulk_remove_couples), res))
            offset += res['nRemoved']

        self.history_data = history

    def update_recover_ts(self, couple_id, ts):
        ts = int(ts)
        res = self.collection.update(
            {'couple': couple_id},
            {'couple': couple_id, 'recover_ts': ts},
            upsert=True)
        if res['ok'] != 1:
            logger.error('Unexpected mongo response during recover ts update: {}'.format(res))
            raise RuntimeError('Mongo operation result: {}'.format(res['ok']))

        # update cached representation
        self.history_data[couple_id] = {'recover_ts': ts}