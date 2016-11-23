import heapq
import logging
import time

from errors import CacheUpstreamError
from external_storage import ExternalStorageConvertQueue, ExternalStorageConvertQueueItem
import helpers
import inventory
import jobs
from mastermind_core.config import config
import storage
from sync import sync_manager
from sync.error import LockFailedError


logger = logging.getLogger('mm.planner.converting')


JOBS_PARAMS = config.get('jobs', {})
CONVERTING_PLANNER_PARAMS = config.get('planner', {}).get('external_storage_converting', {})
LIMITS = CONVERTING_PLANNER_PARAMS.get('limits', {})


class HostsQueue(object):
    def __init__(self):
        self.hosts = []

    def add_host(self, host_state):
        heapq.heappush(self.hosts, (len(host_state.jobs), host_state))

    def __iter__(self):
        hosts_count = len(self.hosts)
        for _ in xrange(hosts_count):
            _, host_state = heapq.heappop(self.hosts)
            yield host_state
            heapq.heappush(self.hosts, (len(host_state.jobs), host_state))


class ExternalStorageConvertingPlanner(object):

    CONVERTING_CANDIDATES = 'converting_candidates'
    CONVERTING_LOCK = 'planner/external_storage_converting'

    def __init__(self, db, job_processor, namespaces_settings):

        self.job_processor = job_processor
        self.namespaces_settings = namespaces_settings
        self.queue = ExternalStorageConvertQueue(db)

    def schedule_tasks(self, tq):

        self._planner_tq = tq

        if CONVERTING_PLANNER_PARAMS.get('enabled', False):
            self._planner_tq.add_task_in(
                self.CONVERTING_CANDIDATES,
                10,
                self._converting_candidates
            )

    SUPPORTED_GROUPSET_TYPES = (
        storage.GROUPSET_LRC,
    )

    @staticmethod
    def _get_convert_groupset_type(groupset_settings):
        if 'type' not in groupset_settings:
            raise ValueError(
                'Converting groupset type is not set '
                '(["planner"]["external_storage_converting"]["convert_groupset"]["type"])'
            )

        if groupset_settings['type'] not in ExternalStorageConvertingPlanner.SUPPORTED_GROUPSET_TYPES:
            raise ValueError('Unsupported groupset type: {}'.format(groupset_settings['type']))

        settings = groupset_settings.get('settings', {})

        return storage.Groupsets.make_groupset_type(
            type=groupset_settings['type'],
            settings=settings,
        )

    def _converting_candidates(self):
        try:
            logger.info('Starting external storage converting planner')

            if 'convert_groupset' not in CONVERTING_PLANNER_PARAMS:
                raise RuntimeError(
                    'Convert groupset type is not set '
                    '(["planner"]["external_storage_converting"]["convert_groupset"])'
                )

            groupset_settings = CONVERTING_PLANNER_PARAMS['convert_groupset']
            convert_groupset_type = self._get_convert_groupset_type(groupset_settings)

            if convert_groupset_type is storage.Lrc822v1Groupset:
                job_type = jobs.JobTypes.TYPE_CONVERT_TO_LRC_GROUPSET_JOB
            else:
                raise ValueError(
                    'Convert job type for groupset type {} is unknown'.format(convert_groupset_type)
                )

            max_converting_jobs = JOBS_PARAMS.get(job_type, {}).get('max_executing_jobs', 3)

            # prechecking for new or pending tasks
            count = self.job_processor.job_finder.jobs_count(
                types=job_type,
                statuses=jobs.Job.ACTIVE_STATUSES,
            )

            if count >= max_converting_jobs:
                logger.info(
                    'Found {} unfinished converting jobs (>= {})'.format(count, max_converting_jobs)
                )
                return

            with sync_manager.lock(ExternalStorageConvertingPlanner.CONVERTING_LOCK, blocking=False):
                self._update_queue_state()
                self._do_converting_candidates(job_type, max_converting_jobs - count)

        except LockFailedError:
            logger.info('External storage converting planner is already running')
        except Exception:
            logger.exception('External storage converting planner failed')
        finally:
            logger.info('External storage converting planner finished')
            self._planner_tq.add_task_in(
                task_id=self.CONVERTING_CANDIDATES,
                secs=CONVERTING_PLANNER_PARAMS.get('generate_plan_period', 1800),
                function=self._converting_candidates,
            )

    def _update_queue_state(self):
        converting_items = self.queue.items(status=ExternalStorageConvertQueue.STATUS_CONVERTING)

        converting_jobs = {}

        for item in converting_items:
            if item.job_id is None:
                logger.error(
                    'Converting queue item for src id {} does not contain job id'.format(
                        item.id
                    )
                )
                continue
            converting_jobs[item.job_id] = item

        for job in self.job_processor.job_finder.jobs(ids=converting_jobs.keys()):

            item = converting_jobs[job.id]

            logger.debug(
                'Converting queue item for src id {src_storage_id}: job {job_id} status: '
                '{status}'.format(
                    src_storage_id=item.id,
                    job_id=job.id,
                    status=job.status,
                )
            )

            if job.status == jobs.Job.STATUS_COMPLETED:
                logger.info(
                    'Converting queue item for src id {src_storage_id}: marked completed'.format(
                        src_storage_id=item.id,
                    )
                )
                item.status = ExternalStorageConvertQueue.STATUS_COMPLETED
                # NOTE: couples may be not known when converting is started,
                # e.g. when using determine_data_size
                item.couples = job.couples
                item._dirty = True
            elif job.status == jobs.Job.STATUS_CANCELLED:
                logger.info(
                    'Converting queue item for src id {src_storage_id}: marked queued'.format(
                        src_storage_id=item.id,
                    )
                )
                item.status = ExternalStorageConvertQueue.STATUS_QUEUED
                item._dirty = True

            item.save()

    def _check_host_resources(self, host_state, job_type_limits):
        for res_type in jobs.Job.RESOURCE_TYPES:
            limit = job_type_limits.get(res_type)

            if limit and host_state.resources[res_type] >= limit:
                logger.info(
                    'Host {host}: skipped, resource {res_type} limit reached ({host_res} >= '
                    '{limit})'.format(
                        host=host_state.hostname,
                        res_type=res_type,
                        host_res=host_state.resources[res_type],
                        limit=limit,
                    )
                )
                return False

        return True

    def _convert_queue_items(self, slots):
        # in case of low values of 'slots'
        min_chunk_size = 10

        skip = 0
        chunk_size = min(int(slots * 1.5), min_chunk_size)

        dcs = LIMITS.get('dcs')

        while True:
            items = self.queue.items(
                dcs=dcs,
                status=ExternalStorageConvertQueue.STATUS_QUEUED,
                limit=chunk_size,
                skip=skip,
                sort_by_priority=True,
            )
            received_result = False
            for item in items:
                yield item
                received_result |= True

            if not received_result:
                break

            skip += chunk_size

    def _do_converting_candidates(self, job_type, job_slots):
        active_jobs = self.job_processor.job_finder.jobs(
            statuses=jobs.Job.ACTIVE_STATUSES,
            types=job_type,
        )

        storage_state = StorageState.current()
        storage_state.account_jobs(active_jobs)

        job_type_limits = JOBS_PARAMS.get(job_type, {}).get('resources_limits', {})

        # NOTE: Run at most one job per host by default
        per_host_limit = LIMITS.get('per_host', 1)

        host_slots = 0

        hosts_queue = HostsQueue()

        for dc_state in storage_state.dcs.itervalues():
            for host_state in dc_state.hosts.itervalues():
                if not self._check_host_resources(host_state, job_type_limits):
                    continue

                if len(host_state.jobs) >= per_host_limit:
                    logger.info(
                        'Host {host}: skipped, convert jobs limit per host is reached ({jobs} >= '
                        '{limit})'.format(
                            host=host_state.hostname,
                            jobs=len(host_state.jobs),
                            limit=per_host_limit,
                        )
                    )
                    continue

                host_slots += per_host_limit - len(host_state.jobs)
                hosts_queue.add_host(host_state)

        logger.info('Available slots: job slots {}, host slots {}'.format(
            job_slots,
            host_slots,
        ))

        slots = min(job_slots, host_slots)

        if not slots:
            return

        host_states = iter(hosts_queue)
        queue_items = self._convert_queue_items(slots)

        created_jobs_count = 0

        # TODO: make top threshold for a number of jobs created at a time

        while created_jobs_count < slots:
            host_state = next(host_states)
            try:
                item = next(queue_items)
            except StopIteration:
                logger.info('Convert queue is exhausted')
                break

            if not inventory.is_external_storage_ready(item.id):
                logger.info(
                    'External storage with id {} is not ready to be converted'.format(item.id)
                )
                continue

            logger.info(
                'Trying to create convert external storage job: src id "{}", host to run on: '
                '{}'.format(
                    item.id,
                    host_state.hostname,
                )
            )

            job = self._convert_external_storage_to_groupset(
                groupset_type=item.groupset['type'],
                groupset_settings=item.groupset['settings'],
                namespace=item.namespace,
                src_storage=item.src_storage,
                src_storage_options=item.src_storage_options,
                determine_data_size=item.determine_data_size,
                converting_host=host_state.host.addr,
                need_approving=not CONVERTING_PLANNER_PARAMS.get('autoapprove', False)
            )

            logger.info(
                'Convert external storage job for src id "{}" created: {}'.format(
                    item.id,
                    job.id
                )
            )

            created_jobs_count += 1

            host_state.add_job(job)

            item.status = ExternalStorageConvertQueue.STATUS_CONVERTING
            item.job_id = job.id
            item._dirty = True
            item.save()

    @helpers.concurrent_handler
    def get_convert_queue_item(self, request):
        if not request.get('id'):
            raise ValueError('Request should contain "id" field')

        try:
            items = self.queue.items(ids=[request['id']])
            item = next(items)
        except StopIteration:
            raise ValueError('External storage with id {} is not found'.format(request['id']))

        return item.dump()

    @helpers.concurrent_handler
    def update_convert_queue_item(self, request):
        if not request.get('id'):
            raise ValueError('Request should contain "id" field')

        try:
            items = self.queue.items(ids=[request['id']])
            item = next(items)
        except StopIteration:
            raise ValueError('External storage with id {} is not found'.format(request['id']))

        if request.get(ExternalStorageConvertQueueItem.PRIORITY):
            item.priority = request[ExternalStorageConvertQueueItem.PRIORITY]
            item._dirty = True

        item.save()

        return item.dump()

    @helpers.concurrent_handler
    def convert_external_storage_to_groupset(self, request):
        if not request.get('src_storage'):
            raise ValueError('Request should contain "src_storage" field')

        if 'src_storage_options' not in request:
            raise ValueError('Request should contain "src_storage_options" field')

        if 'type' not in request:
            raise ValueError('Request should contain groupset "type" field')

        if 'settings' not in request:
            raise ValueError('Request should contain "settings" field')

        if 'namespace' not in request:
            raise ValueError('Request should contain "namespace" field')

        job = self._convert_external_storage_to_groupset(
            groupset_type=request['type'],
            groupset_settings=request['settings'],
            namespace=request['namespace'],
            src_storage=request['src_storage'],
            src_storage_options=request['src_storage_options'],
            groupsets=request.get('groupsets'),
            mandatory_dcs=request.get('mandatory_dcs'),
            determine_data_size=request.get('determine_data_size', False),
        )

        return job.dump()

    def _convert_external_storage_to_groupset(self,
                                              groupset_type,
                                              groupset_settings,
                                              namespace,
                                              src_storage,
                                              src_storage_options=None,
                                              groupsets=None,
                                              mandatory_dcs=None,
                                              determine_data_size=False,
                                              converting_host=None,
                                              need_approving=True):

        start_ts = time.time()

        Groupset = storage.groupsets.make_groupset_type(
            type=groupset_type,
            settings=groupset_settings,
        )

        Groupset.check_settings(groupset_settings)

        if groupsets is None:
            groupsets = []

        if determine_data_size or groupsets:
            # job will use supplied groupsets and choose additional groupsets if
            # necessary
            groups = [
                [int(group_id) for group_id in groupset.split(':')]
                for groupset in groupsets
            ]
        else:
            # choose a single groupset for job to use (and to lock before job starts)
            groups = [
                self.job_processor._select_groups_for_groupset(
                    type=groupset_type,
                    mandatory_dcs=mandatory_dcs or [],
                )
            ]

        logger.info('Groupset selection finished: dds {}, groups {}, {:.2f}s'.format(
            determine_data_size,
            groups,
            time.time() - start_ts
        ))

        for groupset in groups:
            self.job_processor._check_groupset(groupset, type=groupset_type)

        namespace_id = namespace
        ns_settings = self.namespaces_settings.get(namespace_id)
        if ns_settings.deleted:
            raise ValueError('Namespace "{}" is deleted'.format(namespace_id))

        if namespace_id not in storage.namespaces:
            ns = storage.namespaces.add(namespace_id)
        else:
            ns = storage.namespaces[namespace_id]

        if groupset_type == storage.GROUPSET_LRC:
            job_type = jobs.JobTypes.TYPE_CONVERT_TO_LRC_GROUPSET_JOB
        else:
            raise ValueError('Unsupported groupset type: {}'.format(groupset_type))

        job = self.job_processor._create_job(
            job_type=job_type,
            params={
                'ns': ns.id,
                'groups': groups,
                'mandatory_dcs': mandatory_dcs or [],
                'part_size': groupset_settings['part_size'],
                'scheme': groupset_settings['scheme'],
                'determine_data_size': determine_data_size,
                'src_storage': src_storage,
                'src_storage_options': src_storage_options or {},
                'converting_host': converting_host,
                'need_approving': need_approving,
            },
        )

        return job


class StorageState(object):

    def __init__(self):
        self.dcs = {
            dc: DcState(self, dc)
            for dc in self.__dcs()
        }

    @staticmethod
    def __dcs():
        dcs = set()
        for host in storage.hosts:
            try:
                dcs.add(host.dc)
            except CacheUpstreamError:
                continue
        return dcs

    @staticmethod
    def current():
        state = StorageState()

        for group in storage.groups.keys():

            for nb in group.node_backends:
                if nb.stat is None:
                    continue
                try:
                    dc = nb.node.host.dc
                    hostname = nb.node.host.hostname
                except CacheUpstreamError:
                    logger.warn('Skipping host {} because of cache failure'.format(nb.node.host))
                    continue

                state.dcs[dc].hosts.setdefault(
                    nb.node.host,
                    HostState(state.dcs[dc], nb.node.host, hostname)
                )

        return state

    def account_resources(self, job, resource_type):
        for addr, _ in job.resources.get(resource_type, []):

            if addr not in storage.hosts:
                logger.error('Job {}: host with address {} is not found in storage'.format(job.id, addr))
                continue
            host = storage.hosts[addr]
            try:
                dc_state = self.dcs[host.dc]
            except CacheUpstreamError:
                continue
            except KeyError:
                logger.error('Storage state does not contain state for dc "{}"'.format(host.dc))
                continue
            try:
                host_state = dc_state.hosts[host]
            except KeyError:
                logger.error('Dc "{dc}" state does not contain state for host "{host}"'.format(
                    dc=host.dc,
                    host=host.hostname,
                ))
                continue

            host_state.resources[resource_type] += 1

    def account_job(self, job):

        if isinstance(job, jobs.ConvertToLrcGroupsetJob):

            if job.converting_host not in storage.hosts:
                logger.error(
                    'Job {}: host with address {} is not found in storage'.format(
                        job.id,
                        job.converting_host
                    )
                )
                return

            host = storage.hosts[job.converting_host]
            try:
                dc_state = self.dcs[host.dc]
            except CacheUpstreamError:
                return
            except KeyError:
                logger.error('Storage state does not contain state for dc "{}"'.format(host.dc))
                return
            try:
                host_state = dc_state.hosts[host]
            except KeyError:
                logger.error('Dc "{dc}" state does not contain state for host "{host}"'.format(
                    dc=host.dc,
                    host=host.hostname,
                ))
                return

            host_state.add_job(job)

    def account_jobs(self, active_jobs):
        for job in active_jobs:
            # TODO: generalize 'account_resources' method
            # for res_type in jobs.Job.RESOURCE_TYPES:
            #     self.account_resources(job, res_type)

            self.account_resources(job, jobs.Job.RESOURCE_CPU)
            self.account_job(job)


class DcState(object):
    """ Stores accumulated information about certain dc
    """
    def __init__(self, storage_state, dc):
        self.storage_state = storage_state
        self.dc = dc
        self.hosts = {}


class HostState(object):
    """ Stores accumulated information about certain host
    """
    def __init__(self, dc_state, host, hostname):
        self.dc_state = dc_state
        self.host = host
        self.hostname = hostname
        self.resources = {
            res_type: 0
            for res_type in jobs.Job.RESOURCE_TYPES
        }
        self.jobs = []

    def add_job(self, job):
        self.jobs.append(job)
