import logging

from errors import CacheUpstreamError
from infrastructure import infrastructure
import jobs
from mastermind_core.config import config
from mastermind_core import helpers
import storage
from sync import sync_manager
from sync.error import LockFailedError


logger = logging.getLogger('mm.planner.move')


JOBS_PARAMS = config.get('jobs', {})
MOVE_PLANNER_PARAMS = config.get('planner', {}).get('move', {})


class MovePlanner(object):

    MOVE_CANDIDATES = 'move_candidates'
    MOVE_LOCK = 'planner/move'

    def __init__(self, db, niu, job_processor):

        self.job_processor = job_processor

        self.node_info_updater = niu

    def schedule_tasks(self, tq):

        self._planner_tq = tq

        if MOVE_PLANNER_PARAMS.get('enabled', False):
            self._planner_tq.add_task_in(
                self.MOVE_CANDIDATES,
                10,
                self._move_candidates
            )

    def _move_candidates(self):
        try:
            logger.info('Starting move jobs planner')

            max_move_jobs = config.get('jobs', {}).get('move_job', {}).get('max_executing_jobs', 3)

            # prechecking for new or pending tasks
            count = self.job_processor.job_finder.jobs_count(
                types=jobs.JobTypes.TYPE_MOVE_JOB,
                statuses=jobs.Job.ACTIVE_STATUSES,
                # special marker for jobs that are moved by move planner
                move_planner=True,
            )

            if count >= max_move_jobs:
                logger.info('Found {0} unfinished move jobs (>= {1})'.format(count, max_move_jobs))
                return

            with sync_manager.lock(MovePlanner.MOVE_LOCK, blocking=False):
                self._do_move_candidates(max_move_jobs - count)

        except LockFailedError:
            logger.info('Move jobs dc planner is already running')
        except Exception:
            logger.exception('Move jobs planner failed')
        finally:
            logger.info('Move candidates planner finished')
            self._planner_tq.add_task_in(
                task_id=self.MOVE_CANDIDATES,
                secs=MOVE_PLANNER_PARAMS.get('generate_plan_period', 1800),
                function=self._move_candidates,
            )

    def _do_move_candidates(self, max_jobs_count):

        active_jobs = self.job_processor.job_finder.jobs(
            statuses=jobs.Job.ACTIVE_STATUSES
        )
        busy_group_ids = self._busy_group_ids(active_jobs)

        storage_state = StorageState.current(busy_group_ids)

        move_jobs = (j for j in active_jobs if j.type == jobs.JobTypes.TYPE_MOVE_JOB)
        storage_state.account_jobs(move_jobs)

        storage_state.debug()

        for src_dc_state in storage_state.state.itervalues():

            logger.info(
                'Source dc "{dc}": started move candidates planner'.format(dc=src_dc_state.dc)
            )

            source_host_states = src_dc_state.move_out_host_states()

            while src_dc_state.is_valid_source_dc():

                try:
                    src_host_state = next(source_host_states)
                except StopIteration:
                    break

                src_groups = src_host_state.move_out_group_candidates()

                logger.info(
                    'Source dc "{dc}": host {host}: total candidate groups to move: {count}'.format(
                        dc=src_dc_state.dc,
                        host=src_host_state.hostname,
                        count=len(src_groups),
                    )
                )

                if not src_groups:
                    continue

                dst_dc_states = (
                    dc_state
                    for dc_state in storage_state.state.itervalues()
                    if dc_state.unc_percentage > src_dc_state.unc_percentage
                )

                dst_dc_states = sorted(
                    dst_dc_states,
                    key=lambda d: d.unc_percentage - src_dc_state.unc_percentage,
                    reverse=True,
                )

                logger.info(
                    'Source dc "{dc}": current suitable dst dcs: {dst_dcs}'.format(
                        dc=src_dc_state.dc,
                        dst_dcs=[dc_state.dc for dc_state in dst_dc_states],
                    )
                )

                for move_job_plan in self._generate_move_job_plans(src_dc_state,
                                                                   src_host_state,
                                                                   dst_dc_states,
                                                                   src_groups):

                    # TODO: add dry-run mode
                    try:

                        src_total_space = move_job_plan.src_group.get_stat().total_space
                        dst_total_space = sum(
                            ug.get_stat().total_space
                            for ug in move_job_plan.dst_groups
                        )
                        assert dst_total_space >= src_total_space, (
                            'total space of src group "{src_group}" is greater than total space of '
                            'destination groups "{dst_groups}"'.format(
                                src_group=move_job_plan.src_group,
                                dst_groups=[ug.group_id for ug in move_job_plan.dst_groups]
                            )
                        )

                        logger.info(
                            'Source dc "{dc}": creating move job, {plan}'.format(
                                dc=src_dc_state.dc,
                                plan=move_job_plan,
                            )
                        )
                        job = self._create_move_job(move_job_plan)
                    except LockFailedError as e:
                        logger.error(
                            'Failed to create move job, {plan}: {err}'.format(
                                plan=move_job_plan,
                                err=e,
                            )
                        )
                        continue
                    except Exception as e:
                        logger.exception(
                            'Failed to create move job, {plan}'.format(
                                plan=move_job_plan,
                            )
                        )
                        continue

                    storage_state.account_move_job_plan(move_job_plan)
                    storage_state.account_resources(job, jobs.Job.RESOURCE_HOST_IN)
                    storage_state.account_resources(job, jobs.Job.RESOURCE_HOST_OUT)

                    break
                else:
                    logger.info(
                        'Source dc "{dc}": host "{host}": failed to create any move job'.format(
                            dc=src_dc_state.dc,
                            host=src_host_state.hostname,
                        )
                    )
                    continue

    @staticmethod
    def _generate_move_job_plans(src_dc_state, src_host_state, dst_dc_states, src_groups):
        for dst_dc_state in dst_dc_states:
            filtered_src_groups = [
                src_group
                for src_group in src_groups
                if src_group.couple not in dst_dc_state.full_groupsets
            ]

            logger.info(
                'Source dc "{dc}": host "{host}": groups that does not have coupled groups in dst '
                'dc "{dst_dc}": {count}'.format(
                    dc=src_dc_state.dc,
                    host=src_host_state.hostname,
                    dst_dc=dst_dc_state.dc,
                    count=len(filtered_src_groups),
                )
            )

            if not filtered_src_groups:
                continue

            uncoupled_space_min_limit_bytes = MOVE_PLANNER_PARAMS.get(
                'uncoupled_space_min_limit_bytes',
                0
            )

            if uncoupled_space_min_limit_bytes and dst_dc_state.uncoupled_space <= uncoupled_space_min_limit_bytes:
                logger.info(
                    'Source dc "{dc}": dst dc "{dst_dc}" skipped, uncoupled space '
                    '{uncoupled_space} <= {limit} (uncoupled space min limit)'.format(
                        dc=src_dc_state.dc,
                        dst_dc=dst_dc_state.dc,
                        uncoupled_space=helpers.convert_bytes(dst_dc_state.uncoupled_space),
                        limit=helpers.convert_bytes(uncoupled_space_min_limit_bytes),
                    )
                )
                continue

            for move_job_plan in dst_dc_state.move_job_candidates(src_dc_state,
                                                                  src_host_state,
                                                                  filtered_src_groups):
                yield move_job_plan

    def _create_move_job(self, move_job_plan):
        src_group = move_job_plan.src_group
        dst_group, merged_groups = move_job_plan.dst_groups[0], move_job_plan.dst_groups[1:]

        need_approving = not MOVE_PLANNER_PARAMS.get('autoapprove', False)

        job = self.job_processor._create_job(
            jobs.JobTypes.TYPE_MOVE_JOB,
            {
                'group': src_group.group_id,
                'uncoupled_group': dst_group.group_id,
                'merged_groups': [mg.group_id for mg in merged_groups],
                'src_host': src_group.node_backends[0].node.host.addr,
                'src_port': src_group.node_backends[0].node.port,
                'src_family': src_group.node_backends[0].node.family,
                'src_backend_id': src_group.node_backends[0].backend_id,
                'src_base_path': src_group.node_backends[0].base_path,
                'dst_host': dst_group.node_backends[0].node.host.addr,
                'dst_port': dst_group.node_backends[0].node.port,
                'dst_family': dst_group.node_backends[0].node.family,
                'dst_backend_id': dst_group.node_backends[0].backend_id,
                'dst_base_path': dst_group.node_backends[0].base_path,
                'need_approving': need_approving,
            },
            force=True,
        )
        return job

    @staticmethod
    def _busy_group_ids(active_jobs):
        busy_group_ids = set()
        for job in active_jobs:
            busy_group_ids.update(job._involved_groups)
        return busy_group_ids


class StorageState(object):

    def __init__(self):
        self.state = {
            dc: DcState(self, dc)
            for dc in self.__dcs()
        }
        self._stats = {}

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
    def current(busy_group_ids):
        state = StorageState()

        good_uncoupled_groups = set(infrastructure.get_good_uncoupled_groups(
            max_node_backends=1,
            skip_groups=busy_group_ids,
        ))

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

                host_state = state.state[dc].hosts.setdefault(
                    nb.node.host,
                    HostState(state.state[dc], nb.node.host, hostname)
                )

                state.state[dc].account_stat(nb.stat)
                host_state.account_stat(nb.stat)

                if group.type == storage.Group.TYPE_UNCOUPLED:
                    state.state[dc].account_uncoupled_group_stat(nb.stat)
                    host_state.account_uncoupled_group_stat(nb.stat)

                    state._stats[group.group_id] = group.get_stat()

                    if group.group_id in good_uncoupled_groups:
                        host_state.add_uncoupled_group(group)

                elif group.type == storage.Group.TYPE_DATA:

                    if not group.couple:
                        # NOTE: group was not assigned to a couple because of some error
                        continue

                    assert isinstance(group.couple, storage.Couple), \
                        "Group {} of type 'data' is assigned to non-replicas groupset: {}".format(
                            group,
                            group.couple
                        )
                    if group.couple.status != storage.Status.FULL:
                        continue

                    if group.couple.namespace.id == storage.Group.CACHE_NAMESPACE:
                        # there is no point in moving cached keys
                        continue

                    if any(len(g.node_backends) > 1 for g in group.couple.groups):
                        continue

                    if any(g.group_id in busy_group_ids for g in group.couple.groups):
                        # couple is participating in some job, skipping it
                        continue

                    state._stats[group.group_id] = group.get_stat()

                    state.state[dc].add_full_groupset(group.couple)
                    host_state.add_full_group(group)

        return state

    def stats(self, group):
        if group.group_id not in self._stats:
            # cache group stats if possible
            if group.group_id in storage.groups and storage.groups[group.group_id].node_backends:
                self._stats[group.group_id] = storage.groups[group.group_id].get_stat()
        return self._stats[group.group_id]

    def account_resources(self, job, resource_type):
        for addr in job.resources.get(resource_type, []):

            if addr not in storage.hosts:
                logger.error('Job {}: host with address {} is not found in storage'.format(job.id, addr))
                continue
            host = storage.hosts[addr]
            try:
                dc_state = self.state[host.dc]
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

    def account_jobs(self, active_jobs):

        job_plans = []

        for job in active_jobs:
            # TODO: generalize 'account_resources' method
            # for res_type in jobs.Job.RESOURCE_TYPES:
            #     self.account_resources(job, res_type)
            self.account_resources(job, jobs.Job.RESOURCE_HOST_IN)
            self.account_resources(job, jobs.Job.RESOURCE_HOST_OUT)

            try:
                job_plans.append(MoveJobPlan.from_job(self, job))
            except Exception:
                logger.exception('Failed to construct move job plan for job {}'.format(job.id))
                continue

        for job_plan in job_plans:
            self.account_move_job_plan(job_plan)

    def debug(self):
        logger.debug('Storage state: ')
        logger.debug('Storage state: dcs: {}'.format(len(self.state)))
        for dc, dc_state in self.state.iteritems():
            logger.debug('Storage state: dc {dc}: hosts: {hosts}'.format(dc=dc, hosts=len(dc_state.hosts)))
            for host_state in dc_state.hosts.itervalues():
                logger.debug('Storage state: dc {dc}: host {host}: host_in {host_in}, host_out: {host_out}'.format(
                    dc=dc,
                    host=host_state.hostname,
                    host_in=host_state.resources[jobs.Job.RESOURCE_HOST_IN],
                    host_out=host_state.resources[jobs.Job.RESOURCE_HOST_OUT]
                ))

    @property
    def avg_unc_percentage(self):
        total_space = sum(dc_state.total_space for dc_state in self.state.itervalues())
        uncoupled_space = sum(dc_state.uncoupled_space for dc_state in self.state.itervalues())
        return float(uncoupled_space) / total_space

    def account_move_job_plan(self, plan):

        if plan.src_group:

            # moving out src_group from source host_state
            # TODO: when group is moved out, we disable it but do not remove its data, cleanup is
            # done manually. So this group can be considered disabled as of now, and we can just
            # subtract host's total_space
            if plan.src_group in plan.host_out_state.full_groups:
                # plan.host_out_state.full_groups.remove(plan.src_group)
                for cg in plan.src_group.couple.groups:
                    for nb in cg.node_backends:
                        dc_state = self.state[nb.node.host.dc]
                        host_state = dc_state.hosts[nb.node.host]
                        host_state.full_groups.discard(cg)

            try:
                src_total_space = self.stats(plan.src_group).total_space
            except KeyError:
                # group is unavailable, it's stats were not accounted for
                src_total_space = 0
            plan.host_out_state.total_space -= src_total_space

            # moving out src_group from source dc_state
            if plan.src_group.couple in plan.dc_out_state.full_groupsets:
                plan.dc_out_state.full_groupsets.remove(plan.src_group.couple)
            plan.dc_out_state.total_space -= src_total_space

            # moving in src_group to destination host_state
            if plan.src_group in plan.host_in_state.full_groups:
                plan.host_in_state.full_groups.add(plan.src_group)

            # moving in src_group to destination dc_state
            plan.dc_in_state.full_groupsets.add(plan.src_group.couple)

        if plan.dst_groups:
            dst_total_space = 0
            for ug in plan.dst_groups:
                try:
                    dst_total_space += self.stats(ug).total_space
                except KeyError:
                    continue

            plan.host_in_state.uncoupled_space -= dst_total_space
            for ug in plan.dst_groups:
                plan.host_in_state.uncoupled_groups.discard(ug)
            plan.dc_in_state.uncoupled_space -= dst_total_space


class MoveJobPlan(object):
    """ Wrapper to store existing or about to be created move job data.

    Basically this object helps to account job resources and provides well-defined set of objects
    to create new move jobs.
    """
    def __init__(self,
                 dc_out_state,
                 host_out_state,
                 src_group,
                 dc_in_state,
                 host_in_state,
                 dst_groups):

        self.dc_out_state = dc_out_state
        self.host_out_state = host_out_state
        self.src_group = src_group

        self.dc_in_state = dc_in_state
        self.host_in_state = host_in_state
        self.dst_groups = dst_groups

    @staticmethod
    def from_job(storage_state, job):
        if job.group in storage.groups:
            src_group = storage.groups[job.group]
        else:
            # in case when job.group was disabled during move execution
            src_group = None

        dst_groups = [
            storage.groups[gid]
            for gid in [job.uncoupled_group] + job.merged_groups
            if gid in storage.groups
        ]

        src_host = storage.hosts[job.src_host]
        src_dc_state = storage_state.state[src_host.dc]
        src_host_state = src_dc_state.hosts[src_host]

        dst_host = storage.hosts[job.dst_host]
        dst_dc_state = storage_state.state[dst_host.dc]
        dst_host_state = dst_dc_state.hosts[dst_host]

        return MoveJobPlan(
            dc_out_state=src_dc_state,
            host_out_state=src_host_state,
            src_group=src_group,
            dc_in_state=dst_dc_state,
            host_in_state=dst_host_state,
            dst_groups=dst_groups,
        )

    def __str__(self):
        return (
            'plan: move group {src_group} from dc "{src_dc}", host "{src_host}" to dc '
            '"{dst_dc}", host "{dst_host}" (uncoupled groups used: {dst_groups})'.format(
                src_group=self.src_group,
                src_dc=self.dc_out_state.dc,
                src_host=self.host_out_state.hostname,
                dst_dc=self.dc_in_state.dc,
                dst_host=self.host_in_state.hostname,
                dst_groups=[g.group_id for g in self.dst_groups],
            )
        )


class DcState(object):
    """ Stores accumulated information about certain dc:

        total_space: sum of total_space for every group in dc
        uncoupled_space: sum of total_space for every uncoupled group in dc
        full_groupsets: a set of all FULL couples that reside in dc and can be moved out
    """
    def __init__(self, storage_state, dc):
        self.storage_state = storage_state
        self.dc = dc
        self.total_space = 0.0
        self.uncoupled_space = 0.0
        self.full_groupsets = set()
        self.hosts = {}

    def add_full_groupset(self, groupset):
        self.full_groupsets.add(groupset)

    def account_uncoupled_group_stat(self, stat):
        self.uncoupled_space += stat.total_space

    def account_stat(self, stat):
        self.total_space += stat.total_space

    @property
    def unc_percentage(self):
        return float(self.uncoupled_space) / self.total_space

    def is_valid_source_dc(self):
        avg_unc_percentage = self.storage_state.avg_unc_percentage
        if self.unc_percentage > avg_unc_percentage:
            logger.info(
                'Source dc "{dc}": skipped, uncoupled percentage {uncoupled_prc} > '
                '{avg_uncoupled_prc} (average uncoupled percentage)'.format(
                    dc=self.dc,
                    uncoupled_prc=helpers.percent(self.unc_percentage),
                    avg_uncoupled_prc=helpers.percent(avg_unc_percentage),
                )
            )
            return False

        uncoupled_space_max_limit_bytes = MOVE_PLANNER_PARAMS.get(
            'uncoupled_space_max_limit_bytes',
            0
        )

        if uncoupled_space_max_limit_bytes and self.uncoupled_space > uncoupled_space_max_limit_bytes:
            logger.info(
                'Source dc "{dc}": skipped, uncoupled space {uncoupled_space} > '
                '{limit} (uncoupled space max limit)'.format(
                    dc=self.dc,
                    uncoupled_space=helpers.convert_bytes(self.uncoupled_space),
                    limit=helpers.convert_bytes(uncoupled_space_max_limit_bytes),
                )
            )
            return False

        return True

    def move_out_host_states(self):

        move_jobs_limits = JOBS_PARAMS.get(jobs.JobTypes.TYPE_MOVE_JOB, {}).get('resources_limits', {})

        for host_state in self.hosts.itervalues():

            # NOTE: if host out limit is not set for move jobs, we limit move jobs to the default
            # value of 1
            host_out_limit = move_jobs_limits.get(jobs.Job.RESOURCE_HOST_OUT, 1)
            host_state_host_out = host_state.resources[jobs.Job.RESOURCE_HOST_OUT]
            free_slots = host_out_limit - host_state_host_out

            if free_slots <= 0:
                logger.info(
                    'Source dc "{dc}": host {host}: skipped, host out resource is {host_out} '
                    '>= {limit} (move jobs host out limit)'.format(
                        dc=self.dc,
                        host=host_state.hostname,
                        host_out=host_state_host_out,
                        limit=host_out_limit,
                    )
                )
                continue

            logger.debug(
                'Source dc "{dc}": host {host}: has {slots} free host out slots'.format(
                    dc=self.dc,
                    host=host_state.hostname,
                    slots=free_slots,
                )
            )

            for _ in xrange(free_slots):
                yield host_state

    def move_in_host_states(self):

        move_jobs_limits = JOBS_PARAMS.get('move_job', {}).get('resources_limits', {})

        host_states = sorted(
            self.hosts.itervalues(),
            key=lambda hs: hs.unc_percentage,
            reverse=True,
        )

        for host_state in host_states:

            # NOTE: if host in limit is not set for move jobs, we limit move jobs to the default
            # value of 1
            host_in_limit = move_jobs_limits.get(jobs.Job.RESOURCE_HOST_IN, 1)
            host_state_host_in = host_state.resources[jobs.Job.RESOURCE_HOST_IN]
            free_slots = host_in_limit - host_state_host_in

            if free_slots <= 0:
                logger.info(
                    'Dst dc "{dc}": host {host}: skipped, host in resource is {host_in} '
                    '>= {limit} (move jobs host in limit)'.format(
                        dc=self.dc,
                        host=host_state.hostname,
                        host_in=host_state_host_in,
                        limit=host_in_limit,
                    )
                )
                continue

            yield host_state

    def move_job_candidates(self, src_dc_state, src_host_state, src_groups):
        """
        """

        def candidate_sort_key(plan_candidate):
            """ Key for selecting the best candidate among collected source and destination groups

            Sort happens by:
                (
                    waste space: diff between total_space of all merged groups and source group's
                        total_space
                    reverest source group total space: trying to move the largest group possible
                    merged groups count: to prevent a lot of groups merging
                )
            """
            src_total_space = state.stats(plan_candidate.src_group).total_space
            dst_total_space = sum(
                state.stats(g).total_space
                for g in plan_candidate.dst_groups
            )

            waste_space = dst_total_space - src_total_space

            return waste_space, -src_total_space, len(plan_candidate.dst_groups)

        state = self.storage_state

        for host_state in self.move_in_host_states():

            groups_by_fs = {}

            for g in host_state.uncoupled_groups:
                # NOTE: valid uncoupled groups always contain a single backend
                fs = g.node_backends[0].fs
                groups_by_fs.setdefault(fs, []).append(g)

            candidates = []

            for group in src_groups:

                src_total_space = state.stats(group).total_space

                for fs, fs_groups in groups_by_fs.iteritems():
                    total_fs_groups_space = 0
                    fs_candidate_groups = []
                    for fs_group in fs_groups:
                        fs_candidate_groups.append(fs_group)
                        total_fs_groups_space += state.stats(fs_group).total_space
                        if total_fs_groups_space >= src_total_space:
                            break

                    is_good_enough_match = (
                        len(fs_candidate_groups) and
                        total_fs_groups_space >= src_total_space and
                        total_fs_groups_space <= src_total_space * 1.05
                    )

                    if is_good_enough_match:
                        # NOTE: found good enough candidate, no point in further searching
                        yield MoveJobPlan(
                            dc_out_state=src_dc_state,
                            host_out_state=src_host_state,
                            src_group=group,
                            dc_in_state=self,
                            host_in_state=host_state,
                            dst_groups=fs_candidate_groups,
                        )

                    elif total_fs_groups_space >= src_total_space:
                        candidates.append(
                            MoveJobPlan(
                                dc_out_state=src_dc_state,
                                host_out_state=src_host_state,
                                src_group=group,
                                dc_in_state=self,
                                host_in_state=host_state,
                                dst_groups=fs_candidate_groups,
                            )
                        )

            logger.debug(
                'Dst dc "{dc}": host {host}: best fit candidates list is empty, other candidates: '
                '{count}'.format(
                    dc=host_state.dc_state.dc,
                    host=host_state.hostname,
                    count=len(candidates),
                )
            )

            # selecting source and destination groups among not so good enough candidates
            for candidate in sorted(candidates, key=candidate_sort_key):
                yield candidate

            logger.info(
                'Dst dc "{dc}": no suitable uncoupled groups found for candidate source groups'.format(
                    dc=host_state.dc_state.dc,
                )
            )


class HostState(object):
    def __init__(self, dc_state, host, hostname):
        self.dc_state = dc_state
        self.host = host
        self.hostname = hostname
        self.resources = {
            res_type: 0
            for res_type in jobs.Job.RESOURCE_TYPES
        }
        self.total_space = 0.0
        self.uncoupled_space = 0.0

        self.full_groups = set()
        self.uncoupled_groups = set()

    def account_uncoupled_group_stat(self, stat):
        self.uncoupled_space += stat.total_space

    def account_stat(self, stat):
        self.total_space += stat.total_space

    def add_full_group(self, group):
        self.full_groups.add(group)

    def add_uncoupled_group(self, group):
        self.uncoupled_groups.add(group)

    @property
    def unc_percentage(self):
        return float(self.uncoupled_space) / self.total_space

    def move_out_group_candidates(self):
        return self._sample_groups_by_ts_and_couple_dcs(self.full_groups)

    def _sample_groups_by_ts_and_couple_dcs(self, groups):
        """
        Sample groups by its total space and dcs of its neighbouring groups.

        This method splits groups by their approximate total space and then selects
        those that have a unique set of dcs of coupled groups. This helps to narrow
        a list of candidate groups to move from a dc.
        """

        state = self.dc_state.storage_state

        groups_sample = []

        groups = sorted(groups, key=lambda x: state.stats(x).total_space, reverse=True)

        dcs_combinations = set()
        total_space = None

        for group in groups:

            group_total_space = state.stats(group).total_space
            try:
                dcs = tuple(sorted(
                    nb.node.host.dc
                    for g in group.couple.groups
                    for nb in g.node_backends
                ))
            except CacheUpstreamError:
                continue

            if total_space and group_total_space >= total_space * 0.95:
                if dcs in dcs_combinations:
                    continue
            else:
                # dcs_combinations are dropped because next total space group has begun
                dcs_combinations = set()

            groups_sample.append(group)
            total_space = group_total_space
            dcs_combinations.add(dcs)

        return groups_sample
