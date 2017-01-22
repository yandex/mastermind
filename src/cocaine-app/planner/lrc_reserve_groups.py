import logging

from errors import CacheUpstreamError
import infrastructure
import inventory
import jobs
from mastermind_core.config import config
from mastermind_core import helpers
import storage
from sync import sync_manager
from sync.error import LockFailedError
from timer import periodic_timer


logger = logging.getLogger('mm.planner.lrc_reserve')

LRC_RESERVE_PLANNER_PARAMS = config.get('planner', {}).get('lrc_reserve', {})
LRC_CFG = config.get('lrc', {}).get('lrc-8-2-2-v1', {})

try:
    LRC_GROUP_TOTAL_SPACE = helpers.convert_config_bytes_value(
        LRC_CFG.get('group_total_space')
    )
except ValueError:
    logger.warn(
        'Lrc reserve groups planner: failed to setup lrc group total space '
        '["lrc"]["lrc-8-2-2-v1"]["group_total_space"]'
    )
    LRC_GROUP_TOTAL_SPACE = None


class LrcReservePlanner(object):

    PREPARE_LRC_RESERVE = 'prepare_lrc_reserve_groups'
    PREPARE_LRC_RESERVE_LOCK = 'planner/prepare_lrc_reserve_groups'

    def __init__(self, job_processor):
        self.job_processor = job_processor

        self.prepare_reserve_timer = periodic_timer(
            seconds=LRC_RESERVE_PLANNER_PARAMS.get(
                'generate_plan_period',
                60 * 60  # 1 hour
            )
        )

    def schedule_tasks(self, tq):

        self._planner_tq = tq

        if LRC_RESERVE_PLANNER_PARAMS.get('enabled', False):
            if LRC_GROUP_TOTAL_SPACE is None:
                raise ValueError(
                    'Lrc reserve groups planner: required setting '
                    '["lrc"]["lrc-8-2-2-v1"]["group_total_space"] is not set up'
                )
            self._planner_tq.add_task_at(
                self.PREPARE_LRC_RESERVE,
                self.prepare_reserve_timer.next(),
                self._prepare_lrc_reserve_groups,
            )

    def _prepare_lrc_reserve_groups(self):
        try:
            logger.info('Starting lrc reserve groups planner')
            with sync_manager.lock(LrcReservePlanner.PREPARE_LRC_RESERVE_LOCK, blocking=False):
                LrcReserve(self.job_processor).make_jobs()

        except LockFailedError:
            logger.info('Lrc reserve groups planner is already running')
        except Exception:
            logger.exception('Lrc reserve groups planner failed')
        finally:
            logger.info('Lrc reserve groups planner finished')
            self._planner_tq.add_task_at(
                self.PREPARE_LRC_RESERVE,
                self.prepare_reserve_timer.next(),
                self._prepare_lrc_reserve_groups,
            )


class LrcReserve(object):

    TS_TOLERANCE = config.get('total_space_diff_tolerance', 0.05)

    DC_NODE_TYPE = inventory.get_dc_node_type()

    def __init__(self, job_processor):
        self.job_processor = job_processor

        self._dcs = set()
        self._dc_reserved_space = {}

        self._uncoupled_groups = self._uncoupled_groups_by_dc()

        logger.debug('Uncoupled groups for lrc reserved planner: {}'.format(
            ', '.join(
                '{dc}: {count}'.format(dc=dc, count=len(groups))
                for dc, groups in self._uncoupled_groups.iteritems()
            )
        ))

        self._lrc_tree, self._lrc_nodes = self._build_lrc_tree()

        self._account_jobs()

        logger.debug('Uncoupled groups for lrc reserved planner: {}'.format(
            ', '.join(
                '{dc}: {count}'.format(dc=dc, count=len(groups))
                for dc, groups in self._uncoupled_groups.iteritems()
            )
        ))

        self._dc_required_space = helpers.convert_config_bytes_value(
            LRC_RESERVE_PLANNER_PARAMS.get('reserved_space_per_dc', 0)
        )

    def _build_lrc_tree(self):
        node_types = (self.DC_NODE_TYPE,) + ('host',)
        tree, nodes = infrastructure.infrastructure.filtered_cluster_tree(node_types)

        # NOTE:
        # lrc groups that are currently being processed by running build jobs
        # are not accounted here because there is no easy and
        # straightforward way to do this. This is not crucial
        # at the moment.
        lrc_groups = []
        for group in storage.groups.keys():
            if group.type not in storage.Group.TYPES_LRC_8_2_2_V1:
                continue
            if len(group.node_backends) != 1:
                continue

            lrc_groups.append(group)

            nb = group.node_backends[0]
            try:
                dc = nb.node.host.dc
            except CacheUpstreamError:
                continue
            self._dcs.add(dc)

            if group.type == storage.Group.TYPE_RESERVED_LRC_8_2_2_V1:
                self._dc_reserved_space.setdefault(dc, 0)
                self._dc_reserved_space[dc] += nb.stat.total_space

        # TODO: rename, nothing about "ns" here
        infrastructure.infrastructure.account_ns_groups(nodes, lrc_groups)
        infrastructure.infrastructure.update_groups_list(tree)

        return tree, nodes

    def _account_jobs(self):

        running_jobs = self.job_processor.job_finder.jobs(
            types=jobs.JobTypes.TYPE_MAKE_LRC_RESERVED_GROUPS_JOB,
            statuses=jobs.Job.ACTIVE_STATUSES,
            sort=False,
        )

        for job in running_jobs:
            self._account_job(job)

    def _uncoupled_groups_by_dc(self):

        total_space_options = [
            helpers.convert_config_bytes_value(ts)
            for ts in LRC_RESERVE_PLANNER_PARAMS.get('uncoupled_groups_total_space', [])
        ]

        groups_by_total_space = infrastructure.infrastructure.groups_by_total_space(
            match_group_space=True,
            max_node_backends=1,
        )

        def _match_ts(ts):
            if not total_space_options:
                return True

            for option_ts in total_space_options:
                if abs(option_ts - ts) < option_ts * self.TS_TOLERANCE:
                    return True

            return False

        uncoupled_groups_by_dc = {}

        for ts, group_ids in groups_by_total_space.iteritems():
            if not _match_ts(ts):
                continue

            for group_id in group_ids:
                group = storage.groups[group_id]
                try:
                    dc = group.node_backends[0].node.host.dc
                except CacheUpstreamError:
                    continue
                uncoupled_groups_by_dc.setdefault(dc, []).append(group)

        return uncoupled_groups_by_dc

    def make_jobs(self):
        for dc in self._dcs:
            self.make_jobs_for_dc(dc)

    def make_jobs_for_dc(self, dc):
        self._dc_reserved_space.setdefault(dc, 0)

        logger.info('Reserved space for lrc groups in dc {}: {} (required {})'.format(
            dc,
            helpers.convert_bytes(self._dc_reserved_space[dc]),
            helpers.convert_bytes(self._dc_required_space),
        ))

        uncoupled_groups = self._select_uncoupled_group(dc)

        while self._dc_reserved_space[dc] < self._dc_required_space:
            try:
                uncoupled_group = next(uncoupled_groups)
            except StopIteration:
                logger.info('Dc {}: no more uncoupled groups available'.format(dc))
                break

            new_groups_count = self._count_lrc_reserved_groups_number(uncoupled_group)
            new_groups_ids = infrastructure.infrastructure.reserve_group_ids(new_groups_count)

            logger.info('Dc {}: selected uncoupled group {}'.format(dc, uncoupled_group))

            try:
                job = self.job_processor._create_job(
                    jobs.JobTypes.TYPE_MAKE_LRC_RESERVED_GROUPS_JOB,
                    {
                        'uncoupled_group': uncoupled_group.group_id,
                        'dc': dc,
                        'host': uncoupled_group.node_backends[0].node.host.addr,
                        'lrc_groups': new_groups_ids,
                        'scheme': storage.Lrc.Scheme822v1.ID,
                        'total_space': LRC_GROUP_TOTAL_SPACE,
                        'autoapprove': LRC_RESERVE_PLANNER_PARAMS.get('autoapprove', False)
                    }
                )
            except LockFailedError as e:
                logger.error(e)
                continue

            logger.error('Dc {}: create job {}'.format(dc, job.id))

            self._account_selected_group(uncoupled_group, dc)

            logger.info('Reserved space for lrc groups in dc {}: {} (required {})'.format(
                dc,
                helpers.convert_bytes(self._dc_reserved_space[dc]),
                helpers.convert_bytes(self._dc_required_space),
            ))

    @staticmethod
    def _count_lrc_reserved_groups_number(group):
        ts = group.get_stat().total_space
        return int(ts / LRC_GROUP_TOTAL_SPACE)

    def _lrc_groups_on_host(self, group):
        """ Count number of lrc groups residing on the same host as `group`.
        """
        host = group.node_backends[0].node.host
        host_lrc_groups = self._lrc_nodes['host'].get(host.full_path, {}).get('groups', [])
        return len(host_lrc_groups)

    def _select_uncoupled_group(self, dc):
        while self._uncoupled_groups.get(dc, []):
            uncoupled_group = min(
                self._uncoupled_groups[dc],
                key=self._lrc_groups_on_host,
            )

            yield uncoupled_group

    def _account_selected_group(self, group, dc):
        count_lrc_groups = self._count_lrc_reserved_groups_number(group)

        host = group.node_backends[0].node.host

        host_lrc_groups = self._lrc_nodes['host'].get(host.full_path, {}).get('groups', set())
        host_lrc_groups.update(DummyGroup() for _ in xrange(count_lrc_groups))

        self._dc_reserved_space.setdefault(dc, 0)
        self._dc_reserved_space[dc] += group.get_stat().total_space

        # optimize sorting and removing (not all groups but hosts?) (dict?)
        self._uncoupled_groups[dc].remove(group)

    def _account_job(self, job):
        logger.info('Accounting job {}'.format(job.id))

        dc = job.dc

        self._dcs.add(dc)

        if job.host in storage.hosts:
            host = storage.hosts[job.host]
            host_lrc_groups = self._lrc_nodes['host'].get(host.full_path, {}).get('groups', set())
            for group_id in job.lrc_groups:
                if group_id not in storage.groups or storage.groups[group_id] not in host_lrc_groups:
                    host_lrc_groups.add(DummyGroup())

        total_space = job.total_space * len(job.lrc_groups)
        self._dc_reserved_space.setdefault(dc, 0)
        self._dc_reserved_space[dc] += total_space

        if job.uncoupled_group in storage.groups:
            # optimize sorting and removing (not all groups but hosts?) (dict?)
            try:
                self._uncoupled_groups[dc].remove(storage.groups[job.uncoupled_group])
            except ValueError:
                # uncoupled group was already skipped, it is expected
                pass


class DummyGroup(object):
    pass
