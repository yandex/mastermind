import logging

from errors import CacheUpstreamError
import helpers as h
import cluster_tree
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

    @h.concurrent_handler
    def create_lrc_restore_jobs(self, request):
        if 'lrc_groups' not in request:
            raise ValueError('Lrc groups are required')

        lrc_group_ids = request['lrc_groups']

        selector = LrcReserveGroupSelector(self.job_processor)

        res = []
        for lrc_group_id in lrc_group_ids:
            try:
                job = selector.restore_lrc_group(
                    lrc_group_id,
                    need_approving=request.get('need_approving', True),
                )
            except Exception as e:
                res.append(str(e))
                continue
            res.append(job.dump())
        return res


class LrcReserve(object):

    TS_TOLERANCE = config.get('total_space_diff_tolerance', 0.05)

    DC_NODE_TYPE = inventory.get_dc_node_type()

    def __init__(self, job_processor):
        self.job_processor = job_processor

        self._dcs = set()
        self._dc_reserved_space = {}

        self._dc_required_space = helpers.convert_config_bytes_value(
            LRC_RESERVE_PLANNER_PARAMS.get('reserved_space_per_dc', 0)
        )

        self._lrc_reserve_cluster_tree = self._build_cluster_tree()

    def _build_cluster_tree(self):
        # NOTE:
        # lrc groups that are currently being processed by running build jobs
        # are not accounted here because there is no easy and
        # straightforward way to do this. This is not crucial
        # at the moment.
        reserve_lrc_groups = infrastructure.infrastructure.get_good_uncoupled_groups(
            max_node_backends=1,
            types=(storage.Group.TYPE_RESERVED_LRC_8_2_2_V1,),
            status=storage.Status.COUPLED,
            allow_alive_keys=True,  # because of metakey
        )
        uncoupled_groups = infrastructure.infrastructure.get_good_uncoupled_groups(
            max_node_backends=1,
            types=(storage.Group.TYPE_UNCOUPLED,),
        )

        tree = LrcReserveDistributionClusterTree(
            reserve_lrc_groups + uncoupled_groups,
            job_processor=self.job_processor,
            node_types=(self.DC_NODE_TYPE, 'host'),
            on_account_job=self._account_job,
            on_account_group=self._account_group,
        )
        return tree

    def _account_job(self, job):
        if job.type == jobs.JobTypes.TYPE_MAKE_LRC_RESERVED_GROUPS_JOB:
            dc = job.dc
            self._dc_reserved_space.setdefault(dc, 0)
            self._dc_reserved_space[dc] += job.total_space * len(job.lrc_groups)

    def _account_group(self, hdd_node, group):
        cur_node = hdd_node
        while cur_node and cur_node.type != self.DC_NODE_TYPE:
            cur_node = cur_node.parent

        if not cur_node:
            raise RuntimeError('Dc for hdd_node {} is not found'.format(hdd_node))

        dc = cur_node.name
        self._dcs.add(dc)

        if group.type == storage.Group.TYPE_RESERVED_LRC_8_2_2_V1:
            self._dc_reserved_space.setdefault(dc, 0)
            self._dc_reserved_space[dc] += group.get_stat().total_space

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

        while self._dc_reserved_space[dc] < self._dc_required_space:

            job = None

            host_nodes = self._lrc_reserve_cluster_tree.dcs[dc].sorted_subset(type='host')
            for host_node, uncoupled_group in self._select_uncoupled_groups(dc, host_nodes):

                logger.info(
                    'Dc {}: selected uncoupled group {} on host {}'.format(
                        dc,
                        uncoupled_group,
                        host_node.name,
                    )
                )

                new_groups_count = self._count_lrc_reserved_groups_number(uncoupled_group)
                new_groups_ids = infrastructure.infrastructure.reserve_group_ids(new_groups_count)

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

                logger.info('Dc {}: created job {}'.format(dc, job.id))

                self._lrc_reserve_cluster_tree.account_job(job)
                host_nodes.consume(host_node)

                break

            if not job:
                logger.info('Dc {}: no more uncoupled groups'.format(dc))
                break

            logger.info('Reserved space for lrc groups in dc {}: {} (required {})'.format(
                dc,
                helpers.convert_bytes(self._dc_reserved_space[dc]),
                helpers.convert_bytes(self._dc_required_space),
            ))

    def _select_uncoupled_groups(self, dc, host_nodes):

        for host_node in host_nodes:

            if not host_node.artifacts.uncoupled_groups:
                raise StopIteration

            for uncoupled_group in self._select_uncoupled_group_on_host_node(host_node):
                yield host_node, uncoupled_group

    def _select_uncoupled_group_on_host_node(self, host_node):
        lrc_reserve_groups_by_hdd_node = {}
        for group, hdd_node in host_node.artifacts.lrc_reserved_groups.iteritems():
            if hdd_node is None:
                # failed to determine hdd_node for group, skipping
                continue
            lrc_reserve_groups_by_hdd_node.setdefault(hdd_node, 0)
            lrc_reserve_groups_by_hdd_node[hdd_node] += 1

        uncoupled_groups_by_hdd_node = {}
        for uncoupled_group, hdd_node in host_node.artifacts.uncoupled_groups.iteritems():
            uncoupled_groups_by_hdd_node.setdefault(hdd_node, []).append(uncoupled_group)
            lrc_reserve_groups_by_hdd_node.setdefault(hdd_node, 0)

        hdd_nodes = sorted(
            lrc_reserve_groups_by_hdd_node.iteritems(),
            key=lambda h: (uncoupled_groups_by_hdd_node.get(h[0]) is None, h[1])
        )

        for hdd_node, _ in hdd_nodes:
            logger.debug('Trying to use candidate uncoupled groups on host {}, hdd {}'.format(
                host_node.name, hdd_node.name
            ))
            for uncoupled_group in uncoupled_groups_by_hdd_node.get(hdd_node, []):
                yield uncoupled_group
        else:
            logger.info(
                'Host node {host}, hdd node {hdd}, no more uncoupled groups (uncoupled groups on '
                'host: {host_uncoupled_groups})'.format(
                    host=host_node.name,
                    hdd=hdd_node.name,
                    host_uncoupled_groups=len(host_node.artifacts.uncoupled_groups)
                )
            )


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


class DummyGroup(object):
    pass


class LrcReserveDistributionArtifacts(object):

    __slots__ = (
        'uncoupled_groups',
        'uncoupled_groups_space',
        'lrc_reserved_groups',
        'lrc_reserved_groups_space',
    )

    def __init__(self):
        self.uncoupled_groups = {}
        self.uncoupled_groups_space = 0
        self.lrc_reserved_groups = {}
        self.lrc_reserved_groups_space = 0


class LrcReserveDistributionNode(cluster_tree.Node):

    ArtifactsType = LrcReserveDistributionArtifacts

    def key(self):
        lrc_reserved_groups_space = self.artifacts.lrc_reserved_groups_space
        uncoupled_groups_space = self.artifacts.uncoupled_groups_space
        return (
            # a trick to move all host nodes with 0 uncoupled groups to the end of sorted set
            len(self.artifacts.uncoupled_groups) == 0,

            float(lrc_reserved_groups_space) /
            ((lrc_reserved_groups_space + uncoupled_groups_space) or 1)
        )


class LrcReserveDistributionClusterTree(cluster_tree.ClusterTree):

    NodeType = LrcReserveDistributionNode

    def find_jobs(self):
        return self.job_processor.job_finder.jobs(
            types=(
                jobs.JobTypes.TYPE_MAKE_LRC_RESERVED_GROUPS_JOB,
            ),
            statuses=jobs.Job.ACTIVE_STATUSES,
            sort=False,
        )

    def account_group(self, hdd_node, group):
        host_node = hdd_node.parent
        if group.type == storage.Group.TYPE_UNCOUPLED:
            host_node.artifacts.uncoupled_groups[group] = hdd_node
            host_node.artifacts.uncoupled_groups_space += group.get_stat().total_space
        if group.type == storage.Group.TYPE_RESERVED_LRC_8_2_2_V1:
            host_node.artifacts.lrc_reserved_groups[group] = hdd_node
            host_node.artifacts.lrc_reserved_groups_space += group.get_stat().total_space
        super(LrcReserveDistributionClusterTree, self).account_group(hdd_node, group)

    def account_job(self, job):
        if job.type == jobs.JobTypes.TYPE_MAKE_LRC_RESERVED_GROUPS_JOB:
            self._account_make_lrc_reserved_groups_job(job)
        super(LrcReserveDistributionClusterTree, self).account_job(job)

    def _account_make_lrc_reserved_groups_job(self, job):
        logger.info('Accounting job {}'.format(job.id))
        if job.host not in storage.hosts:
           logger.warn('Accounting job {} failed, host {} is not found in storage'.format(job.id, job.host))
           return

        host = storage.hosts[job.host]
        host_node = self.hosts[host.hostname]

        hdd_node = None

        if job.uncoupled_group in host_node.artifacts.uncoupled_groups:
            del host_node.artifacts.uncoupled_groups[job.uncoupled_group]
            uncoupled_group = storage.groups[job.uncoupled_group]
            total_space = uncoupled_group.get_stat().total_space
            host_node.artifacts.uncoupled_groups_space -= total_space

            if uncoupled_group.node_backends:
                fs_id = str(uncoupled_group.node_backends[0].fs)
                hdd_node = self.hdds[fs_id]

        for group_id in job.lrc_groups:
            if group_id not in host_node.artifacts.lrc_reserved_groups:
                if hdd_node:
                    # TODO: hdd_node can be undefined!
                    host_node.artifacts.lrc_reserved_groups[group_id] = hdd_node
                host_node.artifacts.lrc_reserved_groups_space += job.total_space


class LrcReserveGroupSelector(object):

    # NOTE: description of CLUSTER_NODE_LRC_GROUPS_LIMITS can be found in lrc_builder module
    CLUSTER_NODE_LRC_GROUPS_LIMITS = LRC_CFG.get('lrc_groups_per', {})

    def __init__(self, job_processor):

        self.job_processor = job_processor

        reserve_lrc_groups = infrastructure.infrastructure.get_good_uncoupled_groups(
            max_node_backends=1,
            types=(storage.Group.TYPE_RESERVED_LRC_8_2_2_V1,),
            status=storage.Status.COUPLED,
            allow_alive_keys=True,  # because of metakey
        )

        self.reserve_lrc_tree = LrcReserveClusterTree(
            reserve_lrc_groups,
            job_processor=self.job_processor,
            # node_types=(inventory.get_dc_node_type(), 'host'),
        )

        self.host_nodes_by_dc = {}

    @staticmethod
    def _nodes_usage_by_groups(groups):

        nodes_usage = {}

        for group in groups:
            # in case if any group is down we are trying to get it's host from history
            host = infrastructure.infrastructure.get_host_by_group_id(group.group_id)
            if host is None:
                raise RuntimeError('Cannot determine host for group {}'.format(group))

            group_nodes_usage = LrcReserveGroupSelector._nodes_usage_by_host(host)

            for node_type, type_nodes_usage in group_nodes_usage.iteritems():
                for node_path, count in type_nodes_usage.iteritems():
                    nodes_usage.setdefault(node_type, {}).setdefault(node_path, 0)
                    nodes_usage[node_type][node_path] += count

        return nodes_usage

    @staticmethod
    def _nodes_usage_by_host(host):

        nodes_usage = {}

        tree_nodes = []
        cur_node = host.parents
        while 'parent' in cur_node:
            tree_nodes.append(cur_node)
            cur_node = cur_node['parent']
        tree_nodes.append(cur_node)

        parts = []
        for tn in reversed(tree_nodes):
            parts.append(tn['name'])
            node_path = '|'.join(parts)
            nodes_usage.setdefault(tn['type'], {}).setdefault(node_path, 0)
            nodes_usage[tn['type']][node_path] += 1

        return nodes_usage

    def _is_cluster_node_limits_matched(self, host, nodes_usage, host_nodes_usage):
        for node_type, type_nodes_usage in host_nodes_usage.iteritems():
            if node_type in self.CLUSTER_NODE_LRC_GROUPS_LIMITS:
                node_type_limit = self.CLUSTER_NODE_LRC_GROUPS_LIMITS[node_type]
                for node_name, count in type_nodes_usage.iteritems():
                    node_usage_count = nodes_usage.get(node_type, {}).get(node_name, 0) + count
                    if node_usage_count > node_type_limit:
                        logger.debug(
                            'Host {host} will be skipped: found {new_node_count} groups '
                            'in node {node} of type {type}, limit is {limit} groups'.format(
                                host=host.hostname,
                                new_node_count=node_usage_count,
                                node=node_name,
                                type=node_type,
                                limit=node_type_limit,
                            )
                        )
                        return False
        return True

    def restore_lrc_group(self, group_id, need_approving=True):

        logger.info('Selecting lrc reserve group for restoring group {}'.format(group_id))

        group = storage.groups[group_id]
        if not isinstance(group.couple, storage.Lrc822v1Groupset):
            raise ValueError('Group {} does not belong to lrc groupset'.format(group))

        if group.couple.status == storage.Status.ARCHIVED:
            raise ValueError(
                'Group {} will not be restored, groupset is in good state, status "{}"'.format(
                    group,
                    group.couple.status,
                )
            )

        host = infrastructure.infrastructure.get_host_by_group_id(group_id)
        if host is None:
            raise RuntimeError('Cannot determine host for group {}'.format(group_id))

        lrc_group_dc = host.dc

        nodes_usage = self._nodes_usage_by_groups(group.couple.groups)

        host_nodes = self.host_nodes_by_dc.setdefault(
            lrc_group_dc,
            self._prepare_nodes_subset(lrc_group_dc)
        )

        for host_node in host_nodes:

            job = None

            # NOTE: nodes of type 'host' are guaranteed to have 'addr' attribute
            host = storage.hosts[host_node.addr]
            logger.debug('Group {}: checking candidate lrc reserve groups on host {}'.format(
                group_id,
                host.hostname,
            ))

            host_nodes_usage = self._nodes_usage_by_host(host)

            if not self._is_cluster_node_limits_matched(host, nodes_usage, host_nodes_usage):
                continue

            for lrc_reserve_group in self._groups_on_host_node(host_node):
                logger.debug(
                    'Trying to create job using lrc reserve group {}'.format(lrc_reserve_group)
                )
                try:
                    job = self.job_processor._create_job(
                        jobs.JobTypes.TYPE_RESTORE_LRC_GROUP_JOB,
                        {
                            'group': group_id,
                            'lrc_reserve_group': lrc_reserve_group.group_id,
                            'need_approving': need_approving,

                        },
                        force=True,
                    )
                except LockFailedError as e:
                    logger.error(e)
                    continue

                break
            else:
                logger.debug(
                    'Group {}: no appropriate lrc reserve groups are found on host {}'.format(
                        group_id,
                        host.hostname,
                    )
                )
                continue

            if job:
                # rearrange host nodes order if required after successful job creation
                self.reserve_lrc_tree.account_job(job)
                host_nodes.consume(host_node)
                break
        else:
            raise RuntimeError(
                'Failed to find any lrc reserve group to restore group {}'.format(group_id)
            )

        return job

    def _prepare_nodes_subset(self, dc):
        subset = self.reserve_lrc_tree.dcs[dc].sorted_subset(type='host')
        self.host_nodes_by_dc[dc] = subset

    def _hdd_sort_key(self, hdd_node):
        host_node = hdd_node.parent
        return host_node.artifacts.running_lrc_restore_jobs.get(hdd_node.name, 0)

    def _groups_on_host_node(self, host_node):
        sorted_hdd_nodes = sorted(host_node.children.itervalues(), key=self._hdd_sort_key)
        for hdd_node in sorted_hdd_nodes:
            for group in hdd_node.groups.itervalues():
                yield group


class LrcReserveArtifacts(object):

    __slots__ = (
        'running_restore_jobs',
        'running_lrc_restore_jobs',
    )

    def __init__(self):
        self.running_restore_jobs = 0
        self.running_lrc_restore_jobs = {}


class LrcReserveNode(cluster_tree.Node):

    ArtifactsType = LrcReserveArtifacts

    def key(self):
        return (
            self.artifacts.running_restore_jobs,
            sum(
                self.artifacts.running_lrc_restore_jobs.itervalues(),
                0
            ),
            -sum(
                (len(node.groups) for node in self.children.itervalues()),
                0
            ),
        )


class LrcReserveClusterTree(cluster_tree.ClusterTree):

    NodeType = LrcReserveNode

    def find_jobs(self):
        return self.job_processor.job_finder.jobs(
            types=(
                jobs.JobTypes.TYPE_RESTORE_GROUP_JOB,
                jobs.JobTypes.TYPE_RESTORE_LRC_GROUP_JOB,
            ),
            statuses=jobs.Job.ACTIVE_STATUSES,
            sort=False,
        )

    def account_job(self, job):
        if job.type == jobs.JobTypes.TYPE_RESTORE_GROUP_JOB:
            self._account_restore_job(job)
        elif job.type == jobs.JobTypes.TYPE_RESTORE_LRC_GROUP_JOB:
            self._account_restore_lrc_group_job(job)
        super(LrcReserveClusterTree, self).acount_job(job)

    def _account_restore_job(self, job):
        for host_addr in job.resources[jobs.Job.RESOURCE_HOST_IN]:
            if host_addr not in storage.hosts:
                continue
            host = storage.hosts[host_addr]
            self.hosts[host.hostname].artifacts.running_restore_jobs += 1

    def _account_restore_lrc_group_job(self, job):
        logger.debug('Accounting job {}'.format(job.id))

        for host_addr, fs_id in job.resources[jobs.Job.RESOURCE_FS]:
            if host_addr not in storage.hosts:
                continue
            host = storage.hosts[host_addr]
            host_node = self.hosts[host.hostname]

            full_fs_id = '{}:{}'.format(host_addr, fs_id)

            host_node.artifacts.running_lrc_restore_jobs.setdefault(full_fs_id, 0)
            host_node.artifacts.running_lrc_restore_jobs[full_fs_id] += 1

            if full_fs_id not in host_node.children:
                logger.warn("Fs id {} is not found among host {} node's children".format(
                    full_fs_id,
                    host_node.name,
                ))
                continue

            hdd_node = host_node.children[full_fs_id]
            if job.lrc_reserve_group in hdd_node.groups:
                logger.debug(
                    'Group {} found on hdd {} of host {}, removing from lrc reserve tree'.format(
                        job.lrc_reserve_group,
                        hdd_node.name,
                        host_node.name,
                    )
                )
                del hdd_node.groups[job.lrc_reserve_group]
                break
