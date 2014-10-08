from copy import copy
import itertools
from logging import getLogger
import msgpack
import storage
import threading
import time
import traceback

import elliptics
import msgpack

from coll import SortedCollection
from config import config
from infrastructure import infrastructure
import jobs
import keys
from sync import sync_manager
from sync.error import LockFailedError

import timed_queue

logger = getLogger('mm.planner')


class Planner(object):

    MOVE_CANDIDATES = 'move_candidates'
    RECOVER_DC_QUEUE_UPDATE = 'recover_dc_queue_update'
    RECOVER_DC = 'recover_dc'

    def __init__(self, meta_session, job_processor):

        self.params = config.get('planner', config.get('smoother')) or {}

        logger.info('Planner initializing')
        self.candidates = []
        self.recover_dc_queue = []
        self._recover_dc_queue_lock = threading.Lock()
        self.meta_session = meta_session
        self.job_processor = job_processor
        self.__max_plan_length = self.params.get('max_plan_length', 5)
        self.__tq = timed_queue.TimedQueue()
        self.__tq.start()

        if (self.params.get('enabled', False)):
            self.__tq.add_task_in(self.MOVE_CANDIDATES,
                10, self._move_candidates)
            self.__tq.add_task_in(self.RECOVER_DC_QUEUE_UPDATE,
                10, self._recover_dc_queue_update)
            self.__tq.add_task_in(self.RECOVER_DC,
                11, self._recover_dc)


    def _move_candidates(self):
        try:
            logger.info('Starting move candidates planner')

            # prechecking for new or pending tasks
            if self.__executing_jobs(jobs.JobFactory.TYPE_MOVE_JOB):
                return

            self._do_move_candidates()
        except Exception as e:
            logger.error('{0}: {1}'.format(e, traceback.format_exc()))
        finally:
            logger.info('Move candidates planner finished')
            self.__tq.add_task_in(self.MOVE_CANDIDATES,
                self.params.get('generate_plan_period', 1800),
                self._move_candidates)

    def __executing_jobs(self, job_type):
        for job in self.job_processor.jobs.itervalues():
            if job_type != job.type:
                continue
            if job.status in (jobs.Job.STATUS_NOT_APPROVED,):
                logger.info('Planer found at least one not approved job of type {0} '
                    '({1})'.format(job_type, job.id))
                return True
        return False

    def __busy_hosts(self, job_type):
        hosts = set()
        for job in self.job_processor.jobs.itervalues():
            if job_type != job.type:
                continue
            hosts.update([job.src_host, job.dst_host])
        return hosts

    def __apply_plan(self):

        logger.info('self candidates: {0}'.format(self.candidates))

        candidates = [c[0] for c in self.candidates]

        try:

            logger.debug('Lock acquiring')
            with sync_manager.lock(self.job_processor.JOBS_LOCK):
                logger.debug('Lock acquired')

                self.job_processor._do_update_jobs()

                if self.__executing_jobs(jobs.JobFactory.TYPE_MOVE_JOB):
                    raise ValueError('Not finished move jobs are found')

                for i, candidate in enumerate(candidates):
                    logger.info('Candidate {0}: data {1}, ms_error delta {2}:'.format(
                        i, gb(candidate.delta.data_move_size), candidate.delta.ms_error_delta))

                    for src_group, src_dc, dst_group, dst_dc in candidate.moved_groups:

                        assert len(src_group.node_backends) == 1, 'Src group {0} should have only 1 node backend'.format(src_group.group_id)
                        assert len(dst_group.node_backends) == 1, 'Dst group {0} should have only 1 node backend'.format(dst_group.group_id)

                        src_dc_cnt = infrastructure.get_dc_by_host(src_group.node_backends[0].node.host.addr)
                        dst_dc_cnt = infrastructure.get_dc_by_host(dst_group.node_backends[0].node.host.addr)

                        assert src_dc_cnt == src_dc, \
                            'Dc for src group {0} has been changed: {1} != {2}'.format(
                                src_group.group_id, src_dc_cnt, src_dc)
                        assert dst_dc_cnt == dst_dc, \
                            'Dc for dst group {0} has been changed: {1} != {2}'.format(
                                dst_group.group_id, dst_dc_cnt, dst_dc)

                        logger.info('Group {0} ({1}) moves to {2} ({3})'.format(
                            src_group.group_id, src_dc,
                            dst_group.group_id, dst_dc))

                        try:
                            job = self.job_processor.create_job([
                                jobs.JobFactory.TYPE_MOVE_JOB,
                                {'group': src_group.group_id,
                                 'uncoupled_group': dst_group.group_id,
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
                                 }])
                            logger.info('Job successfully created: {0}'.format(job['id']))
                        except Exception as e:
                            logger.error('Failed to create move job for moving '
                                'group {0} ({1}) to {2} ({3}): {4}\n{5}'.format(
                                    src_group.group_id, src_dc,
                                    dst_group.group_id, dst_dc,
                                    e, traceback.format_exc()))
                            raise AssertionError('Job creation failed')

        except ValueError as e:
            logger.error('Plan cannot be applied: {0}'.format(e))

    def _do_move_candidates(self, step=0, busy_hosts=None):
        if step == 0:
            self.candidates = [[StorageState.current()]]
        if busy_hosts is None:
            busy_hosts = self.__busy_hosts(JobFactory.TYPE_MOVE_JOB)
            logger.debug('Busy hosts from executing jobs: {0}'.format(list(busy_hosts)))

        if step >= self.__max_plan_length:
            self.__apply_plan()
            return

        logger.info('Candidates: {0}, step {1}'.format(len(self.candidates[-1]), step))

        tmp_candidates = self._generate_candidates(self.candidates[-1][0], busy_hosts)

        if not tmp_candidates:
            self.__apply_plan()
            return

        max_error_candidate = max(tmp_candidates, key=lambda c: c.delta.weight)

        self.candidates.append([max_error_candidate])
        logger.info('Max error candidate: {0}'.format(max_error_candidate))
        logger.info('Max error candidate moved groups: {0}'.format([
            (src_group, src_dc, dst_group, dst_dc) for src_group, src_dc, dst_group, dst_dc in max_error_candidate.moved_groups]))
        for src_group, src_dc, dst_group, dst_dc in max_error_candidate.moved_groups:
            busy_hosts.add(src_group.node_backends[0].node.host.addr)
            busy_hosts.add(dst_group.node_backends[0].node.host.addr)

        self._do_move_candidates(step=step + 1, busy_hosts=busy_hosts)


    def _generate_candidates(self, candidate, busy_hosts):
        _candidates = []

        avg = candidate.mean_unc_percentage

        base_ms = candidate.state_ms_error

        for c in itertools.permutations(candidate.iteritems(), 2):
            (src_dc, src_dc_state), (dst_dc, dst_dc_state) = c

            if src_dc_state.unc_percentage > avg:
                logger.debug('Pair src {0} and dst {1}: src_dc percentage > avg percentage'.format(
                    src_dc, dst_dc, src_dc_state.unc_percentage, avg))
                continue

            if dst_dc_state.unc_percentage < avg:
                logger.debug('Pair src {0} and dst {1}: dst_dc percentage < avg percentage'.format(
                    src_dc, dst_dc, dst_dc_state.unc_percentage, avg))
                continue

            for src_group in src_dc_state.groups:

                src_group_stat = candidate.stats(src_group)

                if src_group.couple in dst_dc_state.couples:
                    # can't move group to dst_dc because a group
                    # from the same couple is already located there
                    continue

                src_host = src_group.node_backends[0].node.host.addr
                if src_host in busy_hosts:
                    continue

                dst_group = None
                for unc_group in dst_dc_state.uncoupled_groups:
                    unc_group_stat = candidate.stats(unc_group)
                    if unc_group_stat.total_space < src_group_stat.total_space:
                        continue
                    if unc_group_stat.files + unc_group_stat.files_removed > 0:
                        continue
                    elif unc_group_stat.free_space < src_group_stat.used_space:
                        logger.warn('Uncoupled group {0} seems to have a lot of '
                            'used space (supposed to be empty)'.format(
                                unc_group.group_id))
                        continue

                    dst_host = unc_group.node_backends[0].node.host.addr
                    if dst_host in busy_hosts:
                        logger.debug('dst group {0} is skipped, {1} is in busy hosts'.format(
                            unc_group.group_id, dst_host))
                        continue

                    dst_group = unc_group
                    break

                if not dst_group:
                    # no suitable uncoupled group found
                    continue

                new_candidate = candidate.copy()

                new_candidate.move_group(src_dc, src_group, dst_dc, dst_group)

                if new_candidate.state_ms_error < base_ms:
                    logger.debug('good candidate found: {0} group from {1} to {2}, '
                        'deviation changed from {3} to {4} (weight: {5}, lost space {6}) '
                        '(swap with group {7})'.format(
                            src_group.group_id, src_dc, dst_dc, base_ms,
                            new_candidate.state_ms_error, new_candidate.delta.weight,
                            gb(new_candidate.delta.lost_space), dst_group.group_id))
                    _candidates.append(new_candidate)
                else:
                    logger.debug('bad candidate: {0} group from {1} to {2}, '
                        'deviation changed from {3} to {4} (swap with group {5})'.format(
                            src_group.group_id, src_dc, dst_dc, base_ms,
                            new_candidate.state_ms_error, dst_group.group_id))
                    logger.debug('Base candidate:')
                    candidate._debug()
                    logger.debug('New candidate aftere moving:')
                    new_candidate._debug()

                time.sleep(0.1)
            time.sleep(3)

        return _candidates

    def _recover_dc_queue_update(self):
        try:
            logger.info('Updating recover dc queue')

            with self._recover_dc_queue_lock:
                self._do_sync_recover_dc_queue()

        except Exception as e:
            logger.error('Failed to update recover dc queue: {0}\n{1}'.format(
                e, traceback.format_exc()))
        finally:
            logger.info('Updating recover dc queue finished')
            self.__tq.add_task_in(self.RECOVER_DC_QUEUE_UPDATE,
                self.params.get('recover_dc_queue_update_period', 60),
                self._recover_dc_queue_update)

    def _do_sync_recover_dc_queue(self):
        try:
            self.recover_dc_queue = list(msgpack.unpackb(self.meta_session.read_data(
                    keys.MM_PLANNER_RECOVER_DC_QUEUE).get()[0].data))
        except elliptics.NotFoundError:
            self.recover_dc_queue = []

    def _do_update_recover_dc_queue(self):
        self.meta_session.write_data(keys.MM_PLANNER_RECOVER_DC_QUEUE,
            msgpack.packb(self.recover_dc_queue)).get()

    def _recover_dc(self):
        try:
            logger.info('Starting recover dc planner')

            # prechecking for new or pending tasks
            if self.__executing_jobs(jobs.JobFactory.TYPE_RECOVER_DC_JOB):
                logger.info('Unfinished recover dc jobs found')
                return

            self._do_recover_dc()
        except LockFailedError:
            pass
        except Exception as e:
            logger.error('{0}: {1}'.format(e, traceback.format_exc()))
        finally:
            logger.info('Recover dc planner finished')
            self.__tq.add_task_in(self.RECOVER_DC,
                self.params.get('recover_dc_period', 60),
                self._recover_dc)

    def _do_recover_dc(self):

        self.job_processor._do_update_jobs()

        logger.debug('Lock acquiring')
        with sync_manager.lock(self.job_processor.JOBS_LOCK):
            logger.debug('Lock acquired')

            self.job_processor._do_update_jobs()

            if self.__executing_jobs(jobs.JobFactory.TYPE_RECOVER_DC_JOB):
                logger.info('Unfinished recover dc jobs found')
                return

            with self._recover_dc_queue_lock:
                self._do_sync_recover_dc_queue()

                if not self.recover_dc_queue:
                    # create new recover dc queue
                    logger.info('Recover dc queue is empty, setting it up')
                    self.recover_dc_queue = self.__construct_recover_dc_queue()

                for i in xrange(min(self.params.get('recover_dc', {}).get('jobs_batch_size', 10),
                                    len(self.recover_dc_queue))):
                    couple_tuple, group_id = self.recover_dc_queue.pop()

                    couple_str = ':'.join((str(gid) for gid in sorted(couple_tuple)))
                    if not couple_str in storage.couples:
                        logger.warn('Couple {0} is not found in storage'.format(couple_str))
                        continue
                    couple = storage.couples[couple_str]
                    group = storage.groups[group_id]

                    # check if backend exists
                    node_backend = group.node_backends[0]

                    try:
                        job = self.job_processor.create_job(['recover_dc_job', {
                            'group': group.group_id,
                            'host': node_backend.node.host.addr,
                            'port': node_backend.node.port,
                            'family': node_backend.node.family,
                            'backend_id': node_backend.backend_id}])
                        logger.info('Created recover dc job for couple {0}, '
                            'group {1}'.format(couple, group))
                    except Exception as e:
                        logger.error('Failed to create recover dc job: {0}'.format(e))
                        continue

                self._do_update_recover_dc_queue()

    def __construct_recover_dc_queue(self):
        couples_to_recover = []
        for couple in storage.couples:
            # TODO: GOOD_STATUSES or NOT_BAD_STATUSES?
            if couple.status not in storage.GOOD_STATUSES:
                continue
            group_alive_keys = {}
            for group in couple:
                for nb in group.node_backends:
                    group_alive_keys.setdefault(group, 0)
                    group_alive_keys[group] += nb.stat.files
            alive_keys = group_alive_keys.values()
            if all([k == alive_keys[0] for k in alive_keys]):
                # number of keys in all groups is equal
                continue
            min_group = min(group_alive_keys.keys(), key=lambda k: group_alive_keys[k])
            max_group = max(group_alive_keys.keys(), key=lambda k: group_alive_keys[k])
            keys_diff = group_alive_keys[max_group] - group_alive_keys[min_group]

            logger.info('Adding couple {0} to recover dc queue, number of keys '
                'in groups: {1}, recover will be launched on group {2}'.format(
                    min_group.couple.as_tuple(), alive_keys, min_group.group_id))

            couples_to_recover.append((couple, min_group, keys_diff))

        couples_to_recover.sort(key=lambda c: c[2])

        return [(c[0].as_tuple(), c[1].group_id,) for c in couples_to_recover]


class Delta(object):
    def __init__(self):
        self.data_move_size = 0.0
        self.ms_error_delta = 0.0
        self.lost_space = 0.0

    def __add__(self, other):
        res = Delta()
        res.data_move_size = self.data_move_size + other.data_move_size
        res.ms_error_delta = self.ms_error_delta + other.ms_error_delta
        res.lost_space = self.lost_space + other.lost_space

    def copy(self):
        res = Delta()
        res.data_move_size = self.data_move_size
        res.ms_error_delta = self.ms_error_delta
        res.lost_space = self.lost_space
        return res

    @property
    def weight(self):
        return -self.ms_error_delta - (self.lost_space / (50 * 1024 * 1024 * 1024))


class DcState(object):
    def __init__(self, storage_state):
        self.groups = []
        self.total_space = 0.0
        self.uncoupled_space = 0.0
        self.storage_state = storage_state
        self.couples = set()
        self.uncoupled_keys = []
        self.uncoupled_groups = SortedCollection(key=lambda x: storage_state.stats(x).total_space)

    def add_group(self, group):
        self.groups.append(group)
        assert group.couple
        self.couples.add(group.couple)

    def add_uncoupled_group(self, group):
        self.uncoupled_groups.insert(group)
        self.uncoupled_space += self.storage_state.stats(group).total_space

    def copy(self, storage_state):
        obj = DcState(storage_state)
        obj.groups = copy(self.groups)
        obj.total_space, obj.uncoupled_space = self.total_space, self.uncoupled_space
        obj.couples = copy(self.couples)
        obj.uncoupled_groups = SortedCollection(iterable=self.uncoupled_groups._items, key=lambda x: storage_state.stats(x).total_space)
        return obj

    def apply_stat(self, stat):
        self.total_space += stat.total_space

    @property
    def unc_percentage(self):
        return float(self.uncoupled_space) / self.total_space


class StorageState(object):
    def __init__(self):
        self.delta = Delta()
        self.state = dict([(dc, DcState(self)) for dc in self.__dcs()])
        self._stats = {}
        self.moved_groups = []

    @classmethod
    def current(cls):
        obj = cls()
        for couple in cls.__get_full_couples():
            for group in couple.groups:
                dc = group.node_backends[0].node.host.dc
                obj._stats[group.group_id] = group.get_stat()
                obj.state[dc].add_group(group)

        for group in cls.__get_uncoupled_groups():
            dc = group.node_backends[0].node.host.dc
            obj._stats[group.group_id] = group.get_stat()
            obj.state[dc].add_uncoupled_group(group)

        fsids = set()
        for group in storage.groups:
            for nb in group.node_backends:
                if nb.stat is None:
                    continue
                if not nb.stat.fsid in fsids:
                    dc = nb.node.host.dc
                    obj.state[dc].apply_stat(nb.stat)
                    fsids.add(nb.stat.fsid)

        logger.debug('Current state:')
        for dc, dc_state in obj.state.iteritems():
            logger.debug('dc: {0}, total_space {1}, uncoupled_space {2}, unc_percentage {3}'.format(
                dc, dc_state.total_space, dc_state.uncoupled_space, dc_state.unc_percentage))
        return obj

    def copy(self):
        obj = StorageState()

        obj._stats = copy(self._stats)

        for dc, dc_state in self.state.iteritems():
            obj.state[dc] = dc_state.copy(obj)

        obj.delta.data_move_size = self.delta.data_move_size

        return obj

    @property
    def mean_unc_percentage(self):
        unc_space = sum([dc_state.uncoupled_space
            for dc_state in self.state.itervalues()])
        total_space = sum([dc_state.total_space
            for dc_state in self.state.itervalues()])

        return float(unc_space) / total_space


    def _debug(self):
        logger.debug('mean percentage: {0}'.format(self.mean_unc_percentage))
        for dc, dc_state in self.state.iteritems():
            logger.debug('dc: {0}, unc_percentage {1}, unc_space {2} '
                'total_space {3}'.format(dc, dc_state.unc_percentage,
                    dc_state.uncoupled_space, dc_state.total_space))

    def stats(self, group):
        return self._stats[group.group_id]

    @property
    def state_ms_error(self):

        avg = self.mean_unc_percentage

        lms_error = 0.0
        for dc_state in self.state.itervalues():
            lms_error += ((dc_state.unc_percentage - avg) * 100) ** 2

        return lms_error

    def iteritems(self):
        return self.state.iteritems()

    @staticmethod
    def __get_full_couples():
        couples = []

        for couple in storage.couples:
            if couple.status != storage.Status.FULL:
                continue

            for group in couple.groups:
                if len(group.node_backends) > 1:
                    break
            else:
                couples.append(couple)

        return couples

    @staticmethod
    def __get_uncoupled_groups():
        groups = []

        for group in storage.groups:
            if group.couple is not None:
                continue
            if len(group.node_backends) != 1:
                continue
            if group.status != storage.Status.INIT:
                continue

            groups.append(group)

        return groups

    @staticmethod
    def __dcs():
        dcs = set()
        for group in storage.groups:
            for nb in group.node_backends:
                dcs.add(nb.node.host.dc)
        return dcs


    def move_group(self, src_dc, src_group, dst_dc, dst_group):

        old_ms_error = self.state_ms_error

        # logger.info('{0}'.format(self.state))
        self.state[src_dc].groups.remove(src_group)
        self.state[dst_dc].groups.append(src_group)

        # do this only in case if no other group from this couple is located in src_dc
        for group in src_group.couple:
            if group != src_group and group.node_backends[0].node.host.dc == src_dc:
                break
        else:
            self.state[src_dc].couples.remove(src_group.couple)

        self.state[dst_dc].couples.add(src_group.couple)

        self.state[src_dc].uncoupled_space += self.stats(src_group).total_space
        self.state[dst_dc].uncoupled_space -= self.stats(dst_group).total_space

        self.state[dst_dc].uncoupled_groups.remove(dst_group)
        # For now src_group is not swapped with dst_group but just replaces it
        # self.state[src_dc].uncoupled_groups.insert(dst_group)

        # updating delta object
        self.delta.data_move_size += self.stats(src_group).used_space
        self.delta.ms_error_delta += self.state_ms_error - old_ms_error

        self.delta.lost_space += self.stats(dst_group).total_space - self.stats(src_group).used_space

        self.moved_groups.append((src_group, src_dc, dst_group, dst_dc))


def gb(bytes):
    return (bytes / (1024.0 * 1024.0 * 1024))

