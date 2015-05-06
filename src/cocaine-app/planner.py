from copy import copy, deepcopy
import itertools
import heapq
from logging import getLogger
import msgpack
import socket
import storage
import threading
import time
import traceback

import elliptics
import msgpack
import pymongo

from coll import SortedCollection
from config import config
from db.mongo.pool import Collection
import helpers as h
from infrastructure import infrastructure
from infrastructure_cache import cache
import inventory
import jobs
import keys
from manual_locks import manual_locker
from sync import sync_manager
from sync.error import LockFailedError

import timed_queue

logger = getLogger('mm.planner')


class Planner(object):

    MOVE_CANDIDATES = 'move_candidates'
    RECOVER_DC = 'recover_dc'
    COUPLE_DEFRAG = 'couple_defrag'

    RECOVERY_OP_CHUNK = 200

    def __init__(self, meta_session, db, niu, job_processor):

        self.params = config.get('planner', config.get('smoother')) or {}

        logger.info('Planner initializing')
        self.candidates = []
        self.meta_session = meta_session
        self.job_processor = job_processor
        self.__max_plan_length = self.params.get('max_plan_length', 5)
        self.__tq = timed_queue.TimedQueue()

        self.node_info_updater = niu

        if config['metadata'].get('planner', {}).get('db'):
            self.collection = Collection(db[config['metadata']['planner']['db']], 'planner')

            if (self.params.get('enabled', False)):
                self.__tq.add_task_in(self.MOVE_CANDIDATES,
                    10, self._move_candidates)
                self.__tq.add_task_in(self.RECOVER_DC,
                    11, self._recover_dc)
                self.__tq.add_task_in(self.COUPLE_DEFRAG,
                    12, self._couple_defrag)

    def _start_tq(self):
        self.__tq.start()

    def _move_candidates(self):
        try:
            logger.info('Starting move candidates planner')

            max_move_jobs = config.get('jobs', {}).get('move_job',
                {}).get('max_executing_jobs', 3)

            # prechecking for new or pending tasks
            count = self.job_processor.job_finder.jobs_count(types=jobs.JobTypes.TYPE_MOVE_JOB,
                statuses=[jobs.Job.STATUS_NOT_APPROVED,
                          jobs.Job.STATUS_NEW,
                          jobs.Job.STATUS_EXECUTING,
                          jobs.Job.STATUS_PENDING])

            if count >= max_move_jobs:
                logger.info('Found {0} unfinished move jobs '
                    '(>= {1})'.format(count, max_move_jobs))
                return

            self._do_move_candidates(max_move_jobs - count)
        except Exception as e:
            logger.error('{0}: {1}'.format(e, traceback.format_exc()))
        finally:
            logger.info('Move candidates planner finished')
            self.__tq.add_task_in(self.MOVE_CANDIDATES,
                self.params.get('generate_plan_period', 1800),
                self._move_candidates)

    def __busy_hosts(self, job_type):
        not_finished_jobs = self.job_processor.job_finder.jobs(types=job_type, statuses=(
            jobs.Job.STATUS_NOT_APPROVED,
            jobs.Job.STATUS_NEW,
            jobs.Job.STATUS_EXECUTING,
            jobs.Job.STATUS_PENDING,
            jobs.Job.STATUS_BROKEN))

        hosts = set()
        for job in not_finished_jobs:
            if hasattr(job, 'src_host'):
                hosts.add(job.src_host)
            elif hasattr(job, 'src_group'):
                src_group_id = job.src_group
                if src_group_id in storage.groups:
                    src_group = storage.groups[src_group_id]
                    if src_group.node_backends:
                        hosts.add(src_group.node_backends[0].node.host.addr)
            if hasattr(job, 'dst_host'):
                hosts.add(job.dst_host)
            elif hasattr(job, 'group'):
                dst_group_id = job.group
                if dst_group_id in storage.groups:
                    dst_group = storage.groups[dst_group_id]
                    if dst_group.node_backends:
                        hosts.add(dst_group.node_backends[0].node.host.addr)
        return hosts

    def __apply_plan(self):

        logger.info('self candidates: {0}'.format(self.candidates))

        candidates = [c[0] for c in self.candidates]

        try:

            logger.debug('Lock acquiring')
            with sync_manager.lock(self.job_processor.JOBS_LOCK):
                logger.debug('Lock acquired')

                max_move_jobs = config.get('jobs', {}).get('move_job',
                    {}).get('max_executing_jobs', 3)

                # prechecking for new or pending tasks
                jobs_count = self.job_processor.job_finder.jobs_count(types=jobs.JobTypes.TYPE_MOVE_JOB,
                    statuses=[jobs.Job.STATUS_NOT_APPROVED,
                              jobs.Job.STATUS_NEW,
                              jobs.Job.STATUS_EXECUTING,
                              jobs.Job.STATUS_PENDING])

                if jobs_count >= max_move_jobs:
                    raise ValueError('Found {0} unfinished move jobs'.format(
                        jobs_count))

                for i, candidate in enumerate(candidates):
                    logger.info('Candidate {0}: data {1}, ms_error delta {2}:'.format(
                        i, gb(candidate.delta.data_move_size), candidate.delta.ms_error_delta))

                    for src_group, src_dc, dst_group, merged_groups, dst_dc in candidate.moved_groups:

                        assert len(src_group.node_backends) == 1, 'Src group {0} should have only 1 node backend'.format(src_group.group_id)
                        assert len(dst_group.node_backends) == 1, 'Dst group {0} should have only 1 node backend'.format(dst_group.group_id)

                        src_dc_cnt = cache.get_dc_by_host(src_group.node_backends[0].node.host.addr)
                        dst_dc_cnt = cache.get_dc_by_host(dst_group.node_backends[0].node.host.addr)

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
                            job = self.job_processor._create_job(
                                jobs.JobTypes.TYPE_MOVE_JOB,
                                {'group': src_group.group_id,
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
                                 },
                                 force=True)
                            logger.info('Job successfully created: {0}'.format(job.id))
                        except jobs.LockFailedError as e:
                            logger.error('Failed to create move job for moving '
                                'group {0} ({1}) to {2} ({3}): {4}'.format(
                                    src_group.group_id, src_dc,
                                    dst_group.group_id, dst_dc, e))
                        except Exception as e:
                            logger.error('Failed to create move job for moving '
                                'group {0} ({1}) to {2} ({3}): {4}\n{5}'.format(
                                    src_group.group_id, src_dc,
                                    dst_group.group_id, dst_dc,
                                    e, traceback.format_exc()))

        except ValueError as e:
            logger.error('Plan cannot be applied: {0}'.format(e))

    def _do_move_candidates(self, max_plan_length, step=0, busy_hosts=None, busy_group_ids=None):
        if step == 0:
            self.candidates = [[StorageState.current()]]
        if busy_hosts is None:
            busy_hosts = self.__busy_hosts([jobs.JobTypes.TYPE_MOVE_JOB, jobs.JobTypes.TYPE_RESTORE_GROUP_JOB])
            logger.debug('Busy hosts from executing jobs: {0}'.format(list(busy_hosts)))
        if busy_group_ids is None:
            busy_group_ids = set(self.job_processor.get_uncoupled_groups_in_service())
            logger.debug('Busy uncoupled groups from executing jobs: {0}'.format(list(busy_group_ids)))

        if step >= min(self.__max_plan_length, max_plan_length):
            self.__apply_plan()
            return

        logger.info('Candidates: {0}, step {1}'.format(len(self.candidates[-1]), step))

        tmp_candidates = self._generate_candidates(self.candidates[-1][0], busy_hosts, busy_group_ids)

        if not tmp_candidates:
            self.__apply_plan()
            return

        max_error_candidate = max(tmp_candidates, key=lambda c: c.delta.weight)

        self.candidates.append([max_error_candidate])
        logger.info('Max error candidate: {0}'.format(max_error_candidate))
        logger.info('Max error candidate moved groups: {0}'.format([
            (src_group, src_dc, dst_group, uncoupled_groups, dst_dc)
            for src_group, src_dc, dst_group, uncoupled_groups, dst_dc in max_error_candidate.moved_groups]))
        for src_group, src_dc, dst_group, uncoupled_groups, dst_dc in max_error_candidate.moved_groups:
            busy_hosts.add(src_group.node_backends[0].node.host.addr)
            busy_hosts.add(dst_group.node_backends[0].node.host.addr)

        self._do_move_candidates(max_plan_length,
            step=step + 1,
            busy_hosts=busy_hosts,
            busy_group_ids=busy_group_ids)


    def _generate_candidates(self, candidate, busy_hosts, busy_group_ids):
        _candidates = []

        avg = candidate.mean_unc_percentage

        base_ms = candidate.state_ms_error

        uncoupled_groups = infrastructure.get_good_uncoupled_groups(max_node_backends=1)

        for c in candidate.iteritems():
            src_dc, src_dc_state = c

            if src_dc_state.unc_percentage > avg:
                logger.debug('Src dc {0}: src_dc percentage > avg percentage'.format(
                    src_dc))
                continue

            for src_group in src_dc_state.groups:

                src_group_stat = candidate.stats(src_group)

                src_host = src_group.node_backends[0].node.host.addr
                if src_host in busy_hosts:
                    continue

                suitable_uncoupled_groups = self.get_suitable_uncoupled_groups_list(
                    src_group, uncoupled_groups, busy_group_ids=busy_group_ids)

                for candidates in suitable_uncoupled_groups:
                    logger.debug('checking src_group {0} against candidate {1}'.format(
                        src_group.group_id, candidates))

                    unc_group, merged_groups = candidates[0], candidates[1:]

                    dst_host = unc_group.node_backends[0].node.host.addr
                    dst_dc = unc_group.node_backends[0].node.host.dc
                    if dst_dc == src_dc:
                        logger.debug('dst group {0} is skipped, dst dc is the '
                            'same as source dc: {1}'.format(
                                unc_group.group_id, dst_dc))

                    unc_group_stat = candidate.stats(unc_group)

                    # TODO: check merged groups for files

                    if unc_group_stat.files + unc_group_stat.files_removed > 0:
                        continue


                    if dst_host in busy_hosts:
                        logger.debug('dst group {0} is skipped, {1} is in busy hosts'.format(
                            unc_group.group_id, dst_host))
                        continue

                    break
                else:
                    # no suitable uncoupled group found
                    continue

                new_candidate = candidate.copy()
                new_candidate.move_group(src_dc, src_group, dst_dc, unc_group, merged_groups)

                if new_candidate.state_ms_error < base_ms:
                    logger.debug('good candidate found: {0} group from {1} to {2}, '
                        'deviation changed from {3} to {4} (weight: {5}, lost space {6}) '
                        '(swap with group {7})'.format(
                            src_group.group_id, src_dc, dst_dc, base_ms,
                            new_candidate.state_ms_error, new_candidate.delta.weight,
                            gb(new_candidate.delta.lost_space), unc_group.group_id))
                    _candidates.append(new_candidate)
                else:
                    logger.debug('bad candidate: {0} group from {1} to {2}, '
                        'deviation changed from {3} to {4} (swap with group {5})'.format(
                            src_group.group_id, src_dc, dst_dc, base_ms,
                            new_candidate.state_ms_error, unc_group.group_id))
                    logger.debug('Base candidate:')
                    candidate._debug()
                    logger.debug('New candidate aftere moving:')
                    new_candidate._debug()

                time.sleep(0.1)
            time.sleep(3)

        return _candidates

    def _recover_dc(self):
        try:
            logger.info('Starting recover dc planner')

            max_recover_jobs = config.get('jobs', {}).get('recover_dc_job',
                {}).get('max_executing_jobs', 3)
            # prechecking for new or pending tasks
            count = self.job_processor.job_finder.jobs_count(
                types=jobs.JobTypes.TYPE_RECOVER_DC_JOB,
                statuses=[jobs.Job.STATUS_NOT_APPROVED,
                          jobs.Job.STATUS_NEW,
                          jobs.Job.STATUS_EXECUTING])
            if count >= max_recover_jobs:
                logger.info('Found {0} unfinished recover dc jobs '
                    '(>= {1})'.format(count, max_recover_jobs))
                return

            self._do_recover_dc()
        except LockFailedError:
            pass
        except Exception as e:
            logger.error('{0}: {1}'.format(e, traceback.format_exc()))
        finally:
            logger.info('Recover dc planner finished')
            self.__tq.add_task_in(self.RECOVER_DC,
                self.params.get('recover_dc', {}).get('recover_dc_period', 60),
                self._recover_dc)

    def _do_recover_dc(self):

        logger.debug('Lock acquiring')
        with sync_manager.lock(self.job_processor.JOBS_LOCK):
            logger.debug('Lock acquired')

            max_recover_jobs = config.get('jobs', {}).get('recover_dc_job',
                {}).get('max_executing_jobs', 3)

            jobs_count = self.job_processor.job_finder.jobs_count(
                types=jobs.JobTypes.TYPE_RECOVER_DC_JOB,
                statuses=(jobs.Job.STATUS_NOT_APPROVED,
                          jobs.Job.STATUS_NEW,
                          jobs.Job.STATUS_EXECUTING))

            slots = max_recover_jobs - jobs_count
            if slots <= 0:
                logger.info('Found {0} unfinished recover dc jobs'.format(
                    jobs_count))
                return

            created_jobs = 0
            logger.info('Trying to create {0} jobs'.format(slots))

            need_approving = not self.params.get('recover_dc', {}).get('autoapprove', False)

            for couple_id in self._recover_top_weight_couples(slots):

                logger.info('Creating recover job for couple {0}'.format(couple_id))

                couple = storage.couples[couple_id]

                if not _recovery_applicable_couple(couple):
                    logger.info('Couple {0} is no more applicable for recovery job'.format(couple_id))
                    continue

                try:
                    job = self.job_processor._create_job(
                        jobs.JobTypes.TYPE_RECOVER_DC_JOB,
                        {'couple': couple_id,
                         'need_approving': need_approving})
                    logger.info('Created recover dc job for couple {0}, '
                        'job id {1}'.format(couple, job.id))
                    created_jobs += 1
                except Exception as e:
                    logger.error('Failed to create recover dc job for '
                        'couple {0}: {1}'.format(couple_id, e))
                    continue

            logger.info('Successfully created {0} recover dc jobs'.format(created_jobs))

    def sync_recover_data(self):

        recover_data_couples = set()

        offset = 0
        while True:
            cursor = (self.collection.find(fields=['couple'],
                sort=[('couple', pymongo.ASCENDING)],
                skip=offset, limit=self.RECOVERY_OP_CHUNK))
            count = 0
            for rdc in cursor:
                recover_data_couples.add(rdc['couple'])
                count += 1
            offset += count

            if count < self.RECOVERY_OP_CHUNK:
                break

        ts = int(time.time())

        storage_couples = set([str(c) for c in storage.couples.keys()])

        logger.info('recover couples: {0}'.format(recover_data_couples))
        logger.info('storage_couples: {0}'.format(storage_couples))

        add_couples = list(storage_couples - recover_data_couples)
        remove_couples = list(recover_data_couples - storage_couples)

        logger.info('Couples to add to recover data list: {0}'.format(add_couples))
        logger.info('Couples to remove from recover data list: {0}'.format(remove_couples))

        offset = 0
        while offset < len(add_couples):
            bulk_op = self.collection.initialize_unordered_bulk_op()
            bulk_add_couples = add_couples[offset:offset + self.RECOVERY_OP_CHUNK]
            for couple in bulk_add_couples:
                bulk_op.insert({'couple': couple,
                                'recover_ts': ts})
            res = bulk_op.execute()
            if res['nInserted'] != len(bulk_add_couples):
                raise ValueError('failed to add couples recover data: {0}/{1} ({2})'.format(
                    res['nInserted'], len(bulk_add_couples), res))
            offset += res['nInserted']

        offset = 0
        while offset < len(remove_couples):
            bulk_op = self.collection.initialize_unordered_bulk_op()
            bulk_remove_couples = remove_couples[offset:offset + self.RECOVERY_OP_CHUNK]
            bulk_op.find({'couple': {'$in': bulk_remove_couples}}).remove()
            res = bulk_op.execute()
            if res['nRemoved'] != len(bulk_remove_couples):
                raise ValueError('failed to remove couples recover data: {0}/{1} ({2})'.format(
                    res['nRemoved'], len(bulk_remove_couples), res))
            offset += res['nRemoved']

    def _recover_top_weight_couples(self, count):
        keys_diffs_sorted = []
        keys_diffs = {}
        ts_diffs = {}

        coupled_locks = sync_manager.get_children_locks(jobs.Job.COUPLE_LOCK_PREFIX)
        locked_couples = set([lock[len(jobs.Job.COUPLE_LOCK_PREFIX):] for lock in coupled_locks])
        logger.debug('Locked couples: {0}'.format(locked_couples))

        for couple in storage.couples.keys():
            if not _recovery_applicable_couple(couple, locked_couples=locked_couples):
                continue
            c_diff = couple.keys_diff
            keys_diffs_sorted.append((str(couple), c_diff))
            keys_diffs[str(couple)] = c_diff
        keys_diffs_sorted.sort(key=lambda x: x[1])

        KEYS_CF = 86400  # one key loss is equal to one day without recovery
        TS_CF = 1

        cursor = self.collection.find().sort('recover_ts', pymongo.ASCENDING)
        if cursor.count() < len(storage.couples):
            logger.info('Sync recover data is required: {0} records/{1} couples'.format(
                cursor.count(), len(storage.couples)))
            self.sync_recover_data()
            cursor = self.collection.find().sort('recover_ts', pymongo.ASCENDING)

        ts = int(time.time())

        def weight(keys_diff, last_ts):
            return keys_diff * KEYS_CF + (ts - last_ts) * TS_CF

        weights = {}
        candidates = []
        for i in xrange(cursor.count() / self.RECOVERY_OP_CHUNK + 1):
            couples_data = cursor[i * self.RECOVERY_OP_CHUNK:(i + 1) * self.RECOVERY_OP_CHUNK]
            max_recover_ts = None
            for couple_data in couples_data:
                c = couple_data['couple']
                ts_diffs[c] = couple_data['recover_ts']
                if not c in keys_diffs:
                    # couple was not applicable for recovery, skip
                    continue
                weights[c] = weight(keys_diffs[c], ts_diffs[c])
                max_recover_ts = couple_data['recover_ts']

            cursor.rewind()

            top_candidates_len = min(count, len(weights))

            if not top_candidates_len:
                continue

            # TODO: Optimize this
            candidates = sorted(weights.iteritems(), key=lambda x: x[1])
            min_weight_candidate = candidates[-top_candidates_len]
            min_keys_diff = 0
            for candidate in candidates[-top_candidates_len:]:
                c = candidate[0]
                min_keys_diff = min(min_keys_diff, keys_diffs[c])

            missed_candidate = None
            idx = len(keys_diffs_sorted) - 1
            while idx >= 0 and keys_diffs_sorted[idx] >= min_keys_diff:
                c, keys_diff = keys_diffs_sorted[idx]
                idx -= 1
                if c in ts_diffs:
                    continue
                if weight(keys_diff, max_recover_ts) > min_weight_candidate[1]:
                    # found possible candidate
                    missed_candidate = c
                    break

            logger.debug('Current round: {0}, current min weight candidate '
                '{1}, weight: {2}, possible missed candidate is '
                'couple {3}, keys diff: {4} (max recover ts = {5}, diff {6})'.format(
                    i, min_weight_candidate[0], min_weight_candidate[1],
                    missed_candidate, keys_diffs.get(missed_candidate, None),
                    max_recover_ts, ts - max_recover_ts))

            if missed_candidate is None:
                break

        logger.info('Top candidates: {0}'.format(candidates[-count:]))

        return [candidate[0] for candidate in candidates[-count:]]

    def update_recover_ts(self, couple_id, ts):
        ts = int(ts)
        res = self.collection.update({'couple': couple_id},
            {'couple': couple_id, 'recover_ts': ts},
            upsert=True)
        if res['ok'] != 1:
            logger.error('Unexpected mongo response during recover ts update: {0}'.format(res))
            raise RuntimeError('Mongo operation result: {0}'.format(res['ok']))

    def _couple_defrag(self):
        try:
            logger.info('Starting couple defrag planner')

            max_defrag_jobs = config.get('jobs', {}).get('couple_defrag_job',
                {}).get('max_executing_jobs', 3)
            # prechecking for new or pending tasks
            count = self.job_processor.job_finder.jobs_count(
                types=jobs.JobTypes.TYPE_COUPLE_DEFRAG_JOB,
                statuses=[jobs.Job.STATUS_NOT_APPROVED,
                          jobs.Job.STATUS_NEW,
                          jobs.Job.STATUS_EXECUTING])

            if count >= max_defrag_jobs:
                logger.info('Found {0} unfinished couple defrag jobs '
                    '(>= {1})'.format(count, max_defrag_jobs))
                return

            self._do_couple_defrag()
        except LockFailedError:
            pass
        except Exception as e:
            logger.error('{0}: {1}'.format(e, traceback.format_exc()))
        finally:
            logger.info('Couple defrag planner finished')
            self.__tq.add_task_in(self.COUPLE_DEFRAG,
                self.params.get('couple_defrag', {}).get('couple_defrag_period', 60),
                self._couple_defrag)

    def _do_couple_defrag(self):

        logger.debug('Lock acquiring')
        with sync_manager.lock(self.job_processor.JOBS_LOCK):
            logger.debug('Lock acquired')

            max_defrag_jobs = config.get('jobs', {}).get('couple_defrag_job',
                {}).get('max_executing_jobs', 3)

            jobs_count = self.job_processor.job_finder.jobs_count(
                types=jobs.JobTypes.TYPE_COUPLE_DEFRAG_JOB,
                statuses=[jobs.Job.STATUS_NOT_APPROVED,
                          jobs.Job.STATUS_NEW,
                          jobs.Job.STATUS_EXECUTING])

            slots = max_defrag_jobs - jobs_count
            if slots <= 0:
                logger.info('Found {0} unfinished recover dc jobs'.format(
                    jobs_count))
                return

            need_approving = not self.params.get('couple_defrag', {}).get('autoapprove', False)

            couples_to_defrag = []
            for couple in storage.couples:
                if couple.status not in storage.GOOD_STATUSES:
                    continue
                couple_stat = couple.get_stat()
                if couple_stat.files_removed_size == 0:
                    continue

                insufficient_space_nb = None

                for group in couple.groups:
                    for nb in group.node_backends:
                        if nb.stat.free_space < nb.stat.max_blob_base_size * 2:
                            insufficient_space_nb = nb
                            break
                    if insufficient_space_nb:
                        break

                if insufficient_space_nb:
                    logger.warn('Couple {0}: node backend {1} has insufficient '
                        'free space for defragmentation, max_blob_size {2}, '
                        'free_space {3}'.format(
                            str(couple), str(nb), nb.stat.max_blob_base_size,
                            nb.stat.free_space))
                    continue

                logger.info('Couple defrag candidate: {0}, max files_removed_size '
                    'in groups: {1}'.format(str(couple), couple_stat.files_removed_size))

                couples_to_defrag.append((str(couple), couple_stat.files_removed_size))

            created_jobs = 0
            logger.info('Trying to create {0} jobs'.format(slots))

            couples_to_defrag.sort(key=lambda c: c[1])

            while couples_to_defrag and created_jobs < slots:
                couple_tuple, fragmentation = couples_to_defrag.pop()

                try:
                    job = self.job_processor._create_job(
                        jobs.JobTypes.TYPE_COUPLE_DEFRAG_JOB,
                        {'couple': couple_tuple,
                         'need_approving': need_approving})
                    logger.info('Created couple defrag job for couple {0}, job id {1}'.format(
                        couple_tuple, job.id))
                    created_jobs += 1
                except Exception as e:
                    logger.error('Failed to create couple defrag job: {0}'.format(e))
                    continue

            logger.info('Successfully created {0} couple defrag jobs'.format(created_jobs))

    @h.concurrent_handler
    def restore_group(self, request):
        logger.info('----------------------------------------')
        logger.info('New restore group request: ' + str(request))

        if len(request) < 1:
            raise ValueError('Group id is required')

        try:
            group_id = int(request[0])
            if not group_id in storage.groups:
                raise ValueError
        except (TypeError, ValueError):
            raise ValueError('Group {0} is not found'.format(request[0]))

        try:
            use_uncoupled_group = bool(request[1])
        except (TypeError, ValueError, IndexError):
            use_uncoupled_group = False

        try:
            options = request[2]
        except IndexError:
            options = {}

        try:
            force = bool(request[3])
        except IndexError:
            force = False

        group = storage.groups[group_id]
        involved_groups = [group]
        involved_groups.extend(g for g in group.coupled_groups)
        self.node_info_updater.update_status(involved_groups)

        job_params = {'group': group.group_id}

        if len(group.node_backends) > 1:
            raise ValueError('Group {0} has {1} node backends, should have at most one'.format(
                group.group_id, len(group.node_backends)))

        if group.status not in (storage.Status.BAD, storage.Status.INIT, storage.Status.RO):
            raise ValueError('Group {0} has {1} status, should have BAD or INIT'.format(
                group.group_id, group.status))

        if (group.node_backends and group.node_backends[0].status not in (
                storage.Status.STALLED, storage.Status.INIT, storage.Status.RO)):
            raise ValueError('Group {0} has node backend {1} in status {2}, '
                'should have STALLED or INIT status'.format(
                    group.group_id, str(group.node_backends[0]), group.node_backends[0].status))

        if group.couple is None:
            raise ValueError('Group {0} is uncoupled'.format(group.group_id))

        candidates = []
        if not options.get('src_group'):
            for g in group.coupled_groups:
                if len(g.node_backends) != 1:
                    continue
                if not all(nb.status == storage.Status.OK for nb in g.node_backends):
                    continue
                candidates.append(g)

            if not candidates:
                raise ValueError('Group {0} cannot be restored, no suitable coupled '
                    'groups are found'.format(group.group_id))

            candidates.sort(key=lambda g: g.get_stat().files, reverse=True)
            src_group = candidates[0]

        else:
            src_group = storage.groups[int(options['src_group'])]
            if src_group not in group.couple.groups:
                raise ValueError('Group {0} cannot be restored from group {1}, '
                    'it is not member of a couple'.format(group.group_id, src_group.group_id))

        job_params['src_group'] = src_group.group_id

        if use_uncoupled_group:
            nodes_set = infrastructure.get_group_history(group.group_id)['nodes'][-1]['set']
            if len(nodes_set) == 1:
                uncoupled_groups = self.find_uncoupled_groups(group, nodes_set[0][0])
            else:
                uncoupled_groups = self.select_uncoupled_groups(group)
            job_params['uncoupled_group'] = uncoupled_groups[0].group_id
            job_params['merged_groups'] = [g.group_id for g in uncoupled_groups[1:]]

        with sync_manager.lock(self.job_processor.JOBS_LOCK, timeout=self.job_processor.JOB_MANUAL_TIMEOUT):
            job = self.job_processor._create_job(
                jobs.JobTypes.TYPE_RESTORE_GROUP_JOB,
                job_params, force=force)

        return job.dump()

    @h.concurrent_handler
    def move_groups_from_host(self, request):
        try:
            host_or_ip = request[0]
        except IndexError:
            raise ValueError('Host is required')

        try:
            hostname = socket.getfqdn(host_or_ip)
        except Exception as e:
            raise ValueError('Failed to get hostname for {0}: {1}'.format(host_or_ip, e))

        try:
            ips = h.ips_set(hostname)
        except Exception as e:
            raise ValueError('Failed to get ip list for {0}: {1}'.format(hostname, e))

        res = {
            'jobs': [],
            'failed': {},
        }

        with sync_manager.lock(self.job_processor.JOBS_LOCK, timeout=self.job_processor.JOB_MANUAL_TIMEOUT):
            for couple in storage.couples.keys():
                for group in couple.groups:
                    if len(group.node_backends) != 1:
                        # skip group if more than one backend
                        continue

                    if group.node_backends[0].node.host.addr in ips:
                        try:
                            job_params = self._get_move_group_params(group)
                            job = self.job_processor._create_job(
                                jobs.JobTypes.TYPE_MOVE_JOB,
                                job_params, force=True)
                            res['jobs'].append(job.dump())
                        except Exception as e:
                            logger.error('Failed to create move job for group {0}: {1}\n{2}'.format(
                                group.group_id, e, traceback.format_exc()))
                            res['failed'][group.group_id] = str(e)
                            pass

        return res

    @h.concurrent_handler
    def move_group(self, request):
        logger.info('----------------------------------------')
        logger.info('New move group request: ' + str(request))

        if len(request) < 1:
            raise ValueError('Group id is required')

        try:
            group_id = int(request[0])
            if not group_id in storage.groups:
                raise ValueError
        except (TypeError, ValueError):
            raise ValueError('Group {0} is not found'.format(request[0]))

        group = storage.groups[group_id]

        try:
            options = request[1]
        except IndexError:
            options = {}

        try:
            force = bool(request[2])
        except IndexError:
            force = False

        uncoupled_groups = []
        for gid in options.get('uncoupled_groups') or []:
            if not gid in storage.groups:
                raise ValueError('Uncoupled group {0} is not found'.format(gid))
            uncoupled_groups.append(storage.groups[gid])

        job_params = self._get_move_group_params(group, uncoupled_groups=uncoupled_groups)

        logger.debug('Lock acquiring')
        with sync_manager.lock(self.job_processor.JOBS_LOCK, timeout=self.job_processor.JOB_MANUAL_TIMEOUT):
            logger.debug('Lock acquired')
            job = self.job_processor._create_job(
                jobs.JobTypes.TYPE_MOVE_JOB,
                job_params, force=force)

        return job.dump()


    def _get_move_group_params(self, group, uncoupled_groups=None):

        involved_groups = [group]
        involved_groups.extend(g for g in group.coupled_groups)
        self.node_info_updater.update_status(involved_groups)

        if len(group.node_backends) != 1:
            raise ValueError('Group {0} has {1} node backends, currently '
                'only groups with 1 node backend can be used'.format(
                    group.group_id, len(group.node_backends)))

        src_backend = group.node_backends[0]

        if not uncoupled_groups:
            uncoupled_groups = self.select_uncoupled_groups(group)
            logger.info('Group {0} will be moved to uncoupled groups {1}'.format(
                group.group_id, [g.group_id for g in uncoupled_groups]))
        else:
            dc, fsid = None, None
            self.node_info_updater.update_status(uncoupled_groups)
            locked_hosts = manual_locker.get_locked_hosts()
            for unc_group in uncoupled_groups:
                if len(unc_group.node_backends) != 1:
                    raise ValueError('Group {0} has {1} node backends, currently '
                        'only groups with 1 node backend can be used'.format(
                            unc_group.group_id, len(unc_group.node_backends)))

                is_good = infrastructure.is_uncoupled_group_good(unc_group, locked_hosts, max_node_backends=1)
                if not is_good:
                    raise ValueError('Uncoupled group {0} is not applicable'.format(
                        unc_group.group_id))

                nb = unc_group.node_backends[0]
                if not dc:
                    dc, fsid = nb.node.host.dc, nb.fs.fsid
                elif dc != nb.node.host.dc or fsid != nb.fs.fsid:
                    raise ValueError('All uncoupled groups should be located '
                        'on a single hdd on the same host')

                if unc_group.couple or unc_group.status != storage.Status.INIT:
                    raise ValueError('Group {0} is not uncoupled'.format(unc_group.group_id))

        uncoupled_group, merged_groups = uncoupled_groups[0], uncoupled_groups[1:]

        dst_backend = uncoupled_group.node_backends[0]
        if dst_backend.status != storage.Status.OK:
            raise ValueError('Group {0} node backend {1} status is {2}, should be {3}'.format(
                uncoupled_group.group_id, dst_backend, dst_backend.status, storage.Status.OK))

        if config.get('forbidden_dc_sharing_among_groups', False):
            dcs = set()
            for g in group.coupled_groups:
                dcs.update(nb.node.host.dc for nb in g.node_backends)
            if dst_backend.node.host.dc in dcs:
                raise ValueError('Group {0} cannot be moved to uncoupled group {1}, '
                    'couple {2} already has groups in dc {3}'.format(
                        group.group_id, uncoupled_group.group_id,
                        group.couple, dst_backend.node.host.dc))


        job_params = {'group': group.group_id,
                      'uncoupled_group': uncoupled_group.group_id,
                      'merged_groups': [g.group_id for g in merged_groups],
                      'src_host': src_backend.node.host.addr,
                      'src_port': src_backend.node.port,
                      'src_family': src_backend.node.family,
                      'src_backend_id': src_backend.backend_id,
                      'src_base_path': src_backend.base_path,
                      'dst_host': dst_backend.node.host.addr,
                      'dst_port': dst_backend.node.port,
                      'dst_family': dst_backend.node.family,
                      'dst_backend_id': dst_backend.backend_id,
                      'dst_base_path': dst_backend.base_path}

        return job_params

    def account_job(self, ns_current_state, types, job, ns):

        add_groups = []
        remove_groups = []
        uncoupled_group_in_job = False

        try:
            group = storage.groups[job.group]
        except KeyError:
            logger.error('Group {0} is not found in storage (job {1})'.format(
                job.group, job.id))
            return

        try:
            if group.couple.namespace != ns:
                return
        except Exception:
            return

        if getattr(job, 'uncoupled_group', None) is not None:
            uncoupled_group_in_job = True
            try:
                uncoupled_group = storage.groups[job.uncoupled_group]
            except KeyError:
                logger.warn('Uncoupled group {0} is not found in storage '
                    '(job {1})'.format(job.uncoupled_group, job.id))
                pass
            else:
                uncoupled_group_fsid = job.uncoupled_group_fsid
                if uncoupled_group_fsid is None:
                    uncoupled_group_fsid = (uncoupled_group.node_backends and
                        uncoupled_group.node_backends[0].fs.fsid or None)
                if uncoupled_group_fsid:
                    add_groups.append((uncoupled_group, uncoupled_group_fsid))
                else:
                    logger.warn('Uncoupled group {0} has empty backend list '
                        '(job {1})'.format(job.uncoupled_group, job.id))

        # GOOD_STATUSES includes FULL status, but ns_current_state only accounts OK couples!
        # Change behaviour
        if group.couple.status not in storage.GOOD_STATUSES:
            for g in group.coupled_groups:
                for fsid in set(nb.fs.fsid for nb in g.node_backends):
                    add_groups.append((g, fsid))

        if uncoupled_group_in_job and group.couple.status in storage.GOOD_STATUSES:
            remove_groups.append((group, group.node_backends[0].fs.fsid))

        if (job.type == jobs.JobTypes.TYPE_RESTORE_GROUP_JOB and
            not uncoupled_group_in_job and
            group.couple.status not in storage.GOOD_STATUSES):

            # try to add restoring group backend if mastermind knows about it
            if group.node_backends:
                add_groups.append((group, group.node_backends[0].fs.fsid))

        # applying accounting
        dc_node_type = inventory.get_dc_node_type()
        for (group, fsid), diff in zip(remove_groups + add_groups,
            [-1] * len(remove_groups) + [1] * len(add_groups)):

            logger.debug('Accounting group {0}, fsid {1}, diff {2}'.format(group.group_id, fsid, diff))

            if not group.node_backends:
                logger.debug('No node backend for group {0}, job for group {1}'.format(group.group_id, job.group))
                continue

            cur_node = group.node_backends[0].node.host.parents

            parts = []
            parent = cur_node
            while parent:
                parts.insert(0, parent['name'])
                parent = parent.get('parent')

            while cur_node:
                if cur_node['type'] in types:
                    ns_current_type_state = ns_current_state[cur_node['type']]
                    full_path = '|'.join(parts)
                    ns_current_type_state['nodes'][full_path] += diff
                    ns_current_type_state['avg'] += float(diff) / len(ns_current_type_state['nodes'])

                    if cur_node['type'] == 'host' and 'hdd' in types:
                        hdd_path = full_path + '|' + str(fsid)
                        ns_hdd_type_state = ns_current_state['hdd']
                        if hdd_path in ns_hdd_type_state['nodes']:
                            ns_hdd_type_state['nodes'][hdd_path] += diff
                            ns_hdd_type_state['avg'] += float(diff) / len(ns_hdd_type_state['nodes'])
                        else:
                            logger.warn('Hdd path {0} is not found under host {1}'.format(
                                hdd_path, full_path))
                parts.pop()
                cur_node = cur_node.get('parent')


    def get_suitable_uncoupled_groups_list(self, group, uncoupled_groups, busy_group_ids=None):
        logger.debug('{0}, {1}'.format(group.group_id, [g.group_id for g in group.coupled_groups]))
        stats = [g.get_stat() for g in group.couple.groups if g.node_backends]
        if not stats:
            raise RuntimeError('Cannot determine group space requirements')
        required_ts = max(st.total_space for st in stats)
        groups_by_fs = {}

        busy_group_ids = busy_group_ids or set(self.job_processor.get_uncoupled_groups_in_service())

        logger.info('Busy uncoupled groups: {0}'.format(busy_group_ids))

        forbidden_dcs = set()
        if config.get('forbidden_dc_sharing_among_groups', False):
            forbidden_dcs = set([nb.node.host.dc for g in group.coupled_groups
                                                 for nb in g.node_backends])

        for g in uncoupled_groups:
            if g.group_id in busy_group_ids or g.node_backends[0].node.host.dc in forbidden_dcs:
                continue
            fs = g.node_backends[0].fs
            groups_by_fs.setdefault(fs, []).append(g)

        candidates = []

        for fs, groups in groups_by_fs.iteritems():
            fs_candidates = []
            gs = sorted(groups, key=lambda g: g.group_id)

            candidate = []
            ts = 0
            for g in gs:
                candidate.append(g)
                ts += g.get_stat().total_space
                if ts >= required_ts:
                    fs_candidates.append((candidate, ts))
                    candidate = []
                    ts = 0


            fs_candidates = sorted(fs_candidates, key=lambda c: (len(c[0]), c[1]))
            if fs_candidates:
                candidates.append(fs_candidates[0][0])

        return candidates

    def find_uncoupled_groups(self, group, host_addr):
        old_host_tree = cache.get_host_tree(cache.get_hostname_by_addr(host_addr))
        logger.info('Old host tree: {0}'.format(old_host_tree))

        uncoupled_groups = infrastructure.get_good_uncoupled_groups(max_node_backends=1)
        groups_by_dc = {}
        for unc_group in uncoupled_groups:
            dc = unc_group.node_backends[0].node.host.dc
            groups_by_dc.setdefault(dc, []).append(unc_group)

        logger.info('Uncoupled groups by dcs: {0}'.format(
            [(dc, len(groups)) for dc, groups in groups_by_dc.iteritems()]))

        dc_node_type = inventory.get_dc_node_type()
        weights = {}

        candidates = self.get_suitable_uncoupled_groups_list(group, uncoupled_groups)
        for candidate in candidates:
            host_addr = candidate[0].node_backends[0].node.host.addr
            host_tree = cache.get_host_tree(
                cache.get_hostname_by_addr(host_addr))

            diff = 0
            dc = None
            dc_change = False
            old_node = old_host_tree
            node = host_tree
            while old_node:
                if old_node['name'] != node['name']:
                    diff += 1
                if node['type'] == dc_node_type:
                    dc = node['name']
                    dc_change = old_node['name'] != node['name']
                old_node = old_node.get('parent', None)
                node = node.get('parent', None)

            weights[tuple(candidate)] = (dc_change, diff, -len(groups_by_dc[dc]))

        sorted_weights = sorted(weights.items(), key=lambda wi: wi[1])
        if not sorted_weights:
            raise RuntimeError('No suitable uncoupled groups found')

        top_candidate = sorted_weights[0][0]

        logger.info('Top candidate: {0}'.format(
            [g.group_id for g in top_candidate]))

        return top_candidate

    def select_uncoupled_groups(self, group):
        if len(group.node_backends) > 1:
            raise ValueError('Group {0} should have no more than 1 backend'.format(group.group.id))

        types = ['root'] + inventory.get_balancer_node_types() + ['hdd']
        tree, nodes = infrastructure.filtered_cluster_tree(types)

        couple = group.couple
        try:
            ns = couple.namespace
        except ValueError:
            raise RuntimeError('Badly configured namespace for couple {0}'.format(couple))

        infrastructure.account_ns_couples(tree, nodes, ns)
        infrastructure.update_groups_list(tree)

        uncoupled_groups = infrastructure.get_good_uncoupled_groups(max_node_backends=1)
        candidates = self.get_suitable_uncoupled_groups_list(group, uncoupled_groups)
        logger.debug('Candidate groups for group {0}: {1}'.format(group.group_id,
            [[g.group_id for g in c] for c in candidates]))

        units = infrastructure.groups_units(uncoupled_groups, types)

        ns_current_state = infrastructure.ns_current_state(nodes, types[1:])
        logger.debug('Current ns {0} dc state: {1}'.format(
            ns, ns_current_state[inventory.get_dc_node_type()]))

        active_jobs = self.job_processor.job_finder.jobs(
            types=(jobs.JobTypes.TYPE_MOVE_JOB,
                   jobs.JobTypes.TYPE_RESTORE_GROUP_JOB),
            statuses=(jobs.Job.STATUS_NOT_APPROVED,
                      jobs.Job.STATUS_NEW,
                      jobs.Job.STATUS_EXECUTING,
                      jobs.Job.STATUS_PENDING,
                      jobs.Job.STATUS_BROKEN))

        def log_ns_current_state_diff(ns1, ns2, tpl):
            node_type = 'hdd'
            for k in ns1[node_type]['nodes']:
                if ns1[node_type]['nodes'][k] != ns2[node_type]['nodes'][k]:
                    logger.debug(tpl.format(
                        k, ns1[node_type]['nodes'][k], ns2[node_type]['nodes'][k]))

        for job in active_jobs:
            cmp_ns_current_state = deepcopy(ns_current_state)
            self.account_job(ns_current_state, types, job, ns)
            log_ns_current_state_diff(cmp_ns_current_state, ns_current_state,
                'Applying job {0}, path {{0}}, old value: {{1}}, new value: {{2}}'.format(job.id))

        cur_node = tree

        if not candidates:
            raise RuntimeError('No suitable uncoupled groups found')

        for level_type in types[1:]:
            ns_current_type_state = ns_current_state[level_type]

            weights = []
            for candidate in candidates:
                weight_nodes = copy(ns_current_type_state['nodes'])
                units_set = set()
                for merged_unc_group in candidate:
                    for nb_unit in units[merged_unc_group.group_id]:
                        units_set.add(nb_unit[level_type])
                for used_nb_nodes in units_set:
                    weight_nodes[used_nb_nodes] += 1

                weight = sum((c - ns_current_type_state['avg']) ** 2
                             for c in weight_nodes.values())

                heapq.heappush(weights, (weight, candidate))

            max_weight = weights[0][0]
            selected_candidates = []
            while weights:
                c = heapq.heappop(weights)
                if c[0] != max_weight:
                    break
                selected_candidates.append(c[1])

            logger.debug('Level {0}, ns {1}, min weight candidates: {2}'.format(
                level_type, ns, [[g.group_id for g in c] for c in selected_candidates]))

            candidates = selected_candidates

        return candidates[0]

def _recovery_applicable_couple(couple, locked_couples=None):
    if couple.status not in storage.GOOD_STATUSES:
        return False
    if locked_couples and str(couple) in locked_couples:
        return False
    return True


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

        for group in infrastructure.get_good_uncoupled_groups(max_node_backends=1):
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
    def __dcs():
        dcs = set()
        for group in storage.groups:
            for nb in group.node_backends:
                dcs.add(nb.node.host.dc)
        return dcs


    def move_group(self, src_dc, src_group, dst_dc, dst_group, merged_groups):

        old_ms_error = self.state_ms_error

        uncoupled_groups = [dst_group] + merged_groups

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
        for group in uncoupled_groups:
            self.state[dst_dc].uncoupled_space -= self.stats(group).total_space
            self.state[dst_dc].uncoupled_groups.remove(group)
            # For now src_group is not swapped with dst_group but just replaces it
            # self.state[src_dc].uncoupled_groups.insert(dst_group)

        # updating delta object
        self.delta.data_move_size += self.stats(src_group).used_space
        self.delta.ms_error_delta += self.state_ms_error - old_ms_error

        for group in uncoupled_groups:
            self.delta.lost_space += self.stats(group).total_space
        self.delta.lost_space -= self.stats(src_group).used_space

        self.moved_groups.append((src_group, src_dc, dst_group, merged_groups, dst_dc))


def gb(bytes):
    return (bytes / (1024.0 * 1024.0 * 1024))

