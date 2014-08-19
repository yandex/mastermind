from copy import copy
import itertools
from logging import getLogger
import storage
import threading
import traceback

import elliptics
import msgpack

from config import config
from infrastructure import infrastructure
import jobs
from sync import sync_manager
import timed_queue

logger = getLogger('mm.smoother')


# TODO: select appropriate stat value: used_space, free_space, total_space


class Smoother(object):

    MOVE_CANDIDATES = 'move_candidates'

    def __init__(self, meta_session, job_processor):

        logger.info('draft initializing')
        self.candidates = []
        self.meta_session = meta_session
        self.job_processor = job_processor
        self.current_plan = None
        self.__current_plan_lock = threading.Lock()
        self.__max_plan_length = config.get('smoother', {}).get('max_plan_length', 5)
        self.__tq = timed_queue.TimedQueue()
        self.__tq.start()

        if (config.get('smoother', {}).get('enabled', False)):
            self.__tq.add_task_in(self.MOVE_CANDIDATES,
                10, self._move_candidates)

    def _move_candidates(self):
        try:
            logger.info('Starting smoother planning')

            # prechecking for new or pending tasks
            if self.__executing_jobs():
                return

            self._do_move_candidates()
        except Exception as e:
            logger.error('{0}: {1}'.format(e, traceback.format_exc()))
        finally:
            self.__tq.add_task_in(self.MOVE_CANDIDATES,
                config.get('smoother', {}).get('generate_plan_period', 1800),
                self._move_candidates)

    def __executing_jobs(self):
        for job in self.job_processor.jobs.itervalues():
            if job.status not in (jobs.Job.STATUS_COMPLETED, jobs.Job.STATUS_CANCELLED):
                logger.info('Smoother planer found at least one '
                    'not finished job ({0}, status {1}'.format(job.id, job.status))
                return True
        return False


    def __apply_plan(self):

        logger.info('self candidates: {0}'.format(self.candidates))

        candidates = [c[0] for c in self.candidates]

        try:

            logger.debug('Lock acquiring')

            with sync_manager.lock(self.job_processor.JOBS_LOCK):
                logger.debug('Lock acquired')

                self.job_processor._do_update_jobs()

                if self.__executing_jobs():
                    raise ValueError('Not finished jobs are found')

                for i, candidate in enumerate(candidates):
                    logger.info('Candidate {0}: data {1}, ms_error delta {2}:'.format(
                        i, gb(candidate.delta.data_move_size), candidate.delta.ms_error_delta))

                    for src_group, src_dc, dst_group, dst_dc in candidate.moved_groups:

                        assert len(src_group.node_backends) == 1, 'Src group {0} should have only 1 node backend'.format(src_group.group_id)
                        assert len(dst_group.node_backends) == 1, 'Dst group {0} should have only 1 node backend'.format(dst_group.group_id)

                        src_dc_cnt = infrastructure.get_dc_by_host(src_group.node_backends[0].node.host.addr)
                        dst_dc_cnt = infrastructure.get_dc_by_host(dst_group.node_backends[0].node.host.addr)

                        assert (src_dc_cnt == src_dc,
                            'Dc for src group {0} has been changed: {1} != {2}'.format(
                                src_group.group_id, src_dc_cnt, src_dc))
                        assert (dst_dc_cnt == dst_dc,
                            'Dc for dst group {0} has been changed: {1} != {2}'.format(
                                dst_group.group_id, dst_dc_cnt, dst_dc))

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
                                 'src_backend_id': src_group.node_backends[0].backend_id,
                                 'src_base_path': src_group.node_backends[0].base_path,
                                 'dst_host': dst_group.node_backends[0].node.host.addr,
                                 'dst_port': dst_group.node_backends[0].node.port,
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

    def _do_move_candidates(self, step=0):
        if step == 0:
            self.candidates = [[StorageState.current()]]

        if step >= self.__max_plan_length:
            self.__apply_plan()
            return

        logger.info('Candidates: {0}, step {1}'.format(len(self.candidates[-1]), step))

        tmp_candidates = self.generate_candidates(self.candidates[-1][0])

        if not tmp_candidates:
            self.__apply_plan()
            return

        max_error_candidate = max(tmp_candidates, key=lambda c: c.delta.weight)

        self.candidates.append([max_error_candidate])
        logger.info('Max error candidate: {0}'.format(max_error_candidate))
        logger.info('Max error candidate moved groups: {0}'.format([
            (src_group, src_dc, dst_group, dst_dc) for src_group, src_dc, dst_group, dst_dc in max_error_candidate.moved_groups]))


        self._do_move_candidates(step=step + 1)


    def generate_candidates(self, candidate):
        _candidates = []

        avg = candidate.full_space_mean()

        base_ms = candidate.state_ms_error(avg)

        for c in itertools.combinations(candidate.iteritems(), 2):
            (src_dc, src_dc_state), (dst_dc, dst_dc_state) = c

            if src_dc_state.full_space < avg:
                continue

            if dst_dc_state.full_space >= src_dc_state.full_space:
                continue

            for src_group in src_dc_state.groups:

                src_group_stat = candidate.stats(src_group)

                if src_group.couple in dst_dc_state.couples:
                    # can't move group to dst_dc because a group
                    # from the same couple is already located there
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

                    dst_group = unc_group
                    break

                if not dst_group:
                    # no suitable uncoupled group found
                    continue

                new_candidate = candidate.copy()

                new_candidate.move_group(src_dc, src_group, dst_dc, dst_group)

                if new_candidate.state_ms_error(avg) < base_ms:
                    logger.debug('good candidate found: {0} group from {1} to {2}, '
                        'error from {3} to {4} (swap with group {5})'.format(
                            src_group.group_id, src_dc, dst_dc, base_ms,
                            new_candidate.state_ms_error(avg), dst_group.group_id))
                    _candidates.append(new_candidate)

        return _candidates


class Delta(object):
    def __init__(self):
        self.data_move_size = 0.0
        self.ms_error_delta = 0.0

    def __add__(self, other):
        res = Delta()
        res.data_move_size = self.data_move_size + other.data_move_size
        res.ms_error_delta = self.ms_error_delta + other.ms_error_delta

    def copy(self):
        res = Delta()
        res.data_move_size = self.data_move_size
        res.ms_error_delta = self.ms_error_delta
        return res

    @property
    def weight(self):
        return -self.ms_error_delta - (self.data_move_size / (50 * 1024 * 1024 * 1024))


class DcState(object):
    def __init__(self, storage_state):
        self.groups = []
        self.full_space = 0.0
        self.storage_state = storage_state
        self.couples = set()
        self.uncoupled_keys = []
        self.uncoupled_groups = SortedCollection(key=lambda x: storage_state.stats(x).total_space)

    def add_group(self, group):
        self.groups.append(group)
        self.full_space += self.storage_state.stats(group).used_space
        assert group.couple
        self.couples.add(group.couple)

    def add_uncoupled_group(self, group):
        self.uncoupled_groups.insert(group)

    def copy(self, storage_state):
        obj = DcState(storage_state)
        obj.groups = copy(self.groups)
        obj.full_space = self.full_space
        obj.couples = copy(self.couples)
        obj.uncoupled_groups = SortedCollection(iterable=self.uncoupled_groups._items, key=lambda x: storage_state.stats(x).total_space)
        return obj


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

        return obj

    def copy(self):
        obj = StorageState()

        obj._stats = copy(self._stats)

        for dc, dc_state in self.state.iteritems():
            obj.state[dc] = dc_state.copy(obj)

        obj.delta = self.delta.copy()

        return obj

    def full_space_mean(self):
        full_space = 0.0
        for dc, dc_state in self.state.iteritems():
            full_space += dc_state.full_space

        return full_space / len(self.state)

    def stats(self, group):
        return self._stats[group.group_id]

    def state_ms_error(self, avg):

        lms_error = 0.0
        for dc_state in self.state.itervalues():
            # normalizing to TBs and square
            lms_error += ((dc_state.full_space - avg) / (1024.0 * 1024.0 * 1024.0 * 1024.0)) ** 2

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
            if not len(group.node_backends):
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

        avg = self.full_space_mean()
        old_ms_error = self.state_ms_error(avg)

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

        used_space = self.stats(src_group).used_space
        self.state[src_dc].full_space -= used_space
        self.state[dst_dc].full_space += used_space

        self.state[dst_dc].uncoupled_groups.remove(dst_group)
        # For now src_group is not swapped with dst_group but just replaces it
        # self.state[src_dc].uncoupled_groups.insert(dst_group)

        # change total space and update used_space - group is being moved to another hdd
        # TODO: fix src_stat - should change as well when moving to another node
        src_stat = self.stats(src_group)
        dst_stat = self.stats(dst_group)

        # swapping total_space stats
        dst_stat.total_space, src_stat.total_space = src_stat.total_space, dst_stat.total_space

        # ... and updating used space accordingly
        dst_stat.free_space = dst_stat.total_space - dst_stat.used_space
        src_stat.free_space = src_stat.total_space - src_stat.used_space

        # updating delta object
        self.delta.data_move_size += used_space
        self.delta.ms_error_delta += self.state_ms_error(avg) - old_ms_error

        self.moved_groups.append((src_group, src_dc, dst_group, dst_dc))


def gb(bytes):
    return (bytes / (1024.0 * 1024.0 * 1024))





from bisect import bisect_left, bisect_right

class SortedCollection(object):
    '''Sequence sorted by a key function.

    SortedCollection() is much easier to work with than using bisect() directly.
    It supports key functions like those use in sorted(), min(), and max().
    The result of the key function call is saved so that keys can be searched
    efficiently.

    Instead of returning an insertion-point which can be hard to interpret, the
    five find-methods return a specific item in the sequence. They can scan for
    exact matches, the last item less-than-or-equal to a key, or the first item
    greater-than-or-equal to a key.

    Once found, an item's ordinal position can be located with the index() method.
    New items can be added with the insert() and insert_right() methods.
    Old items can be deleted with the remove() method.

    The usual sequence methods are provided to support indexing, slicing,
    length lookup, clearing, copying, forward and reverse iteration, contains
    checking, item counts, item removal, and a nice looking repr.

    Finding and indexing are O(log n) operations while iteration and insertion
    are O(n).  The initial sort is O(n log n).

    The key function is stored in the 'key' attibute for easy introspection or
    so that you can assign a new key function (triggering an automatic re-sort).

    In short, the class was designed to handle all of the common use cases for
    bisect but with a simpler API and support for key functions.

    >>> from pprint import pprint
    >>> from operator import itemgetter

    >>> s = SortedCollection(key=itemgetter(2))
    >>> for record in [
    ...         ('roger', 'young', 30),
    ...         ('angela', 'jones', 28),
    ...         ('bill', 'smith', 22),
    ...         ('david', 'thomas', 32)]:
    ...     s.insert(record)

    >>> pprint(list(s))         # show records sorted by age
    [('bill', 'smith', 22),
     ('angela', 'jones', 28),
     ('roger', 'young', 30),
     ('david', 'thomas', 32)]

    >>> s.find_le(29)           # find oldest person aged 29 or younger
    ('angela', 'jones', 28)
    >>> s.find_lt(28)           # find oldest person under 28
    ('bill', 'smith', 22)
    >>> s.find_gt(28)           # find youngest person over 28
    ('roger', 'young', 30)

    >>> r = s.find_ge(32)       # find youngest person aged 32 or older
    >>> s.index(r)              # get the index of their record
    3
    >>> s[3]                    # fetch the record at that index
    ('david', 'thomas', 32)

    >>> s.key = itemgetter(0)   # now sort by first name
    >>> pprint(list(s))
    [('angela', 'jones', 28),
     ('bill', 'smith', 22),
     ('david', 'thomas', 32),
     ('roger', 'young', 30)]

    '''

    def __init__(self, iterable=(), key=None):
        self._given_key = key
        key = (lambda x: x) if key is None else key
        decorated = sorted((key(item), item) for item in iterable)
        self._keys = [k for k, item in decorated]
        self._items = [item for k, item in decorated]
        self._key = key

    def _getkey(self):
        return self._key

    def _setkey(self, key):
        if key is not self._key:
            self.__init__(self._items, key=key)

    def _delkey(self):
        self._setkey(None)

    key = property(_getkey, _setkey, _delkey, 'key function')

    def clear(self):
        self.__init__([], self._key)

    def copy(self):
        return self.__class__(self, self._key)

    def __len__(self):
        return len(self._items)

    def __getitem__(self, i):
        return self._items[i]

    def __iter__(self):
        return iter(self._items)

    def __reversed__(self):
        return reversed(self._items)

    def __repr__(self):
        return '%s(%r, key=%s)' % (
            self.__class__.__name__,
            self._items,
            getattr(self._given_key, '__name__', repr(self._given_key))
        )

    def __reduce__(self):
        return self.__class__, (self._items, self._given_key)

    def __contains__(self, item):
        k = self._key(item)
        i = bisect_left(self._keys, k)
        j = bisect_right(self._keys, k)
        return item in self._items[i:j]

    def index(self, item):
        'Find the position of an item.  Raise ValueError if not found.'
        k = self._key(item)
        i = bisect_left(self._keys, k)
        j = bisect_right(self._keys, k)
        return self._items[i:j].index(item) + i

    def count(self, item):
        'Return number of occurrences of item'
        k = self._key(item)
        i = bisect_left(self._keys, k)
        j = bisect_right(self._keys, k)
        return self._items[i:j].count(item)

    def insert(self, item):
        'Insert a new item.  If equal keys are found, add to the left'
        k = self._key(item)
        i = bisect_left(self._keys, k)
        self._keys.insert(i, k)
        self._items.insert(i, item)

    def insert_right(self, item):
        'Insert a new item.  If equal keys are found, add to the right'
        k = self._key(item)
        i = bisect_right(self._keys, k)
        self._keys.insert(i, k)
        self._items.insert(i, item)

    def remove(self, item):
        'Remove first occurence of item.  Raise ValueError if not found'
        i = self.index(item)
        del self._keys[i]
        del self._items[i]

    def find(self, k):
        'Return first item with a key == k.  Raise ValueError if not found.'
        i = bisect_left(self._keys, k)
        if i != len(self) and self._keys[i] == k:
            return self._items[i]
        raise ValueError('No item found with key equal to: %r' % (k,))

    def find_le(self, k):
        'Return last item with a key <= k.  Raise ValueError if not found.'
        i = bisect_right(self._keys, k)
        if i:
            return self._items[i-1]
        raise ValueError('No item found with key at or below: %r' % (k,))

    def find_lt(self, k):
        'Return last item with a key < k.  Raise ValueError if not found.'
        i = bisect_left(self._keys, k)
        if i:
            return self._items[i-1]
        raise ValueError('No item found with key below: %r' % (k,))

    def find_ge(self, k):
        'Return first item with a key >= equal to k.  Raise ValueError if not found'
        i = bisect_left(self._keys, k)
        if i != len(self):
            return self._items[i]
        raise ValueError('No item found with key at or above: %r' % (k,))

    def find_gt(self, k):
        'Return first item with a key > k.  Raise ValueError if not found'
        i = bisect_right(self._keys, k)
        if i != len(self):
            return self._items[i]
        raise ValueError('No item found with key above: %r' % (k,))


