from collections import defaultdict
import logging
import math
import threading
import time
import traceback

import elliptics
import msgpack
import pymongo

import balancer
from cache_transport import cache_task_manager
from config import config
from db.mongo.pool import Collection
from errors import CacheUpstreamError
import helpers as h
import inventory
from infrastructure import infrastructure
import jobs
import keys
import storage
import timed_queue


logger = logging.getLogger('mm.cache')

CACHE_CFG = config.get('cache', {})
CACHE_CLEANER_CFG = CACHE_CFG.get('cleaner', {})
CACHE_GROUP_PATH_PREFIX = CACHE_CFG.get('group_path_prefix')


class CacheManager(object):
    def __init__(self, node, niu, job_processor, db):
        self.node = node
        self.niu = niu
        self.session = elliptics.Session(self.node)
        wait_timeout = config.get('elliptics', {}).get('wait_timeout', 5)
        self.session.set_timeout(wait_timeout)

        self.service_metakey = str(
            self.session.transform(keys.SYMMETRIC_GROUPS_KEY))

        try:
            keys_db_uri = config['metadata']['cache']['db']
        except KeyError:
            logger.error('Config parameter metadata.cache.db is required '
                         'for cache manager')
            raise
        self.keys_db = Collection(db[keys_db_uri], 'keys')
        self.distributor = CacheDistributor(
            self.node, self.keys_db, job_processor)

        self.top_keys = {}

        self.__tq = timed_queue.TimedQueue()

        self.nodes_update()
        self.update_cache_groups()

        self.__tq.add_task_in(
            'monitor_top_stats',
            CACHE_CFG.get('top_update_period', 1800),
            self.monitor_top_stats)

    def _start_tq(self):
        self.__tq.start()

    STAT_CATEGORIES = elliptics.monitor_stat_categories.top

    def monitor_top_stats(self):
        try:
            start_ts = time.time()
            logger.info('Monitor top stats update started')

            logger.info('Before calculating routes')
            host_addrs = set(r.address
                             for r in self.session.routes.get_unique_routes())
            logger.info('Unique routes calculated')

            requests = []
            for address in host_addrs:
                session = self.session.clone()
                session.set_direct_id(address)
                logger.debug('Request for top of node {0}'.format(address))
                requests.append((session.monitor_stat(address,
                                                      self.STAT_CATEGORIES),
                                 address))

            new_top_keys = {}
            for result, address in requests:
                try:
                    h.process_elliptics_async_result(
                        result, self.update_top, new_top_keys)
                except Exception as e:
                    logger.error(
                        'Failed to request monitor_stat for node {0}: '
                        '{1}\n{2}'.format(address, e, traceback.format_exc()))
                    continue

            self.top_keys = new_top_keys

            self.distributor.distribute(self.top_keys)

        except Exception as e:
            logger.error('Failed to update monitor top stats: {0}\n{1}'.format(
                e, traceback.format_exc()))
            pass
        finally:
            logger.info(
                'Monitor top stats update finished, time: {0:.3f}'.format(
                    time.time() - start_ts))
            self.__tq.add_task_in(
                'monitor_top_stats',
                CACHE_CFG.get('top_update_period', 1800),
                self.monitor_top_stats)

    def update_top(self, m_stat, new_top_keys,
                   elapsed_time=None, end_time=None):

        node_addr = '{0}:{1}'.format(m_stat.address.host, m_stat.address.port)
        logger.debug(
            'Top updating: node {0} statistics time: {1}.{2:03d}'.format(
                node_addr, elapsed_time.tsec,
                int(round(elapsed_time.tnsec / (1000.0 * 1000.0)))))
        logger.info('Stats: {0}'.format(node_addr))

        top_keys = m_stat.statistics

        if 'top' not in top_keys:
            logger.error('No top data available for node {0}'.format(node_addr))
            return

        update_period = top_keys['top']['period_in_seconds']

        for key in top_keys['top']['top_by_size']:
            # skip mastermind service key (keys.SYMMETRIC_GROUPS_KEY)
            if key['id'] == self.service_metakey:
                continue

            if not key['group'] in storage.groups:
                logger.error(
                    'Key {}: source group {} is not found in storage'.format(
                        key['id'], key['group']))
                continue

            group = storage.groups[key['group']]
            if group.type == storage.Group.TYPE_CACHE:
                # this is a cache group, so their is no straightforward way to
                # get original key couple; the only way is to search metadb for
                # corresponding key id and cache group, and take couple id and
                # ns from matching record
                logger.debug(
                    'Key {}: cache group {}, establishing couple '
                    'and ns'.format(key['id'], group.group_id))
                keys = list(self.keys_db.find({'id': key['id'],
                                               'cache_groups': group.group_id}))
                if len(keys) > 1:
                    logger.error(
                        'Key {}: matched {} keys in metadb by cache '
                        'group {}'.format(key['id'], len(keys), group.group_id))
                    continue

                if not keys:
                    logger.warn(
                        'Key {}: found on top statistics for cache group {}, '
                        'not found in metadb'.format(key['id'], group.group_id))
                    continue

                key_record = keys[0]
                couple_id, ns = key_record['couple'], key_record['ns']

            else:
                if not group.couple:
                    logger.error(
                        'Key {}: source group {} does not belong to any '
                        'couple'.format(key['id'], group.group_id))
                    continue

                couple_id = str(group.couple)
                try:
                    ns = group.couple.namespace
                except ValueError:
                    logger.error(
                        'Key {}: couple of source group {} has broken '
                        'namespaces settings'.format(key['id'], group.group_id))
                    continue

            new_top_keys.setdefault(
                (key['id'], couple_id),
                self.distributor._new_key_stat(key['id'], couple_id, ns))
            top_key = new_top_keys[(key['id'], couple_id)]
            top_key['groups'].append(key['group'])
            top_key['size'] += key['size']
            top_key['frequency'] += key['frequency']
            top_key['period_in_seconds'] = update_period

    @h.concurrent_handler
    def get_top_keys(self, request):
        return self.top_keys

    def nodes_update(self):
        try:
            start_ts = time.time()
            logger.info('Cluster updating: node statistics collecting started')
            self.niu.monitor_stats()
        except Exception as e:
            logger.info(
                'Failed to fetch nodes statictics: {0}\n{1}'.format(
                    e, traceback.format_exc()))
        finally:
            logger.info(
                'Cluster updating: node statistics collecting '
                'finished, time: {0:.3f}'.format(time.time() - start_ts))
            reload_period = config.get('nodes_reload_period', 60)
            self.__tq.add_task_in(
                'node_statistics_update',
                reload_period, self.nodes_update)

    def update_cache_groups(self):
        try:
            start_ts = time.time()
            logger.info(
                'Cluster updating: updating group coupling info started')

            self._mark_cache_groups()

            self.niu.update_symm_groups_async()

            logger.info('Detected cache groups: {0}'.format(
                len(storage.cache_couples)))

        except Exception as e:
            logger.info('Failed to update groups: {0}\n{1}'.format(
                e, traceback.format_exc()))
        finally:
            logger.info(
                'Cluster updating: updating group coupling info '
                'finished, time: {0:.3f}'.format(time.time() - start_ts))
            reload_period = config.get('nodes_reload_period', 60)
            self.__tq.add_task_in(
                'cache_groups_update',
                reload_period, self.update_cache_groups)

    def _mark_cache_groups(self):
        if not CACHE_GROUP_PATH_PREFIX:
            return

        # searching for unmarked cache groups to mark them
        for group in infrastructure.get_good_uncoupled_groups(
                types=[storage.Group.TYPE_UNMARKED]):

            if not group.node_backends[0].base_path.startswith(
                    CACHE_GROUP_PATH_PREFIX):
                continue

            logger.info(
                'Detected unmarked cache group {0} '
                '(type: {1}, path {2}, meta {3})'.format(
                    group, group.type,
                    group.node_backends[0].base_path, group.meta))

            packed = msgpack.packb(group.compose_cache_group_meta())
            try:
                s = self.session.clone()
                s.add_groups([group.group_id])
                balancer.consistent_write(
                    s, keys.SYMMETRIC_GROUPS_KEY, packed)
                logger.info('Successfully marked cache group {0}'.format(
                    group))
            except Exception as e:
                logger.error(
                    'Failed to write meta key for group {0}: {1}\n{2}'.format(
                        group, e, traceback.format_exc()))
                continue

            try:
                group.parse_meta(packed)
                group.update_status()
            except Exception as e:
                logger.error(
                    'Failed to update status for group {0}: {1}\n{2}'.format(
                        group, e, traceback.format_exc()))
                continue

    @h.concurrent_handler
    def cache_statistics(self, request):

        keys = defaultdict(lambda: {
            'id': None,
            'top_rate': 0.0,  # rate according to elliptics top statistics
            'mm_rate': 0.0,   # rate used to distribute key
            'cache_groups': [],
        })

        for key_stat in self.top_keys.itervalues():
            key_id = key_stat['id']
            key_couple = key_stat['couple']
            key = keys[(key_id, key_couple)]
            key['id'] = key_id
            key['top_rate'] = _key_bw(key_stat)
        for key_cached in self.distributor._get_distributed_keys():
            key_id = key_cached['id']
            key_couple = key_cached['couple']
            key = keys[(key_id, key_couple)]
            key['id'] = key_id
            key['cache_groups'] = key_cached['cache_groups'][:]
            key['mm_rate'] = key_cached['rate']

        cache_groups = defaultdict(dict)
        groups_units = self.distributor.groups_units
        dc_node_type = self.distributor.dc_node_type
        for cache_group in self.distributor.cache_groups:
            cg = cache_groups[cache_group.group_id]
            cg_units = groups_units[cache_group.group_id]
            cg['dc'] = self.distributor._group_unit(cg_units[0][dc_node_type])
            cg['host'] = self.distributor._group_unit(cg_units[0]['host'])

        return {'keys': dict(keys),
                'cache_groups': dict(cache_groups)}

    @h.concurrent_handler
    def cache_clean(self, request):
        self.distributor.cleaner.clean(self.top_keys)
        self.__tq.add_task_in(
            'defrag_cache_groups', 60,
            self.distributor.cleaner.defrag_cache_groups)
        return True

    @h.concurrent_handler
    def cache_groups(self, request):
        cache_groups = []
        for couple in storage.cache_couples:
            cache_groups.extend(couple.groups)
        return [cg.group_id for cg in cache_groups]


class CacheDistributor(object):
    def __init__(self, node, keys_db, job_processor):
        self.node = node
        self.session = elliptics.Session(self.node)
        wait_timeout = config.get('elliptics', {}).get('wait_timeout', 5)
        self.session.set_timeout(wait_timeout)

        self.bandwidth_per_copy = CACHE_CFG.get('bandwidth_per_copy', 5242880)

        self.copies_reduce_factor = CACHE_CFG['copies_reduce_factor']
        assert 0.0 < self.copies_reduce_factor <= 1.0, "Copies reduce factor "\
            "should be in (0.0, 1.0] interval"

        self.copies_expand_step = CACHE_CFG['copies_expand_step']
        assert self.copies_expand_step > 0, "Copies expand step "\
            "should be > 0"

        self.keys_db = keys_db

        self.cleaner = CacheCleaner(self, job_processor)

        self.groups_units = {}
        self.cache_groups = {}
        self.executing_tasks = []
        self._cache_groups_lock = threading.Lock()

        self.node_types = inventory.get_balancer_node_types()
        self.dc_node_type = inventory.get_dc_node_type()

        self.dryrun = CACHE_CFG.get('dryrun', False)

    def _get_distributed_keys(self):
        return self.keys_db.find()

    def _new_key(self, key_stat):
        assert len(key_stat.get('groups', [])), \
            'Empty groups list for key {0}'.format(key_stat)

        couple = storage.couples[key_stat['couple']]
        lookups = self._lookup_key(key_stat['id'], couple.as_tuple())
        key_size = max(l.size for l in lookups.itervalues())
        return {
            'id': key_stat['id'],
            'couple': key_stat['couple'],
            'ns': key_stat['ns'],
            'size': key_size,
            'data_groups': list(storage.couples[key_stat['couple']].as_tuple()),
            'rate': 0,
            'cache_groups': [],
            'expand_ts': int(time.time())
        }

    def _new_key_stat(self, key_id, couple_str, ns):
        return {'groups': [],
                'couple': couple_str,
                'ns': ns,
                'id': key_id,
                'size': 0,
                'frequency': 0,
                'period_in_seconds': 1}

    def _key_copies_diff(self, key, top):
        if (key['id'], key['couple']) not in top:
            key_stat = self._new_key_stat(key['id'], key['couple'], key['ns'])
            copies_diff = -len(key['cache_groups'])
        else:
            key_stat = top[(key['id'], key['couple'])]
            key_copies_num = _key_bw(key_stat) / self.bandwidth_per_copy
            copies_diff = self._count_key_copies_diff(key, key_copies_num)

        return copies_diff, key_stat

    def distribute(self, top):
        """ Distributes top keys among available cache groups.
        Parameter "top" is a map of key id to key top statistics:

        {('123', '42:69'): {'groups': [42, 69],
                            'couple': '42:69',
                            'ns': 'magic',
                            'size': 31415,
                            'id': 123,
                            'size': 1024,    # approximate size of key traffic
                            'frequency': 2,  # approximate number of key events
                            'period': 1      # statistics collection period
                           }, ...
        }"""

        self._update_cache_groups()

        top = self._filter_by_bandwidth(top)
        logger.info('Keys after applying bandwidth filter: {0}'.format(
            [elliptics.Id(key_k[0].encode('utf-8')) for key_k in top]))

        # update currently distributed keys
        logger.info('Updating already distributed keys')
        for key in self._get_distributed_keys():
            copies_diff, key_stat = self._key_copies_diff(key, top)
            top.pop((key['id'], key['couple']), None)

            if copies_diff <= 0:
                logger.info(
                    'Key {}, couple {}, bandwidth {}; '
                    'cached, extra copies: {}, skipped'.format(
                        key['id'], key['couple'],
                        mb_per_s(_key_bw(key_stat)), -copies_diff))
                continue

            logger.info(
                'Key {}, couple {}, bandwidth {}; '
                'cached, expanding to {} more '
                'copies'.format(
                    key['id'], key['couple'],
                    mb_per_s(_key_bw(key_stat)), copies_diff))
            with self._cache_groups_lock:
                try:
                    self._update_key(key, key_stat, copies_diff)
                except Exception:
                    logger.exception(
                        'Key {}, couple {}: failed to expand'.format(
                            key['id'], key['couple']))
                    continue

        # process new keys
        logger.info('Distributing new keys')
        for (key_id, key_couple), key_stat in top.iteritems():
            try:
                key = self._new_key(key_stat)
            except Exception as e:
                logger.exception(
                    'Key {}, couple {}: failed to create new key record, '
                    '{}:'.format(key_id, key_couple, e))
                continue
            copies_diff, key_stat = self._key_copies_diff(key, top)
            if copies_diff == 0:
                logger.info(
                    'Key {}, couple {}, bandwidth {}; not cached, '
                    'does not require cache copies, skipped'.format(
                        key['id'], key['couple'], mb_per_s(_key_bw(key_stat))))
                continue
            logger.info(
                'Key {}, couple {}, bandwidth {}; not cached, '
                'expanding to {} copies'.format(
                    key['id'], key['couple'],
                    mb_per_s(_key_bw(key_stat)), copies_diff))
            with self._cache_groups_lock:
                try:
                    self._update_key(key, key_stat, copies_diff)
                except Exception:
                    logger.exception(
                        'Key {}, couple {}: failed to expand'.format(
                            key['id'], key['couple']))
                    continue

    def _update_key(self, key, key_stat, copies_diff):
        key['rate'] = _key_bw(key_stat)
        if copies_diff > 0:
            self._increase_copies(key, key_stat, copies_diff)
        else:
            self._decrease_copies(key, -copies_diff)

    def _decrease_copies(self, key, count):
        group_ids = key['cache_groups']

        count = min(count, len(group_ids))

        queue = []
        ok_groups = []

        for group_id in group_ids:
            if group_id not in self.cache_groups:
                # first candidates for key removal will be the groups that are
                # unavailable at the moment
                queue.append(group_id)
                continue

            cache_group = self.cache_groups[group_id]
            if cache_group.status != storage.Status.COUPLED:
                queue.append(group_id)
                continue

            ok_groups.append(cache_group)

        logger.info('Key {0}: inactive cache groups: {1}'.format(
            key['id'], queue))

        ok_groups.sort(
            key=lambda cg: cg.effective_free_space / cg.effective_space)

        queue.extend([g.group_id for g in ok_groups])

        logger.info(
            'Key {0}: will be removed from {1} groups in order '
            '{2}'.format(key['id'], count, queue))

        for group_id in queue[:count]:
            try:
                self._remove_key_from_group(key, group_id)
            except Exception as e:
                logger.error(
                    'Key {0}: failed to remove key from group {1}: '
                    '{2}\n{3}'.format(
                        key['id'], group_id, e, traceback.format_exc()))
                continue

    def _remove_key_from_group(self, key, group_id):
        """
        Creates task for gatling gun on destination group,
        updates key in meta database
        """
        if not self.dryrun:
            cache_task_manager.put_task(self._serialize(
                self._gatlinggun_task(key, group_id, [], 'remove')))
        key['cache_groups'].remove(group_id)
        if len(key['cache_groups']):
            self.keys_db.update({'id': key['id'], 'couple': key['couple']}, key)
        else:
            self.keys_db.remove({'id': key['id'], 'couple': key['couple']})

    def _group_unit(self, full_path):
        return full_path.rsplit('|', 1)[-1]

    def _lookup_key(self, key_id, group_ids):
        eid = elliptics.Id(key_id.encode('utf-8'))

        lookups = []
        for group_id in group_ids:
            s = self.session.clone()
            s.set_exceptions_policy(elliptics.exceptions_policy.no_exceptions)
            s.add_groups([group_id])
            lookups.append((s.lookup(eid), eid, group_id))

        lookup_by_group = {}
        not_found_count = 0

        def set_group_lookup(lookup, group_id,
                             elapsed_time=None, end_time=None):
            global not_found_count
            if lookup.error.code:
                if lookup.error.code == -2:
                    logger.warn(
                        'Key {}: lookup returned -2, group {}/{}'.format(
                            key_id, lookup.group_id, group_id))
                    not_found_count += 1
                    return
                else:
                    raise lookup.error
            lookup_by_group[group_id] = lookup

        logger.debug('Key {}: performing lookups on groups {}'.format(
            key_id, group_ids))

        for result, eid, group_id in lookups:
            try:
                h.process_elliptics_async_result(
                    result, set_group_lookup, group_id, raise_on_error=False)
            except Exception as e:
                logger.exception(
                    'Failed to lookup key {0} on group {1}'.format(
                        eid, group_id))
                continue

        if not lookup_by_group:
            if len(lookups) == not_found_count:
                raise ValueError('key has already been removed from couple')
            else:
                raise RuntimeError('all lookups for key failed')

        return lookup_by_group

    def _key_by_dc(self, key):
        eid = elliptics.Id(key['id'].encode('utf-8'))

        lookups = []

        # add other cache groups as a source when key size is fixed
        for group_id in key['data_groups']:
            s = self.session.clone()
            s.set_exceptions_policy(elliptics.exceptions_policy.no_exceptions)
            s.add_groups([group_id])
            lookups.append((s.lookup(eid), eid, group_id))

        key_by_dc = {}
        not_found_count = 0

        def set_key_size_by_dc(lookup, group_id,
                               elapsed_time=None, end_time=None):
            global not_found_count
            if lookup.error.code:
                if lookup.error.code == -2:
                    not_found_count += 1
                    return
                else:
                    raise lookup.error

            dc = storage.groups[group_id].node_backends[0].node.host.dc
            key_by_dc[dc] = {
                'group': group_id,
                'size': lookup.size,
            }

        for result, eid, group_id in lookups:
            try:
                h.process_elliptics_async_result(
                    result, set_key_size_by_dc, group_id, raise_on_error=False)
            except Exception as e:
                logger.error(
                    'Failed to lookup key {0} on group {1}: {2}'.format(
                        eid, group_id, e))
                continue

        if len(lookups) == not_found_count:
            logger.info('Key {0}: has already been removed from couple'.format(
                key['id']))

        return key_by_dc

    def _increase_copies(self, key, key_stat, count):
        key_by_dc = self._key_by_dc(key)
        if not key_by_dc:
            logger.error(
                'Key {0}: failed to lookup key in any of '
                'couple dcs'.format(key['id']))
            return
        key_size = max([key_by_dc[dc]['size'] for dc in key_by_dc])

        dc_weights = dict((k, 1) for k in key_by_dc.keys())
        candidates_by_dc = {}

        for dc, weight in dc_weights.iteritems():
            if dc not in key_by_dc:
                continue
            candidates_by_dc[dc] = DcKeyCacheCandidates(
                dc, weight, key_by_dc[dc]['group'])

        busy_cache_groups = set()
        # search only by key id because all cache groups
        # with given key id should be eliminated
        for cached_key in self.keys_db.find({'id': key['id']}):
            busy_cache_groups.update(cached_key['cache_groups'])
            if key['couple'] == cached_key['couple']:
                for cgid in cached_key['cache_groups']:
                    if cgid not in self.cache_groups:
                        continue
                    cg = self.cache_groups[cgid]
                    if cg.dc not in candidates_by_dc:
                        continue
                    candidates_by_dc[cg.dc].account_key_copy(cg)

        copies_num = len(key['cache_groups']) + len(key['data_groups']) + count
        bw_per_copy = (float(key_stat['size']) / key_stat['period_in_seconds'] /
                       copies_num)

        for cg in self.cache_groups:
            if cg in busy_cache_groups:
                continue
            if cg.dc not in candidates_by_dc:
                continue
            if cg.tx_rate is None:
                logger.debug(
                    'Key {0}: tx rate for cache group {1} is unavailable '
                    'at the moment'.format(key['id'], cg.group_id))
                continue
            if cg.effective_free_space < key_size:
                logger.debug(
                    'Key {0}: not enough free space on cache group '
                    '{1}: {2} < {3}'.format(
                        key['id'], cg.group_id, cg.effective_free_space,
                        key_size))
                continue
            if cg.tx_rate_left < bw_per_copy:
                logger.debug(
                    'Key {0}: not enough tx rate on cache group '
                    '{1}: {2} < {3}'.format(
                        key['id'], cg.group_id, cg.tx_rate_left, bw_per_copy))
                continue
            candidates_by_dc[cg.dc].add_candidate(cg)

        for dc, dc_candidates in candidates_by_dc.iteritems():
            dc_candidates.sort_candidates()

        copies = 0
        for cg in self._best_cache_groups(candidates_by_dc, count):
            self._add_key_to_group(key, cg.group_id,
                                   [key_by_dc[cg.dc]['group']], bw_per_copy,
                                   key_size)
            copies += 1
        if copies < count:
            logger.warn('Key {}, couple {}: added only {}/{} copies'.format(
                key['id'], key['couple'], copies, count))

    def _best_cache_groups(self, candidates_by_dc, count):

        def vector_normalize(v):
            length = math.sqrt(sum(x ** 2 for x in v))
            return [x / length for x in v]

        def weight(weights, counts):
            norm_c = vector_normalize(counts)
            norm_w = vector_normalize(weights)
            deltas = sorted(norm_c[i] - norm_w[i]
                            for i in xrange(len(weights)))
            return sum(d ** 2 for d in deltas)

        candidates = candidates_by_dc.values()

        weights = [d.weight for d in candidates]
        for _ in xrange(count):
            cand_weights = []
            dc_counts = [d.key_copies for d in candidates]
            for idx in xrange(len(dc_counts)):
                cand = dc_counts[:]
                cand[idx] += 1
                cand_weights.append((weight(weights, cand), idx))
            cand_weights.sort()
            for _, idx in cand_weights:
                cg = candidates[idx].pop_candidate()
                if cg:
                    yield cg
                    break
            else:
                raise StopIteration

    def _add_key_to_group(self, key, group_id, data_groups, tx_rate, size):
        """
        Creates task for gatling gun on destination group,
        updates key in meta database
        """
        task = self._gatlinggun_task(key, group_id, data_groups, 'add',
                                     tx_rate=tx_rate, size=size)
        if not self.dryrun:
            cache_task_manager.put_task(self._serialize(task))
            logger.debug('Key {}, task for gatlinggun created for cache '
                         'group {}'.format(key['id'], group_id))
        key['cache_groups'].append(group_id)
        key['expand_ts'] = int(time.time())
        self.keys_db.update({'id': key['id'], 'couple': key['couple']},
                            key, upsert=True)
        return task

    @staticmethod
    def _serialize(task):
        return msgpack.packb(task)

    @staticmethod
    def _unserialize(task):
        return msgpack.unpackb(task)

    def _gatlinggun_task(self, key, group, data_groups, action,
                         tx_rate=None, size=None):
        assert isinstance(group, int), "Group for gatlinggun task should be "\
            "int, not {0}".format(type(group).__name__)
        task = {
            'key': key['id'],
            'group': group,
            'sgroups': data_groups,
            'action': action,
        }
        if tx_rate:
            task['tx_rate'] = tx_rate
        if size:
            task['size'] = size
        return task

    def _count_key_copies_diff(self, key, req_copies_num):
        data_groups_num = len(key['data_groups'])
        copies_num = len(key['cache_groups']) + len(key['data_groups'])

        req_copies = int(math.ceil(req_copies_num))
        if req_copies < math.ceil(copies_num * self.copies_reduce_factor):
            return max(data_groups_num, req_copies) - copies_num
        elif req_copies >= copies_num + self.copies_expand_step:
            return req_copies - copies_num
        return 0

    def _filter_by_bandwidth(self, top):
        filtered_top = {}
        for key_k, key_stat in top.iteritems():
            if _key_bw(key_stat) < self.bandwidth_per_copy:
                continue
            filtered_top[key_k] = key_stat
        return filtered_top

    def _cache_group_by_group_id(self, group_id):
        if group_id not in storage.groups:
            return None

        group = storage.groups[group_id]
        if group.type != storage.Group.TYPE_CACHE:
            return None

        return group

    def _update_cache_groups(self):
        new_groups = {}
        for group in storage.groups:
            if (group.type != storage.Group.TYPE_CACHE or
                    group.status != storage.Status.COUPLED):
                continue
            new_groups[group] = group

        new_cache_groups = {}
        for group in new_groups:
            cache_group = CacheGroup(group)
            new_cache_groups[cache_group] = cache_group

        new_groups_units = infrastructure.groups_units(new_groups.keys(),
                                                       self.node_types)

        new_executing_tasks = cache_task_manager.list()
        for packed_task in new_executing_tasks:
            task = self._unserialize(packed_task)
            group_id = task['group']
            if group_id not in new_cache_groups:
                logger.warn('Task destination group {0} is not found among '
                            'good cache groups'.format(group_id))
                continue
            cache_group = new_cache_groups[group_id]
            cache_group.account_task(task)

        # DEBUG
        for cg in new_cache_groups.itervalues():
            logger.debug(
                'Cache group {0}: fes {1} ({2} - {3} - {4})'.format(
                    cg, cg.effective_free_space,
                    cg.group.node_backends[0].effective_space,
                    cg.stat.used_space, cg.reserved_space))
        # DEBUG END

        with self._cache_groups_lock:
            self.cache_groups = new_cache_groups
            self.groups_units = new_groups_units
            self.executing_tasks = new_executing_tasks


class CacheCleaner(object):

    DIRTY_COEF_THRESHOLD = CACHE_CLEANER_CFG.get('dirty_coef_threshold', 0.6)

    def __init__(self, distributor, job_processor):
        self.distributor = distributor
        self.job_processor = job_processor

    def clean(self, top):
        start_ts = time.time()
        logger.info('Cache cleaning started')
        try:
            cache_groups = self.distributor.cache_groups

            dirty_cgs = dict((k, cg) for k, cg in cache_groups.iteritems()
                             if cg.dirty_coef >= self.DIRTY_COEF_THRESHOLD)
            bad_dirty_cgs = set((k, cg) for k, cg in dirty_cgs.iteritems()
                                if cg.group.status != storage.Status.COUPLED)

            for key in self._clean_candidates():
                copies_diff, key_stat = self.distributor._key_copies_diff(
                    key, top)
                logger.info(
                    'Key {}, couple {}, bandwidth {}, cleaning from cache, '
                    'extra copies {}'.format(
                        key['id'], key['couple'],
                        mb_per_s(_key_bw(key_stat)), -copies_diff))
                if copies_diff >= 0:
                    continue

                cg_candidates = [cgid for cgid in key['cache_groups']
                                 if cgid in dirty_cgs]
                if not cg_candidates:
                    logger.info(
                        'Key {}, couple {}, no dirty cache '
                        'groups candidates'.format(
                            key['id'], key['couple']))
                    continue
                cg_candidates.sort(
                    key=lambda cgid: (cgid in bad_dirty_cgs,
                                      dirty_cgs[cgid].dirty_coef),
                    reverse=True)
                target_cg = cg_candidates[:-copies_diff]
                logger.info(
                    'Key {}, couple {}, will be removed from '
                    'cache groups {}'.format(
                        key['id'], key['couple'], target_cg))

                for cgid in target_cg:
                    try:
                        self.distributor._remove_key_from_group(key, cgid)
                        dirty_cgs[cgid].account_removed_key(key['size'])
                    except Exception:
                        logger.exception(
                            'Key {}, couple {}, failed to remove '
                            'from group {}'.format(
                                key['id'], key['couple'], cgid))
                        continue

        except Exception:
            logger.exception('Failed to perform cache cleaning')
            pass
        finally:
            logger.info(
                'Cache cleaning finished, time: {0:.3f}'.format(
                    time.time() - start_ts))

    def defrag_cache_groups(self):
        logger.exception('Cache groups defragmentation started')
        try:
            start_ts = time.time()
            for cg in self.distributor.cache_groups.itervalues():

                logger.debug('Processing cache group {}'.format(cg.group_id))

                if cg.status != storage.Status.COUPLED:
                    logger.warn(
                        'Cache group {} will be skipped, status is {}'.format(
                            cg.group_id, cg.status))
                    continue

                if not cg.group.want_defrag:
                    logger.debug(
                        'Processing cache group {}'.format(cg.group_id))
                    continue

                logger.info(
                    'Defragmentation job will be created for cache group '
                    '{}'.format(cg.group_id))

                try:
                    job = self.job_processor._create_job(
                        jobs.JobTypes.TYPE_COUPLE_DEFRAG_JOB,
                        {'couple': str(cg.group.couple),
                         'need_approving': False,
                         'is_cache_couple': True})

                    logger.info(
                        'Successfully created defrag job for cache group {}, '
                        'job id {}'.format(cg.group_id, job.id))
                except Exception:
                    logger.exception('Failed to create cache group defrag job')
                    continue
        except Exception:
            logger.exception('Cache groups defragmentation failed')
            pass
        finally:
            logger.info(
                'Cache groups defragmentation finished, time: '
                '{0:.3f}'.format(time.time() - start_ts))

    def _clean_candidates(self):
        keys_db = self.distributor.keys_db
        expand_threshold = CACHE_CLEANER_CFG.get('expand_threshold', 21600)
        expand_ts_max = int(time.time()) - expand_threshold
        return keys_db.find({'expand_ts': {'$lt': expand_ts_max}}).sort(
            'expand_ts', pymongo.ASCENDING)


def mb_per_s(bytes_per_s):
    return '{0:.3f} Mb/s'.format(bytes_per_s / (1024.0 ** 2))


def _key_bw(key_stat):
    return float(key_stat['size']) / key_stat['period_in_seconds']


class CacheGroup(object):

    MAX_NODE_NETWORK_BANDWIDTH = CACHE_CFG.get('max_node_network_bandwidth',
                                               104857600)

    def __init__(self, group):
        self.group = group
        self.group_id = group.group_id
        self.stat = group.get_stat()
        self.reserved_tx_rate, self.reserved_space = 0.0, 0
        self.removed_keys_size = 0

    @property
    def effective_free_space(self):
        return max(self.group.effective_space - self.stat.used_space -
                   self.reserved_space, 0)

    @property
    def effective_space(self):
        return self.group.effective_space

    @property
    def dirty_coef(self):
        return 1.0 - min(
            1.0,
            (self.effective_free_space + self.stat.files_removed_size +
             self.removed_keys_size) / max(self.effective_space, 1))

    def account_removed_key(self, key_size):
        self.removed_keys_size += key_size

    @property
    def dc(self):
        return self.group.node_backends[0].node.host.dc

    @property
    def tx_rate(self):
        nb_tx_rate = self.group.node_backends[0].node.stat.tx_rate
        if nb_tx_rate is None:
            return None
        return min(
            (self.group.node_backends[0].node.stat.tx_rate +
             self.reserved_tx_rate),
            self.MAX_NODE_NETWORK_BANDWIDTH)

    @property
    def tx_rate_left(self):
        tx_rate = self.tx_rate
        if tx_rate is None:
            return None
        return self.MAX_NODE_NETWORK_BANDWIDTH - self.tx_rate

    def account_task(self, task):
        if task['action'] == 'add':
            self.reserved_tx_rate += task['tx_rate']
            self.reserved_space += task['size']

    @property
    def status(self):
        return self.group.status

    def __str__(self):
        return str(self.group_id)

    def __hash__(self):
        return hash(self.group_id)

    def __eq__(self, other):
        return self.group_id == other

    @property
    def weight(self):
        return 2.0 - (min(1.0,
                          self.tx_rate_left / self.MAX_NODE_NETWORK_BANDWIDTH) +
                      self.effective_free_space / self.effective_space)


class DcKeyCacheCandidates(object):
    def __init__(self, dc, weight, src_group_id):
        self.dc = dc
        self.weight = weight
        self.src_group_id = src_group_id
        self.key_copies = 0
        self.candidates = []
        self.node_types = inventory.get_balancer_node_types()

    def add_candidate(self, cg):
        self.candidates.append(cg)

    def account_key_copy(self, cg):
        self.key_copies += 1

    def pop_candidate(self):
        return self.candidates and self.candidates.pop(0) or None

    @staticmethod
    def units_diff(u1, u2):
            diff = 0
            for k in u1.keys():
                diff += int(u1[k] != u2[k])
            return diff

    def group_units(self):
        return infrastructure.groups_units(
            [cg.group for cg in self.candidates] +
            [storage.groups[self.src_group_id]],
            self.node_types)

    def sort_candidates(self):
        weights = {}
        gu = self.group_units()
        if self.src_group_id not in gu:
            raise RuntimeError(
                'Failed to get parents for source group {}'.format(
                    self.src_group_id))
        for cg in self.candidates:
            if cg.group_id not in gu:
                continue
            weights[cg] = (
                DcKeyCacheCandidates.units_diff(gu[self.src_group_id][0],
                                                gu[cg.group_id][0]),
                cg.weight
            )
        self.candidates = [w[0] for w in sorted(
            weights.iteritems(), key=lambda w: w[1])]
