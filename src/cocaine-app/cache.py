from collections import defaultdict
import copy
import logging
import math
import threading
import time
import traceback

import elliptics
import msgpack

import balancer
from cache_transport import cache_task_manager
from collections import defaultdict
from config import config
from db.mongo.pool import Collection
from errors import CacheUpstreamError
import helpers as h
import inventory
from infrastructure import infrastructure
import keys
from manual_locks import manual_locker
import node_info_updater
import storage
import timed_queue
from timer import periodic_timer


logger = logging.getLogger('mm.cache')

CACHE_CFG = config.get('cache', {})
CACHE_GROUP_PATH_PREFIX = CACHE_CFG.get('group_path_prefix')


class CacheManager(object):
    def __init__(self, node, db):
        self.node = node
        self.session = elliptics.Session(self.node)
        wait_timeout = config.get('elliptics', {}).get('wait_timeout', 5)
        self.session.set_timeout(wait_timeout)

        self.service_metakey = str(
            self.session.transform(keys.SYMMETRIC_GROUPS_KEY))

        try:
            keys_db_uri = config['metadata']['cache']['db']
        except KeyError:
            logger.error('Config parameter metadata.cache.db is required for cache manager')
            raise
        self.keys_db = Collection(db[keys_db_uri], 'keys')
        self.distributor = CacheDistributor(self.node, self.keys_db)

        self.niu = node_info_updater.NodeInfoUpdater(self.node, None)

        self.top_keys = {}

        self.__tq = timed_queue.TimedQueue()

        self.nodes_update()
        self.update_cache_groups()

        self.__tq.add_task_in('monitor_top_stats',
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
            host_addrs = set(r.address for r in self.session.routes.get_unique_routes())
            logger.info('Unique routes calculated')

            requests = []
            for address in host_addrs:
                session = self.session.clone()
                session.set_direct_id(address)
                logger.debug('Request for top of node {0}'.format(address))
                requests.append((session.monitor_stat(address,
                    self.STAT_CATEGORIES), address))

            new_top_keys = {}
            for result, address in requests:
                try:
                    h.process_elliptics_async_result(result, self.update_top, new_top_keys)
                except Exception as e:
                    logger.error('Failed to request monitor_stat for node {0}: '
                        '{1}\n{2}'.format(address, e, traceback.format_exc()))
                    continue

            self.top_keys = new_top_keys

            self.distributor.distribute(self.top_keys)

        except Exception as e:
            logger.error('Failed to update monitor top stats: {0}\n{1}'.format(
                e, traceback.format_exc()))
            pass
        finally:
            logger.info('Monitor top stats update finished, time: {0:.3f}'.format(time.time() - start_ts))
            self.__tq.add_task_in('monitor_top_stats',
                CACHE_CFG.get('top_update_period', 1800),
                self.monitor_top_stats)

    def update_top(self, m_stat, new_top_keys, elapsed_time=None, end_time=None):

        node_addr = '{0}:{1}'.format(m_stat.address.host, m_stat.address.port)
        logger.debug('Top updating: node {0} statistics time: {1}.{2:03d}'.format(
            node_addr, elapsed_time.tsec, int(round(elapsed_time.tnsec / (1000.0 * 1000.0)))))
        logger.info('Stats: {0}'.format(node_addr))

        top_keys = m_stat.statistics

        if not 'top' in top_keys:
            logger.error('No top data available for node {0}'.format(node_addr))
            return

        update_period = top_keys['top']['period_in_seconds']

        for key in top_keys['top']['top_by_size']:
            # skip mastermind service key (keys.SYMMETRIC_GROUPS_KEY)
            if key['id'] == self.service_metakey:
                continue

            if not key['group'] in storage.groups:
                logger.error('Key {}: source group {} is not found in storage'.format(
                    key['id'], key['group']))
                continue

            group = storage.groups[key['group']]
            if group.type == storage.Group.TYPE_CACHE:
                # this is a cache group, so their is no straightforward way to get
                # original key couple; the only way is to search metadb for
                # corresponding key id and cache group, and take couple id and
                # ns from matching record
                logger.debug('Key {}: cache group {}, establishing couple '
                    'and ns'.format(key['id'], group.group_id))
                keys = list(self.keys_db.find({'id': key['id'],
                                               'cache_groups': group.group_id}))
                if len(keys) > 1:
                    logger.error('Key {}: matched {} keys in metadb by cache '
                        'group {}'.format(key['id'], len(keys), group.group_id))
                    continue

                if not keys:
                    logger.warn('Key {}: found on top statistics for cache group {}, '
                        'not found in metadb'.format(key['id'], group.group_id))
                    continue

                key_record = keys[0]
                couple_id, ns = key_record['couple'], key_record['ns']

            else:
                if not group.couple:
                    logger.error('Key {}: source group {} does not belong to any couple'.format(
                        key['id'], group.group_id))
                    continue

                couple_id = str(group.couple)
                try:
                    ns = group.couple.namespace
                except ValueError:
                    logger.error('Key {}: couple of source group {} has broken '
                        'namespaces settings'.format(key['id'], group.group_id))
                    continue

            new_top_keys.setdefault((key['id'], couple_id),
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
            logger.info('Failed to fetch nodes statictics: {0}\n{1}'.format(e,
                traceback.format_exc()))
        finally:
            logger.info('Cluster updating: node statistics collecting '
                'finished, time: {0:.3f}'.format(time.time() - start_ts))
            reload_period = config.get('nodes_reload_period', 15)
            self.__tq.add_task_in('node_statistics_update',
                reload_period, self.nodes_update)

    def update_cache_groups(self):
        try:
            start_ts = time.time()
            logger.info('Cluster updating: updating group coupling info started')
            groups = []
            for nb in storage.node_backends:
                # if nb.base_path and nb.base_path.startswith(CACHE_GROUP_PATH_PREFIX) and nb.group:
                if nb.group and not nb.group.couple:
                    groups.append(nb.group)
            self.niu.update_symm_groups_async(groups=groups)

            self._mark_cache_groups()

            logger.info('Detected cache groups: {0}'.format(
                len([g for g in groups if g.type == storage.Group.TYPE_CACHE])))

        except Exception as e:
            logger.info('Failed to update groups: {0}\n{1}'.format(
                e, traceback.format_exc()))
        finally:
            logger.info('Cluster updating: updating group coupling info '
                'finished, time: {0:.3f}'.format(time.time() - start_ts))
            reload_period = config.get('nodes_reload_period', 60)
            self.__tq.add_task_in('cache_groups_update',
                reload_period, self.update_cache_groups)

    def _mark_cache_groups(self):
        if not CACHE_GROUP_PATH_PREFIX:
            return

        # searching for unmarked cache groups to mark them
        for group in infrastructure.get_good_uncoupled_groups(
            types=[storage.Group.TYPE_UNMARKED]):

            if not group.node_backends[0].base_path.startswith(CACHE_GROUP_PATH_PREFIX):
                continue

            logger.info('Detected unmarked cache group {0} '
                '(type: {1}, path {2}, meta {3})'.format(
                    group, group.type, group.node_backends[0].base_path, group.meta))

            packed = msgpack.packb(group.compose_cache_group_meta())
            try:
                s = self.session.clone()
                s.add_groups([group.group_id])
                balancer.consistent_write(
                    s, keys.SYMMETRIC_GROUPS_KEY, packed)
                logger.info('Successfully marked cache group {0}'.format(
                    group))
            except Exception as e:
                logger.error('Failed to write meta key for group {0}: {1}\n{2}'.format(
                             group, e, traceback.format_exc()))
                continue

            try:
                group.parse_meta(packed)
                group.update_status()
            except Exception as e:
                logger.error('Failed to update status for group {0}: {1}\n{2}'.format(
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
        top_keys = self.top_keys
        for key_stat in self.top_keys.itervalues():
            key_id = key_stat['id']
            key_couple = key_stat['couple']
            key = keys[(key_id, key_couple)]
            key['id'] = key_id
            key['top_rate'] = self.distributor._key_bw(key_stat)
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


class CacheDistributor(object):
    def __init__(self, node, keys_db):
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

        data_group = None
        for gid in key_stat['groups']:
            group = storage.groups[gid]
            if group.type == storage.Group.TYPE_CACHE:
                continue
            if group.couple:
                data_group = group
                break
            else:
                logger.error('Key {0}: group {1} appears to be a data group, '
                    'but does not belong to any couple'.format(key_stat['id'], group.group_id))
                continue
        else:
            raise ValueError('Key {0}: key groups does not belong to '
                'any couple: {1}'.format(key_stat['id'], key_stat['groups']))
        return {
            'id': key_stat['id'],
            'couple': key_stat['couple'],
            'ns': key_stat['ns'],
            'sgroups': list(storage.couples[key_stat['couple']].as_tuple()),
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

    def distribute(self, top):
        """ Distributes top keys among available cache groups.
        Parameter "top" is a map of key id to key top statistics:

        {('123', '42:69'): {'groups': [42, 69],
                            'couple': '42:69',
                            'ns': 'magic',
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

        def process_key(key):
            if (key['id'], key['couple']) not in top:
                copies_diff = -len(key['cache_groups'])
                key_stat = self._new_key_stat(key['id'])
                logger.info('Key {0}: not in top anymore, will be removed'.format(key['id']))
            else:
                key_stat = top[(key['id'], key['couple'])]
                key_copies_num = self._key_bw(key_stat) / self.bandwidth_per_copy
                copies_diff = self._key_copies_diff(key, key_copies_num)

                logger.info('Key {}, couple {}: bandwidth {}; total number of copies '
                    '(including base ones): {}, additional number of copies: '
                    '{}'.format(key['id'], key['couple'], mbit_per_s(self._key_bw(key_stat)),
                        key_copies_num, copies_diff))

            if copies_diff == 0:
                return

            with self._cache_groups_lock:
                self._update_key(key, key_stat, copies_diff)

        # update currently distributed keys
        for key in self._get_distributed_keys():
            process_key(key)
            top.pop((key['id'], key['couple']), None)

        # process new keys
        for (key_id, key_couple), key_stat in top.iteritems():
            try:
                key = self._new_key(key_stat)
            except Exception as e:
                logger.exception('Key {}, couple {}: failed to create new key record, '
                    '{}:'.format(key_id, key_couple, e))
                continue
            process_key(key)

    def _update_key(self, key, key_stat, copies_diff):
        key['rate'] = self._key_bw(key_stat)
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
            if not group_id in self.cache_groups:
                # first candidates for key removal will be the groups that are
                # unavailable at the moment
                queue.append(group_id)
                continue

            cache_group = self.cache_groups[group_id]
            if cache_group.status != storage.Status.COUPLED:  # subject to change
                queue.append(group_id)
                continue

            ok_groups.append(cache_group)

        logger.info('Key {0}: inactive cache groups: {1}'.format(key['id'], queue))

        ok_groups.sort(key=lambda g: g.group.node_backends[0].node.stat.tx_rate,
            reverse=True)

        queue.extend([g.group_id for g in ok_groups])

        logger.info('Key {0}: will be removed from {1} groups in order '
            '{2}'.format(key['id'], count, queue))

        for group_id in queue[:count]:
            try:
                self._remove_key_from_group(key, group_id)
            except Exception as e:
                logger.error('Key {0}: failed to remove key from group {1}: '
                    '{2}\n{3}'.format(key['id'], group_id, e, traceback.format_exc()))
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
            self.keys_db.update({'id': key['id']}, key)
        else:
            self.keys_db.remove({'id': key['id']})

    def _group_unit(self, full_path):
        return full_path.rsplit('|', 1)[-1]

    def _key_by_dc(self, key):
        eid = elliptics.Id(key['id'].encode('utf-8'))

        lookups = []
        for group_id in key['sgroups']:  # add other cache groups as a source when key size is fixed
            s = self.session.clone()
            s.set_exceptions_policy(elliptics.exceptions_policy.no_exceptions)
            s.add_groups([group_id])
            lookups.append((s.lookup(eid), eid, group_id))

        key_by_dc = {}
        not_found_count = 0

        def set_key_size_by_dc(lookup, group_id, elapsed_time=None, end_time=None):
            # logger.debug('Groups units for group {0}: {1}'.format(group_id, self.groups_units[group_id]))
            if lookup.error.code:
                if lookup.error.code == -2:
                    not_found_count += 1
                    return
                else:
                    raise lookup.error

            dc = self._group_unit(self.groups_units[group_id][0][self.dc_node_type])
            key_by_dc[dc] = {
                'group': group_id,
                'size': lookup.size,
            }

        for result, eid, group_id in lookups:
            try:
                h.process_elliptics_async_result(result,
                    set_key_size_by_dc, group_id, raise_on_error=False)
            except Exception as e:
                logger.error('Failed to lookup key {0} on group {1}: '
                    '{2}'.format(eid, group_id, e))
                continue

        if len(lookups) == not_found_count:
            logger.info('Key {0}: has already been removed from couple'.format(key['id']))

        return key_by_dc

    def _increase_copies(self, key, key_stat, count):
        update_groups_ids = [group_id
                             for group_id in key['cache_groups'] + key['sgroups']
                             if group_id not in self.groups_units and
                             group_id in storage.groups]

        self.groups_units.update(infrastructure.groups_units(
            [storage.groups[group_id] for group_id in update_groups_ids],
            self.node_types))

        key_by_dc = self._key_by_dc(key)
        if not key_by_dc:
            logger.error('Key {0}: failed to lookup key in any of '
                'couple dcs'.format(key['id']))
            return

        # filter unsuitable groups by network rate and free space
        copies_num = len(key['cache_groups']) + len(key['sgroups']) + count
        bw_per_copy = float(key_stat['size']) / key_stat['period_in_seconds'] / copies_num

        cache_groups = []
        logger.info('Total cache groups available: {0}'.format(len(self.cache_groups)))
        for cache_group in self.cache_groups.itervalues():
            if cache_group.group_id in key['cache_groups']:
                continue
            if cache_group.tx_rate is None:
                logger.debug('Key {0}: tx rate for cache group {1} is unavailable '
                    'at the moment'.format(key['id'], cache_group.group_id))
                continue
            # TODO: decide on caching keys from different dc
            # dc = self._group_unit(self.groups_units[cache_group.group_id][0][self.dc_node_type])
            # if dc not in key_by_dc:
            #     continue
            key_max_size = max([key_by_dc[dc]['size'] for dc in key_by_dc])
            # if cache_group.effective_free_space < key_by_dc[dc]['size']:
            if cache_group.effective_free_space < key_max_size:
                logger.debug('Key {0}: not enough free space on cache group '
                    '{1}: {2} < {3}'.format(key['id'], cache_group.group_id, cache_group.effective_free_space, key_max_size))
                continue
            if cache_group.tx_rate_left < bw_per_copy:
                logger.debug('Key {0}: not enough tx rate on cache group '
                    '{1}: {2} < {3}'.format(key['id'], cache_group.group_id, cache_group.tx_rate_left, bw_per_copy))
                continue
            cache_groups.append(cache_group)

        logger.info('Key {0}: appropriate cache groups: {1}'.format(key['id'], cache_groups))

        copies_count_by_dc = defaultdict(int)
        # create map dc -> number of copies, used later for selecting destination
        # cache group
        for group_id in key['cache_groups'] + key['sgroups']:
            if group_id in self.groups_units:
                dc = self._group_unit(self.groups_units[group_id][0][self.dc_node_type])
                copies_count_by_dc[dc] += 1

        def units_diff(u1, u2):
            diff = 0
            for k in u1.keys():
                diff += int(u1[k] != u2[k])
            return diff

        # select dc of a group with minimal node network load for the case
        # when there is no available cache groups in key dcs
        min_load_dc = (None, None)
        for group_id in key['sgroups']:
            if not group_id in storage.groups:
                continue
            node = storage.groups[group_id].node_backends[0].node
            if min_load_dc[1] is None or node.stat.tx_rate < min_load_dc[1]:
                try:
                    min_load_dc = node.host.dc, node.stat.tx_rate
                except CacheUpstreamError:
                    logger.warn('Skipping {} because of cache failure'.format(node.host))
                    continue

        if min_load_dc[0] is None:
            logger.error('Key {}: failed to select minimal load dc for key'.format(
                key['id']))
            return

        # count group weights among cache groups that are left
        weights = {}
        for cache_group in cache_groups:
            dc = self._group_unit(self.groups_units[cache_group.group_id][0][self.dc_node_type])
            if dc in key_by_dc:
                sgroup = key_by_dc[dc]['group']
            elif min_load_dc[0]:
                sgroup = key_by_dc[min_load_dc[0]]['group']
            else:
                continue
            weights[cache_group] = (
                units_diff(self.groups_units[sgroup][0], self.groups_units[cache_group.group_id][0]),
                copies_count_by_dc[dc],
                cache_group.weight
            )

        # select <count> groups
        new_cache_groups = []
        while len(new_cache_groups) < count and weights:
            cache_group_w = sorted(weights.iteritems(), key=lambda w: w[1])[0]
            logger.info('Key {0}: top 10 cache groups weights: {1}'.format(key['id'], cache_group_w[:10]))
            cache_group, weight = cache_group_w
            del weights[cache_group]

            dc = self._group_unit(self.groups_units[cache_group.group_id][0][self.dc_node_type])
            if dc in key_by_dc:
                sgroup = key_by_dc[dc]['group']
            else:
                dc = min_load_dc[0]
                sgroup = key_by_dc[min_load_dc[0]]['group']

            try:
                task = self._add_key_to_group(key, cache_group.group_id, [sgroup],
                    bw_per_copy, key_by_dc[dc]['size'])
                cache_group.account_task(task)
                copies_count_by_dc[dc] += 1
                new_cache_groups.append(cache_group.group_id)
            except Exception as e:
                logger.error('Failed to add key {0} to group {1}: '
                    '{2}\n{3}'.format(key['id'], cache_group.group_id,
                        e, traceback.format_exc()))
                continue

        logger.info('Key {0}: cache tasks created for groups {1}'.format(
            key['id'], new_cache_groups))

    def _add_key_to_group(self, key, group_id, sgroups, tx_rate, size):
        """
        Creates task for gatling gun on destination group,
        updates key in meta database
        """
        task = self._gatlinggun_task(key, group_id, sgroups, 'add',
                tx_rate=tx_rate, size=size)
        if not self.dryrun:
            cache_task_manager.put_task(self._serialize(task))
        key['cache_groups'].append(group_id)
        key['expand_ts'] = int(time.time())
        self.keys_db.update({'id': key['id']}, key, upsert=True)
        return task

    @staticmethod
    def _serialize(task):
        return msgpack.packb(task)

    @staticmethod
    def _unserialize(task):
        return msgpack.unpackb(task)

    def _gatlinggun_task(self, key, group, sgroups, action, tx_rate=None, size=None):
        assert isinstance(group, int), "Group for gatlinggun task should be "\
                "int, not {0}".format(type(group).__name__)
        task = {
            'key': key['id'],
            'group': group,
            'sgroups': sgroups,
            'action': action,
        }
        if tx_rate:
            task['tx_rate'] = tx_rate
        if size:
            task['size'] = size
        return task

    def _key_bw(self, key_stat):
        return float(key_stat['size']) / key_stat['period_in_seconds']

    def _key_copies_diff(self, key, req_copies_num):
        sgroups_num = len(key['sgroups'])
        copies_num = len(key['cache_groups']) + len(key['sgroups'])

        req_copies = int(math.ceil(req_copies_num))
        if req_copies < math.ceil(copies_num * self.copies_reduce_factor):
            return max(sgroups_num, req_copies) - copies_num
        elif req_copies >= copies_num + self.copies_expand_step:
            return req_copies - copies_num
        return 0

    def _filter_by_bandwidth(self, top):
        filtered_top = {}
        for key_k, key_stat in top.iteritems():
            if self._key_bw(key_stat) < self.bandwidth_per_copy:
                continue
            filtered_top[key_k] = key_stat
        return filtered_top

    def _cache_group_by_group_id(self, group_id):
        if not group_id in storage.groups:
            return None

        group = storage.groups[group_id]
        if group.type != storage.Group.TYPE_CACHE:
            return None

        return group

    def _update_cache_groups(self):
        new_groups = {}
        for group in storage.groups:
            if group.type != storage.Group.TYPE_CACHE or group.status != storage.Status.COUPLED:  # status is subject to change
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
            if not group_id in new_cache_groups:
                logger.warn('Task destination group {0} is not found among '
                    'good cache groups'.format(group_id))
                continue
            cache_group = new_cache_groups[group_id]
            cache_group.account_task(task)


        # DEBUG
        for cg in new_cache_groups.itervalues():
            logger.debug('Cache group {0}: fes {1} ({2} - {3} - {4})'.format(
                cg, cg.effective_free_space, cg.group.node_backends[0].effective_space,
                cg.stat.used_space, cg.reserved_space))
        # DEBUG END

        with self._cache_groups_lock:
            self.cache_groups = new_cache_groups
            self.groups_units = new_groups_units
            self.executing_tasks = new_executing_tasks


def mbit_per_s(bytes_per_s):
    return '{0:.3f} Mbit/s'.format(bytes_per_s / (1024.0 ** 2))


class CacheGroup(object):

    MAX_NODE_NETWORK_BANDWIDTH = CACHE_CFG.get('max_node_network_bandwidth', 104857600)

    def __init__(self, group):
        self.group = group
        self.group_id = group.group_id
        self.stat = group.get_stat()
        self.reserved_tx_rate, self.reserved_space = 0.0, 0

    @property
    def effective_free_space(self):
        return max(self.group.effective_space - self.stat.used_space - self.reserved_space, 0)

    @property
    def tx_rate(self):
        nb_tx_rate = self.group.node_backends[0].node.stat.tx_rate
        if nb_tx_rate is None:
            return None
        return min(self.group.node_backends[0].node.stat.tx_rate + self.reserved_tx_rate,
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
        return 2.0 - (self.tx_rate_left / self.MAX_NODE_NETWORK_BANDWIDTH +
                      self.effective_free_space / self.stat.total_space)