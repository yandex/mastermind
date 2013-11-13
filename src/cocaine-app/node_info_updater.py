# -*- coding: utf-8 -*-
import json
import sys
import threading
import time
import traceback

from cocaine.services import Service
import elliptics

import balancer
import balancelogicadapter as bla
from config import config
import keys
import timed_queue
import storage


GROUP_META_UPDATE_TASK_ID = 'update_symms_for_group_%d'
COUPLE_META_UPDATE_TASK_ID = 'update_meta_for_couple_%s'


class NodeInfoUpdater:

    STORAGE_STATE_CACHE_KEY = 'mastermind_storage'
    STORAGE_STATE_VERSION = '1'
    DEFAULT_STORAGE_STATE_VALID_TIME = 600

    def __init__(self, logging, node):
        logging.info("Created NodeInfoUpdater")
        self.__logging = logging
        self.__node = node
        self.__tq = timed_queue.TimedQueue()
        self.__tq.start()
        self.__session = elliptics.Session(self.__node)
        self.__nodeUpdateTimestamps = (time.time(), time.time())
        self.__cache = Service('cache')
        delayed = self.try_restore_from_cache()
        self.loadNodes(delayed=delayed)

    def try_restore_from_cache(self):
        try:
            cached_state = self.__cache.perform_sync('get', self.STORAGE_STATE_CACHE_KEY).next()
            if cached_state[0] == False:
                raise ValueError('No cached state available')
            self.restore_state(cached_state[1])
            self.__logging.info('Successfully restored from cache')
        except Exception as e:
            self.__logging.info('Failed to restore state from cache: %s\n%s' % (str(e), traceback.format_exc()))
            return False
        return True

    def execute_tasks(self, delayed):
        try:

            raw_stats = self.__session.stat_log()
            storage.update_statistics(raw_stats)

            # need to use keys method to avoid runtime error when
            # storage.groups changes (when 'delayed' is False)
            for group in storage.groups.keys():
                if delayed:
                    self.__tq.add_task_in(
                        GROUP_META_UPDATE_TASK_ID % group.group_id,
                        config.get('symm_group_read_gap', 1),
                        self.updateSymmGroup,
                        group)
                else:
                    self.updateSymmGroup(group)

            for couple in storage.couples:
                if delayed:
                    self.__tq.add_task_in(
                        COUPLE_META_UPDATE_TASK_ID % str(couple),
                        config.get('couple_read_gap', 1),
                        self.updateCoupleMeta,
                        couple)
                else:
                    self.updateCoupleMeta(couple)

        except Exception as e:
            self.__logging.info('Failed to initialize node updater: %s\n%s' % (str(e), traceback.format_exc()))
            pass

    def loadNodes(self, delayed=True):
        self.__logging.info("Start loading units")
        try:

            self.execute_tasks(delayed)

            try:
                max_group = int(self.__node.meta_session.read_data(keys.MASTERMIND_MAX_GROUP_KEY))
            except:
                max_group = 0
            curr_max_group = max((g.group_id for g in storage.groups))
            if curr_max_group > max_group:
                self.__node.meta_session.write_data(keys.MASTERMIND_MAX_GROUP_KEY, str(curr_max_group))

            self.store_state()

        except Exception as e:
            self.__logging.error("Error while loading node stats: %s\n%s" % (str(e), traceback.format_exc()))
        finally:
            reload_period = config.get('nodes_reload_period', 60)
            self.__tq.add_task_in("load_nodes", reload_period, self.loadNodes)
            self.__nodeUpdateTimestamps = self.__nodeUpdateTimestamps[1:] + (time.time(),)
            bla.setConfigValue("dynamic_too_old_age", max(time.time() - self.__nodeUpdateTimestamps[0], reload_period * 3))

    def updateSymmGroup(self, group):
        try:
            self.__logging.info("Trying to read symmetric groups from group %d" % (group.group_id))
            self.__session.add_groups([group.group_id])
            meta = self.__session.read_data(keys.SYMMETRIC_GROUPS_KEY)
            group.parse_meta(meta)
            couples = group.meta['couple']
            self.__logging.info("Read symmetric groups from group %d: %s" % (group.group_id, couples))
            for group_id2 in couples:
                if group_id2 != group.group_id:
                    self.__logging.info("Scheduling update for group %s" % group_id2)
                    self.__tq.hurry(GROUP_META_UPDATE_TASK_ID % group_id2)

            couple_str = ':'.join((str(g) for g in sorted(couples)))
            self.__logging.info('%s in storage.couples: %s' % (couple_str, couple_str in storage.couples))
            self.__logging.info('Keys in storage.couples: %s' % [str(c) for c in storage.couples])

            if not couple_str in storage.couples:
                self.__logging.info("Creating couple %s" % (couple_str))
                for gid in couples:
                    if not gid in storage.groups:
                        self.__logging.info("Group %s doesn't exist in all_groups, add fake data with couples=%s" % (gid, couples))
                        storage.groups.add(gid)
                c = storage.couples.add([storage.groups[gid] for gid in couples])
                self.__logging.info("Created couple %s %s" % (c, repr(c)))
                self.__tq.add_task_in(COUPLE_META_UPDATE_TASK_ID % str(c),
                                      config.get('couple_read_gap', 1),
                                      self.updateCoupleMeta,
                                      c)
            else:
                self.__logging.info("Couple %s already exists" % couple_str)

            storage.couples[couple_str].update_status()
        except Exception as e:
            self.__logging.error("Failed to read symmetric_groups from group %d (%s), %s" % (group.group_id, str(e), traceback.format_exc()))
            group.parse_meta(None)
            if group.couple:
                group.couple.update_status()
            else:
                group.update_status()
        except:
            self.__logging.error("Failed2 to read symmetric_groups from group %d (%s), %s" % (group.group_id, sys.exc_info()[0], traceback.format_exc()))
            group.parse_meta(None)
            if group.couple:
                group.couple.update_status()
            else:
                group.update_status()

    def updateCoupleMeta(self, couple):
        try:
            self.__logging.info('Reading couple %s meta data' % couple)
            data = None
            try:
                data = self.__node.meta_session.read_latest(keys.MASTERMIND_COUPLE_META_KEY % str(couple))
            except elliptics.NotFoundError:
                self.__logging.info('No meta data found for couple %s' % couple)
                couple.parse_meta(None)
                couple.update_status()
                return
            couple.parse_meta(data)
            self.__logging.info('Parsed meta for couple %s' % couple)
        except BaseException as e:
            self.__logging.error('Failed to read meta for couple %s (%s), %s' % (couple, str(e), traceback.format_exc()))
            couple.parse_meta(None)

        couple.update_status()

    def restore_state(self, state):
        state = json.loads(state)
        if state['version'] != self.STORAGE_STATE_VERSION:
            raise ValueError('Unsupported storage state version: %s, required: %s' %
                             (state['version'], self.STORAGE_STATE_VERSION))

        cache_age = time.time() - state['timestamp']
        cache_valid_age = config.get('storage_cache_valid_time',
                                      self.DEFAULT_STORAGE_STATE_VALID_TIME)
        if cache_age > cache_valid_age:
            self.__logging.debug('Cache age is %s seconds, valid age is %s seconds' %
                                 (cache_age, cache_valid_age))
            raise ValueError('Cache is stale and cannot be used')

        for g in state['state']['groups']:
            group = storage.groups.add(g['group_id'])
            for n in g['nodes']:
                addr = n['host'].encode('utf-8')
                if not addr in storage.hosts:
                    storage.hosts.add(addr)
                    self.__logging.info('Adding host %s' % (addr))
                host = storage.hosts[addr]
                node = storage.nodes.add(host, n['port'])
                group.add_node(node)
                node.destroyed = n['destroyed']
                node.read_only = n['read_only']
                stat = storage.NodeStat.unserialize(n['stat'])
                self.__logging.info('stat: %s' % stat)
                node.stat = stat
                node.update_status()
            group.meta = g['meta']

        for c in state['state']['couples']:
            groups = [storage.groups[g] for g in c['groups']]
            couple = storage.couples.add(groups)
            couple.meta = c['meta']
            couple.update_status()

        self.__logging.info('%s' % ([couple for couple in storage.couples],))

    def store_state(self):
        state = json.dumps({
            'version': self.STORAGE_STATE_VERSION,
            'timestamp': time.time(),
            'state': {
                'groups': [g.serialize() for g in storage.groups],
                'couples': [c.serialize() for c in storage.couples],
            }
        })
        self.__logging.info('Saving cached state')
        self.__cache.put(self.STORAGE_STATE_CACHE_KEY, state)

    def stop(self):
        self.__tq.shutdown()
