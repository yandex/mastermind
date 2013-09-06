# -*- coding: utf-8 -*-
import elliptics

import balancelogicadapter as bla
import timed_queue
import threading
import traceback
import time
import storage

symmetric_groups_key = "metabalancer\0symmetric_groups"
mastermind_max_group_key = "mastermind:max_group"

__config = {}
__config_lock = threading.Lock()

def get_symm_group_update_task_id(group_id):
    return "update_symms_for_group_%s" % str(group_id)

def set_config_value(key, value):
    with __config_lock:
        __config[key] = value

def get_config_value(key, default):
    with __config_lock:
        return __config.get(key, default)

class NodeInfoUpdater:
    def __init__(self, logging, node):
        logging.info("Created NodeInfoUpdater")
        self.__logging = logging
        self.__node = node
        self.__tq = timed_queue.TimedQueue()
        self.__tq.start()
        self.__session = elliptics.Session(self.__node)
        self.__nodeUpdateTimestamps = (time.time(), time.time())
        self.loadNodes()

    def loadNodes(self):
        self.__logging.info("Start loading units")
        try:
            raw_stats = self.__session.stat_log()

            storage.update_statistics(raw_stats)

            for group in storage.groups:
                self.__tq.add_task_in(
                    get_symm_group_update_task_id(group.group_id),
                    get_config_value("symm_group_read_gap", 1),
                    self.updateSymmGroup,
                    group)
            try:
                max_group = int(self.__node.meta_session.read_data(mastermind_max_group_key))
            except:
                max_group = 0
            curr_max_group = max((g.group_id for g in storage.groups))
            if curr_max_group > max_group:
                self.__node.meta_session.write_data(mastermind_max_group_key, str(curr_max_group))
        except Exception as e:
            self.__logging.error("Error while loading node stats: %s\n%s" % (str(e), traceback.format_exc()))
        finally:
            reload_period = get_config_value("nodes_reload_period", 60)
            self.__tq.add_task_in("load_nodes", reload_period, self.loadNodes)
            self.__nodeUpdateTimestamps = self.__nodeUpdateTimestamps[1:] + (time.time(),)
            bla.setConfigValue("dynamic_too_old_age", max(time.time() - self.__nodeUpdateTimestamps[0], reload_period*3))

    def updateSymmGroup(self, group):
        try:
            self.__logging.info("Trying to read symmetric groups from group %d" % (group.group_id))
            self.__session.add_groups([group.group_id])
            meta = self.__session.read_data(symmetric_groups_key)
            group.parse_meta(meta)
            couples = group.meta['couple']
            self.__logging.info("Read symmetric groups from group %d: %s" % (group.group_id, str(couples)))
            for group_id2 in couples:
                if group_id2 != group.group_id:
                    self.__logging.info("Scheduling update for group %d" % group_id2)
                    self.__tq.hurry(get_symm_group_update_task_id(group_id2))

                    if not group_id2 in storage.groups:
                        self.__logging.info("Group %d doesn't exist in all_groups, add fake data with couples=%s" % (group_id2, str(couples)))
                        storage.groups.add(group_id2)

            couple_str = ':'.join((str(g) for g in sorted(couples)))
            self.__logging.info(couple_str + ' in storage.couples: ' + str( couple_str in storage.couples))
            self.__logging.info('Keys in storage.couples: %s' % (str([str(c) for c in storage.couples])))
            if not couple_str in storage.couples:
                self.__logging.info("Creating couple %s" % (couple_str))
                c = storage.couples.add([storage.groups[gid] for gid in couples])
                self.__logging.info("Created couple %s %s" % (str(c), repr(c)))
            else:
                self.__logging.info("Couple %s already exists" % (couple_str))
            storage.couples[couple_str].update_status()

            group.update_status()
        except Exception as e:
            self.__logging.error("Failed to read symmetric_groups from group %d (%s), %s" % (group.group_id, str(e), traceback.format_exc()))
            group.parse_meta(None)
            group.update_status()
        except:
            self.__logging.error("Failed2 to read symmetric_groups from group %d (%s), %s" % (group.group_id, sys.exc_info()[0], traceback.format_exc()))
            group.parse_meta(None)
            group.update_status()

    def stop(self):
        self.__tq.shutdown()
