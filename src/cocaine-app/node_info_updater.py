# -*- coding: utf-8 -*-
import elliptics

import balancelogicadapter as bla
import timed_queue
import threading
import msgpack
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
        global _tq
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

            for raw_node in raw_stats:
                bla.add_raw_node(raw_node)

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
            self.__session.add_groups([group.group_id])
            couples = msgpack.unpackb(self.__session.read_data(symmetric_groups_key))
            self.__logging.info("Read symmetric groups from group %d: %s" % (group.group_id, str(couples)))
            group.parse_meta({'couple': couples})
            for group_id2 in couples:
                if group_id2 != group_id:
                    self.__logging.info("Scheduling update for group %d" % group_id2)
                    self.__tq.hurry(get_symm_group_update_task_id(group_id2))

                    if not group_id2 in storage.groups:
                        self.__logging.info("Group %d doesn't exist in all_groups, add fake data with couples=%s" % (group_id2, str(couples)))
                        storage.groups.add(group_id2)

            group.update_status()
        except Exception as e:
            self.__logging.error("Failed to read symmetric_groups from group %d (%s), %s" % (group_id, str(e), traceback.format_exc()))
            group.parse_meta(None)
            group.update_status()
        except:
            self.__logging.error("Failed2 to read symmetric_groups from group %d (%s), %s" % (group_id, sys.exc_info()[0], traceback.format_exc()))
            group.parse_meta(None)
            group.update_status()

    def stop(self):
        self.__tq.shutdown()
