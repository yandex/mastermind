# -*- coding: utf-8 -*-
import elliptics

import balancelogicadapter as bla
import timed_queue
import threading
import msgpack
import traceback

symmetric_groups_key = "metabalancer\0symmetric_groups"
mastermind_max_group_key = "mastermind:max_group"

__config = {}
__config_lock = threading.Lock()

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
        self.loadNodes()

    def loadNodes(self):
        self.__logging.info("Start loading units")
        try:
            raw_stats = self.__session.stat_log()

            for raw_node in raw_stats:
                bla.add_raw_node(raw_node)

            for group_id in bla.all_group_ids():
                self.__tq.add_task_in(
                    get_config_value("symm_group_read_gap", 1),
                    self.updateSymmGroup,
                    group_id)
            try:
                max_group = int(self.__node.meta_session.read(mastermind_max_group_key))
            except:
                max_group = 0
            curr_max_group = max(bla.all_group_ids())
            if curr_max_group > max_group:
                self.__node.meta_session.write(mastermind_max_group_key, str(curr_max_group))
        except Exception as e:
            self.__logging.error("Error while loading node stats: %s\n%s" % (str(e), traceback.format_exc()))
        finally:
            self.__tq.add_task_in(get_config_value("nodes_reload_period", 60), self.loadNodes)

    def updateSymmGroup(self, group_id):
        try:
            self.__session.add_groups([group_id])
            couples = msgpack.unpackb(self.__session.read_data(symmetric_groups_key))
            self.__logging.info("Read symmetric groups from group %d: %s" % (group_id, str(couples)))
            bla.get_group(int(group_id)).setCouples(couples)
        except Exception as e:
            self.__logging.error("Failed to read symmetric_groups from group %d (%s)" % (group_id, str(e)))
            bla.get_group(int(group_id)).unsetCouples()
        except:
            self.__logging.error("Failed to read symmetric_groups from group %d (%s)" % (group_id, sys.exc_info()[0]))
            bla.get_group(int(group_id)).unsetCouples()

    def stop(self):
        self.__tq.shutdown()