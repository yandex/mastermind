# -*- coding: utf-8 -*-

from time import time
import copy
import threading

import inventory

all_nodes = {}
all_nodes_lock = threading.Lock()

all_groups = {}
all_groups_lock = threading.Lock()

groups_in_couple = set()
groups_in_couple_lock = threading.Lock()

def all_group_ids():
    with all_groups_lock:
        return list(all_groups.iterkeys())

symm_groups_map = {}
symm_groups_map_lock = threading.Lock()
__config = {}
__config_lock = threading.Lock()

def setConfig(mastermind_config):
    lconfig = {}
    lconfig["MinimumFreeSpaceInKbToParticipate"] = mastermind_config.get("min_free_space", 256) * 1024
    lconfig["MinimumFreeSpaceRelativeToParticipate"] = mastermind_config.get("min_free_space_relative", 0.15)
    lconfig["MinimumUnitsWithPositiveWeight"] = mastermind_config.get("min_units", 1)
    lconfig["AdditionalUnitsNumber"] = mastermind_config.get("add_units", 1)
    lconfig["AdditionalUnitsPercentage"] = mastermind_config.get("add_units_relative", 0.10)
    lconfig["AdditionalMessagePerSecondNumber"] = mastermind_config.get("add_rps", 20)
    lconfig["AdditionalMessagePerSecondPercentage"] = mastermind_config.get("add_rps_relative", 0.15)
    lconfig["TailHeightPercentage"] = mastermind_config.get("tail_height_relative", 0.95)
    lconfig["TailHeightSpaceInKb"] = mastermind_config.get("tail_height", 500) * 1024
    lconfig["WeightMultiplierHead"] = mastermind_config.get("multiplier_head", 1000000)
    lconfig["WeightMultiplierTail"] = mastermind_config.get("multiplier_tail", 600000)
    lconfig["MinimumWeight"] = mastermind_config.get("min_weight", 10000)
    global __config
    with __config_lock:
        __config = lconfig

def getConfig():
    with __config_lock:
        return copy.copy(__config)

def setConfigValue(key, value):
    global __config
    with __config_lock:
        __config[key] = value

class NodeStateData:
    def __init__(self, raw_node):
        self.first = True
        self.lastTime = time()

        self.lastRead = raw_node["storage_commands"]["READ"][0] + \
                raw_node["proxy_commands"]["READ"][0]
        self.lastWrite = raw_node["storage_commands"]["WRITE"][0] + \
                raw_node["proxy_commands"]["WRITE"][0]

        self.freeSpaceInKb = float(raw_node['counters']['DNET_CNTR_BAVAIL'][0]) / 1024 * raw_node['counters']['DNET_CNTR_BSIZE'][0]
        self.freeSpaceRelative = float(raw_node['counters']['DNET_CNTR_BAVAIL'][0]) / raw_node['counters']['DNET_CNTR_BLOCKS'][0]
        self.loadAverage = float((raw_node['counters'].get('DNET_CNTR_DU1') or raw_node['counters']["DNET_CNTR_LA1"])[0]) / 100

    def gen(self, raw_node):
        result = NodeStateData(raw_node)
        result.first = False

        if(result.lastRead < self.lastRead or result.lastWrite < self.lastWrite):
            # Stats were reset
            last_read = 0
            last_write = 0
        else:
            last_read = self.lastRead
            last_write = self.lastWrite

        dt = result.lastTime - self.lastTime
        counters = raw_node['counters']

        result.realPutPerPeriod = (result.lastWrite - self.lastWrite) / dt
        result.realGetPerPeriod = (result.lastRead - self.lastRead) / dt
        result.maxPutPerPeriod = result.realPutPerPeriod / result.loadAverage
        result.maxGetPerPeriod = result.realGetPerPeriod / result.loadAverage

        return result

class NodeState:
    def __init__(self, raw_node):
        self.__address = raw_node["addr"]
        self.__groupId = raw_node["group_id"]
        self.__data = NodeStateData(raw_node)

    def update(self, raw_node):
        self.__data = self.__data.gen(raw_node)

    def __str__(self):
        result = str(self.__address)
        result += "; realPutPerSecond: " + str(self.realPutPerPeriod())
        result += "; maxPutPerSecond: " + str(self.maxPutPerPeriod())
        result += "; realGetPerSecond: " + str(self.realGetPerPeriod())
        result += "; maxGetPerSecond: " + str(self.maxGetPerPeriod())
        result += "; freeSpaceInKb: " + str(self.freeSpaceInKb())
        result += "; freeSpaceRelative: " + str(self.freeSpaceRelative())
        return result

    def addr(self):
        return self.__address

    def realPutPerPeriod(self):
        if self.__data.first:
            return 0
        else:
            return self.__data.realPutPerPeriod

    def maxPutPerPeriod(self):
        if self.__data.first:
            return 0
        else:
            return self.__data.maxPutPerPeriod

    def realGetPerPeriod(self):
        if self.__data.first:
            return 0
        else:
            return self.__data.realGetPerPeriod

    def maxGetPerPeriod(self):
        if self.__data.first:
            return 0
        else:
            return self.__data.maxGetPerPeriod

    def freeSpaceInKb(self):
        return self.__data.freeSpaceInKb

    def freeSpaceRelative(self):
        return self.__data.freeSpaceRelative

    def age(self):
        return time() - self.__data.lastTime

    def info(self):
        result = dict()
        result['group_id'] = self.__groupId
        result['addr'] = self.__address
        result['free_space_rel'] = self.__data.freeSpaceRelative
        result['free_space_abs'] = self.__data.freeSpaceInKb / 1024 / 1024
        result['la'] = self.__data.loadAverage
        result['dc'] = self.get_dc()
        return result

    def get_dc(self):
        host = self.__address.split(':')[0]
        return inventory.get_dc_by_host(host)

class GroupState:
    def __init__(self, raw_node):
        self.__nodes = {}
        self.__groupId = raw_node["group_id"]
        self.update(raw_node)

    def update(self, raw_node):
        address = raw_node["addr"]
        if address in self.__nodes:
            self.__nodes[address].update(raw_node)
        else:
            with all_nodes_lock:
                if not address in all_nodes:
                    all_nodes[address] = NodeState(raw_node)
                self.__nodes[address] = all_nodes[address]

    def setCouples(self, couples):
        with symm_groups_map_lock:
            symm_groups_map[self.__groupId] = couples
        if couples is not None:
            for group in couples:
                with groups_in_couple_lock:
                    groups_in_couple.add(group)

    def unsetCouples(self):
        self.setCouples(None)

    def checkCouples(self, couples):
        return (symm_groups_map[self.__groupId] == couples)

    def groupId(self):
        return self.__groupId

    def __str__(self):
        result = "groupId: " + str(self.groupId())
        result += " ["
        for node in self.__nodes.itervalues():
            result += str(node) + ", "
        result += "]"
        return result

    def realPutPerPeriod(self):
        return sum([node.realPutPerPeriod() for node in self.__nodes.itervalues()])

    def maxPutPerPeriod(self):
        return sum([node.maxPutPerPeriod() for node in self.__nodes.itervalues()])

    def realGetPerPeriod(self):
        return sum([node.realGetPerPeriod() for node in self.__nodes.itervalues()])

    def maxGetPerPeriod(self):
        return sum([node.maxGetPerPeriod() for node in self.__nodes.itervalues()])

    def freeSpaceInKb(self):
        return min([node.freeSpaceInKb() for node in self.__nodes.itervalues()]) * len(self.__nodes)

    def freeSpaceRelative(self):
        return min([node.freeSpaceRelative() for node in self.__nodes.itervalues()])

    def age(self):
        return max([node.age() for node in self.__nodes.itervalues()])

    def info(self):
        result = {}
        result['nodes'] = [node.info() for node in self.__nodes.itervalues()]
        with groups_in_couple_lock:
            in_couple = (self.__groupId in groups_in_couple)

        if in_couple:
            with symm_groups_map_lock:
                couples = symm_groups_map[self.__groupId]
                if couples is not None and is_group_good(couples):
                    result['status'] = 'coupled'
                    result['couples'] = couples
                else:
                    result['status'] = 'bad'
                    result['couples'] = couples
        return result

    def get_dc(self):
        return [node for node in self.__nodes.itervalues()][0].get_dc()

def GroupSizeEquals(size):
    return lambda symm_group, size=size: len(symm_group.unitId()) == size

class SymmGroup:
    def __init__(self, group_ids, data_type = None):
        self.__group_ids = group_ids
        self.__group_list = []
        with all_groups_lock:
            for id in group_ids:
                group = all_groups.get(id, None)
                if group is not None:
                    self.__group_list.append(group)
        self.__data_type = data_type

    def __str__(self):
        result = "groups: " + str(self.unitId())
        result += " ["
        for group in self.__group_list:
            result += str(group) + ", "
        result += "]"
        result += "; realPutPerSecond: " + str(self.realPutPerPeriod())
        result += "; maxPutPerSecond: " + str(self.maxPutPerPeriod())
        result += "; realGetPerSecond: " + str(self.realGetPerPeriod())
        result += "; maxGetPerSecond: " + str(self.maxGetPerPeriod())
        result += "; freeSpaceInKb: " + str(self.freeSpaceInKb())
        result += "; freeSpaceRelative: " + str(self.freeSpaceRelative())
        return result

    def realPutPerPeriod(self):
        return max([group.realPutPerPeriod() for group in self.__group_list])

    def maxPutPerPeriod(self):
        return min([group.maxPutPerPeriod() for group in self.__group_list])

    def realGetPerPeriod(self):
        return sum([group.realGetPerPeriod() for group in self.__group_list])

    def maxGetPerPeriod(self):
        return sum([group.maxGetPerPeriod() for group in self.__group_list])

    def freeSpaceInKb(self):
        return min([group.freeSpaceInKb() for group in self.__group_list])

    def freeSpaceRelative(self):
        return min([group.freeSpaceRelative() for group in self.__group_list])

    def unitId(self):
        return tuple([group.groupId() for group in self.__group_list])

    def inService(self):
        return False

    def writeEnable(self):
        return True

    def isBad(self):
        too_old_age = getConfig().get("dynamic_too_old_age", 120)
        return (len(self.__group_ids) != len(self.__group_list)
                or not all([group.checkCouples(self.__group_ids) and group.age() <= too_old_age for group in self.__group_list]))

    def dataType(self):
        return self.__data_type or composeDataType(str(len(self.__group_list)))

def is_group_good(couples):
    return all([symm_groups_map.get(group, None) == couples for group in couples])

def filter_symm_groups(group_id = None):
    with symm_groups_map_lock:
        all_symm_groups = set(symm_groups_map.values())
        if None in all_symm_groups:
            all_symm_groups.remove(None)
        good_groups = set()
        bad_groups = set()
        for couples in all_symm_groups:
            if group_id is None or group_id in couples:
                if is_group_good(couples):
                    good_groups.add(couples)
                else:
                    bad_groups.add(couples)
    return (good_groups, bad_groups)

def uncoupled_groups():
    with all_groups_lock:
        all_groups_set = set(all_groups.iterkeys())
    with groups_in_couple_lock:
        return list(all_groups_set.difference(groups_in_couple))

def remove_group(group_ids):
    for group_id in group_ids:
        get_group(group_id).unsetCouples()
        with groups_in_couple_lock:
            groups_in_couple.remove(group_id)

def add_raw_node(raw_node):
    group_id = raw_node["group_id"]
    with all_groups_lock:
        if not group_id in all_groups:
            all_groups[group_id] = GroupState(raw_node)
        else:
            all_groups[group_id].update(raw_node)

def get_group(id):
    with all_groups_lock:
        return all_groups[id]
