# -*- coding: utf-8 -*-

from time import time
import copy

all_nodes = {}
all_groups = {}

__config = {}

def reset():
    global all_nodes, all_groups
    all_nodes = {}
    all_groups = {}

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
    __config = lconfig

class NodeState:
    def __init__(self, raw_node):
        self.__lastTime = time()
        self.__address = raw_node["addr"]
        self.__groupId = raw_node["group_id"]
        
        self.__lastRead = raw_node["storage_commands"]["READ"][0] + \
                raw_node["proxy_commands"]["READ"][0]
        self.__lastWrite = raw_node["storage_commands"]["WRITE"][0] + \
                raw_node["proxy_commands"]["WRITE"][0]
        
        self.__maxPutPerPeriod = 0
        self.__realPutPerPeriod = 0
        self.__maxGetPerPeriod = 0
        self.__realGetPerPeriod = 0
        self.commonCount(raw_node)
        
    def __str__(self):
        result = "addr: " + self.addr()
        result += "; __lastTime: " + str(self.__lastTime)
        result += "; __lastRead: " + str(self.__lastRead)
        result += "; __lastWrite: " + str(self.__lastWrite)
        result += "; realPutPerSecond: " + str(self.realPutPerPeriod())
        result += "; maxPutPerSecond: " + str(self.maxPutPerPeriod())
        result += "; realGetPerSecond: " + str(self.realGetPerPeriod())
        result += "; maxGetPerSecond: " + str(self.maxGetPerPeriod())
        result += "; freeSpaceInKb: " + str(self.freeSpaceInKb())
        result += "; freeSpaceRelative: " + str(self.freeSpaceRelative())
        return result

    def update(self, raw_node):
        currentTime = time()
        
        read = raw_node["storage_commands"]["READ"][0] + \
                raw_node["proxy_commands"]["READ"][0]
        write = raw_node["storage_commands"]["WRITE"][0] + \
                raw_node["proxy_commands"]["WRITE"][0]
        
        if(read < self.__lastRead or write < self.__lastWrite):
            self.__lastRead = 0
            self.__lastWrite = 0
            
        dt = currentTime - self.__lastTime
        counters = raw_node['counters']
        loadAverage = float((counters.get('DNET_CNTR_DU1') or counters["DNET_CNTR_LA1"])[0]) / 100
        
        self.__realPutPerPeriod = (write - self.__lastWrite) / dt
        self.__realGetPerPeriod = (read - self.__lastRead) / dt
        self.__maxPutPerPeriod = self.__realPutPerPeriod / loadAverage
        self.__maxGetPerPeriod = self.__realGetPerPeriod / loadAverage
        self.__lastRead = read
        self.__lastWrite = write
        self.__lastTime = currentTime
        self.commonCount(raw_node)

    def commonCount(self, raw_node):
        self.__freeSpaceInKb = float(raw_node['counters']['DNET_CNTR_BAVAIL'][0]) / 1024 * raw_node['counters']['DNET_CNTR_BSIZE'][0]
        self.__freeSpaceRelative = float(raw_node['counters']['DNET_CNTR_BAVAIL'][0]) / raw_node['counters']['DNET_CNTR_BLOCKS'][0]

    def addr(self):
        return self.__address
    def realPutPerPeriod(self):
        return self.__realPutPerPeriod
    def maxPutPerPeriod(self):
        return self.__maxPutPerPeriod
    def realGetPerPeriod(self):
        return self.__realGetPerPeriod
    def maxGetPerPeriod(self):
        return self.__maxGetPerPeriod
    def freeSpaceInKb(self):
        return self.__freeSpaceInKb
    def freeSpaceRelative(self):
        return self.__freeSpaceRelative

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
            if not address in all_nodes:
                all_nodes[address] = NodeState(raw_node)
            self.__nodes[address] = all_nodes[address]
    
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
        return sum([node.freeSpaceInKb() for node in self.__nodes.itervalues()])

    def freeSpaceRelative(self):
        return min([node.freeSpaceRelative() for node in self.__nodes.itervalues()])

def composeDataType(size):
    return "symm" + str(size)

def config(size):
    result = copy.deepcopy(__config)
    result["UnitDataType"] = composeDataType(size)
    return result

class SymmGroup:
    def __init__(self, group_ids):
        self.__group_list = [all_groups[id] for id in group_ids]
    
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
        return False
    
    def dataType(self):
        return composeDataType(str(len(self.__group_list)))

def add_raw_node(raw_node):
    group_id = raw_node["group_id"]
    if not group_id in all_groups:
        all_groups[group_id] = GroupState(raw_node)
    else:
        all_groups[group_id].update(raw_node)
