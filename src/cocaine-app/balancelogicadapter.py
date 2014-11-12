# -*- coding: utf-8 -*-
from time import time
import copy
import threading

import inventory
import storage


__config = {}
__config_lock = threading.Lock()


def setConfig(mastermind_config):
    global __config
    lconfig = {}
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
    with __config_lock:
        __config = lconfig


def getConfig():
    with __config_lock:
        return copy.copy(__config)


def setConfigValue(key, value):
    with __config_lock:
        __config[key] = value


def GroupSizeEquals(size):
    return lambda symm_group, size=size: len(symm_group.unitId()) == size


def GroupNamespaceEquals(namespace):
    return lambda symm_group, namespace=namespace: symm_group.namespace == namespace


def _and(*lambdas):
    def combined(x):
        for l in lambdas:
            if not l(x):
                return False
        return True

    return combined


class SymmGroup:
    def __init__(self, couple):
        self.couple = couple
        self.stat = self.couple.get_stat()
        self.status = self.couple.status

    def __str__(self):
        return '<SymmGroup object: couple=%s, stat=%s>' % (repr(self.couple), repr(self.stat))

    def realPutPerPeriod(self):
        return self.stat.write_rps

    def maxPutPerPeriod(self):
        return self.stat.max_write_rps

    def realGetPerPeriod(self):
        return self.stat.read_rps

    def maxGetPerPeriod(self):
        return self.stat.max_read_rps

    def freeSpaceInKb(self):
        if not self.stat:
            return 0
        return self.stat.free_space / 1024

    def freeSpaceRelative(self):
        return float(self.stat.free_space) / self.stat.total_space

    def unitId(self):
        return self.couple

    @property
    def namespace(self):
        return self.couple.namespace

    def inService(self):
        return False

    def writeEnable(self):
        return self.status == storage.Status.OK

    def isBad(self):
        # too_old_age = getConfig().get("dynamic_too_old_age", 120)
        # return self.status != storage.Status.OK or self.stat.ts < (time() - too_old_age)
        return self.status != storage.Status.OK

    def dataType(self):
        return composeDataType(str(self.couple))
