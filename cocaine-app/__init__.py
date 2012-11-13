# encoding: utf-8

from cocaine.decorators import timer, zeromq
from cocaine.context import Log, manifest

from time import sleep

import traceback
import sys
sys.path.append('/usr/lib')

import elliptics

import msgpack
import balancer

logging = Log()

log = elliptics.Logger(manifest()["dnet_log"], manifest()["dnet_log_mask"])
n = elliptics.Node(log)

for host in manifest()["elliptics_nodes"]:
    try:
        logging.error("host: " + str(host))
        n.add_remote(host[0], host[1])
    except Exception as e:
        logging.error("Error: " + str(e) + "\n" + traceback.format_exc())

meta_node = elliptics.Node(log)
for host in manifest()["metadata"]["nodes"]:
    try:
        logging.error("host: " + str(host))
        meta_node.add_remote(host[0], host[1])
    except Exception as e:
        logging.error("Error: " + str(e) + "\n" + traceback.format_exc())
meta_session = elliptics.Session(meta_node)
meta_session.add_groups(list(manifest()["metadata"]["groups"]))

n.meta_session = meta_session

'''
def calc_rating(node):
    node['rating'] = node['free_space_rel'] * 1000 + (node['la'] + 0.1) * 100

def parse(raw_node):
    ret = dict()

    ret['group_id'] = raw_node["group_id"]
    ret['addr'] = raw_node['addr']

    bsize = raw_node['counters']['DNET_CNTR_BSIZE'][0]
    avail = raw_node['counters']['DNET_CNTR_BAVAIL'][0]
    total = raw_node['counters']['DNET_CNTR_BLOCKS'][0]

    ret['free_space_rel'] = float(avail) / total;
    ret['free_space_abs'] = float(avail) / 1024 / 1024 / 1024 * bsize

    ret['la'] = float(raw_node['counters']['DNET_CNTR_LA15'][0]) / 100

    return ret
'''

@timer
def aggregate():
    balancer.aggregate(n)

@timer
def collect():
    if "symmetric_groups" in manifest() and manifest()["symmetric_groups"]:
        balancer.collect(n)

@zeromq
def balance(request):
    logging.info("Request: %s" % str(request))
    return balancer.balance(n, request)

@zeromq
def get_groups(request):
    return list(set(balancer.get_groups(n).values()))

@zeromq
def get_symmetric_groups(request):
    return balancer.get_symmetric_groups(n)

@zeromq
def get_bad_groups(request):
    return balancer.get_bad_groups(n)

@zeromq
def get_empty_groups(request):
    return balancer.get_empty_groups(n)

@zeromq
def get_group_info(request):
    return balancer.get_group_info(n, request)

@zeromq
def couple_groups(request):
    return balancer.couple_groups(n, request)

@zeromq
def repair_groups(request):
    return balancer.repair_groups(n, request)

@zeromq
def get_next_group_number(request):
    return balancer.get_get_next_group_number(n, request)

@zeromq
def get_dc_by_host(request):
    return balancer.get_dc_by_host(request)

