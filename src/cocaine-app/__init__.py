#!/usr/bin/python
# encoding: utf-8

from cocaine.worker import Worker
from cocaine.logger import Logger
from cocaine.service.services import Service

from time import sleep
from functools import wraps

import traceback
import sys
sys.path.append('/usr/lib')

import elliptics

import msgpack, json
import balancer
import balancelogicadapter
import node_info_updater

logging = Logger()

with open('/etc/elliptics/mastermind.conf', 'r') as config_file:
    config = json.load(config_file)

log = elliptics.Logger(str(config["dnet_log"]), config["dnet_log_mask"])
n = elliptics.Node(log)

for host in config["elliptics_nodes"]:
    try:
        logging.info("host: " + str(host))
        n.add_remote(str(host[0]), host[1])
    except Exception as e:
        logging.error("Error: " + str(e) + "\n" + traceback.format_exc())

meta_node = elliptics.Node(log)
for host in config["metadata"]["nodes"]:
    try:
        logging.info("host: " + str(host))
        meta_node.add_remote(str(host[0]), host[1])
    except Exception as e:
        logging.error("Error: " + str(e) + "\n" + traceback.format_exc())
meta_session = elliptics.Session(meta_node)
meta_session.add_groups(list(config["metadata"]["groups"]))

n.meta_session = meta_session

balancelogicadapter.setConfig(config["balancer_config"])

niu = node_info_updater.NodeInfoUpdater(logging, n)

W = Worker()

def register_handle(h):
    global W

    @wraps(h)
    def wrapper(request, response):
        data = yield request.read()
        data = msgpack.unpackb(data)
        msgpack.pack(h(data), response)

    W.on(h.__name__, wrapper)
    return wrapper

@register_handle
def balance(request):
    logging.info("Request: %s" % str(request))
    return balancer.balance(n, request)

@register_handle
def get_groups(request):
    return balancer.get_groups(n)

@register_handle
def get_symmetric_groups(request):
    return balancer.get_symmetric_groups(n)

@register_handle
def get_bad_groups(request):
    return balancer.get_bad_groups(n)

@register_handle
def get_empty_groups(request):
    return balancer.get_empty_groups(n)

@register_handle
def get_group_info(request):
    return balancer.get_group_info(n, request)

@register_handle
def couple_groups(request):
    return balancer.couple_groups(n, request)

@register_handle
def break_couple(request):
    return balancer.break_couple(n, request)

@register_handle
def repair_groups(request):
    return balancer.repair_groups(n, request)

@register_handle
def get_next_group_number(request):
    return balancer.get_get_next_group_number(n, request)

@register_handle
def get_dc_by_host(request):
    return balancer.get_dc_by_host(request)

@register_handle
def get_group_weights(request):
    return balancer.get_group_weights(n)
