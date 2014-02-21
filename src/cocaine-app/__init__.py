#!/usr/bin/python
# encoding: utf-8
from functools import wraps, partial
import sys
from time import sleep
import traceback
import types

from cocaine.worker import Worker
from cocaine.logging import Logger

sys.path.append('/usr/lib')

import json
import msgpack

import elliptics

import balancer
import balancelogicadapter
import infrastructure
import cache
import minions
import node_info_updater
import statistics
from config import config


logging = Logger()

i = iter(xrange(100))
logging.info("trace %d" % (i.next()))

logging.debug("config: %s" % str(config["elliptics_nodes"]))

logging.info("trace %d" % (i.next()))
log = elliptics.Logger(str(config["dnet_log"]), config["dnet_log_mask"])
n = elliptics.Node(log)

logging.info("trace %d" % (i.next()))
try:
    for host in config["elliptics_nodes"]:
        logging.debug("Adding node %s" % str(host))
        try:
            logging.info("host: " + str(host))
            n.add_remote(str(host[0]), host[1])
        except Exception as e:
            logging.error("Error: " + str(e) + "\n" + traceback.format_exc())
#except Exception as e:
except:
    #logging.error("1Error: " + str(e) + "\n" + traceback.format_exc())
    logging.info("trace error")

logging.info("trace %d" % (i.next()))
meta_node = elliptics.Node(log)
for host in config["metadata"]["nodes"]:
    try:
        logging.info("host: " + str(host))
        meta_node.add_remote(str(host[0]), host[1])
    except Exception as e:
        logging.error("Error: " + str(e) + "\n" + traceback.format_exc())

wait_timeout = config.get('wait_timeout', 5)
logging.info('sleeping for wait_timeout for nodes '
             'to collect data ({0} sec)'.format(wait_timeout))
sleep(wait_timeout)

meta_session = elliptics.Session(meta_node)
meta_session.set_timeout(wait_timeout)
meta_session.add_groups(list(config["metadata"]["groups"]))
logging.info("trace %d" % (i.next()))
n.meta_session = meta_session

balancelogicadapter.setConfig(config["balancer_config"])


logging.info("trace %d" % (i.next()))
logging.info("before creating worker")
W = Worker(disown_timeout=config.get('disown_timeout', 2))
logging.info("after creating worker")


b = balancer.Balancer(n)


def register_handle(h):
    @wraps(h)
    def wrapper(request, response):
        try:
            data = yield request.read()
            data = msgpack.unpackb(data)
            logging.info("Running handler for event %s, data=%s" % (h.__name__, str(data)))
            #msgpack.pack(h(data), response)
            response.write(h(data))
        except Exception as e:
            logging.error("Balancer error: %s" % traceback.format_exc().replace('\n', '    '))
            response.write({"Balancer error": str(e)})
        response.close()

    W.on(h.__name__, wrapper)
    logging.info("Registering handler for event %s" % h.__name__)
    return wrapper


def init_infrastructure():
    infstruct = infrastructure.infrastructure
    infstruct.init(n)
    register_handle(infstruct.restore_group_cmd)
    b.set_infrastructure(infstruct)


def init_node_info_updater():
    logging.info("trace node info updater %d" % (i.next()))
    niu = node_info_updater.NodeInfoUpdater(logging, n)
    register_handle(niu.force_nodes_update)

    return niu


def init_cache():
    manager = cache.CacheManager()
    if 'cache' in config:
        manager.setup(n.meta_session, config['cache'].get('index_prefix', 'cached_files_'))
        [manager.add_namespace(ns) for ns in config['cache'].get('namespaces', [])]

    # registering cache handlers
    register_handle(manager.get_cached_keys)
    register_handle(manager.get_cached_keys_by_group)
    register_handle(manager.upload_list)

    return manager


def init_statistics():
    stat = statistics.Statistics(b)
    register_handle(stat.get_flow_stats)
    return stat


def init_minions():
    m = minions.Minions(n)
    register_handle(m.get_command)
    register_handle(m.get_commands)
    register_handle(m.execute_cmd)
    register_handle(m.minion_history_log)
    return m


init_cache()
init_infrastructure()
init_node_info_updater()
init_statistics()
init_minions()

for handler in balancer.handlers(b):
    logging.info("registering bounded function %s" % handler)
    register_handle(handler)

logging.info("Starting worker")
W.run()
logging.info("Initialized")
