#!/usr/bin/python
# encoding: utf-8
from functools import wraps, partial
import logging
import sys
from time import sleep
import traceback
import types
import uuid

from cocaine.worker import Worker

sys.path.append('/usr/lib')

import json
import msgpack

import elliptics

import log
import balancer
import balancelogicadapter
import infrastructure
import jobs
import cache
import minions
import node_info_updater
from planner import Planner
from config import config


logger = logging.getLogger('mm.init')

i = iter(xrange(100))
logger.info("trace %d" % (i.next()))

nodes = config.get('elliptics', {}).get('nodes', []) or config["elliptics_nodes"]
logger.debug("config: %s" % str(nodes))

logger.info("trace %d" % (i.next()))
log = elliptics.Logger(str(config["dnet_log"]), config["dnet_log_mask"])

node_config = elliptics.Config()
node_config.io_thread_num = config.get('io_thread_num', 1)
node_config.nonblocking_io_thread_num = config.get('nonblocking_io_thread_num', 1)
node_config.net_thread_num = config.get('net_thread_num', 1)

logger.info('Node config: io_thread_num {0}, nonblocking_io_thread_num {1}, '
    'net_thread_num {2}'.format(node_config.io_thread_num, node_config.nonblocking_io_thread_num,
        node_config.net_thread_num))

n = elliptics.Node(log, node_config)

logger.info("trace %d" % (i.next()))

addresses = [elliptics.Address(host=str(node[0]), port=node[1], family=node[2])
             for node in nodes]

try:
    n.add_remotes(addresses)
except Exception as e:
    logger.error('Failed to connect to any elliptics storage node: {0}'.format(
        e))
    raise ValueError('Failed to connect to any elliptics storage node')

logger.info("trace %d" % (i.next()))
meta_node = elliptics.Node(log, node_config)

addresses = [elliptics.Address(host=str(node[0]), port=node[1], family=node[2])
             for node in config["metadata"]["nodes"]]
logger.info('Connecting to meta nodes: {0}'.format(config["metadata"]["nodes"]))

try:
    meta_node.add_remotes(addresses)
except Exception as e:
    logger.error('Failed to connect to any elliptics meta storage node: {0}'.format(
        e))
    raise ValueError('Failed to connect to any elliptics storage META node')


wait_timeout = config.get('wait_timeout', 5)
logger.info('sleeping for wait_timeout for nodes '
             'to collect data ({0} sec)'.format(wait_timeout))
sleep(wait_timeout)

meta_wait_timeout = config['metadata'].get('wait_timeout', 5)

meta_session = elliptics.Session(meta_node)
meta_session.set_timeout(meta_wait_timeout)
meta_session.add_groups(list(config["metadata"]["groups"]))
logger.info("trace %d" % (i.next()))
n.meta_session = meta_session

balancelogicadapter.setConfig(config["balancer_config"])


logger.info("trace %d" % (i.next()))
logger.info("before creating worker")
W = Worker(disown_timeout=config.get('disown_timeout', 2))
logger.info("after creating worker")


b = balancer.Balancer(n)


def register_handle(h):
    @wraps(h)
    def wrapper(request, response):
        req_uid = uuid.uuid4().hex
        try:
            data = yield request.read()
            data = msgpack.unpackb(data)
            logger.info(":{req_uid}: Running handler for event {0}, "
                "data={1}".format(h.__name__, str(data), req_uid=req_uid))
            #msgpack.pack(h(data), response)
            response.write(h(data))
        except Exception as e:
            logger.error(":{req_uid}: handler for event {0}, "
                "data={1}: Balancer error: {2}".format(
                    h.__name__, str(data),
                    traceback.format_exc().replace('\n', '    '),
                    req_uid=req_uid))
            response.write({"Balancer error": str(e)})
        finally:
            logger.info(':{req_uid}: Finished handler for event {0}, '
                'data={1}'.format(h.__name__, str(data), req_uid=req_uid))
        response.close()

    W.on(h.__name__, wrapper)
    logger.info("Registering handler for event %s" % h.__name__)
    return wrapper


def init_infrastructure():
    infstruct = infrastructure.infrastructure
    infstruct.init(n)
    register_handle(infstruct.shutdown_node_cmd)
    register_handle(infstruct.start_node_cmd)
    register_handle(infstruct.disable_node_backend_cmd)
    register_handle(infstruct.enable_node_backend_cmd)
    register_handle(infstruct.reconfigure_node_cmd)
    register_handle(infstruct.recover_group_cmd)
    register_handle(infstruct.defrag_node_backend_cmd)
    register_handle(infstruct.search_history_by_path)
    b.set_infrastructure(infstruct)


def init_node_info_updater():
    logger.info("trace node info updater %d" % (i.next()))
    niu = node_info_updater.NodeInfoUpdater(n)
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
    register_handle(b.statistics.get_flow_stats)
    register_handle(b.statistics.get_groups_tree)
    register_handle(b.statistics.get_couple_statistics)
    return b.statistics


def init_minions():
    m = minions.Minions(n)
    register_handle(m.get_command)
    register_handle(m.get_commands)
    register_handle(m.execute_cmd)
    register_handle(m.minion_history_log)
    return m


def init_planner(job_processor):
    planner = Planner(n.meta_session, job_processor)
    register_handle(planner.restore_group)


def init_job_processor(minions):
    j = jobs.JobProcessor(n, minions)
    register_handle(j.create_job)
    register_handle(j.cancel_job)
    register_handle(j.approve_job)
    register_handle(j.get_job_list)
    register_handle(j.get_job_status)
    register_handle(j.retry_failed_job_task)
    register_handle(j.skip_failed_job_task)
    return j


init_cache()
init_infrastructure()
b.niu = init_node_info_updater()
init_statistics()
m = init_minions()
j = init_job_processor(m)
init_planner(j)


for handler in balancer.handlers(b):
    logger.info("registering bounded function %s" % handler)
    register_handle(handler)

logger.info("Starting worker")
W.run()
logger.info("Initialized")
