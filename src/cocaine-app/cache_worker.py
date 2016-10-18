#!/usr/bin/env python
import logging
import sys
import signal
import time

# NB: pool should be initialized before importing
# any of cocaine-framework-python modules to avoid
# tornado ioloop dispatcher issues
import monitor_pool

from cocaine.asio.exceptions import LocatorResolveError
from cocaine.worker import Worker
import elliptics

import log

try:
    log.setup_logger('mm_cache_logging')
    logger = logging.getLogger('mm.init')
except LocatorResolveError:
    log.setup_logger()
    logger = logging.getLogger('mm.init')
    logger.warn('mm_cache_logging is not set up properly in '
        'cocaine.conf, fallback to default logging service')

import storage
import cache
import external_storage
import infrastructure
import infrastructure_cache
import jobs
import couple_records
import node_info_updater
from mastermind_core.config import config
from mastermind_core.db.mongo.pool import MongoReplicaSetClient
import helpers as h
from namespaces import NamespacesSettings


def init_elliptics_node():
    nodes = config.get('elliptics', {}).get('nodes', []) or config["elliptics_nodes"]
    logger.debug("config: %s" % str(nodes))

    log = elliptics.Logger(str(config["dnet_log"]), config["dnet_log_mask"])

    node_config = elliptics.Config()
    node_config.io_thread_num = config.get('io_thread_num', 1)
    node_config.nonblocking_io_thread_num = config.get('nonblocking_io_thread_num', 1)
    node_config.net_thread_num = config.get('net_thread_num', 1)

    logger.info('Node config: io_thread_num {0}, nonblocking_io_thread_num {1}, '
                'net_thread_num {2}'.format(
                    node_config.io_thread_num, node_config.nonblocking_io_thread_num,
                    node_config.net_thread_num))

    n = elliptics.Node(log, node_config)

    addresses = []
    for node in nodes:
        try:
            addresses.append(elliptics.Address(
                host=str(node[0]), port=node[1], family=node[2]))
        except Exception as e:
            logger.error('Failed to connect to storage node: {0}:{1}:{2}'.format(
                node[0], node[1], node[2]))
            pass

    try:
        n.add_remotes(addresses)
    except Exception as e:
        logger.error('Failed to connect to any elliptics storage node: {0}'.format(
            e))
        raise ValueError('Failed to connect to any elliptics storage node')

    meta_node = elliptics.Node(log, node_config)

    addresses = []
    for node in config["metadata"]["nodes"]:
        try:
            addresses.append(elliptics.Address(
                host=str(node[0]), port=node[1], family=node[2]))
        except Exception as e:
            logger.error('Failed to connect to meta node: {0}:{1}:{2}'.format(
                node[0], node[1], node[2]))
            pass

    logger.info('Connecting to meta nodes: {0}'.format(config["metadata"]["nodes"]))

    try:
        meta_node.add_remotes(addresses)
    except Exception as e:
        logger.error('Failed to connect to any elliptics meta storage node: {0}'.format(
            e))
        raise ValueError('Failed to connect to any elliptics storage META node')

    meta_wait_timeout = config['metadata'].get('wait_timeout', 5)

    meta_session = elliptics.Session(meta_node)
    meta_session.set_timeout(meta_wait_timeout)
    meta_session.add_groups(list(config["metadata"]["groups"]))
    n.meta_session = meta_session

    wait_timeout = config.get('elliptics', {}).get('wait_timeout', 5)
    time.sleep(wait_timeout)

    return n


def init_meta_db():
    meta_db = None

    mrsc_options = config['metadata'].get('options', {})

    if config['metadata'].get('url'):
        meta_db = MongoReplicaSetClient(config['metadata']['url'], **mrsc_options)
    return meta_db


def init_namespaces_settings(meta_db):
    namespaces_settings = NamespacesSettings(meta_db)
    return namespaces_settings


def init_infrastructure_cache_manager(W, n):
    icm = infrastructure_cache.InfrastructureCacheManager()
    return icm


def init_node_info_updater(n, namespaces_settings):
    return node_info_updater.NodeInfoUpdater(n, None, namespaces_settings)


def init_infrastructure(W, n, namespaces_settings):
    infstruct = infrastructure.infrastructure
    infstruct.init(n, None, None, namespaces_settings)
    return infstruct


def init_cache_worker(W, n, niu, j, meta_db, namespaces_settings):
    if not config.get("cache"):
        logger.error('Cache is not set up in config ("cache" key), '
                     'will not be initialized')
        return None
    if not config.get('metadata', {}).get('cache', {}).get('db'):
        logger.error('Cache metadata db is not set up ("metadata.cache.db" key), '
                     'will not be initialized')
        return None
    c = cache.CacheManager(n, niu, j, meta_db, namespaces_settings)
    h.register_handle(W, c.get_top_keys)
    h.register_handle(W, c.cache_statistics)
    h.register_handle(W, c.cache_clean)
    h.register_handle(W, c.cache_groups)
    h.register_handle(W, c.get_cached_keys)
    h.register_handle(W, c.update_cache_key_upload_status)
    h.register_handle(W, c.update_cache_key_removal_status)

    return c


def init_job_finder(meta_db):
    if not config['metadata'].get('jobs', {}).get('db'):
        logger.error('Job finder metadb is not set up '
                     '("metadata.jobs.db" key), will not be initialized')
        return None
    jf = jobs.JobFinder(meta_db)
    return jf


def init_job_processor(jf, meta_db, niu):
    if jf is None:
        logger.error('Job processor will not be initialized because '
                     'job finder is not initialized')
        return None
    j = jobs.JobProcessor(
        jf,
        n,
        meta_db,
        niu,
        minions_monitor=None,
        external_storage_meta=external_storage.ExternalStorageMeta(meta_db),
        couple_record_finder=couple_records.CoupleRecordFinder(meta_db),
    )
    return j


if __name__ == '__main__':

    def term_handler(signo, frame):
        # required to guarantee execution of cleanup functions registered
        # with atexit.register
        sys.exit(0)

    signal.signal(signal.SIGTERM, term_handler)

    n = init_elliptics_node()

    logger.info("before creating worker")
    W = Worker(disown_timeout=config.get('disown_timeout', 2))
    logger.info("after creating worker")

    meta_db = init_meta_db()
    if meta_db is None:
        s = 'Meta db should be configured in "metadata" config section'
        logger.error(s)
        raise RuntimeError(s)

    namespaces_settings = init_namespaces_settings(meta_db)

    i = init_infrastructure(W, n, namespaces_settings)
    icm = init_infrastructure_cache_manager(W, n)

    niu = init_node_info_updater(n, namespaces_settings)
    jf = init_job_finder(meta_db)
    j = init_job_processor(jf, meta_db, niu)

    c = init_cache_worker(W, n, niu, j, meta_db, namespaces_settings)

    icm._start_tq()
    c and c._start_tq()

    logger.info("Starting cache worker")
    W.run()
    logger.info("Cache worker initialized")
