#!/usr/bin/env python
# coding=utf-8
from __future__ import absolute_import, unicode_literals

import logging
import signal
import sys
import time

import elliptics
from cocaine.worker import Worker

# storage should be imported before balancer
# TODO: remove this dependency
import storage
from balancer import Balancer
from couple_records import CoupleRecordFinder
from external_storage import ExternalStorageMeta
from helpers import register_handle_wne
from infrastructure import infrastructure
from jobs import JobFinder, JobProcessor
from log import setup_logger
from mastermind_core.config import (
    DNET_LOG, DNET_LOG_MASK, ELLIPTICS_NODES, ELLIPTICS_WAIT_TIMEOUT, IO_THREAD_NUM,
    METADATA_JOBS_DB, NET_THREAD_NUM, NON_BLOCKING_IO_THREAD_NUM
)
from mastermind_core.meta_db import meta_db
from namespaces import NamespacesSettings
from node_info_updater import NodeInfoUpdater
# TODO rename, shadow stdlib module https://docs.python.org/2/library/sched.html
from sched import Scheduler
from sched.defrag_starter import DefragStarter
from sched.recover_starter import RecoveryStarter
from sched.ttl_cleanup_starter import TtlCleanupStarter

logger = logging.getLogger('mm.scheduler_worker')


def init_elliptics_node():
    logger.info('config: %s', ELLIPTICS_NODES)

    log = elliptics.Logger(DNET_LOG, DNET_LOG_MASK)

    node_config = elliptics.Config()
    node_config.io_thread_num = IO_THREAD_NUM
    node_config.nonblocking_io_thread_num = NON_BLOCKING_IO_THREAD_NUM
    node_config.net_thread_num = NET_THREAD_NUM

    logger.info(
        'Node config: io_thread_num %d, nonblocking_io_thread_num %d, net_thread_num %d',
        node_config.io_thread_num,
        node_config.nonblocking_io_thread_num,
        node_config.net_thread_num
    )

    elliptics_node = elliptics.Node(log, node_config)

    addresses = []
    for node in ELLIPTICS_NODES:
        try:
            addresses.append(elliptics.Address(host=node[0], port=node[1], family=node[2]))
        except Exception:
            logger.error('Invalid node address: %s:%d:%d', node[0], node[1], node[2])
    try:
        elliptics_node.add_remotes(addresses)
    except Exception as e:
        logger.error('Failed to connect to any elliptics storage node: %s', e)
        raise ValueError('Failed to connect to any elliptics storage node')

    time.sleep(ELLIPTICS_WAIT_TIMEOUT)
    return elliptics_node


def init_namespaces_settings(meta_db):
    return NamespacesSettings(meta_db)


def init_job_finder(meta_db):
    if not METADATA_JOBS_DB:
        logger.error(
            'Job finder metadb is not set up ("metadata.jobs.db" key), will not be initialized'
        )
        return None
    return JobFinder(meta_db)


def init_infrastructure(elliptics_node, namespaces_settings):
    infrastructure.init(elliptics_node, None, None, namespaces_settings)
    return infrastructure


def init_node_info_updater(elliptics_node, job_finder, namespaces_settings):
    node_info_updater = NodeInfoUpdater(elliptics_node, job_finder, namespaces_settings)
    node_info_updater.start()
    return node_info_updater


def init_job_processor(job_finder, elliptics_node, meta_db, node_info_updater):
    if job_finder is None:
        logger.error('Job processor will not be initialized because job finder is not initialized')
        return None
    return JobProcessor(
        job_finder,
        elliptics_node,
        meta_db,
        node_info_updater,
        minions_monitor=None,
        external_storage_meta=ExternalStorageMeta(meta_db),
        couple_record_finder=CoupleRecordFinder(meta_db),
    )


def init_scheduler(meta_db, job_processor):
    scheduler = Scheduler(meta_db, job_processor)
    register_starters(scheduler)
    return scheduler


def register_starters(scheduler):
    DefragStarter(scheduler)
    RecoveryStarter(scheduler)
    TtlCleanupStarter(scheduler)
    # TODO add more starters when they done


# TODO args not needed but register_handle_wne pass param
def ping(*args):
    return 'Ok'


def init_balancer(elliptics_node, meta_db, namespaces_settings):
    return Balancer(elliptics_node, meta_db, namespaces_settings)


def init_scheduler_worker(worker, balancer):
    register_handle_wne(worker, ping)
    register_handle_wne(worker, balancer.get_group_info)


def main():
    setup_logger()

    def term_handler(signo, frame):
        # required to guarantee execution of cleanup functions registered with atexit.register
        sys.exit(0)

    signal.signal(signal.SIGTERM, term_handler)
    elliptics_node = init_elliptics_node()

    namespaces_settings = init_namespaces_settings(meta_db)
    job_finder = init_job_finder(meta_db)

    init_infrastructure(elliptics_node, namespaces_settings)
    node_info_updater = init_node_info_updater(elliptics_node, job_finder, namespaces_settings)

    job_processor = init_job_processor(job_finder, elliptics_node, meta_db, node_info_updater)
    logger.info('Before creating scheduler worker')
    worker = Worker()
    logger.info('After creating scheduler worker')
    init_scheduler(meta_db, job_processor)

    balancer = init_balancer(elliptics_node, meta_db, namespaces_settings)
    init_scheduler_worker(worker, balancer)

    logger.info('Starting scheduler worker')
    worker.run()
    logger.info('Scheduler worker stopped')


if __name__ == '__main__':
    main()
