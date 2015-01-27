#!/usr/bin/env python
import datetime
import json
import logging
import msgpack
from Queue import Queue
import sys
import time

import elliptics
import msgpack

import pymongo
from pymongo.mongo_replica_set_client import MongoReplicaSetClient


logger = logging.getLogger('mm.convert')
logger.addHandler(logging.FileHandler('/tmp/04-move_jobs_to_mongodb.log'))


CONFIG_PATH = '/etc/elliptics/mastermind.conf'

try:

    with open(CONFIG_PATH, 'r') as config_file:
        config = json.load(config_file)

except Exception as e:
    raise ValueError('Failed to load config file %s: %s' % (CONFIG_PATH, e))



class TagSecondaryIndex(object):

    BATCH_SIZE = 500

    def __init__(self, main_idx, idx_tpl, key_tpl, meta_session, logger=None, namespace=None, batch_size=BATCH_SIZE):
        self.main_idx = main_idx
        self.idx_tpl = idx_tpl
        self.key_tpl = key_tpl
        self.meta_session = meta_session.clone()
        if namespace:
            self.meta_session.set_namespace(namespace)
        self.batch_size = batch_size
        self.logger = logger

    def __iter__(self):
        idxes = [idx.id for idx in
            self.meta_session.clone().find_all_indexes([self.main_idx]).get()]

        for data in self._iter_keys(idxes):
            yield data

    def tagged(self, tag):
        idxes = [idx.id for idx in
            self.meta_session.clone().find_all_indexes([self.main_idx, self.idx_tpl % tag])]

        for data in self._iter_keys(idxes):
            yield data

    def __setitem__(self, key, val):
        eid = self.meta_session.transform(self.key_tpl % key)
        self.meta_session.clone().write_data(eid, val)

    def __getitem__(self, key):
        eid = self.meta_session.transform(self.key_tpl % key)
        return self.meta_session.clone().read_latest(eid).get()[0].data

    def set_tag(self, key, tag=None):
        eid = self.meta_session.transform(self.key_tpl % key)
        tags = [self.main_idx]
        if tag:
            tags.append(self.idx_tpl % tag)
        self.meta_session.clone().set_indexes(eid, tags, [''] * len(tags))

    def _fetch_response_data(self, req):
        data = None
        try:
            result = req[1]
            result.wait()
            data = result.get()[0].data
        except Exception as e:
            self.logger.error('Failed to fetch record from tagged index: {0}'.format(req[0]))
        return data

    def _iter_keys(self, keys):
        if not keys:
            return

        q = Queue(self.batch_size)
        count = 0

        s = self.meta_session.clone()

        for k in keys:
            if not q.full():
                q.put((k, s.read_latest(k)))
            else:
                data = self._fetch_response_data(q.get())
                q.put((k, s.read_latest(k)))
                if data:
                    yield data

        while q.qsize():
            data = self._fetch_response_data(q.get())
            if data:
                yield data



def make_elliptics_node():
    log = elliptics.Logger('/tmp/ell-namespace-convert.log', config["dnet_log_mask"])
    node_config = elliptics.Config()
    meta_node = elliptics.Node(log, node_config)

    addresses = [elliptics.Address(host=str(node[0]), port=node[1], family=node[2])
                 for node in config["metadata"]["nodes"]]
    logger.info('Connecting to meta nodes: {0}'.format(config["metadata"]["nodes"]))
    meta_wait_timeout = config['metadata'].get('wait_timeout', 5)

    try:
        meta_node.add_remotes(addresses)
    except Exception as e:
        logger.error('Failed to connect to any elliptics meta storage node: {0}'.format(
            e))
        raise ValueError('Failed to connect to any elliptics storage META node')

    meta_session = elliptics.Session(meta_node)
    meta_session.set_timeout(meta_wait_timeout)
    meta_session.add_groups(list(config["metadata"]["groups"]))


    nodes = config.get('elliptics', {}).get('nodes', []) or config["elliptics_nodes"]
    logger.debug("config elliptics nodes: %s" % str(nodes))

    node_config = elliptics.Config()
    node_config.io_thread_num = config.get('io_thread_num', 1)
    node_config.nonblocking_io_thread_num = config.get('nonblocking_io_thread_num', 1)
    node_config.net_thread_num = config.get('net_thread_num', 1)

    logger.info('Node config: io_thread_num {0}, nonblocking_io_thread_num {1}, '
        'net_thread_num {2}'.format(node_config.io_thread_num, node_config.nonblocking_io_thread_num,
            node_config.net_thread_num))

    n = elliptics.Node(log, node_config)

    addresses = [elliptics.Address(host=str(node[0]), port=node[1], family=node[2])
                 for node in nodes]

    try:
        n.add_remotes(addresses)
    except Exception as e:
        logger.error('Failed to connect to any elliptics storage node: {0}'.format(
            e))
        raise ValueError('Failed to connect to any elliptics storage node')

    n.meta_session = meta_session
    wait_timeout = config.get('elliptics', {}).get('wait_timeout', 5)
    s = elliptics.Session(n)
    s.set_timeout(wait_timeout)

    print 'sleeping for wait timeout: {0} seconds'.format(wait_timeout)
    time.sleep(wait_timeout)

    return n


def jobs_index(n):
    return TagSecondaryIndex(
            'mastermind:jobs_idx',
            'mastermind:jobs_idx:%s',
            'mastermind:jobs_%s',
            n.meta_session,
            logger=logger,
            namespace='jobs')


def add_months(dt, months):
    month = dt.month - 1 + months
    year = dt.year + month / 12
    month = month % 12 + 1
    return datetime.date(year, month, dt.day)


def get_collection():
    mrsc_options = config['metadata'].get('options', {})
    mrsc_options['read_preference'] = pymongo.ReadPreference.PRIMARY
    meta_db = MongoReplicaSetClient(config['metadata']['url'], **mrsc_options)
    return meta_db[config['metadata']['jobs']['db']]['jobs']

if __name__ == '__main__':

    if len(sys.argv) < 2 or sys.argv[1] not in ('convert', 'check'):
        print "Usage: {0} convert".format(sys.argv[0])
        sys.exit(1)

    dt = datetime.date(year=2014, month=5, day=1)
    finish_dt = datetime.date(year=2015, month=2, day=1)
    n = make_elliptics_node()
    jobs_idx = jobs_index(n)
    collection = get_collection()

    if sys.argv[1] == 'convert':
        total_count = 0
        total_added = 0
        jobs_ids_set = set()

        while dt <= finish_dt:
            dt = add_months(dt, 1)
            tag = dt.strftime('%Y-%m')
            count = 0
            added_count = 0
            for job_data in jobs_idx.tagged(tag):
                job = json.loads(job_data)
                jobs_ids_set.add(job['id'])
                count += 1
                total_count += 1
                # print "Processed namespaces: {0}/{1}\r".format(processed, total_ns),
                if job.get('create_ts') is None and job.get('start_ts') is None and job.get('finish_ts') is None:
                    continue

                if not job.get('create_ts'):
                    job['create_ts'] = job.get('start_ts') or job.get('finish_ts')
                if not job.get('start_ts') and job['status'] not in ('not_approved', 'new'):
                    job['start_ts'] = job.get('create_ts')
                if not job.get('finish_ts') and job['status'] in ('completed', 'cancelled'):
                    job['finish_ts'] = job.get('start_ts')

                for i in xrange(3):
                    try:
                        if not collection.find_one({'id': job['id']}):
                            added_count += 1
                            total_added += 1
                            collection.insert(job, upsert=True)
                        break
                    except Exception as e:
                        pass
                else:
                    print "\nfailed to write key: {0}: {1}\n".format(job['id'], job_data)

                print "\rtag: {0}, count: {1}, total in mongo: {2}, added this time: {3}".format(
                    tag, count, total_count, added_count),
            print

        print "Total count: {0}, set count: {1}, total added: {2}".format(
            total_count, len(jobs_ids_set), total_added)
        print

    elif sys.argv[1] == 'check':

        total_count = 0
        jobs_ids_set = set()
        added_count = 0

        job_ = None

        while dt <= finish_dt:
            dt = add_months(dt, 1)
            tag = dt.strftime('%Y-%m')
            count = 0
            for job_data in jobs_idx.tagged(tag):
                job = json.loads(job_data)
                jobs_ids_set.add(job['id'])
                count += 1
                total_count += 1
                if job.get('create_ts') is None and job.get('start_ts') is None and job.get('finish_ts') is None:
                    continue
                if not collection.find_one({'id': job['id']}):
                    added_count += 1
                print "\rtag: {0}, count: {1}".format(tag, count),
                job_ = job_data
            print

        print "Total count: {0}, to add {1}".format(total_count, added_count)

        # print job_

