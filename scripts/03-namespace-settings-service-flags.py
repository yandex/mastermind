#!/usr/bin/env python
import json
import logging
import msgpack
from Queue import Queue
import sys
import time

import elliptics
import msgpack


logger = logging.getLogger('mm.convert')


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


def namespace_index(n):
    ns_settings_idx = \
        TagSecondaryIndex('mastermind:ns_settings_idx',
                          None,
                          'mastermind:ns_setttings:%s',
                          n.meta_session,
                          logger=logger,
                          namespace='namespaces')

    return ns_settings_idx


def namespace_settings(ns_settings_idx):
    ns_settings = {}

    start_ts = time.time()
    for data in ns_settings_idx:
        settings = msgpack.unpackb(data)
        logger.debug('fetched namespace settings for "{0}" '
            '({1:.4f}s)'.format(settings['namespace'], time.time() - start_ts))
        ns = settings['namespace']
        del settings['namespace']
        ns_settings[ns] = settings

    return ns_settings


def process_namespace(ns_settings_idx, namespace, settings):
    settings['namespace'] = namespace
    if not '__service' in settings:
        settings['__service'] = {}
        ns_settings_idx[namespace] = msgpack.packb(settings)


if __name__ == '__main__':

    if len(sys.argv) < 2 or sys.argv[1] not in ('convert',):
        print "Usage: {0} convert".format(sys.argv[0])
        sys.exit(1)

    if sys.argv[1] == 'convert':

        n = make_elliptics_node()
        ns_settings_idx = namespace_index(n)
        ns_settings = namespace_settings(ns_settings_idx)

        total_ns = len(ns_settings)
        processed = 0
        for ns, settings in ns_settings.iteritems():

            process_namespace(ns_settings_idx, ns, settings)
            processed += 1
            print "Processed namespaces: {0}/{1}\r".format(processed, total_ns),

        print
