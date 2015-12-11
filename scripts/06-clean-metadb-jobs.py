#!/usr/bin/env python
import datetime
import json
import logging
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


def make_meta_session():
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

    time.sleep(5)

    return meta_session


def process_jobs(s):

    s.set_namespace('jobs')
    s.set_timeout(600)

    indexes = ['mastermind:jobs_idx']

    dt = datetime.datetime(2014, 01, 01)
    while dt < datetime.datetime.now():
        indexes.append('mastermind:jobs_idx:%s' % dt.strftime('%Y-%m'))
        if dt.month == 12:
            dt = dt.replace(year=dt.year + 1, month=1)
        else:
            dt = dt.replace(month=dt.month + 1)

    for index in indexes:
        print index
        res = s.find_all_indexes([index]).get()
        print "Index {0}, keys {1}".format(index, len(res))
        if not len(res):
            continue
        try:
            s.remove_index(res[0].indexes[0].index, True).get()
        except Exception as e:
            print "Failed to clean index {0}: {1}".format(index, e)

if __name__ == '__main__':

    if len(sys.argv) < 2 or sys.argv[1] not in ('clean',):
        print "Usage: {0} clean".format(sys.argv[0])
        sys.exit(1)

    if sys.argv[1] == 'clean':

        s = make_meta_session()
        process_jobs(s)

        print
