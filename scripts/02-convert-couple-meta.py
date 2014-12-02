#!/usr/bin/env python
from copy import deepcopy
import datetime
import json
import logging
import msgpack
import sys
import time

import elliptics


logger = logging.getLogger('mm.convert')



CONFIG_PATH = '/etc/elliptics/mastermind.conf'

try:

    with open(CONFIG_PATH, 'r') as config_file:
        config = json.load(config_file)

except Exception as e:
    raise ValueError('Failed to load config file %s: %s' % (CONFIG_PATH, e))


log = elliptics.Logger('/tmp/ell-frozen-convert.log', config["dnet_log_mask"])
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

wait_timeout = config.get('elliptics', {}).get('wait_timeout', 5)
s = elliptics.Session(n)

print 'sleeping for wait timeout: {0} seconds'.format(wait_timeout)
time.sleep(wait_timeout)


def read_key_retry(s, key, retries):
    for i in xrange(retries):
        try:
            return s.read_data(s.transform(key)).get()[0].data
        except elliptics.TimeoutError:
            time.sleep(1)
        except elliptics.NotFoundError:
            return
    raise ValueError('Failed to read key from groups: {0}'.format(s.groups))


def write_key_retry(session, groups, key, data, retries):
    s = session.clone()
    s.add_groups(groups)
    s.set_checker(elliptics.checkers.all)

    data = msgpack.packb(data)

    for i in xrange(retries):
        try:
            res = s.write_data(key, data).get()
            return
        except Exception as e:
            logger.error('Failed to write key to groups: {0} ({1})'.format(
                groups, data))
            time.sleep(1)
    raise ValueError('Failed to write key to groups: {0} ({1})'.format(
        groups, data))


def parse_meta(meta):
    if meta is None:
        return

    parsed = msgpack.unpackb(meta)
    if isinstance(parsed, tuple):
        parsed_meta = {'version': 1, 'couple': parsed, 'namespace': 'default', 'frozen': False}
    elif isinstance(parsed, dict) and parsed['version'] == 2:
        parsed_meta = parsed
    else:
        raise Exception('Unable to parse meta')

    return parsed_meta

def find_couples(s):

    processed = 0

    processed_groups = set()
    processed_couples = set()
    bad_groups = []

    while True:
        print
        couples = set()
        groups = s.routes.groups()
        for g in groups:
            if g in processed_groups:
                continue

            s.add_groups([g])

            try:
                meta_key = read_key_retry(s, 'metabalancer\0symmetric_groups', 3)
            except ValueError:
                bad_groups.append(g)
            else:
                meta = parse_meta(meta_key)
                if meta and meta['couple'] not in processed_couples:
                    couples.add(meta['couple'])
                    processed += 1
                    print "Processed groups: {0}/{1}\r".format(processed, len(groups)),
                    processed_couples.add(meta['couple'])

            processed_groups.add(g)

        if not couples:
            break

        yield couples

    print
    print "Bad groups: {0}".format(bad_groups)


def parse_couple_meta(meta):
    if meta is None:
        return

    meta = msgpack.unpackb(meta)
    if meta['version'] == 1:
        parsed_meta = meta
    else:
        raise ValueError('Unable to parse couple meta')

    return parsed_meta

def process_couple(meta_session, couple, session):
    bad_couples = []

    s = session.clone()

    key = 'mastermind:couple_meta:%s' % ':'.join(map(str, couple))

    meta_key = read_key_retry(meta_session, key, 3)

    meta = parse_couple_meta(meta_key)
    if meta and meta['frozen']:
        print 'Couple {0} is frozen'.format(couple)

        s.add_groups([couple[0]])
        group_meta = parse_meta(read_key_retry(s, 'metabalancer\0symmetric_groups', 3))

        new_group_meta = deepcopy(group_meta)
        new_group_meta['version'] = 2
        new_group_meta['frozen'] = True

        print 'Couple {0}: replacing {1} with {2}'.format(
            couple, group_meta, new_group_meta)

        try:
            write_key_retry(s, couple, 'metabalancer\0symmetric_groups', new_group_meta, 3)
        except Exception as e:
            print 'Failed to process couple {0}: {1}'.format(couple, e)
            pass


def clean_old_couple_meta_key(meta_session, couple):

    key = 'mastermind:couple_meta:%s' % ':'.join(map(str, couple))

    try:
        meta_session.remove(key).get()
    except elliptics.NotFoundError:
        pass
    except Exception as e:
        print 'Failed to clean couple {0} key: {1}'.format(couple, e)
        pass

if __name__ == '__main__':

    if len(sys.argv) < 3 or sys.argv[1] not in ('convert', 'clean'):
        print "Usage: {0} convert|clean".format(sys.argv[0])
        sys.exit(1)

    if sys.argv[1] == 'convert':

        for couples in find_couples(s):
            for couple in couples:
                process_couple(meta_session, couple, s)

        print

    elif sys.argv[1] == 'clean':

        for couples in find_couples(s):
            for couple in couples:
                clean_old_couple_meta_key(meta_session, couple)

        print "Done"
