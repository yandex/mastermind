#!/usr/bin/env python
import json
import sys
import time

import elliptics
import msgpack
import pymongo

from elliptics_indices import TagSecondaryIndex


CONFIG_PATH = '/etc/elliptics/mastermind.conf'

try:

    with open(CONFIG_PATH, 'r') as config_file:
        config = json.load(config_file)

except Exception as e:
    raise ValueError('Failed to load config file %s: %s' % (CONFIG_PATH, e))


def get_mongo_client():
    if not config.get('metadata', {}).get('url', ''):
        raise ValueError('Mongo db url is not set')
    return pymongo.mongo_replica_set_client.MongoReplicaSetClient(config['metadata']['url'])


def get_meta_session():
    log = elliptics.Logger('/tmp/ell-ns-settings-convert.log', config["dnet_log_mask"])
    node_config = elliptics.Config()
    meta_node = elliptics.Node(log, node_config)

    nodes = config['metadata']['nodes']

    addresses = [
        elliptics.Address(host=host, port=port, family=family)
        for (host, port, family) in nodes
    ]
    print 'Connecting to meta nodes: {0}'.format(nodes)

    try:
        meta_node.add_remotes(addresses)
    except Exception:
        raise ValueError('Failed to connect to any elliptics storage META node')

    meta_session = elliptics.Session(meta_node)

    meta_wait_timeout = config['metadata'].get('wait_timeout', 5)
    meta_session.set_timeout(meta_wait_timeout)
    meta_session.add_groups(list(config["metadata"]["groups"]))

    time.sleep(meta_wait_timeout)

    return meta_session


def get_ns_settings(session):
    return TagSecondaryIndex(
        main_idx='mastermind:ns_settings_idx',
        idx_tpl=None,
        key_tpl='mastermind:ns_setttings:%s',
        meta_session=session,
        namespace='namespaces',
    )


def create_collection(db, coll_name):
    try:
        db.create_collection(coll_name)
    except pymongo.errors.CollectionInvalid:
        raise RuntimeError('Collection coll_name already exists')


def create_indexes(coll):
    coll.ensure_index([
        ('namespace', pymongo.ASCENDING),
    ])


def get_namespace_settings(session):
    ns_settings_idx = get_ns_settings(session)
    for data in ns_settings_idx:
        yield msgpack.unpackb(data)


def convert(session, coll):
    for settings in get_namespace_settings(session):
        print 'Converting settings for namespace "' + settings['namespace'] + '"'
        print settings
        print
        coll.update(
            spec={'namespace': settings['namespace']},
            document=settings,
            upsert=True,
        )


if __name__ == '__main__':

    if len(sys.argv) < 2 or sys.argv[1] not in ('create', 'indexes', 'convert'):
        print "Usage: {0} (create|indexes|convert)".format(sys.argv[0])
        sys.exit(1)

    coll_name = 'settings'

    db_name = config.get('metadata', {}).get('namespaces', {}).get('db', '')
    if not db_name:
        print 'Namespaces database name is not found in config'
        sys.exit(1)

    if sys.argv[1] == 'create':
        mc = get_mongo_client()

        create_collection(mc[db_name], coll_name)
        coll = mc[db_name][coll_name]
        create_indexes(coll)

        print 'Successfully created collection {} and its indexes'.format(coll_name)

    elif sys.argv[1] == 'indexes':
        mc = get_mongo_client()

        coll = mc[db_name][coll_name]
        create_indexes(coll)

        print 'Successfully created indexes for collection {}'.format(coll_name)

    elif sys.argv[1] == 'convert':
        mc = get_mongo_client()
        session = get_meta_session()

        coll = mc[db_name][coll_name]
        convert(session, coll)
