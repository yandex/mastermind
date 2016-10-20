#!/usr/bin/env python
import json
import sys

import pymongo


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


def create_collection(db, coll_name):
    try:
        db.create_collection(coll_name)
    except pymongo.errors.CollectionInvalid:
        raise RuntimeError('Collection {} already exists'.format(coll_name))


def create_indexes(coll):
    coll.ensure_index([
        ('max_group', pymongo.DESCENDING),
    ])


if __name__ == '__main__':

    if len(sys.argv) < 2 or sys.argv[1] not in ('create', 'indexes',):
        print "Usage: {0} (create|indexes)".format(sys.argv[0])
        sys.exit(1)

    coll_name = 'commands'

    db_name = config.get('metadata', {}).get('minions', {}).get('db', '')
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
