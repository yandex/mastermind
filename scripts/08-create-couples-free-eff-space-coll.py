#!/usr/bin/env python
import datetime
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
        raise RuntimeError('Collection coll_name already exists')


def create_indexes(coll):
    coll.create_index([
        ('namespace', pymongo.ASCENDING),
        ('ts', pymongo.ASCENDING),
    ])


def check_period(coll):
    period = config.get('metadata', {}).get('statistics', {}).get('collect_period', 300)
    for ns in coll.distinct('namespace'):
        ts = None
        for rec in coll.find({'namespace': ns}, sort=[('ts', pymongo.ASCENDING)]):
            if ts:
                diff_ts = rec['ts'] - ts

                if diff_ts < period * 0.9:
                    print (
                        'Ns {}: record detected at {}, previous was at {}, '
                        'diff is {}s (too fast)'.format(
                            ns,
                            datetime.datetime.fromtimestamp(rec['ts']),
                            datetime.datetime.fromtimestamp(ts),
                            int(diff_ts)
                        )
                    )
                elif diff_ts > period * 1.1:
                    print (
                        'Ns {}: record detected at {}, previous was at {}, '
                        'diff is {}s (too slow)'.format(
                            ns,
                            datetime.datetime.fromtimestamp(rec['ts']),
                            datetime.datetime.fromtimestamp(ts),
                            int(diff_ts)
                        )
                    )
            ts = rec['ts']


if __name__ == '__main__':

    if len(sys.argv) < 2 or sys.argv[1] not in ('create', 'check'):
        print "Usage: {0} create".format(sys.argv[0])
        sys.exit(1)

    coll_name = 'couple_free_effective_space'

    if sys.argv[1] == 'create':
        mc = get_mongo_client()
        db_name = config.get('metadata', {}).get('statistics', {}).get('db', '')
        if not db_name:
            print 'Statistics database name is not found in config'
            sys.exit(1)

        create_collection(mc[db_name], coll_name)
        coll = mc[db_name][coll_name]

        create_indexes(coll)

        print 'Successfully created collection {} and its indexes'.format(coll_name)

    elif sys.argv[1] == 'check':
        mc = get_mongo_client()

        db_name = config.get('metadata', {}).get('statistics', {}).get('db', '')
        if not db_name:
            print 'Statistics database name is not found in config'
            sys.exit(1)

        check_period(mc[db_name][coll_name])

        print
