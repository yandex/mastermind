#!/usr/bin/env python
import datetime
import json
import sys
import time

import elliptics
import msgpack
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


def get_meta_session():
    log = elliptics.Logger('/tmp/ell-commands-convert.log', config["dnet_log_mask"])
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


def create_collection(db, coll_name):
    try:
        db.create_collection(coll_name)
    except pymongo.errors.CollectionInvalid:
        raise RuntimeError('Collection coll_name already exists')


def create_indexes(coll):
    coll.ensure_index([
        ('uid', pymongo.ASCENDING),
    ])

    coll.ensure_index([
        ('exit_code', pymongo.ASCENDING),
    ])


MINION_HISTORY_KEY = 'minion_cmd_log:%s'


def get_commands(session):

    index_months = []
    cur_data = datetime.date.today()

    # converting commands that were executed during the last 3 months
    for _ in xrange(3):
        index_months.append(cur_data.strftime('%Y-%m'))
        cur_data = cur_data.replace(day=1) - datetime.timedelta(days=1)

    for month in index_months:
        for res in session.find_all_indexes([MINION_HISTORY_KEY % month]):
            last_err = None
            for _ in xrange(3):
                try:
                    command = session.clone().list_indexes(res.id).get()[0].data
                except elliptics.Error as e:
                    last_err = e
                    time.sleep(5)
                    continue
                break
            else:
                raise RuntimeError('Failed to convert command: {}'.format(last_err))
            yield msgpack.unpackb(command)


def convert(session, coll):
    for command in get_commands(session):
        print 'Converting command "' + command['uid'] + '"'
        print

        if 'output' in command:
            del command['output']
        if 'error_output' in command:
            del command['error_output']

        coll.update(
            spec={'uid': command['uid']},
            document=command,
            upsert=True,
        )


if __name__ == '__main__':

    if len(sys.argv) < 2 or sys.argv[1] not in ('create', 'indexes', 'convert'):
        print "Usage: {0} (create|indexes|convert)".format(sys.argv[0])
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

    elif sys.argv[1] == 'convert':
        mc = get_mongo_client()
        session = get_meta_session()

        coll = mc[db_name][coll_name]
        convert(session, coll)
