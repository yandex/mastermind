#!/usr/bin/env python
import json
import logging
import socket
import sys
import time

import elliptics
import msgpack
import pymongo


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

    nodes = config['metadata']['nodes']

    addresses = [
        elliptics.Address(host=host, port=port, family=family)
        for (host, port, family) in nodes
    ]
    logger.info('Connecting to meta nodes: {0}'.format(nodes))

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


def get_mongo_client():
    mongo_url = config.get('metadata', {}).get('url', '')
    if not mongo_url:
        raise ValueError('Mongo db url is not set')
    return pymongo.mongo_replica_set_client.MongoReplicaSetClient(mongo_url)


def create_collection(db, coll_name):
    try:
        db.create_collection(coll_name)
    except pymongo.errors.CollectionInvalid:
        raise ValueError('Collection {} already exists'.format(coll_name))


def create_indexes(coll):
    coll.ensure_index([
        ('group_id', pymongo.ASCENDING)
    ])
    coll.ensure_index(
        [
            ('nodes.set.hostname', pymongo.ASCENDING),
            ('nodes.set.port', pymongo.ASCENDING),
            ('nodes.set.family', pymongo.ASCENDING),
            ('nodes.set.backend_id', pymongo.ASCENDING),
            ('nodes.set.path', pymongo.ASCENDING),
        ],
        name='hostname_1.port_1.family_1.backend_id_1.path_1'
    )
    coll.ensure_index([
        ('nodes.timestamp', pymongo.ASCENDING),
        ('nodes.type', pymongo.ASCENDING),
    ])
    coll.ensure_index([
        ('couples.timestamp', pymongo.ASCENDING),
        ('couples.type', pymongo.ASCENDING),
    ])


class GroupHistory(object):

    GROUP_ID = 'group_id'
    COUPLES = 'couples'
    NODES = 'nodes'

    PRIMARY_ID_KEY = GROUP_ID

    PARAMS = (GROUP_ID, COUPLES, NODES)

    def __init__(self, **init_params):
        super(GroupHistory, self).__init__()

        self.group_id = init_params.get(self.GROUP_ID, None)
        self.couples = [
            GroupCoupleRecord(**record)
            for record in init_params.get(self.COUPLES, [])
        ]
        self.nodes = [
            GroupNodeBackendsSetRecord(**record)
            for record in init_params.get(self.NODES, [])
        ]

    @property
    def id(self):
        return self.group_id

    @classmethod
    def new(cls, **kwargs):
        super(GroupHistory, cls).new(**kwargs)
        group_history = cls(**kwargs)
        group_history._dirty = True
        return group_history

    def dump(self):
        return {
            'group_id': self.group_id,
            'couples': [cr.dump() for cr in self.couples],
            'nodes': [nbrs.dump() for nbrs in self.nodes],
        }

    @staticmethod
    def list(collection, **kwargs):
        """
        TODO: This should be moved to some kind of Mongo Session object
        """
        def condition(field_name, field_val):
            if isinstance(field_val, (list, tuple)):
                return {field_name: {'$in': list(field_val)}}
            else:
                return {field_name: field_val}

        params = {}

        for k, v in kwargs.iteritems():
            if v is None:
                continue
            params.update(condition(k, v))

        return collection.find(params)


class GroupNodeBackendsSet(list):
    def __init__(self, *args, **kwargs):
        super(GroupNodeBackendsSet, self).__init__(*args, **kwargs)
        if len(self) != len(set(self)):
            raise ValueError('Node backends set should contain only unique elements')

    def append(self, item):
        if item in self:
            raise ValueError('Node backend {} is already in node backends set')
        super(GroupNodeBackendsSet, self).append(item)

    def extend(self, items):
        for item in items:
            self.append(item)

    def insert(self, idx, item):
        if item in self:
            raise ValueError('Node backend {} is already in node backends set')
        super(GroupNodeBackendsSet, self).insert(idx, item)

    def __setitem__(self, idx, item):
        raise NotImplementedError('GroupNodeBackends set does not support __setitem__ method')

    def __setslice__(self, i, j, items):
        raise NotImplementedError('GroupNodeBackends set does not support __setslice__ method')

    def __getslice__(self, i, j):
        return GroupNodeBackendsSet(super(GroupNodeBackendsSet, self).__getslice__(i, j))

    def __add__(self, other):
        obj = GroupNodeBackendsSet()
        for nb in self:
            obj.append(nb)
        for nb in other:
            obj.append(nb)
        return obj


class GroupNodeBackendsSetRecord(object):
    # TODO: Move constats to history module
    def __init__(self, set, timestamp=None, type='automatic'):
        self.set = GroupNodeBackendsSet(
            nbr if isinstance(nbr, GroupNodeBackendRecord) else GroupNodeBackendRecord(**nbr)
            for nbr in set
        )
        self.timestamp = timestamp
        self.type = type

    def __ne__(self, other):
        return not self == other

    def __eq__(self, other):
        def make_cast(s):
            cast = {}
            for nbr in s.set:
                cast.setdefault(nbr, 0)
                cast[nbr] += 1
            return cast
        c1 = make_cast(self)
        c2 = make_cast(other)
        return c1 == c2

    def dump(self):
        return {
            'set': [nbr.dump() for nbr in self.set],
            'timestamp': self.timestamp,
            'type': self.type,
        }

    def __repr__(self):
        return '<GroupNodeBackendsSetRecord: set {}, timestamp {}, type {}>'.format(
            [repr(nbr) for nbr in self.set],
            self.timestamp,
            self.type
        )


class GroupNodeBackendRecord(object):
    def __init__(self, hostname, port, family, backend_id, path):
        self.hostname = hostname
        self.port = port
        self.family = family
        self.backend_id = backend_id
        self.path = path

    def __hash__(self):
        return hash((self.hostname, self.port, self.family, self.backend_id, self.path))

    def __eq__(self, other):
        return (self.hostname == other.hostname and
                self.port == other.port and
                self.family == other.family and
                self.backend_id == other.backend_id and
                self.path == other.path)

    def dump(self):
        return {
            'hostname': self.hostname,
            'port': self.port,
            'family': self.family,
            'backend_id': self.backend_id,
            'path': self.path,
        }

    def __repr__(self):
        return (
            '<GroupNodeBackendRecord: '
            'hostname={hostname}, '
            'port={port}, '
            'family={family}, '
            'backend_id={backend_id}, '
            'path={path}>'.format(
                hostname=self.hostname,
                port=self.port,
                family=self.family,
                backend_id=self.backend_id,
                path=self.path
            )
        )


class GroupCoupleRecord(object):
    # TODO: Move constats to history module
    def __init__(self, couple, timestamp=None, type='automatic'):
        self.couple = couple
        self.timestamp = timestamp
        self.type = type

    def __ne__(self, other):
        return not self == other

    def __eq__(self, other):
        return set(self.couple) == set(other.couple)

    def dump(self):
        return {
            'couple': self.couple,
            'type': self.type,
            'timestamp': self.timestamp,
        }

    def __repr__(self):
        return '<GroupCoupleRecord: couple {}, timestamp {}, type {}>'.format(
            self.couple,
            self.timestamp,
            self.type
        )


def migrate_history(s, coll):

    ip_hostname_cache = {}

    def ip_to_hostname(ip_address):
        if ip_address in ip_hostname_cache:
            return ip_hostname_cache[ip_address]

        try:
            res = socket.gethostbyaddr(ip_address)[0]
        except Exception as e:
            print 'Failed to resolve {}: {}'.format(ip_address, e)
            return ip_address

        ip_hostname_cache[ip_address] = res
        return res

    def _unserialize(data):
            group_state = msgpack.unpackb(data)
            group_state['nodes'] = list(group_state['nodes'])
            group_state['couples'] = list(group_state.get('couples', []))
            return group_state

    def _node_state_type(node_state):
        explicit_type = node_state.get('type')
        if explicit_type:
            return explicit_type
        is_manual = node_state.get('manual', False)
        if is_manual:
            return 'manual'
        return 'automatic'

    def make_couple_records(old_group_history):
        couple_records = []
        for old_couple_record in old_group_history['couples']:
            new_couple_record = GroupCoupleRecord(
                couple=old_couple_record['couple'],
                timestamp=old_couple_record['timestamp'],
                type=_node_state_type(old_couple_record)
            )
            if not couple_records or couple_records[-1] != new_couple_record:
                couple_records.append(new_couple_record)

        return couple_records

    def make_node_backends_records(old_group_history):
        node_backends_records = []
        for old_nbs_record in old_group_history['nodes']:

            processed_nbs = set()
            nbs_set = GroupNodeBackendsSet()
            for nb in old_nbs_record['set']:
                ip_address, port, family, backend_id, path = nb
                if path is None:
                    continue
                gnbr = GroupNodeBackendRecord(
                    hostname=ip_to_hostname(ip_address),
                    port=port,
                    family=family,
                    backend_id=backend_id,
                    path=path
                )
                if gnbr not in processed_nbs:
                    nbs_set.append(gnbr)
                    processed_nbs.add(gnbr)

            new_nbs_record = GroupNodeBackendsSetRecord(
                set=nbs_set,
                timestamp=old_nbs_record['timestamp'],
                type=_node_state_type(old_nbs_record)
            )
            if not node_backends_records or node_backends_records[-1] != new_nbs_record:
                node_backends_records.append(new_nbs_record)

        return node_backends_records

    for idx in s.find_all_indexes(['mastermind:groups_idx']):
        old_group_history = _unserialize(idx.indexes[0].data)

        gh = GroupHistory(group_id=old_group_history['id'])
        for node_backend_record in make_node_backends_records(old_group_history):
            gh.nodes.append(node_backend_record)
        for couple_record in make_couple_records(old_group_history):
            gh.couples.append(couple_record)

        coll.update({'group_id': old_group_history['id']}, gh.dump(), upsert=True)


if __name__ == '__main__':

    if len(sys.argv) < 2 or sys.argv[1] not in ('create', 'indexes', 'convert'):
        print "Usage: {0} create".format(sys.argv[0])
        sys.exit(1)

    coll_name = 'history'

    db_name = config.get('metadata', {}).get('history', {}).get('db', '')
    if not db_name:
        print 'History database name is not found in config'
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

        s = make_meta_session()
        mc = get_mongo_client()

        coll = mc[db_name][coll_name]
        migrate_history(s, coll)
