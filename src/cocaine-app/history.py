import logging

from mastermind_core.config import config
from mastermind_core.db.mongo import MongoObject
from mastermind_core.db.mongo.pool import Collection


logger = logging.getLogger('mm.infrastructure')


class GroupHistory(MongoObject):
    """
    Group's complete history

    History objects can provide two types of changes that were recorded by mastermind:
        - group's affiliation with a couple;
        - node backends that were a part of group at any time.

    Every state record contains timestamp and record type which represents a reason
    why the state was changed so that process of group evolution could be easily traced.

    Attributes:
        group_id - id of a group to which history belongs;
        couples - iterable of GroupCoupleRecord which represents the points in time
            when group changed its affiliation with a couple;
        nodes - iterable of GroupNodeBackendsSetRecord which represents changes
            of underlying node backends set.
    """

    GROUP_ID = 'group_id'
    COUPLES = 'couples'
    NODES = 'nodes'

    PRIMARY_ID_KEY = GROUP_ID

    def __init__(self, **init_params):
        super(GroupHistory, self).__init__()

        self.group_id = init_params.get(self.GROUP_ID)
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
        group_history = cls(**kwargs)
        group_history._dirty = True
        return group_history

    def dump(self):
        return {
            'group_id': self.group_id,
            'couples': [cr.dump() for cr in self.couples],
            'nodes': [nbsr.dump() for nbsr in self.nodes],
        }


class GroupStateRecord(object):
    """
    Base class for any group history state record

    Every state record should contain following attributes:
        timestamp - timestamp of record creation;
        type - one of the following:
            * automatic:
                such records are created by periodic tasks inside mastermind worker
                when it decides that most recent group record does not
                correspond to group's current state;
            * manual:
                manual records are created with one-time API calls, e.g. via mastermind
                util, to make deliberate changes to group history record;
            * job:
                job records are created by jobs mechanism to deliberately change group
                history record.

    """
    HISTORY_RECORD_AUTOMATIC = 'automatic'
    HISTORY_RECORD_MANUAL = 'manual'
    HISTORY_RECORD_JOB = 'job'

    def __init__(self, timestamp=None, type=HISTORY_RECORD_AUTOMATIC):
        self.timestamp = timestamp
        self.type = type


class GroupNodeBackendsSet(list):
    """
    List of unique GroupNodeBackendRecord objects

    Used as a set for GroupNodeBackendsSetRecord.
    """
    def __init__(self, *args, **kwargs):
        super(GroupNodeBackendsSet, self).__init__(*args, **kwargs)
        if len(self) != len(set(self)):
            raise ValueError('Node backends set should contain only unique elements')

    def append(self, item):
        if item in self:
            raise ValueError('Node backend {} is already in node backends set'.format(item))
        super(GroupNodeBackendsSet, self).append(item)

    def extend(self, items):
        for item in items:
            self.append(item)

    def insert(self, idx, item):
        if item in self:
            raise ValueError('Node backend {} is already in node backends set'.format(item))
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


class GroupNodeBackendsSetRecord(GroupStateRecord):
    """
    Record that represents group's node backend set recorded at certain time

    Attributes:
        set - iterable of GroupNodeBackendRecord objects
        timestamp - timestamp of record creation
        type - type of the record, GroupStateRecord.HISTORY_*
    """
    def __init__(self, set, timestamp=None, type=GroupStateRecord.HISTORY_RECORD_AUTOMATIC):
        super(GroupNodeBackendsSetRecord, self).__init__(timestamp, type)
        self.set = GroupNodeBackendsSet(
            nbr if isinstance(nbr, GroupNodeBackendRecord) else GroupNodeBackendRecord(**nbr)
            for nbr in set
        )

    def __ne__(self, other):
        return not self == other

    def __eq__(self, other):
        return set(nbr for nbr in self.set) == set(nbr for nbr in other.set)

    def dump(self):
        return {
            'set': [nbr.dump() for nbr in self.set],
            'timestamp': self.timestamp,
            'type': self.type,
        }

    def __repr__(self):
        return '<GroupNodeBackendsSetRecord: set {set}, timestamp {ts}, type {type}>'.format(
            set=[repr(nbr) for nbr in self.set],
            ts=self.timestamp,
            type=self.type
        )


class GroupNodeBackendRecord(object):
    """
    Represents node backend that was noted as a part of a group at a certain time

    A set of such objects is united under a single GroupNodeBackendsSet object,
    which takes part in GroupNodeBackendsSetRecord along with record's timestamp and type.
    """
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


class GroupCoupleRecord(GroupStateRecord):
    """
    Record that represents group's affiliation with couple

    Attributes:
        couple - iterable of integer group ids
        timestamp - timestamp of record creation
        type - type of the record, GroupStateRecord.HISTORY_*
    """
    def __init__(self, couple, timestamp=None, type=GroupStateRecord.HISTORY_RECORD_AUTOMATIC):
        super(GroupCoupleRecord, self).__init__(timestamp, type)
        self.couple = couple

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
        return '<GroupCoupleRecord: couple {couple}, timestamp {ts}, type {type}>'.format(
            couple=self.couple,
            ts=self.timestamp,
            type=self.type
        )


class GroupHistoryNotFoundError(Exception):
    pass


class GroupHistoryFinder(object):

    def __init__(self, db):
        self.collection = Collection(db[config['metadata']['history']['db']], 'history')

    def _get_group_history(self, group_id):
        group_history_list = (
            self.collection
            .list(group_id=group_id)
            .limit(1)
        )
        try:
            group_history = GroupHistory(**group_history_list[0])
        except IndexError:
            raise GroupHistoryNotFoundError(
                'Group history for group {} is not found'.format(group_id)
            )
        group_history.collection = self.collection
        return group_history

    def group_history(self, group_id):
        try:
            return self._get_group_history(group_id)
        except GroupHistoryNotFoundError:
            return None
        except Exception:
            logger.exception('History finder failed')
            raise

    def group_histories(self, group_ids=None):
        for gh_data in self.collection.list(group_id=group_ids):
            try:
                gh = GroupHistory(**gh_data)
            except Exception:
                logger.exception('Failed to parse group history object: {}'.format(gh_data['_id']))
                continue

            gh.collection = self.collection
            gh._dirty = False
            yield gh

    def search_by_group_ids(self, group_ids):
        group_history_records = self.collection.find({'group_id': {'$in': group_ids}})
        for group_history_record in group_history_records:
            gh = GroupHistory(**group_history_record)
            gh.collection = self.collection
            yield gh

    def search_by_node_backend(self,
                               hostname=None,
                               port=None,
                               family=None,
                               backend_id=None,
                               path=None):
        node_backend_pattern = {}
        if hostname:
            node_backend_pattern['hostname'] = hostname
        if port:
            node_backend_pattern['port'] = port
        if family:
            node_backend_pattern['family'] = family
        if backend_id:
            node_backend_pattern['backend_id'] = backend_id
        if path:
            if not path.startswith('^'):
                path = '^' + path
            if not path.endswith('$'):
                path = path + '$'
            path = path.replace('*', '.*')
            node_backend_pattern['path'] = {'$regex': path}

        if not node_backend_pattern:
            raise ValueError('Filter parameters for node backends should be supplied')

        group_histories_records = self.collection.find({
            'nodes.set': {
                '$elemMatch': node_backend_pattern
            }
        })

        ghs = []
        for group_history_record in group_histories_records:
            gh = GroupHistory(**group_history_record)
            gh.collection = self.collection
            ghs.append(gh)
        return ghs

    def search_by_history_record(self,
                                 start_ts=None,
                                 finish_ts=None,
                                 type=None):
        record_pattern = {}
        if type:
            record_pattern.update(Collection.condition('type', type))
        if start_ts or finish_ts:
            timestamp_pattern = {}
            if start_ts:
                timestamp_pattern['$gte'] = start_ts
            if finish_ts:
                timestamp_pattern['$lt'] = finish_ts
            record_pattern['timestamp'] = timestamp_pattern
        or_pattern = {'$or': [
            {
                'couples': {
                    '$elemMatch': record_pattern,
                }
            },
            {
                'nodes': {
                    '$elemMatch': record_pattern,
                }
            },
        ]}

        ghs = []
        for group_history_record in self.collection.find(or_pattern):
            gh = GroupHistory(**group_history_record)
            gh.collection = self.collection
            ghs.append(gh)
        return ghs
