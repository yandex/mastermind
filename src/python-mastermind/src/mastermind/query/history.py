import copy
from datetime import datetime

from mastermind.query import Query


DT_FORMAT = '%Y-%m-%d %H:%M:%S'


class GroupHistory(object):
    def __init__(self, group_id, couples=None, nodes=None):
        self.group_id = group_id
        self.couples = [CoupleHistoryRecord(c) for c in couples or []]
        self.nodes = [NodeBackendSetHistoryRecord(c) for c in nodes or []]


class CoupleHistoryRecord(object):
    def __init__(self, data):
        self.couple = data['couple']
        self.timestamp = data['timestamp']

    def __str__(self):
        return '[{}] {}'.format(datetime.fromtimestamp(self.timestamp).strftime(DT_FORMAT),
                                self.couple)

    def __repr__(self):
        return '<{}: {}>'.format(type(self).__name__, str(self))


class NodeBackendSetHistoryRecord(object):
    def __init__(self, data):
        self.set = [NodeBackendHistoryRecord(**ns) for ns in data['set']]
        self.timestamp = data['timestamp']
        self.type = data['type']

    def __str__(self):
        return '[{}] ({})'.format(datetime.fromtimestamp(self.timestamp).strftime(DT_FORMAT),
                                  ','.join(str(hr) for hr in self.set))

    def __repr__(self):
        return '<{}: {}>'.format(type(self).__name__, str(self))


class NodeBackendHistoryRecord(object):
    def __init__(self, hostname, port, family, backend_id, path):
        self.hostname = hostname
        self.port = port
        self.family = family
        self.backend_id = backend_id
        self.path = path

    def __str__(self):
        return '{hostname}:{port}:{family}/{backend_id} {path}'.format(
            hostname=self.hostname,
            port=self.port,
            family=self.family,
            backend_id=self.backend_id,
            path=self.path)

    def __repr__(self):
        return '<{}: {}>'.format(type(self).__name__, str(self))


class GroupHistoriesQuery(Query):
    def __init__(self, client, filter=None):
        super(GroupHistoriesQuery, self).__init__(client)
        self._filter = filter or {}

    def __iter__(self):
        histories = self.client.request('get_group_histories_list', {'filter': self._filter})
        for history_data in histories:
            gh = GroupHistory(
                history_data['group_id'],
                couples=history_data['couples'],
                nodes=history_data['nodes'],
            )
            yield gh

    def filter(self, **kwargs):
        """Filter group histories list.

        Keyword args:
          group_ids:
            get history for groups in @group_ids list

        Returns:
          New group histories query object with selected filter parameters.
        """
        updated_filter = copy.copy(self._filter)
        if 'group_ids' in kwargs:
            updated_filter['group_ids'] = kwargs['group_ids']
        return GroupHistoriesQuery(self.client, filter=updated_filter)
