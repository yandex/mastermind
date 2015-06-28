from datetime import datetime


DT_FORMAT = '%Y-%m-%d %H:%M:%S'


class GroupHistory(object):
    def __init__(self, couples=None, nodes=None):
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
        self.set = [NodeBackendHistoryRecord(ns) for ns in data['set']]
        self.timestamp = data['timestamp']
        self.type = data['type']

    def __str__(self):
        return '[{}] ({})'.format(datetime.fromtimestamp(self.timestamp).strftime(DT_FORMAT),
                                  ','.join(str(hr) for hr in self.set))

    def __repr__(self):
        return '<{}: {}>'.format(type(self).__name__, str(self))


class NodeBackendHistoryRecord(object):
    def __init__(self, data):
        self.addr = data[0]
        self.port = data[1]
        self.family = data[2]
        self.backend_id = data[3]
        self.base_path = data[4]

    def __str__(self):
        return '{host}:{port}:{family}/{backend_id} {path}'.format(
            host=self.addr,
            port=self.port,
            family=self.family,
            backend_id=self.backend_id,
            path=self.base_path)

    def __repr__(self):
        return '<{}: {}>'.format(type(self).__name__, str(self))
