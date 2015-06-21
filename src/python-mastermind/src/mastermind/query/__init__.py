import functools

import mastermind.client


class Query(object):
    def __init__(self, client):
        self.client = client or mastermind.client.DummyClient()

    @classmethod
    def _object(cls, object_or_id, client):
        if isinstance(object_or_id, cls):
            return object_or_id
        return cls(object_or_id, client)

    @staticmethod
    def not_idempotent(method):
        @functools.wraps(method)
        def wrapper(self, *args, **kwargs):
            if 'attempts' not in kwargs:
                kwargs['attempts'] = 1
            return method(self, *args, **kwargs)
        return wrapper

    @classmethod
    def from_data(cls, data, client):
        obj = cls(cls._raw_id(data), client)
        obj._set_raw_data(data)
        return obj


class LazyDataObject(object):

    @staticmethod
    def _lazy_load(method):
        @functools.wraps(method)
        def wrapper(self, *args, **kwargs):
            if not hasattr(self, '_data'):
                self._set_raw_data(self._fetch_data())
            return method(self, *args, **kwargs)
        return wrapper

    def _set_raw_data(self, data):
        data = self._preprocess_raw_data(data)
        self._data = data

    def _expire(self):
        if hasattr(self, '_data'):
            delattr(self, '_data')

    def _fetch_data(self):
        raise NotImplemented()

    def _raw_id(self):
        raise NotImplemented()

    def _preprocess_raw_data(self, data):
        return data

    def serialize(self):
        return self._data
