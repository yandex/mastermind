import functools


class Query(object):
    def __init__(self, client):
        self.client = client

    @classmethod
    def _object(cls, client, object_or_id):
        if isinstance(object_or_id, cls):
            return object_or_id
        return cls(client, object_or_id)

    @staticmethod
    def not_idempotent(method):
        @functools.wraps(method)
        def wrapper(self, *args, **kwargs):
            if 'attempts' not in kwargs:
                kwargs['attempts'] = 1
            return method(self, *args, **kwargs)
        return wrapper


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
        if 'Error' in data:
            raise RuntimeError(data['Error'])
        if 'Balancer error' in data:
            raise RuntimeError(data['Balancer error'])
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
