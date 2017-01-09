import functools

import pymongo

# import helpers
from mastermind_core.config import config
from mastermind_core.db.mongo import MongoObject
from mastermind_core.db.mongo.pool import Collection


class ExternalStorageConvertQueueItem(MongoObject):

    ID = 'id'

    SRC_STORAGE = 'src_storage'
    SRC_STORAGE_OPTIONS = 'src_storage_options'
    DCS = 'dcs'
    STATUS = 'status'
    COUPLES = 'couples'
    NAMESPACE = 'namespace'
    GROUPSET = 'groupset'
    JOB_ID = 'job_id'
    DETERMINE_DATA_SIZE = 'determine_data_size'
    PRIORITY = 'priority'

    PRIMARY_ID_KEY = ID

    def __init__(self, **init_params):
        super(ExternalStorageConvertQueueItem, self).__init__()

        self.id = init_params[self.ID]
        self.src_storage = init_params[self.SRC_STORAGE]
        self.src_storage_options = init_params[self.SRC_STORAGE_OPTIONS]
        self.dcs = init_params[self.DCS]
        self.status = init_params[self.STATUS]
        self.couples = init_params[self.COUPLES]
        self.namespace = init_params[self.NAMESPACE]
        self.groupset = init_params[self.GROUPSET]
        self.job_id = init_params[self.JOB_ID]
        self.determine_data_size = init_params[self.DETERMINE_DATA_SIZE]
        self.priority = init_params[self.PRIORITY]

    @classmethod
    def new(cls, **kwargs):
        # TODO: move this to base class @new method?
        mapping = cls(**kwargs)
        mapping._dirty = True
        return mapping

    def dump(self):
        return {
            self.ID: self.id,
            self.SRC_STORAGE: self.src_storage,
            self.SRC_STORAGE_OPTIONS: self.src_storage_options,
            self.DCS: self.dcs,
            self.STATUS: self.status,
            self.COUPLES: self.couples,
            self.NAMESPACE: self.namespace,
            self.GROUPSET: self.groupset,
            self.JOB_ID: self.job_id,
            self.DETERMINE_DATA_SIZE: self.determine_data_size,
            self.PRIORITY: self.priority,
        }


class ExternalStorageConvertQueue(object):

    STATUS_QUEUED = 'queued'
    STATUS_CONVERTING = 'converting'
    STATUS_COMPLETED = 'completed'

    def __init__(self, db):
        self.convert_queue = None
        if db:
            self.convert_queue = Collection(
                db[config['metadata']['external_storage']['db']],
                'convert_queue'
            )

    def _check_coll(method):
        @functools.wraps(method)
        def wrapper(self, *args, **kwargs):
            if self.convert_queue is None:
                raise RuntimeError('External storage database is not set up')
            return method(self, *args, **kwargs)

        return wrapper

    @_check_coll
    def items(self,
              ids=None,
              src_storage=None,
              status=None,
              dcs=None,
              limit=None,
              skip=None,
              sort_by_priority=None):

        params = {}
        if ids is not None:
            params['id'] = {'$in': ids}
        if src_storage is not None:
            params['src_storage'] = src_storage
        if status is not None:
            params['status'] = status
        if dcs:
            params['dcs'] = {'$in': dcs}

        request_options = {}
        if sort_by_priority:
            request_options['sort'] = [
                (ExternalStorageConvertQueueItem.PRIORITY, pymongo.DESCENDING),
            ]

        if limit:
            # pymongo uses 0 value to mean 'unlimited', so we cannot use None
            request_options['limit'] = limit
        if skip:
            request_options['skip'] = skip
        request = self.convert_queue.find(params, **request_options)
        for data in request:
            item = ExternalStorageConvertQueueItem(**data)
            item.collection = self.convert_queue
            item._dirty = False
            yield item
