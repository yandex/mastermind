import functools

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
    def items(self, src_storage=None, status=None, dcs=None, limit=None):
        params = {}
        if src_storage is not None:
            params['src_storage'] = src_storage
        if status is not None:
            params['status'] = status
        if dcs:
            params['dcs'] = {'$in': dcs}
        request = self.convert_queue.find(params)
        if limit:
            # pymongo uses 0 value to mean 'unlimited', so we cannot use None
            request = request.limit(limit)
        for data in request:
            item = ExternalStorageConvertQueueItem(**data)
            item.collection = self.convert_queue
            item._dirty = False
            yield item