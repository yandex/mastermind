import json
import logging

logger = logging.getLogger('mm.mongo')


class MongoObject(object):

    def __init__(self, *args, **kwargs):
        super(MongoObject, self).__init__(*args, **kwargs)
        self._dirty = False

    @classmethod
    def new(cls, *args, **kwargs):
        pass

    def dump(self):
        assert self.MODEL, 'Model for type {0} is not defined'.format(type(self))
        return self.MODEL.dump(self)

    def save(self):
        if not self._dirty:
            logger.debug('Object with id {0} has no _dirty flag set'.format(self.id))
            return

        res = self.collection.update({'id': self.id}, self.dump(), upsert=True)
        if res['ok'] != 1:
            logger.error('Unexpected mongo response: {0}, saving object {1}'.format(res, self.dump()))
            raise RuntimeError('Mongo operation result: {0}'.format(res['ok']))
        self._dirty = False

    @staticmethod
    def updated_since(collection, update_ts):
        params = {}
        if update_ts:
            params['update_ts'] = {'$gte': int(update_ts)}
        logger.debug('Collection updated_since request')
        return collection.find(params)
