import logging

logger = logging.getLogger('mm.mongo')


class MongoObject(object):

    def __init__(self, *args, **kwargs):
        super(MongoObject, self).__init__(*args, **kwargs)
        self._dirty = False

    @classmethod
    def new(cls, *args, **kwargs):
        pass

    def save(self):
        if not self._dirty:
            logger.debug('Object with id {0} has no _dirty flag set'.format(self.id))
            return

        res = self.collection.update({'id': self.id}, self.dump(), upsert=True)
        if res['ok'] != 1:
            logger.error('Unexpected mongo response: {0}, saving object {1}'.format(res, self.dump()))
            raise RuntimeError('Mongo operation result: {0}'.format(res['ok']))
        self._dirty = False
