

class API_ERROR_CODE(object):

    LOCK = 20
    LOCK_FAILED = 21
    LOCK_ALREADY_ACQUIRED = 22
    LOCK_INCONSISTENT = 23


class LockError(Exception):

    PARAMS = ()

    def __init__(self, *args, **kwargs):
        super(LockError, self).__init__(*args)
        for param in self.PARAMS:
            setattr(self, param, kwargs.get(param))

    @property
    def code(self):
        return API_ERROR_CODE.LOCK

    def dump(self):
        res = {
            'msg': str(self),
            'code': self.code
        }
        for param in self.PARAMS:
            res[param] = getattr(self, param)
        return res

class LockFailedError(LockError):

    PARAMS = ('lock_id',)

    @property
    def code(self):
        return API_ERROR_CODE.LOCK_FAILED

    def __str__(self):
        return 'Failed to acquire lock {0}'.format(self.lock_id)


class LockAlreadyAcquiredError(LockFailedError):

    PARAMS = ('lock_id', 'holder_id', 'lock_ids', 'holders_ids')

    @property
    def code(self):
        return API_ERROR_CODE.LOCK_ALREADY_ACQUIRED


class InconsistentLockError(LockAlreadyAcquiredError):
    @property
    def code(self):
        return API_ERROR_CODE.LOCK_INCONSISTENT
