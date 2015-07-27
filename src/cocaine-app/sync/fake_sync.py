from contextlib import contextmanager
import logging
from threading import Lock

from sync.error import LockAlreadyAcquiredError


logger = logging.getLogger('mm.sync')


class SyncManager(object):

    def __init__(self, *args, **kwargs):
        self.locks = {}
        self.__locks_lock = Lock()

    @contextmanager
    def lock(self, lockid, blocking=True, timeout=None):
        """ Locks mastermind jobs list.
        This is just a demo implementation that provides locking among
        different threads of the same process, you should provide your
        own implemetation using locking primitives available in your
        infrastructure.
        """
        with self.__locks_lock:
            lock = self.locks.setdefault(lockid, Lock())
        with lock:
            yield

    def persistent_locks_acquire(self, locks, data=''):
        with self.__locks_lock:
            already_locked = []
            for lockid in locks:
                lock = self.locks.setdefault(lockid, Lock())
                if lock.locked():
                    already_locked.append(lockid)
            if already_locked:
                raise LockAlreadyAcquiredError(lock_id=already_locked[0],
                                               holder_id='',
                                               lock_ids=already_locked,
                                               holders_ids=[''])
            for lockid in locks:
                self.locks[lockid].acquire()
        return True

    def persistent_locks_release(self, locks, check=''):
        for lockid in locks:
            lock = self.locks.get(lockid)
            if lock and lock.locked():
                return lock.release()
            else:
                logger.warn('Persistent lock {0} is already removed'.format(lockid))

    def get_children_locks(self, lock_prefix):
        return [lock_id for lock_id in self.locks
                if lock_id.startswith(lock_prefix)]
