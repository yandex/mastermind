from contextlib import contextmanager
import logging
import threading
from time import sleep
import traceback

from kazoo.client import KazooClient
from kazoo.exceptions import (
    SessionExpiredError,
    NodeExistsError,
    NoNodeError,
    KazooException,
    ZookeeperError,
)
from kazoo.retry import KazooRetry, RetryFailedError

# from queue import FilteredLockingQueue
# from errors import ConnectionError, InvalidDataError
from lock import Lock
from log import _handler
from sync.error import LockError, LockFailedError, LockAlreadyAcquiredError, InconsistentLockError


logger = logging.getLogger('mm.sync')

kazoo_logger = logging.getLogger('kazoo')
kazoo_logger.propagate = False
kazoo_logger.addHandler(_handler)
kazoo_logger.setLevel(logging.INFO)


class ZkSyncManager(object):

    RETRIES = 2

    def __init__(self, host='127.0.0.1:2181', lock_path_prefix='/mastermind/locks/'):
        self.client = KazooClient(host, timeout=3)
        logger.info('Connecting to zookeeper host {0}, '
            'lock_path_prefix: {1}'.format(host, lock_path_prefix))
        try:
            self.client.start()
        except Exception as e:
            logger.error(e)
            raise
        self.locks = {}
        self.__locks_lock = threading.Lock()

        self._retry = KazooRetry(max_tries=self.RETRIES)

        self.lock_path_prefix = lock_path_prefix

    @contextmanager
    def lock(self, lockid):
        with self.__locks_lock:
            lock = self.locks.setdefault(lockid, Lock(self.client, self.lock_path_prefix + lockid))
        with lock:
            yield

    def persistent_locks_acquire(self, locks, data=''):
        try:
            retry = self._retry.copy()
            result = retry(self._inner_persistent_locks_acquire, locks=locks, data=data)
        except RetryFailedError:
            raise LockError
        except KazooException as e:
            logger.error('Failed to fetch persistent locks {0}: {1}\n{2}'.format(
                locks, e, traceback.format_exc()))
            raise LockError
        return result

    def _inner_persistent_locks_acquire(self, locks, data):

        ensured_paths = set()

        tr = self.client.transaction()
        for lockid in locks:
            parts = lockid.rsplit('/', 1)
            if len(parts) == 2 and parts[0] not in ensured_paths:
                self.client.ensure_path(parts[0])
                ensured_paths.add(parts[0])
            tr.create(self.lock_path_prefix + lockid, data)

        failed = False
        failed_lock = None
        result = tr.commit()
        for i, res in enumerate(result):
            if isinstance(res, ZookeeperError):
                failed = True
            if isinstance(res, NodeExistsError):
                failed_lock = locks[i]

        if failed_lock:
            data = self.client.get(self.lock_path_prefix + failed_lock)
            logger.warn('Persistent lock {0} is already set by {1}'.format(failed_lock, data[0]))
            raise LockAlreadyAcquiredError(
                'Lock for {0} is already acquired by job {1}'.format(failed_lock, data[0]),
                lock_id=failed_lock, holder_id=data[0])
        elif failed:
            logger.error('Failed to set persistent locks {0}, result: {1}'.format(
                locks, result))
            raise LockError

        return True

    def persistent_locks_release(self, locks, check=''):
        try:
            retry = self._retry.copy()
            result = retry(self.__inner_persistent_locks_release, locks=locks, check=check)
        except RetryFailedError:
            raise LockError
        except KazooException as e:
            logger.error('Failed to remove persistent locks {0}: {1}\n{2}'.format(
                locks, e, traceback.format_exc()))
            raise LockError
        return result

    def __inner_persistent_locks_release(self, locks, check):
        for lockid in locks:
            try:
                if check:
                    data = self.client.get(self.lock_path_prefix + lockid)
                    if data[0] != check:
                        logger.error('Lock {0} has inconsistent data: {1}, '
                            'expected {2}'.format(lockid, data[0], check))
                        raise InconsistentLockError(lock_id=lockid, holder_id=data[0])
                self.client.delete(self.lock_path_prefix + lockid)
            except NoNodeError:
                logger.warn('Persistent lock {0} is already removed')
                pass
        return True
