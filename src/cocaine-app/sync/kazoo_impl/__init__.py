from contextlib import contextmanager
import logging
import threading
from time import sleep

from kazoo.client import KazooClient
from kazoo.exceptions import SessionExpiredError

# from queue import FilteredLockingQueue
# from errors import ConnectionError, InvalidDataError
from lock import Lock
from log import _handler


logger = logging.getLogger('mm.sync')

kazoo_logger = logging.getLogger('kazoo')
kazoo_logger.propagate = False
kazoo_logger.addHandler(_handler)
kazoo_logger.setLevel(logging.INFO)


class ZkSyncManager(object):

    CONSUME_RETRIES = 2
    SESSION_RESTORE_PAUSE = 0.5

    ZK_LOCK_PATH_PREFIX = '/mastermind/locks/'

    def __init__(self, host='127.0.0.1:2181'):
        self.client = KazooClient(host, timeout=3)
        logger.info('Connecting to zookeeper host {0}'.format(host))
        try:
            self.client.start()
        except Exception as e:
            logger.error(e)
            raise
        self.locks = {}
        self.__locks_lock = threading.Lock()

    @contextmanager
    def lock(self, lockid):
        with self.__locks_lock:
            lock = self.locks.setdefault(lockid, Lock(self.client, self.ZK_LOCK_PATH_PREFIX + lockid))
        with lock:
            yield
