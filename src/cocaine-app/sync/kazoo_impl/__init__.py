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
        # self.group = group
        self.client = KazooClient(host, timeout=3)
        logger.info('Connecting to zookeeper host {0}'.format(host))
        try:
            self.client.start()
        except Exception as e:
            logger.error(e)
            raise
        # self.q = FilteredLockingQueue(self.client, '/cache', self.__filter)
        # self.timeout = timeout
        # self.interval = interval
        self.locks = {}
        self.__locks_lock = threading.Lock()
        # self.__jobs_lock = Lock(self.client, '/locks/jobs')

    # @contextmanager
    # def item(self):
    #     try:
    #         task = self.q.get(self.timeout)
    #         yield task
    #         self.retry(self.q.consume, self.CONSUME_RETRIES)
    #     except ConnectionError:
    #         # in case of connection error we should retry the task execution
    #         raise
    #     except InvalidDataError:
    #         # in case of invalid data we can safely consume the item
    #         self.retry(self.q.consume, self.CONSUME_RETRIES)
    #         raise
    #     except Exception as e:
    #         self.q.unlock()
    #         raise

    # def retry(self, func, retries):
    #     for i in xrange(retries):
    #         try:
    #             func()
    #             break
    #         except SessionExpiredError:
    #             # trying to restore session
    #             sleep(self.SESSION_RESTORE_PAUSE)
    #             continue
    #     else:
    #         raise SessionExpiredError

    # def put(self, data):
    #     self.q.put(data)

    @contextmanager
    def lock(self, lockid):
        with self.__locks_lock:
            lock = self.locks.setdefault(lockid, Lock(self.client, self.ZK_LOCK_PATH_PREFIX + lockid))
        with lock:
            yield
