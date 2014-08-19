from contextlib import contextmanager
from threading import Lock


class SyncManager(object):

    def __init__(self, *args, **kwargs):
        # self.tasks = {}
        self.locks = {}
        self.__locks_lock = Lock()

    # def put(self, task):
    #     self.tasks[task['key']] = task

    @contextmanager
    def lock(self, lockid):
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
