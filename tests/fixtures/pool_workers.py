import pytest
from tornado import gen

from mastermind import pool


@pytest.fixture
def delay_task_worker_pool(processes, task_delay):
    """Pool of DelayTaskWorker processes"""

    class DelayTaskWorker(pool.PoolWorker):
        """Worker emulating async tasks execution

        Returns task that was passed to it as a result.
        """
        def __init__(self,
                     ioloop=None,
                     task_delay=0.0,
                     **kwds):
            super(DelayTaskWorker, self).__init__(ioloop=ioloop, **kwds)
            self._task_delay = task_delay

        @gen.coroutine
        def process(self, task):
            yield gen.sleep(self._task_delay)
            raise gen.Return(task)

    return pool.Pool(
        processes=processes,
        worker=DelayTaskWorker,
        w_initkwds={
            'task_delay': task_delay,
            'tasks_fetch_period': 0.001,
        }
    )
