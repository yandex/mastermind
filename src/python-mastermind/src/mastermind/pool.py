"""
Module providing the `Pool` class for managing a process pool

Pool overrides the standard library implementation (multiprocessing/pool.py).

Standard implementation of Pool supports only synchronous processing of
tasks by target function (see `worker` function in multiprocessing/pool.py).
In case when a task is partially I/O-bound we can benefit from processing
several tasks at a time.
Current implementation provides PoolWorker base class that controls
task fetching from queue, processes tasks as coroutines and
pushes results to result queue (basically replaces original
multiprocessing.pool.worker's routine).

NB: this module mostly uses original code-style of standard library
for easier code maintaining when comparing code with the one from standard
library.

This implementation uses the original task format of the original pool:

    (job_id, task_id, func, args, kwds),

where:
    - job is a batch of tasks and is used on the client side to
    pass results to the user;
    - task id is sequential integer which is used to restore order
    of results;
    - `func` is used by the original Pool implementation and is not
    supported, this is replaced by subclassing PoolWorker and
    providing its `process` method;
    - args and kwds are passed to `process` method of worker
    implementation.

This format is not changed to retain support of ApplyResult-
and IMapIterator-based result objects of standard implementation.

Due to the fact that this implementation redefines some "private"
methods of `Pool` and `SimpleQueue` classes, it is heavily
dependent on current implementation. This means that it should be
covered by tests thoroughly to spot any inconsistencies and
implementation changes.
"""

from collections import Iterator
from datetime import timedelta
import logging
from multiprocessing.pool import Pool as OriginalPool
from multiprocessing.queues import SimpleQueue as OriginalSimpleQueue
from multiprocessing.queues import Empty
from multiprocessing.util import debug

from tornado import gen
from tornado.ioloop import IOLoop


class PoolWorker(object):
    """A pool worker base class for processing pool tasks

    Fetches and processes tasks on asynchronous mode.
    Derived class should implement 'process' method.

    NB: 'process' method implementation should minimize the usage
    of blocking operation except when it is necessary.

    WARNING: task_queue and result_queue are required to be set
    by set_* methods before pool worker can run.
    """

    RUNNING = 0
    STOPPED = 1

    def __init__(self,
                 ioloop=None,
                 tasks_fetch_period=0.1,
                 max_tasks_per_period=1):
        self._task_queue = None
        self._result_queue = None
        self._ioloop = ioloop or IOLoop.current()
        self.logger = logging.getLogger('worker_pool')
        self._tasks_fetch_period = timedelta(seconds=tasks_fetch_period)
        self._max_tasks_per_period = max_tasks_per_period
        self._state = PoolWorker.RUNNING
        self._executing = 0

    def set_task_queue(self, task_queue):
        self._task_queue = task_queue

    def set_result_queue(self, result_queue):
        self._result_queue = result_queue

    def run(self):
        assert self._task_queue, 'Task queue should be set'
        assert self._result_queue, 'Result queue should be set'

        self._ioloop.add_callback(self._process_tasks)
        self._ioloop.start()

    def process(self, *args, **kwargs):
        raise NotImplemented

    @gen.coroutine
    def _process_task(self, job_id, task_id, *args, **kwds):
        """Process a single task from task queue

        Runs processing task as a coroutine and pushes result
        to result queue.

        Result is a tuple of the following form:
            (job id, task id in a job, (success, result)),

        where:
            - job is a batch of tasks and is used on the client side to
            pass results to the user;
            - task id is sequential integer which is used to restore order
            of results;
            - `success` is a boolean flag that becomes False when exception was
            raised;
            - `result` is any picklable data that should be returned to the
            client
        """
        try:
            self.logger.debug(
                'Processing task {args}, {kwds} (job {job_id}, task {task_id})'.format(
                    args=args,
                    kwds=kwds,
                    job_id=job_id,
                    task_id=task_id,
                )
            )
            result = yield self.process(*args, **kwds)
            self._result_queue.put(
                (
                    job_id,
                    task_id,
                    (True, result)
                )
            )
        except Exception as e:
            self.logger.exception(
                'Task {args}, {kwds} (job {job_id}, task {task_id}) processing failed'.format(
                    args=args,
                    kwds=kwds,
                    job_id=job_id,
                    task_id=task_id,
                )
            )
            self._result_queue.put(
                (
                    job_id,
                    task_id,
                    (False, RuntimeError(str(e)))
                )
            )
        finally:
            self._executing -= 1
            self._check_stop()

    def _process_tasks(self):
        """Fetch tasks from task queue if available and pass them for processing

        This task is run periodically (because SimpleQueue does not provide
        callbacks and/or asynchronous interface) and fetched tasks from task queue.

        Task format is described above.
        """
        self.logger.info('Fetching tasks')
        try:
            for _ in xrange(self._max_tasks_per_period):
                task = self._task_queue.get(block=False)
                if task is None:
                    self._state = PoolWorker.STOPPED
                    self._check_stop()
                    return

                job_id, task_id, _, args, kwds = task
                self.logger.debug(
                    'Fetched task {args}, {kwds} (job {job_id}, task {task_id})'.format(
                        args=args,
                        kwds=kwds,
                        job_id=job_id,
                        task_id=task_id,
                    )
                )
                self._executing += 1
                self._ioloop.add_callback(self._process_task, job_id, task_id, *args)
        except Empty:
            pass
        finally:
            if self._state == PoolWorker.RUNNING:
                self._ioloop.add_timeout(self._tasks_fetch_period, self._process_tasks)

    def _check_stop(self):
        """Check if all tasks have been processed
        """
        if self._state == PoolWorker.STOPPED and self._executing == 0:
            self._ioloop.stop()


def run_worker(task_queue, result_queue, PoolWorker, initkwds={}):
    ioloop = IOLoop.current()

    worker = PoolWorker(
        ioloop=ioloop,
        **initkwds
    )
    worker.set_task_queue(task_queue)
    worker.set_result_queue(result_queue)
    worker.run()


class Pool(OriginalPool):
    """Class which supports an async version of the `apply()` builtin

    Original implementation is extended in two ways:
        - uses extended SimpleQueue with non-blocking mode;
        - uses custom implementation of run_worker method which
          allows to run tornado-based workers and process
          tasks in asynchronous mode.

    Parameters:
        worker: PoolWorker-based class that will be instanced in a worker process;
        w_initkwds: kwds that should be passed to worker on initialization
                    NB: *args is not supported;
        *args, **kwargs: parameters to pass to original Pool base class.
    """
    def __init__(self, worker=PoolWorker, w_initkwds={}, *args, **kwargs):
        self._worker = worker
        self._w_initkwds = w_initkwds

        super(Pool, self).__init__(*args, **kwargs)

    def _repopulate_pool(self):
        """Bring the number of pool processes up to the specified number,
        for use after reaping workers which have exited.
        """
        for i in range(self._processes - len(self._pool)):
            w = self.Process(
                target=run_worker,
                args=(
                    self._inqueue,
                    self._outqueue,
                    self._worker,
                    self._w_initkwds
                )
            )
            self._pool.append(w)
            w.name = w.name.replace('Process', 'PoolWorker')
            w.daemon = True
            w.start()
            debug('added worker')

    def _setup_queues(self):
        self._inqueue = SimpleQueue()
        self._outqueue = SimpleQueue()
        self._quick_put = self._inqueue._writer.send
        self._quick_get = self._outqueue._reader.recv


class SimpleQueue(OriginalSimpleQueue):
    """Simplified Queue type -- really just a locked pipe

    Extends original implementation's get method with 'block' parameter,
    which allows SimpleQueue instance to be used in non-blocking mode.
    """

    def _make_methods(self):
        super(SimpleQueue, self)._make_methods()

        recv = self._reader.recv
        rpoll = self._reader.poll
        racquire, rrelease = self._rlock.acquire, self._rlock.release

        def get(block=True):
            racquire()
            try:
                if block:
                    return recv()
                else:
                    if not rpoll():
                        raise Empty
                    # recv() is expected to return right away since rpoll() returned True
                    # and we are still under the lock
                    return recv()
            finally:
                rrelease()
        self.get = get


def skip_exceptions(result, on_exc=None):
    if not isinstance(result, Iterator):
        raise TypeError('Iterator object is expected')

    while True:
        try:
            yield result.next()
        except StopIteration:
            raise
        except Exception as e:
            if on_exc:
                on_exc(e)
            continue
