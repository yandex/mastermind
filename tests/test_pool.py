import multiprocessing
import time

import pytest

from mastermind import pool


class TimingWrapper(object):
    """Executes function and stores execution elapsed time
    """

    def __init__(self, func):
        self.func = func
        self.elapsed = None

    def __call__(self, *args, **kwds):
        t = time.time()
        try:
            return self.func(*args, **kwds)
        finally:
            self.elapsed = time.time() - t


class TestPool(object):
    """Test basic functionality of overridden multiprocessing.Pool implementation
    """

    def test_make_pool(self):
        """Pool initialization"""
        p = pool.Pool(processes=2)
        assert len(p._pool) == 2

    @pytest.mark.parametrize(
        'processes, task_delay',
        [(4, 5.0)]
    )
    def test_terminate(self, delay_task_worker_pool):
        """Pool termination"""
        delay_task_worker_pool.imap_unordered(None, xrange(100))
        delay_task_worker_pool.terminate()

        join = TimingWrapper(delay_task_worker_pool.join)
        join()

        assert 0 <= join.elapsed <= 0.2

    @pytest.mark.parametrize(
        'processes, task_delay',
        [(2, 1.0)]
    )
    def test_close(self, delay_task_worker_pool):
        """Pool soft close"""
        delay_task_worker_pool.imap_unordered(None, xrange(2))
        delay_task_worker_pool.close()

        join = TimingWrapper(delay_task_worker_pool.join)
        join()

        assert 1 <= join.elapsed <= 1.3

    @pytest.mark.parametrize(
        'processes, task_delay',
        [(2, 1.0)]
    )
    def test_restore_pool(self, delay_task_worker_pool):
        """Resurrection of workers in case of worker process termination"""
        delay_task_worker_pool._pool[0].terminate()
        time.sleep(0.2)
        assert len(delay_task_worker_pool._pool) == 2
        for w in delay_task_worker_pool._pool:
            assert w.is_alive() is True
            assert w.exitcode is None

    @pytest.mark.parametrize(
        'processes, task_delay',
        [(4, 0.0)]
    )
    def test_imap_unordered(self, delay_task_worker_pool):
        """imap_unordered with default chunksize (1)"""
        RESULTS_NUM = 21
        res = delay_task_worker_pool.imap_unordered(None, xrange(RESULTS_NUM))
        assert range(RESULTS_NUM) == sorted(list(res))
        delay_task_worker_pool.close()

    @pytest.mark.xfail
    @pytest.mark.parametrize(
        'processes, task_delay',
        [(4, 0.0)]
    )
    def test_imap_unordered_chunk(self, delay_task_worker_pool):
        """imap_unordered with chunksize > 1"""
        RESULTS_NUM = 21
        res = delay_task_worker_pool.imap_unordered(
            None,
            xrange(RESULTS_NUM),
            chunksize=4
        )
        assert range(RESULTS_NUM) == sorted(list(res))
        delay_task_worker_pool.close()

    @pytest.mark.parametrize(
        'processes, task_delay',
        [(2, 0.0)]
    )
    def test_apply(self, delay_task_worker_pool):
        assert delay_task_worker_pool.apply(None, (1,)) == 1

    @pytest.mark.xfail
    @pytest.mark.parametrize(
        'processes, task_delay',
        [(2, 0.0)]
    )
    def test_map(self, delay_task_worker_pool):
        """map with default chunksize (None)"""
        RESULTS_NUM = 5
        assert delay_task_worker_pool.map(None, xrange(RESULTS_NUM)) == range(RESULTS_NUM)

    @pytest.mark.xfail
    @pytest.mark.parametrize(
        'processes, task_delay',
        [(2, 0.0)]
    )
    def test_map_chunk(self, delay_task_worker_pool):
        """map with chunksize 1"""
        RESULTS_NUM = 5
        assert delay_task_worker_pool.map(
            None, xrange(RESULTS_NUM), chunksize=1
        ) == range(RESULTS_NUM)

    @pytest.mark.parametrize(
        'processes, task_delay',
        [(2, 0.0)]
    )
    def test_apply_async(self, delay_task_worker_pool):
        """Blocking apply_async"""
        delay_task_worker_pool.apply_async(None, (1,)).get() == 1

    @pytest.mark.parametrize(
        'processes, task_delay',
        [(2, 1.0)]
    )
    def test_apply_async_timeout(self, delay_task_worker_pool):
        """Timeout on apply_async call"""
        with pytest.raises(multiprocessing.TimeoutError):
            delay_task_worker_pool.apply_async(None, (1,)).get(0.5)

    @pytest.mark.xfail
    @pytest.mark.parametrize(
        'processes, task_delay',
        [(1, 0.0)]
    )
    def test_apply_map_async(self, delay_task_worker_pool):
        """Blocking map_async"""
        RESULTS_NUM = 5
        assert delay_task_worker_pool.map_async(
            None,
            xrange(RESULTS_NUM)
        ).get() == range(RESULTS_NUM)

    @pytest.mark.parametrize(
        'processes, task_delay',
        [(2, 1.0)]
    )
    def test_apply_map_async_timeout(self, delay_task_worker_pool):
        """Timeout on map_async call"""
        RESULTS_NUM = 5
        with pytest.raises(multiprocessing.TimeoutError):
            delay_task_worker_pool.map_async(None, xrange(RESULTS_NUM)).get(0.5)

    @pytest.mark.parametrize(
        'processes, task_delay',
        [(2, 0.0)]
    )
    def test_imap(self, delay_task_worker_pool):
        """imap in iterator and list mode"""
        RESULTS_NUM = 50
        assert list(
            delay_task_worker_pool.imap(None, xrange(RESULTS_NUM))
        ) == range(RESULTS_NUM)
        it = delay_task_worker_pool.imap(None, xrange(RESULTS_NUM))
        for i in xrange(RESULTS_NUM):
            assert it.next() == i
