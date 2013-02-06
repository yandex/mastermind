# -*- coding: utf-8 -*-
import threading
import heapq
import time

class TimedQueue:
    def __init__(self):
        self.__shutting_down = False
        self.__shutdown_lock = threading.Lock()
        self.__heap = []
        self.__heap_lock = threading.Lock()
        self.__loop_thread = threading.Thread(target = TimedQueue.loop, args=(self,))

    def start(self):
        self.__loop_thread.start()

    def __del__(self):
        if not self._is_shutting_down():
            self.shutdown()

    def _is_shutting_down(self):
        with self.__shutdown_lock:
            shutting_down = self.__shutting_down
        return shutting_down

    def loop(self):
        while not self._is_shutting_down():
            work = None
            with self.__heap_lock:
                if self.__heap and time.time() >= self.__heap[0][0]:
                    work = heapq.heappop(self.__heap)
            if work is None:
                time.sleep(1)
            else:
                work[1](*(work[2]), **(work[3]))

    def add_task_in(self, secs, function, *args, **kwargs):
        self.add_task_at(time.time() + secs, function, *args, **kwargs)

    def add_task_at(self, at, function, *args, **kwargs):
        if self._is_shutting_down():
            return
        func_tuple = (at, function, args, kwargs)
        with self.__heap_lock:
            heapq.heappush(self.__heap, func_tuple)

    def shutdown(self):
        with self.__shutdown_lock:
            self.__shutting_down = True
        self.__loop_thread.join()
