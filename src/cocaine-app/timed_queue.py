# -*- coding: utf-8 -*-
import threading
import heapq
import time

class Task:
    def __init__(self, task_id, function, args, kwargs):
        self.__id = task_id
        self.__function = function
        self.__args = args
        self.__kwargs = kwargs
        self.__done = False
    def execute(self):
        try:
            self.__function(*self.__args, **self.__kwargs)
        finally:
            self.__done = True
    def done(self):
        return self.__done
    def id(self):
        return self.__id

class TimedQueue:
    def __init__(self):
        self.__shutting_down = False
        self.__shutdown_lock = threading.Lock()
        self.__heap = []
        self.__hurry = []
        self.__task_by_id = {}
        self.__heap_lock = threading.Lock()
        self.__loop_thread = threading.Thread(target = TimedQueue.loop, args=(self,))
        self.__loop_thread.setDaemon(True)

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
            task = None
            with self.__heap_lock:
                if self.__hurry:
                    task = self.__hurry.pop()
                elif self.__heap and time.time() >= self.__heap[0][0]:
                    task = heapq.heappop(self.__heap)[1]
            if task is None:
                time.sleep(1)
            else:
                with self.__heap_lock:
                    id = task.id()
                    if id in self.__task_by_id:
                        del self.__task_by_id[id]
                if not task.done():
                    try:
                        task.execute()
                    except:
                        # Task should handle its exceptions. If it doesn't, will lose it here.
                        # The loop should not stop because of it.
                        pass

    def add_task_in(self, task_id, secs, function, *args, **kwargs):
        self.add_task_at(task_id, time.time() + secs, function, *args, **kwargs)

    def add_task_at(self, task_id, at, function, *args, **kwargs):
        if self._is_shutting_down():
            return
        with self.__heap_lock:
            if task_id in self.__task_by_id:
                raise Exception("Task with ID %s already exists" % task_id)
            task = Task(task_id, function, args, kwargs)
            heapq.heappush(self.__heap, (at, task))
            self.__task_by_id[task_id] = task
    
    def hurry(self, task_id):
        with self.__heap_lock:
            if task_id in self.__task_by_id:
                self.__hurry.append(self.__task_by_id[task_id])

    def shutdown(self):
        with self.__shutdown_lock:
            self.__shutting_down = True
        self.__loop_thread.join()
