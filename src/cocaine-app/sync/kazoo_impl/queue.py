from kazoo.exceptions import NoNodeError, SessionExpiredError
from kazoo.protocol.states import EventType
from kazoo.recipe.queue import LockingQueue as BrokenLockingQueue


class LockingQueue(BrokenLockingQueue):

    def consume(self):
        """Removes a currently processing entry from the queue.
        Fix https://github.com/python-zk/kazoo/commit/8f6bf35da791e1c6253ab2cdb2f3ceab5ee295f7
        Not currently fixed in stable version.

        :returns: True if element was removed successfully, False otherwise.
        :rtype: bool
        """
        if self.processing_element is not None and self.holds_lock():
            id_, value = self.processing_element
            with self.client.transaction() as transaction:
                transaction.delete("{path}/{id}".format(
                    path=self._entries_path,
                    id=id_))
                transaction.delete("{path}/{id}".format(
                    path=self._lock_path,
                    id=id_))
            self.processing_element = None
            return True
        else:
            return False

    def holds_lock(self):
        """Checks if a node still holds the lock.
        Also cleans processing element in case of node dissapearing.

        :returns: True if a node still holds the lock, False otherwise.
        :rtype: bool

        """
        if self.processing_element is None:
            return False
        lock_id, _ = self.processing_element
        lock_path = "{path}/{id}".format(path=self._lock_path, id=lock_id)
        try:
            self.client.sync(lock_path)
            value, stat = self.client.retry(self.client.get, lock_path)
        except (NoNodeError, SessionExpiredError):
            # node has already been removed, probably after session expiration
            self.processing_element = None
            raise
        return value == self.id


class FilteredLockingQueue(LockingQueue):
    def __init__(self, client, path, filter_func):
        super(FilteredLockingQueue, self).__init__(client, path)
        self.filter_func = filter_func
        self.items_cache = {}

    def _get_data_cached(self, item):
        if item in self.items_cache:
            return self.items_cache[item]

        data = self.client.retry(self.client.get,
            "{path}/{id}".format(
                path=self._entries_path,
                id=item))

        self.items_cache[item] = data
        return data

    def _inner_get(self, timeout):
        flag = self.client.handler.event_object()
        lock = self.client.handler.lock_object()
        canceled = False
        value = []

        def check_for_updates(event):
            if not event is None and event.type != EventType.CHILD:
                return
            with lock:
                if canceled or flag.isSet():
                    return
                items = self.client.retry(self.client.get_children,
                    self._entries_path,
                    check_for_updates)

                taken = self.client.retry(self.client.get_children,
                    self._lock_path,
                    check_for_updates)

                available = self._filter_locked(items, taken)

                items_to_take = []
                for item in available:
                    try:
                        data = self._get_data_cached(item)
                        if self.filter_func(data[0]):
                            items_to_take.append(item)
                    except:
                        pass

                if len(items_to_take) > 0:
                    ret = self._take(items_to_take[0])
                    if not ret is None:
                        # By this time, no one took the task
                        value.append(ret)
                        flag.set()

        check_for_updates(None)
        retVal = None
        flag.wait(timeout)
        with lock:
            canceled = True
            if len(value) > 0:
                # We successfully locked an entry
                self.processing_element = value[0]
                retVal = value[0][1]
        return retVal

    def list(self, func=None):
        """Returns list of all entries.
        If func parameter is supplied, it is applied to each result entry
        (via map function).

        :returns: List of all entries in the queue.
        :rtype: list
        """
        self.client.retry(self.client.ensure_path, self._entries_path)

        items = [self._get_data_cached(item)[0] for item in self.client.retry(
            self.client.get_children, self._entries_path)]
        if func:
            return map(func, items)
        return items
