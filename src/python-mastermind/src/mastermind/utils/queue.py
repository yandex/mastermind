import os.path
import uuid

from kazoo.exceptions import NoNodeError, NodeExistsError, KazooException
from kazoo.retry import RetryFailedError


class LockingQueue(object):
    """A distributed queue with priority and locking support.

    Upon retrieving an entry from the queue, the entry gets locked with an
    ephemeral node (instead of deleted). If an error occurs, this lock gets
    released so that others could retake the entry. This adds a little penalty
    as compared to :class:`Queue` implementation.

    The user should call the :meth:`FilteredLockingQueue.get` method first to lock and
    retrieve the next entry. When finished processing the entry, a user should
    call the :meth:`FilteredLockingQueue.consume` method that will remove the entry
    from the queue.

    This queue will not track connection status with ZooKeeper. If a node locks
    an element, then loses connection with ZooKeeper and later reconnects, the
    lock will probably be removed by Zookeeper in the meantime, but a node
    would still think that it holds a lock. The user should check the
    connection status with Zookeeper or call :meth:`FilteredLockingQueue.holds_lock`
    method that will check if a node still holds the lock.

    """
    lock = "/taken"
    entries = "/entries"
    entry = "entry"

    def __init__(self, client, path, group_id):
        """
        :param client: A :class:`~kazoo.client.KazooClient` instance.
        :param path: The queue path prefix to use in ZooKeeper.
        :param group_id: Local group id, will be added to path to filter
          the tasks assigned to local elliptics group
        """
        self.client = client
        self.path = os.path.join(path, str(group_id))
        self.id = uuid.uuid4().hex.encode()
        self._lock_path = os.path.normpath(self.path) + self.lock
        self._entries_path = os.path.normpath(self.path) + self.entries
        self.structure_paths = (self._lock_path, self._entries_path)
        self.ensured_path = False

    def _check_put_arguments(self, value, priority=100):
        if not isinstance(value, bytes):
            raise TypeError("value must be a byte string")
        if not isinstance(priority, int):
            raise TypeError("priority must be an int")
        elif priority < 0 or priority > 999:
            raise ValueError("priority must be between 0 and 999")

    def _ensure_paths(self):
        if not self.ensured_path:
            # make sure our parent / internal structure nodes exists
            for path in self.structure_paths:
                self.client.ensure_path(path)
            self.ensured_path = True

    def __len__(self):
        self._ensure_paths()
        _, stat = self.client.retry(self.client.get, self._entries_path)
        return stat.children_count

    def put(self, value, priority=100):
        """Put an entry into the queue.

        :param value: Byte string to put into the queue.
        :param priority:
            An optional priority as an integer with at most 3 digits.
            Lower values signify higher priority.

        """
        self._check_put_arguments(value, priority)
        self._ensure_paths()

        self.client.create(
            "{path}/{prefix}-{priority:03d}-".format(
                path=self._entries_path,
                prefix=self.entry,
                priority=priority),
            value, sequence=True)

    def __iter__(self):
        self._ensure_paths()
        items = self.client.retry(self.client.get_children, self._entries_path)
        for item in items:
            try:
                with LockedItem(self.client,
                                self._entries_path, self._lock_path,
                                item, self.id) as locked_item:
                    yield locked_item
            except KazooException:
                continue

    def list(self):
        self._ensure_paths()
        for item in self.client.retry(self.client.get_children, self._entries_path):
            try:
                yield self.client.retry(self.client.get, '{path}/{id}'.format(
                    path=self._entries_path, id=item))[0]
            except NoNodeError:
                # node is already consumed by some other processed, skip
                continue


class LockError(KazooException):
    pass


class LockedItem(object):

    def __init__(self, client, entries_path, lock_path, entry_id, lock_id):
        self._client = client
        self._entries_path = entries_path
        self._lock_path = lock_path
        self.entry_id = entry_id
        self.lock_id = lock_id
        self.data = None

    @property
    def path(self):
        """ Returns full lock path """
        return self._lock_path

    def __enter__(self):
        self.acquire()
        return self

    def acquire(self):
        """Acquires the lock for the given entry.

        :returns: True if a lock was acquired succesfully, otherwise raises
          some kind of KazooException.
        :rtype: bool
        """
        try:
            self._client.retry(
                self._client.create,
                '{path}/{id}'.format(path=self._lock_path,
                                     id=self.entry_id),
                self.lock_id,
                ephemeral=True)
        except (NodeExistsError, RetryFailedError):
            if not self.holds_lock():
                raise LockError

        try:
            value, stat = self._client.retry(
                self._client.get,
                '{path}/{id}'.format(path=self._entries_path, id=self.entry_id))
        except (NoNodeError, RetryFailedError):
            if self.holds_lock():
                self._client.retry(self._inner_release)

        self.data = value
        return True

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()

    def release(self):
        """Releases previously acquired lock for the given entry.

        :returns: True if a lock was released succesfully, False in case of a
          non-retryable error or if entry is already locked by someone else.
        :rtype: bool
        """
        try:
            self._client.retry(self._inner_release)
        except LockError:
            return False
        return True

    def _inner_release(self):
        if not self.holds_lock():
            raise LockError
        self._client.delete('{path}/{id}'.format(
            path=self._lock_path,
            id=self.entry_id))

    def holds_lock(self):
        """Checks if a node still holds the lock.

        :returns: True if a node still holds the lock, False otherwise.
        :rtype: bool
        """
        lock_path = '{path}/{id}'.format(path=self._lock_path, id=self.entry_id)
        try:
            self._client.retry(self._client.sync, lock_path)
            value, stat = self._client.retry(self._client.get, lock_path)
        except NoNodeError:
            # node has already been removed, probably after session expiration
            return False
        return value == self.lock_id

    def consume(self):
        """Removes locked item entry from the queue.

        :returns: None
        :rtype: bool
        """
        self._client.retry(self._inner_consume)

    def _inner_consume(self):
        if not self.holds_lock():
            raise LockError
        self._client.delete('{path}/{id}'.format(
            path=self._entries_path,
            id=self.entry_id))
