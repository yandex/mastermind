import functools
import logging

from errors import CacheUpstreamError
from infrastructure_cache import cache
from mastermind import helpers as mh
import storage


logger = logging.getLogger('mm.storage')


class Host(object):
    def __init__(self, addr):
        self.addr = addr
        self._dc = None
        self.hostname = None
        self.nodes = []

    def update(self, state):
        self.hostname = state['name']
        self._dc = state['dc']

    @property
    def hostname_or_not(self):
        return self.hostname

    @property
    def dc(self):
        if self._dc is None:
            raise CacheUpstreamError('Host {} ({}): dc is unknown'.format(
                self.hostname,
                self.addr,
            ))
        return self._dc

    @property
    def dc_or_not(self):
        return self._dc

    @property
    def parents(self):
        return cache.get_host_tree(self.hostname)

    @property
    def full_path(self):
        parent = self.parents
        parts = [parent['name']]
        while 'parent' in parent:
            parent = parent['parent']
            parts.append(parent['name'])
        return '|'.join(reversed(parts))

    def index(self):
        return self.__str__()

    def __eq__(self, other):

        if isinstance(other, basestring):
            return self.addr == other

        if isinstance(other, Host):
            return self.addr == other.addr

        return False

    def __hash__(self):
        return hash(self.__str__())

    def __repr__(self):
        return ('<Host object: addr=%s, nodes=[%s] >' %
                (self.addr, ', '.join((repr(n) for n in self.nodes))))

    def __str__(self):
        return self.addr


class NodeBackendStat(object):

    __slots__ = ('data', 'commands_stat')

    KEY_TIMESTAMP = 'timestamp'

    KEY_FREE_SPACE = 'free_space'
    KEY_TOTAL_SPACE = 'total_space'
    KEY_USED_SPACE = 'used_space'

    KEY_VFS_FREE_SPACE = 'vfs_free_space'
    KEY_VFS_TOTAL_SPACE = 'vfs_total_space'
    KEY_VFS_USED_SPACE = 'vfs_used_space'

    KEY_FRAGMENTATION = 'fragmentation'

    KEY_RECORDS = 'records'
    KEY_RECORDS_REMOVED = 'records_removed'
    KEY_RECORDS_REMOVED_SIZE = 'records_removed_size'

    KEY_WANT_DEFRAG = 'want_defrag'
    KEY_DEFRAG_STATE = 'defrag_state'

    KEY_BLOB_SIZE = 'blob_size'
    KEY_BLOB_SIZE_LIMIT = 'blob_size_limit'
    KEY_MAX_BLOB_BASE_SIZE = 'max_blob_base_size'

    KEY_IO_BLOCKING_SIZE = 'io_blocking_size'
    KEY_IO_NONBLOCKING_SIZE = 'io_nonblocking_size'

    KEY_STAT_COMMIT_ROFS_ERRORS_DIFF = 'stat_commit_rofs_errors_diff'

    def __init__(self):
        self.data = {
            self.KEY_TIMESTAMP: {
                'tv_sec': 0,
                'tv_usec': 0,
            },

            self.KEY_FREE_SPACE: 0,
            self.KEY_TOTAL_SPACE: 0,
            self.KEY_USED_SPACE: 0,

            self.KEY_VFS_FREE_SPACE: 0,
            self.KEY_VFS_TOTAL_SPACE: 0,
            self.KEY_VFS_USED_SPACE: 0,

            self.KEY_FRAGMENTATION: 0.0,

            self.KEY_RECORDS: 0,
            self.KEY_RECORDS_REMOVED: 0,
            self.KEY_RECORDS_REMOVED_SIZE: 0,

            self.KEY_WANT_DEFRAG: 0,
            self.KEY_DEFRAG_STATE: None,

            self.KEY_BLOB_SIZE: 0,
            self.KEY_BLOB_SIZE_LIMIT: 0,
            self.KEY_MAX_BLOB_BASE_SIZE: 0,

            self.KEY_IO_BLOCKING_SIZE: 0,
            self.KEY_IO_NONBLOCKING_SIZE: 0,

            self.KEY_STAT_COMMIT_ROFS_ERRORS_DIFF: 0,
        }

        self.commands_stat = storage.CommandsStat()

    def update(self, data):
        self.data = data

    @property
    def ts(self):
        return mh.elliptics_time_to_ts(self.data[self.KEY_TIMESTAMP])

    @property
    def free_space(self):
        return self.data[self.KEY_FREE_SPACE]

    @property
    def total_space(self):
        return self.data[self.KEY_TOTAL_SPACE]

    @property
    def used_space(self):
        return self.data[self.KEY_USED_SPACE]

    @property
    def vfs_free_space(self):
        return self.data[self.KEY_VFS_FREE_SPACE]

    @property
    def vfs_total_space(self):
        return self.data[self.KEY_VFS_TOTAL_SPACE]

    @property
    def vfs_used_space(self):
        return self.data[self.KEY_VFS_USED_SPACE]

    @property
    def fragmentation(self):
        return self.data[self.KEY_FRAGMENTATION]

    @property
    def files(self):
        return self.data[self.KEY_RECORDS]

    @property
    def files_removed(self):
        return self.data[self.KEY_RECORDS_REMOVED]

    @property
    def files_removed_size(self):
        return self.data[self.KEY_RECORDS_REMOVED_SIZE]

    @property
    def defrag_state(self):
        return self.data[self.KEY_DEFRAG_STATE]

    @property
    def want_defrag(self):
        return self.data[self.KEY_WANT_DEFRAG]

    @property
    def blob_size(self):
        return self.data[self.KEY_BLOB_SIZE]

    @property
    def blob_size_limit(self):
        return self.data[self.KEY_BLOB_SIZE_LIMIT]

    @property
    def max_blob_base_size(self):
        return self.data[self.KEY_MAX_BLOB_BASE_SIZE]

    @property
    def io_blocking_size(self):
        return self.data[self.KEY_IO_BLOCKING_SIZE]

    @property
    def io_nonblocking_size(self):
        return self.data[self.KEY_IO_NONBLOCKING_SIZE]

    @property
    def stat_commit_errors(self):
        return self.data[self.KEY_STAT_COMMIT_ROFS_ERRORS_DIFF]

    def __add__(self, other):

        ts = min(
            (self.data[self.KEY_TIMESTAMP], other.data[self.KEY_TIMESTAMP]),
            key=lambda t: (t['tv_sec'], t['tv_usec'])
        )

        stat = type(self)()

        stat.update({
            self.KEY_TIMESTAMP: ts,

            self.KEY_FREE_SPACE: self.free_space + other.free_space,
            self.KEY_TOTAL_SPACE: self.total_space + other.total_space,
            self.KEY_USED_SPACE: self.used_space + other.used_space,

            self.KEY_VFS_FREE_SPACE: self.vfs_free_space + other.vfs_free_space,
            self.KEY_VFS_TOTAL_SPACE: self.vfs_total_space + other.vfs_total_space,
            self.KEY_VFS_USED_SPACE: self.vfs_used_space + other.vfs_used_space,

            self.KEY_FRAGMENTATION: (
                float(self.files_removed + other.files_removed) /
                max(self.files_removed + other.files_removed + self.files + other.files, 1)
            ),

            self.KEY_RECORDS: self.files + other.files,
            self.KEY_RECORDS_REMOVED: self.files_removed + other.files_removed,
            self.KEY_RECORDS_REMOVED_SIZE: self.files_removed_size + other.files_removed_size,

            self.KEY_WANT_DEFRAG: max(self.want_defrag, other.want_defrag),
            self.KEY_DEFRAG_STATE: max(self.defrag_state, other.defrag_state),

            self.KEY_BLOB_SIZE: max(self.blob_size, other.blob_size),
            self.KEY_BLOB_SIZE_LIMIT: min(self.blob_size_limit, other.blob_size_limit),
            self.KEY_MAX_BLOB_BASE_SIZE: max(self.max_blob_base_size, other.max_blob_base_size),

            self.KEY_IO_BLOCKING_SIZE: max(self.io_blocking_size, other.io_blocking_size),
            self.KEY_IO_NONBLOCKING_SIZE: max(self.io_nonblocking_size, other.io_nonblocking_size),

            self.KEY_STAT_COMMIT_ROFS_ERRORS_DIFF: max(self.stat_commit_errors, other.stat_commit_errors),
        })

        return stat

    def __mul__(self, other):
        ts = min(
            (self.data[self.KEY_TIMESTAMP], other.data[self.KEY_TIMESTAMP]),
            key=lambda t: (t['tv_sec'], t['tv_usec'])
        )

        # files and files_removed are taken from the stat object with maximum
        # total number of keys. If total number of keys is equal,
        # the stat object with larger number of removed keys is more up-to-date
        files_stat = max(self, other, key=lambda stat: (stat.files + stat.files_removed, stat.files_removed))

        # ATTENTION: fragmentation coefficient in this case would not necessary
        # be equal to [removed keys / total keys]
        fragmentation = max(self.fragmentation, other.fragmentation)

        stat = type(self)()

        stat.update({
            self.KEY_TIMESTAMP: ts,

            self.KEY_FREE_SPACE: min(self.free_space, other.free_space),
            self.KEY_TOTAL_SPACE: min(self.total_space, other.total_space),
            self.KEY_USED_SPACE: max(self.used_space, other.used_space),

            self.KEY_VFS_FREE_SPACE: min(self.vfs_free_space, other.vfs_free_space),
            self.KEY_VFS_TOTAL_SPACE: min(self.vfs_total_space, other.vfs_total_space),
            self.KEY_VFS_USED_SPACE: max(self.vfs_used_space, other.vfs_used_space),

            self.KEY_FRAGMENTATION: fragmentation,

            self.KEY_RECORDS: files_stat.files,
            self.KEY_RECORDS_REMOVED: files_stat.files_removed,
            self.KEY_RECORDS_REMOVED_SIZE: max(self.files_removed_size, other.files_removed_size),

            self.KEY_WANT_DEFRAG: max(self.want_defrag, other.want_defrag),
            self.KEY_DEFRAG_STATE: max(self.defrag_state, other.defrag_state),

            self.KEY_BLOB_SIZE: max(self.blob_size, other.blob_size),
            self.KEY_BLOB_SIZE_LIMIT: min(self.blob_size_limit, other.blob_size_limit),
            self.KEY_MAX_BLOB_BASE_SIZE: max(self.max_blob_base_size, other.max_blob_base_size),

            self.KEY_IO_BLOCKING_SIZE: max(self.io_blocking_size, other.io_blocking_size),
            self.KEY_IO_NONBLOCKING_SIZE: max(self.io_nonblocking_size, other.io_nonblocking_size),

            self.KEY_STAT_COMMIT_ROFS_ERRORS_DIFF: max(self.stat_commit_errors, other.stat_commit_errors),
        })

        return stat


def status_change_log(f):
    @functools.wraps(f)
    def wrapper(self, *args, **kwargs):
        prev_status = self.status
        res = f(self, *args, **kwargs)
        if prev_status != self.status:
            logger.info(
                '{type} {id} status updated from {prev_status} to {cur_status}'
                '{status_text}'.format(
                    type=type(self).__name__,
                    id=self,
                    prev_status=prev_status,
                    cur_status=self.status,
                    status_text=(
                        ' ({})'.format(self.status_text)
                        if self.status_text else
                        ''
                    ),
                )
            )
        return res
    return wrapper


class NodeBackendBase(object):

    __slots__ = (
        'data',
        'node',
        'backend_id',
        'fs',
        'group',
        'stat',
    )

    KEY_BASE_PATH = 'base_path'
    KEY_READ_ONLY = 'read_only'
    KEY_STATUS = 'status'
    KEY_STATUS_TEXT = 'status_text'

    def __init__(self, node, backend_id):
        self.node = node
        self.backend_id = backend_id

        self.fs = None
        self.group = None
        self.stat = None

        self.data = {
            self.KEY_BASE_PATH: None,
            self.KEY_READ_ONLY: False,
            self.KEY_STATUS: storage.Status.INIT,
            self.KEY_STATUS_TEXT: 'Node {} is not inititalized yet'.format(self),
        }

    @status_change_log
    def update(self, data):
        self.data = data

    @property
    def base_path(self):
        return self.data[self.KEY_BASE_PATH]

    @property
    def read_only(self):
        return self.data[self.KEY_READ_ONLY]

    @property
    def status(self):
        return self.data[self.KEY_STATUS]

    @property
    def status_text(self):
        return self.data[self.KEY_STATUS_TEXT]


class FsBase(object):

    __slots__ = (
        'data',
        'host',
        'fsid',
        'node_backends',
        'stat',
    )

    KEY_STATUS = 'status'
    KEY_STATUS_TEXT = 'status_text'

    def __init__(self, host, fsid):
        self.host = host
        self.fsid = fsid
        self.data = {
            self.KEY_STATUS: storage.Status.OK,
            self.KEY_STATUS_TEXT: '',
        }

        self.node_backends = {}
        self.stat = None

    @status_change_log
    def update(self, data):
        self.data = data

    @property
    def status(self):
        return self.data[self.KEY_STATUS]

    @property
    def status_text(self):
        return self.data[self.KEY_STATUS_TEXT]


class FsStat(object):

    __slots__ = ('data', 'commands_stat')

    KEY_TIMESTAMP = 'timestamp'
    KEY_TOTAL_SPACE = 'total_space'
    KEY_FREE_SPACE = 'free_space'

    KEY_DISK_UTIL = 'disk_util'
    KEY_DISK_UTIL_READ = 'disk_util_read'
    KEY_DISK_UTIL_WRITE = 'disk_util_write'

    KEY_DISK_READ_RATE = 'disk_read_rate'
    KEY_DISK_WRITE_RATE = 'disk_write_rate'

    def __init__(self):
        self.data = {
            self.KEY_TIMESTAMP: {
                'tv_sec': 0,
                'tv_usec': 0,
            },
            self.KEY_TOTAL_SPACE: 0,
            self.KEY_FREE_SPACE: 0,

            self.KEY_DISK_UTIL: 0.0,
            self.KEY_DISK_UTIL_READ: 0.0,
            self.KEY_DISK_UTIL_WRITE: 0.0,

            self.KEY_DISK_READ_RATE: 0.0,
            self.KEY_DISK_WRITE_RATE: 0.0,
        }

        self.commands_stat = storage.CommandsStat()

    def update(self, data):
        self.data = data

    @property
    def ts(self):
        return mh.elliptics_time_to_ts(self.data[self.KEY_TIMESTAMP])

    @property
    def total_space(self):
        return self.data[self.KEY_TOTAL_SPACE]

    @property
    def free_space(self):
        return self.data[self.KEY_FREE_SPACE]

    @property
    def disk_util(self):
        return self.data[self.KEY_DISK_UTIL]

    @property
    def disk_util_read(self):
        return self.data[self.KEY_DISK_UTIL_READ]

    @property
    def disk_util_write(self):
        return self.data[self.KEY_DISK_UTIL_WRITE]

    @property
    def disk_read_rate(self):
        return self.data[self.KEY_DISK_READ_RATE]

    @property
    def disk_write_rate(self):
        return self.data[self.KEY_DISK_WRITE_RATE]


class GroupBase(object):

    __slots__ = (
        'data',
        'group_id',
        'couple',
        'active_job',
        'node_backends',
    )

    KEY_STATUS = 'status'
    KEY_STATUS_TEXT = 'status_text'
    KEY_META = 'metadata'
    KEY_TYPE = 'type'

    def __init__(self, group_id, node_backends=None):
        self.group_id = group_id
        self.node_backends = []

        self.data = {
            self.KEY_STATUS: storage.Status.INIT,
            self.KEY_STATUS_TEXT: 'Group {} is not inititalized yet'.format(self),
            self.KEY_META: None,
            self.KEY_TYPE: storage.Group.TYPE_UNKNOWN,
        }
        self.active_job = None
        self.couple = None

        for node_backend in node_backends or []:
            self.add_node_backend(node_backend)

    @status_change_log
    def update(self, data):
        self.data = data

    @property
    def status(self):
        return self.data[self.KEY_STATUS]

    @property
    def status_text(self):
        return self.data[self.KEY_STATUS_TEXT]

    @property
    def meta(self):
        return self.data[self.KEY_META]

    @property
    def type(self):
        return self.data[self.KEY_TYPE]
