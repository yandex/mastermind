import errno
import logging
import os.path
import time

import msgpack

import helpers as h
from infrastructure_cache import cache
from mastermind_core.config import config
import storage


logger = logging.getLogger('mm.storage')


NODE_BACKEND_STAT_STALE_TIMEOUT = config.get('node_backend_stat_stale_timeout', 120)


class Host(object):
    def __init__(self, addr):
        self.addr = addr
        self.nodes = []

    @property
    def hostname(self):
        return cache.get_hostname_by_addr(self.addr)

    @property
    def hostname_or_not(self):
        return cache.get_hostname_by_addr(self.addr, strict=False)

    @property
    def dc(self):
        return cache.get_dc_by_host(self.hostname)

    @property
    def dc_or_not(self):
        return cache.get_dc_by_host(self.hostname, strict=False)

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
    def __init__(self):
        self.ts = None

        self.free_space, self.total_space, self.used_space = 0, 0, 0
        self.vfs_free_space, self.vfs_total_space, self.vfs_used_space = 0, 0, 0

        self.commands_stat = storage.CommandsStat()

        self.fragmentation = 0.0
        self.files = 0
        self.files_removed, self.files_removed_size = 0, 0

        self.defrag_state = None
        self.want_defrag = 0

        self.blob_size_limit = 0
        self.max_blob_base_size = 0
        self.blob_size = 0

        self.start_stat_commit_err_count = 0
        self.cur_stat_commit_err_count = 0

        self.io_blocking_size = 0
        self.io_nonblocking_size = 0

        self.backend_start_ts = 0

        self.stat_commit_errors = 0

    def update(self, raw_stat, collect_ts):
        self.ts = collect_ts

        self.vfs_total_space = raw_stat['backend']['vfs']['blocks'] * \
            raw_stat['backend']['vfs']['bsize']
        self.vfs_free_space = raw_stat['backend']['vfs']['bavail'] * \
            raw_stat['backend']['vfs']['bsize']
        self.vfs_used_space = self.vfs_total_space - self.vfs_free_space

        self.files = raw_stat['backend']['summary_stats']['records_total'] - \
            raw_stat['backend']['summary_stats']['records_removed']
        self.files_removed = raw_stat['backend']['summary_stats']['records_removed']
        self.files_removed_size = raw_stat['backend']['summary_stats']['records_removed_size']
        self.fragmentation = float(self.files_removed) / ((self.files + self.files_removed) or 1)

        self.defrag_state = raw_stat['status']['defrag_state']
        self.want_defrag = raw_stat['backend']['summary_stats']['want_defrag']

        self.blob_size_limit = raw_stat['backend']['config'].get('blob_size_limit', 0)
        if self.blob_size_limit > 0:
            self.total_space = self.blob_size_limit
            self.used_space = raw_stat['backend']['summary_stats'].get('base_size', 0)
            self.free_space = min(max(0, self.total_space - self.used_space), self.vfs_free_space)
        else:
            self.total_space = self.vfs_total_space
            self.free_space = self.vfs_free_space
            self.used_space = self.vfs_used_space

        if len(raw_stat['backend'].get('base_stats', {})):
            self.max_blob_base_size = max(
                [blob_stat['base_size']
                    for blob_stat in raw_stat['backend']['base_stats'].values()])
        else:
            self.max_blob_base_size = 0

        self.blob_size = raw_stat['backend']['config']['blob_size']

        self.io_blocking_size = raw_stat['io']['blocking']['current_size']
        self.io_nonblocking_size = raw_stat['io']['nonblocking']['current_size']

        self.cur_stat_commit_err_count = raw_stat['stats'].get(
            'stat_commit', {}).get('errors', {}).get(errno.EROFS, 0)

        if self.backend_start_ts < raw_stat['status']['last_start']['tv_sec']:
            self.backend_start_ts = raw_stat['status']['last_start']['tv_sec']
            self._reset_stat_commit_errors()

        if self.cur_stat_commit_err_count < self.start_stat_commit_err_count:
            self._reset_stat_commit_errors()

        self.stat_commit_errors = self.cur_stat_commit_err_count - self.start_stat_commit_err_count

        self.commands_stat.update(raw_stat['commands'], collect_ts)

    def _reset_stat_commit_errors(self):
        self.start_stat_commit_err_count = self.cur_stat_commit_err_count

    def __add__(self, other):
        res = NodeBackendStat()

        res.ts = min(self.ts, other.ts)

        res.total_space = self.total_space + other.total_space
        res.free_space = self.free_space + other.free_space
        res.used_space = self.used_space + other.used_space

        res.files = self.files + other.files
        res.files_removed = self.files_removed + other.files_removed
        res.files_removed_size = self.files_removed_size + other.files_removed_size
        res.fragmentation = float(res.files_removed) / (res.files_removed + res.files or 1)

        res.blob_size_limit = min(self.blob_size_limit, other.blob_size_limit)
        res.max_blob_base_size = max(self.max_blob_base_size, other.max_blob_base_size)
        res.blob_size = max(self.blob_size, other.blob_size)

        res.io_blocking_size = max(self.io_blocking_size, other.io_blocking_size)
        res.io_nonblocking_size = max(self.io_nonblocking_size, other.io_nonblocking_size)

        return res

    def __mul__(self, other):
        res = NodeBackendStat()

        res.ts = min(self.ts, other.ts)

        res.total_space = min(self.total_space, other.total_space)
        res.free_space = min(self.free_space, other.free_space)
        res.used_space = max(self.used_space, other.used_space)

        # files and files_removed are taken from the stat object with maximum
        # total number of keys. If total number of keys is equal,
        # the stat object with larger number of removed keys is more up-to-date
        files_stat = max(self, other, key=lambda stat: (stat.files + stat.files_removed, stat.files_removed))
        res.files = files_stat.files
        res.files_removed = files_stat.files_removed
        res.files_removed_size = max(self.files_removed_size, other.files_removed_size)

        # ATTENTION: fragmentation coefficient in this case would not necessary
        # be equal to [removed keys / total keys]
        res.fragmentation = max(self.fragmentation, other.fragmentation)

        res.blob_size_limit = min(self.blob_size_limit, other.blob_size_limit)
        res.max_blob_base_size = max(self.max_blob_base_size, other.max_blob_base_size)
        res.blob_size = max(self.blob_size, other.blob_size)

        res.io_blocking_size = max(self.io_blocking_size, other.io_blocking_size)
        res.io_nonblocking_size = max(self.io_nonblocking_size, other.io_nonblocking_size)

        return res


class NodeBackendBase(object):

    __slots__ = (
        'node',
        'backend_id',
        'fs',
        'group',
        'stat',

        'disabled',
        'start_ts',
        'stalled',

        'read_only',
        'base_path',
        'status',
        'status_text',
    )

    def __init__(self, node, backend_id):

        self.node = node
        self.backend_id = backend_id

        self.fs = None
        self.group = None
        self.stat = None

        self.disabled = False
        self.start_ts = 0
        self.stalled = False

        self.read_only = False
        self.base_path = None
        self.status = storage.Status.INIT
        self.status_text = "Node %s is not inititalized yet" % (self.__str__())

    def disable(self):
        self.disabled = True

    def enable(self):
        self.disabled = False

    def make_read_only(self):
        self.read_only = True

    def make_writable(self):
        self.read_only = False

    def update_statistics(self, new_stat, collect_ts):
        if self.stat is None:
            self.stat = NodeBackendStat()
        self.base_path = os.path.dirname(new_stat['backend']['config'].get('data') or
                                         new_stat['backend']['config'].get('file')) + '/'
        self.stat.update(new_stat, collect_ts)

    def update_status(self):
        if not self.stat:
            self.status = storage.Status.INIT
            self.status_text = 'No statistics gathered for node backend {0}'.format(self.__str__())

        elif self.disabled:
            self.status = storage.Status.STALLED
            self.status_text = 'Node backend {0} has been disabled'.format(str(self))

        elif self.stalled:
            self.status = storage.Status.STALLED
            self.status_text = ('Statistics for node backend {} is too old: '
                                'it was gathered {} seconds ago'.format(
                                    self.__str__(), int(time.time() - self.stat.ts)))

        elif self.fs.status == storage.Status.BROKEN:
            self.status = storage.Status.BROKEN
            self.status_text = ("Node backends' space limit is not properly "
                                "configured on fs {0}".format(self.fs.fsid))

        elif self.read_only:
            self.status = storage.Status.RO
            self.status_text = 'Node backend {0} is in read-only state'.format(str(self))

        else:
            self.status = storage.Status.OK
            self.status_text = 'Node {0} is OK'.format(str(self))

        return self.status

    def update_statistics_status(self):
        if not self.stat:
            return

        self.stalled = self.stat.ts < (time.time() - NODE_BACKEND_STAT_STALE_TIMEOUT)


class FsBase(object):

    __slots__ = (
        'host',
        'fsid',
        'node_backends',
        'stat',
        'status',
    )

    def __init__(self, host, fsid):
        self.host = host
        self.fsid = fsid
        self.status = storage.Status.OK

        self.node_backends = {}

        self.stat = None

    def update_statistics(self, new_stat, collect_ts):
        if self.stat is None:
            self.stat = FsStat()
        self.stat.update(self, new_stat, collect_ts)

    def update_commands_stats(self):
        self.stat.update_commands_stats(self.node_backends)

    def update_status(self):
        nbs = self.node_backends.keys()
        prev_status = self.status

        total_space = 0
        for nb in nbs:
            if nb.status not in (storage.Status.OK, storage.Status.BROKEN):
                continue
            total_space += nb.stat.total_space

        if total_space > self.stat.total_space:
            self.status = storage.Status.BROKEN
        else:
            self.status = storage.Status.OK

        # TODO: unwind cycle dependency between node backend status and fs
        # status. E.g., check node backend status and file system status
        # separately on group status updating.

        if self.status != prev_status:
            logger.info('Changing status of fs {0}, affecting node backends {1}'.format(
                self.fsid, [str(nb) for nb in nbs]))
            for nb in nbs:
                nb.update_status()


class FsStat(object):

    SECTOR_BYTES = 512

    def __init__(self):
        self.ts = None
        self.total_space = 0

        self.dstat = {}
        self.vfs_stat = {}
        self.disk_util = 0.0
        self.disk_util_read = 0.0
        self.disk_util_write = 0.0

        self.disk_read_rate = 0.0
        self.disk_write_rate = 0.0

        self.commands_stat = storage.CommandsStat()

    def apply_new_dstat(self, new_dstat):
        if not self.dstat.get('ts'):
            return
        diff_ts = new_dstat['ts'] - self.dstat['ts']
        if diff_ts <= 1.0:
            return

        disk_util = h.unidirectional_value_map(
            self.disk_util,
            self.dstat['io_ticks'],
            new_dstat['io_ticks'],
            func=lambda ov, nv: (nv - ov) / diff_ts / float(10 ** 3)
        )
        self.disk_util = disk_util

        read_ticks = new_dstat['read_ticks'] - self.dstat['read_ticks']
        write_ticks = new_dstat['write_ticks'] - self.dstat['write_ticks']
        total_rw_ticks = read_ticks + write_ticks
        self.disk_util_read = h.unidirectional_value_map(
            self.disk_util_read,
            self.dstat['read_ticks'],
            new_dstat['read_ticks'],
            func=lambda ov, nv: (total_rw_ticks and
                                 disk_util * (nv - ov) / float(total_rw_ticks) or
                                 0.0)
        )
        self.disk_util_write = h.unidirectional_value_map(
            self.disk_util_write,
            self.dstat['write_ticks'],
            new_dstat['write_ticks'],
            func=lambda ov, nv: (total_rw_ticks and
                                 disk_util * (nv - ov) / float(total_rw_ticks) or
                                 0.0)
        )
        self.disk_read_rate = h.unidirectional_value_map(
            self.disk_read_rate,
            self.dstat['read_sectors'],
            new_dstat['read_sectors'],
            func=lambda ov, nv: (nv - ov) * self.SECTOR_BYTES / float(diff_ts)
        )

    def apply_new_vfs_stat(self, new_vfs_stat):
        new_free_space = new_vfs_stat['bavail'] * new_vfs_stat['bsize']
        if self.vfs_stat.get('ts'):
            diff_ts = new_vfs_stat['ts'] - self.vfs_stat['ts']
            if diff_ts > 1.0:
                written_bytes = self.free_space - new_free_space
                self.disk_write_rate = written_bytes / diff_ts

        self.total_space = new_vfs_stat['blocks'] * new_vfs_stat['bsize']
        self.free_space = new_free_space

    def update(self, fs, raw_stat, collect_ts):
        self.ts = collect_ts
        vfs_stat = raw_stat['vfs']
        dstat_stat = raw_stat['dstat']

        if 'error' in dstat_stat:
            # do not update state
            new_dstat = {}
            logger.error(
                '{fs}: dstat error: {dstat}'.format(
                    fs=fs,
                    dstat=dstat_stat,
                )
            )
        else:
            new_dstat = dstat_stat
            new_dstat['ts'] = (dstat_stat['timestamp']['tv_sec'] +
                               dstat_stat['timestamp']['tv_usec'] / float(10 ** 6))
            self.apply_new_dstat(new_dstat)

        self.dstat = new_dstat

        if 'error' in vfs_stat:
            # do not update state
            new_vfs_stat = {}
            logger.error(
                '{fs}: vfs stat error: {vfs_stat}'.format(
                    fs=fs,
                    vfs_stat=vfs_stat,
                )
            )
        else:
            new_vfs_stat = vfs_stat
            new_vfs_stat['ts'] = (vfs_stat['timestamp']['tv_sec'] +
                                  vfs_stat['timestamp']['tv_usec'] / float(10 ** 6))
            self.apply_new_vfs_stat(new_vfs_stat)

        self.vfs_stat = new_vfs_stat

    def update_commands_stats(self, node_backends):
        self.commands_stat = sum((nb.stat.commands_stat for nb in node_backends), storage.CommandsStat())


class GroupBase(object):

    def __init__(self, group_id, node_backends=None):
        self.group_id = group_id
        self.status = storage.Status.INIT
        self.node_backends = []
        self.couple = None
        self.meta = None
        self.status_text = "Group %s is not inititalized yet" % (self)
        self.active_job = None

        self._type = storage.Group.TYPE_UNKNOWN

        for node_backend in node_backends or []:
            self.add_node_backend(node_backend)

    @property
    def type(self):
        """ Return current group type

        Groups can have different types depending
        on the type of data they store or state they are in:
            - unknown - group is known of but metakey was not read;
            - uncoupled - group has no metakey (but can be assigned to a couple,
            so don't forget to check status);
            - data - group has metakey and belongs to a regular data couple;
            - uncoupled_cache - group has no metakey but its path matches cache
            groups' path from config (such group should be marked as 'cache');
            - cache - group stores gatlinggun cache data;
        """
        if self._type == storage.Group.TYPE_UNCOUPLED and self.couple:
            # This is a special case for couples with some groups
            # with empty meta key.
            # When group belongs to a couple but is disabled or unavailable,
            # it can be added to Storage before the group that it is coupled
            # with or after. In former case mastermind cannot determine that
            # this group is actually a 'data' group so it considers it
            # 'uncoupled' one. But when cluster traversal is over mastermind has
            # enough data for this group to become a 'data' one (since it now
            # should have 'couple' attribute set up).
            # NOTE: this condition can be evaluated to true only once after this
            # group was added to Storage.
            self._type = self._get_type(self.meta)
        return self._type

    def _get_type(self, meta):

        if self.meta:
            if 'type' in self.meta and self.meta['type'] not in self.AVAILABLE_TYPES:
                logger.error('Unknown type "{type}" of group {group}'.format(
                    group=self,
                    type=self.meta['type'],
                ))
                return self.TYPE_UNKNOWN

            return self.meta.get('type', self.TYPE_DATA)

        else:

            if self.couple:
                return storage.Group.TYPE_DATA

            def is_cache_group_backend(nb):
                if not storage.CACHE_GROUP_PATH_PREFIX:
                    return False
                return nb.base_path.startswith(storage.CACHE_GROUP_PATH_PREFIX)

            is_uncoupled_cache_group = any(is_cache_group_backend(nb) for nb in self.node_backends)
            if is_uncoupled_cache_group:
                return self.TYPE_UNCOUPLED_CACHE

            return storage.Group.TYPE_UNCOUPLED

    def reset_meta(self):
        self.meta = None

    def parse_meta(self, raw_meta):

        if raw_meta is None:
            self.meta = None
        else:
            meta = msgpack.unpackb(raw_meta)

            if isinstance(meta, (tuple, list)):
                self.meta = {'version': 1, 'couple': meta, 'namespace': self.DEFAULT_NAMESPACE, 'frozen': False}
            elif isinstance(meta, dict) and meta['version'] == 2:
                self.meta = meta
            else:
                raise Exception('Unable to parse meta')

        self._type = self._get_type(self.meta)
        logger.debug(
            'Group {group}: meta parsed, group type is determined as "{type}"'.format(
                group=self,
                type=self._type,
            )
        )

    def equal_meta(self, other):
        if type(self.meta) != type(other.meta):
            return False
        if self.meta is None:
            return True

        negligeable_keys = ['service', 'version']
        for key in set(self.meta.keys() + other.meta.keys()):
            if key in negligeable_keys:
                continue
            if self.meta.get(key) != other.meta.get(key):
                return False

        return True

    def update_status_recursive(self):
        if self.couple:
            self.couple.update_status()
            # update status of a couple if group is a part of a groupset
            if self.couple.couple is not self.couple:
                # self.couple is actually a groupset, and self.couple.couple is a couple
                # TODO: replace self.couple with a groupset when new Couple object is
                # implemented
                self.couple.couple.update_status()
        else:
            self.update_status()

    def update_status(self):
        """Updates group's own status.
        WARNING: This method should not take into consideration any of the
        groups' state coupled with itself nor any of the couple attributes,
        properties, state, etc."""

        if not self.node_backends:
            logger.info('Group {0}: no node backends, status set to INIT'.format(self.group_id))
            self.status = storage.Status.INIT
            self.status_text = ('Group {0} is in INIT state because there is '
                                'no node backends serving this group'.format(self))
            return self.status

        # TODO: add status INIT for group if .couple is None

        if storage.FORBIDDEN_DHT_GROUPS and len(self.node_backends) > 1:
            self.status = storage.Status.BROKEN
            self.status_text = ('Group {} is in BROKEN state because '
                                'is has {} node backends but only 1 is allowed'.format(
                                    self.group_id, len(self.node_backends)))
            return self.status

        # node statuses should be updated before group status is set
        # statuses = tuple(nb.update_status() for nb in self.node_backends)
        statuses = tuple(nb.status for nb in self.node_backends)

        if not self.meta:
            self.status = storage.Status.INIT
            self.status_text = ('Group {0} is in INIT state because meta key '
                                'was not read from it'.format(self))
            return self.status

        if storage.Status.BROKEN in statuses:
            self.status = storage.Status.BROKEN
            self.status_text = ('Group {0} has BROKEN status because '
                                'some node statuses are BROKEN'.format(self))
            return self.status

        if self.type == self.TYPE_DATA:
            # perform checks for common data group
            status = self.update_storage_group_status()
            if status:
                return status
        elif self.type == self.TYPE_CACHE:
            pass

        if storage.Status.RO in statuses:
            self.status = storage.Status.RO
            self.status_text = ('Group {0} is in Read-Only state because '
                                'there is RO node backends'.format(self))

            service_status = self.meta.get('service', {}).get('status')
            if service_status == storage.Status.MIGRATING:
                if self.active_job and self.meta['service']['job_id'] == self.active_job['id']:
                    self.status = storage.Status.MIGRATING
                    self.status_text = ('Group {0} is migrating, job id is {1}'.format(
                        self, self.meta['service']['job_id']))
                else:
                    self.status = storage.Status.BAD
                    self.status_text = ('Group {0} has no active job, but marked as '
                                        'migrating by job id {1}'.format(
                                            self, self.meta['service']['job_id']))

            return self.status

        if not all(st == storage.Status.OK for st in statuses):
            self.status = storage.Status.BAD
            self.status_text = ('Group {0} is in Bad state because '
                                'some node statuses are not OK'.format(self))
            return self.status

        self.status = storage.Status.COUPLED
        self.status_text = 'Group {0} is OK'.format(self)

        return self.status

    def update_storage_group_status(self):
        if not self.meta['couple']:
            self.status = storage.Status.INIT
            self.status_text = ('Group {0} is in INIT state because there is '
                                'no coupling info'.format(self))
            return self.status

        if not self.couple:
            self.status = storage.Status.BAD
            self.status_text = ('Group {0} is in Bad state because '
                                'couple was not created'.format(self))
            return self.status

        if not self.couple.check_groups(self.meta['couple']):
            self.status = storage.Status.BAD
            self.status_text = ('Group {} is in Bad state because couple check fails'.format(self))
            return self.status

        if not self.meta.get('namespace'):
            self.status = storage.Status.BAD
            self.status_text = ('Group {0} is in Bad state because '
                                'no namespace has been assigned to it'.format(self))
            return self.status

        if self.group_id not in self.meta['couple']:
            self.status = storage.Status.BROKEN
            self.status_text = ('Group {0} is in BROKEN state because '
                                'its group id is missing from coupling info'.format(self))
            return self.status
