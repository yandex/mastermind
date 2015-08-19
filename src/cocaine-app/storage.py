# -*- coding: utf-8 -*-
import datetime
import errno
import functools
import logging
import math
import os.path
import time
import types

import msgpack

from errors import CacheUpstreamError
import jobs.job
from jobs.job_types import JobTypes
from infrastructure import infrastructure
from infrastructure_cache import cache
from config import config
from mastermind.query.couples import Couple as CoupleInfo
from mastermind.query.groups import Group as GroupInfo

logger = logging.getLogger('mm.storage')


RPS_FORMULA_VARIANT = config.get('rps_formula', 0)
VFS_RESERVED_SPACE = config.get('reserved_space', 112742891520)  # default is 105 Gb for one vfs
NODE_BACKEND_STAT_STALE_TIMEOUT = config.get('node_backend_stat_stale_timeout', 120)

FORBIDDEN_DHT_GROUPS = config.get('forbidden_dht_groups', False)
FORBIDDEN_DC_SHARING_AMONG_GROUPS = config.get('forbidden_dc_sharing_among_groups', False)
FORBIDDEN_NS_WITHOUT_SETTINGS = config.get('forbidden_ns_without_settings', False)
FORBIDDEN_UNMATCHED_GROUP_TOTAL_SPACE = config.get('forbidden_unmatched_group_total_space', False)

CACHE_GROUP_PATH_PREFIX = config.get('cache', {}).get('group_path_prefix')


def ts_str(ts):
    return time.asctime(time.localtime(ts))


class Status(object):
    INIT = 'INIT'
    OK = 'OK'
    FULL = 'FULL'
    COUPLED = 'COUPLED'
    BAD = 'BAD'
    BROKEN = 'BROKEN'
    RO = 'RO'
    STALLED = 'STALLED'
    FROZEN = 'FROZEN'

    MIGRATING = 'MIGRATING'

    SERVICE_ACTIVE = 'SERVICE_ACTIVE'
    SERVICE_STALLED = 'SERVICE_STALLED'


GOOD_STATUSES = set([Status.OK, Status.FULL])
NOT_BAD_STATUSES = set([Status.OK, Status.FULL, Status.FROZEN])


class ResourceError(KeyError):
    def __str__(self):
        return str(self.args[0])


class Repositary(object):
    def __init__(self, constructor, resource_desc=None):
        self.elements = {}
        self.constructor = constructor
        self.resource_desc = resource_desc or self.constructor.__name__

    def add(self, *args, **kwargs):
        e = self.constructor(*args, **kwargs)
        self.elements[e] = e
        return e

    def get(self, key):
        try:
            return self.elements[key]
        except KeyError:
            raise ResourceError('{} {} is not found'.format(
                self.resource_desc, key))

    def remove(self, key):
        return self.elements.pop(key)

    def __getitem__(self, key):
        return self.get(key)

    def __contains__(self, key):
        return key in self.elements

    def __iter__(self):
        return self.elements.__iter__()

    def __repr__(self):
        return '<Repositary object: [%s] >' % (', '.join((repr(e) for e in self.elements.itervalues())))

    def keys(self):
        return self.elements.keys()

    def __len__(self):
        return len(self.elements)


class NodeStat(object):
    def __init__(self):
        self.ts = None
        self.load_average = 0.0
        self.tx_bytes, self.rx_bytes = None, None
        self.tx_rate, self.rx_rate = None, None

    def update(self, raw_stat, collect_ts):
        self.load_average = float(raw_stat['procfs']['vm']['la'][0]) / 100
        interfaces = raw_stat['procfs'].get('net', {}).get('net_interfaces', {})

        new_rx_bytes = sum(map(
            lambda if_: if_[1].get('receive', {}).get('bytes', 0.0) if if_[0] != 'lo' else 0.0,
            interfaces.items()))
        new_tx_bytes = sum(map(
            lambda if_: if_[1].get('transmit', {}).get('bytes', 0.0) if if_[0] != 'lo' else 0.0,
            interfaces.items()))

        if self.ts is not None and collect_ts > self.ts:
            # conditions are checked for the case of *x_bytes counter overflow
            if new_tx_bytes >= self.tx_bytes:
                self.tx_rate = float(new_tx_bytes - self.tx_bytes) / (collect_ts - self.ts)
            if new_rx_bytes >= self.rx_bytes:
                self.rx_rate = float(new_rx_bytes - self.rx_bytes) / (collect_ts - self.ts)

        self.tx_bytes = new_tx_bytes
        self.rx_bytes = new_rx_bytes

        self.ts = collect_ts

    def __add__(self, other):
        res = NodeStat()

        res.ts = min(self.ts, other.ts)
        res.load_average = max(self.load_average, other.load_average)

        return res

    def __mul__(self, other):
        res = NodeStat()
        res.ts = min(self.ts, other.ts)
        res.load_average = max(self.load_average, other.load_average)

        return res


class NodeBackendStat(object):
    def __init__(self, node_stat):
        self.node_stat = node_stat
        self.ts = None

        self.free_space, self.total_space, self.used_space = 0, 0, 0
        self.vfs_free_space, self.vfs_total_space, self.vfs_used_space = 0, 0, 0

        self.last_read, self.last_write = 0, 0
        self.read_rps, self.write_rps = 0, 0

        # Tupical SATA HDD performance is 100 IOPS
        # It will be used as first estimation for maximum node performance
        self.max_read_rps, self.max_write_rps = 100, 100

        self.fragmentation = 0.0
        self.files = 0
        self.files_removed, self.files_removed_size = 0, 0

        self.fsid = None
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

    def update(self, raw_stat, collect_ts):

        if self.ts and collect_ts > self.ts:
            dt = collect_ts - self.ts

            if 'dstat' in raw_stat['backend'] and 'error' not in raw_stat['backend']['dstat']:
                last_read = raw_stat['backend']['dstat']['read_ios']
                last_write = raw_stat['backend']['dstat']['write_ios']

                self.read_rps = (last_read - self.last_read) / dt
                self.write_rps = (last_write - self.last_write) / dt

                # Disk usage should be used here instead of load average
                self.max_read_rps = self.max_rps(self.read_rps, self.node_stat.load_average)
                self.max_write_rps = self.max_rps(self.write_rps, self.node_stat.load_average)

                self.last_read = last_read
                self.last_write = last_write

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

        self.fsid = raw_stat['backend']['vfs']['fsid']
        self.defrag_state = raw_stat['status']['defrag_state']
        self.want_defrag = raw_stat['backend']['summary_stats']['want_defrag']

        self.blob_size_limit = raw_stat['backend']['config'].get('blob_size_limit', 0)
        if self.blob_size_limit > 0:
            # vfs_total_space can be less than blob_size_limit in case of misconfiguration
            self.total_space = min(self.blob_size_limit, self.vfs_total_space)
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

    def _reset_stat_commit_errors(self):
        self.start_stat_commit_err_count = self.cur_stat_commit_err_count

    @property
    def stat_commit_errors(self):
        if self.cur_stat_commit_err_count < self.start_stat_commit_err_count:
            self._reset_stat_commit_errors()
        return self.cur_stat_commit_err_count - self.start_stat_commit_err_count

    def max_rps(self, rps, load_avg, variant=RPS_FORMULA_VARIANT):

        if variant == 0:
            return max(rps / max(load_avg, 0.01), 100)

        rps = max(rps, 1)
        max_avg_norm = 10.0
        avg = max(min(float(load_avg), 100.0), 0.0) / max_avg_norm
        avg_inverted = 10.0 - avg

        if variant == 1:
            max_rps = ((rps + avg_inverted) ** 2) / rps
        elif variant == 2:
            max_rps = ((avg_inverted) ** 2) / rps
        else:
            raise ValueError('Unknown max_rps option: %s' % variant)

        return max_rps

    def __add__(self, other):
        node_stat = self.node_stat + other.node_stat
        res = NodeBackendStat(node_stat)

        res.ts = min(self.ts, other.ts)

        res.node_stat = node_stat

        res.total_space = self.total_space + other.total_space
        res.free_space = self.free_space + other.free_space
        res.used_space = self.used_space + other.used_space

        res.read_rps = self.read_rps + other.read_rps
        res.write_rps = self.write_rps + other.write_rps

        res.max_read_rps = self.max_read_rps + other.max_read_rps
        res.max_write_rps = self.max_write_rps + other.max_write_rps

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
        node_stat = self.node_stat * other.node_stat
        res = NodeBackendStat(node_stat)

        res.ts = min(self.ts, other.ts)

        res.node_stat = node_stat

        res.total_space = min(self.total_space, other.total_space)
        res.free_space = min(self.free_space, other.free_space)
        res.used_space = max(self.used_space, other.used_space)

        res.read_rps = max(self.read_rps, other.read_rps)
        res.write_rps = max(self.write_rps, other.write_rps)

        res.max_read_rps = min(self.max_read_rps, other.max_read_rps)
        res.max_write_rps = min(self.max_write_rps, other.max_write_rps)

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

    def __repr__(self):
        return ('<NodeBackendStat object: ts=%s, write_rps=%d, max_write_rps=%d, read_rps=%d, '
                'max_read_rps=%d, total_space=%d, free_space=%d, files=%s, files_removed=%s, '
                'fragmentation=%s, node_load_average=%s>' % (
                    ts_str(self.ts), self.write_rps, self.max_write_rps, self.read_rps,
                    self.max_read_rps, self.total_space, self.free_space, self.files,
                    self.files_removed, self.fragmentation, self.node_stat.load_average))


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
        if isinstance(other, str):
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


class Node(object):
    def __init__(self, host, port, family):
        self.host = host
        self.port = int(port)
        self.family = int(family)
        self.host.nodes.append(self)

        self.stat = None

    def update_statistics(self, new_stat, collect_ts):
        if self.stat is None:
            self.stat = NodeStat()
        self.stat.update(new_stat, collect_ts)

    def __repr__(self):
        return '<Node object: host={host}, port={port}>'.format(
            host=str(self.host), port=self.port)

    def __str__(self):
        return '{host}:{port}'.format(host=self.host.addr, port=self.port)

    def __hash__(self):
        return hash(self.__str__())

    def __eq__(self, other):
        if isinstance(other, (str, unicode)):
            return self.__str__() == other

        if isinstance(other, Node):
            return self.host.addr == other.host.addr and self.port == other.port


class FsStat(object):
    def __init__(self):
        self.ts = None
        self.total_space = 0
        self.io_ticks = None

    def update(self, raw_stat, collect_ts):
        self.ts = collect_ts
        vfs_stat = raw_stat['vfs']
        self.total_space = vfs_stat['blocks'] * vfs_stat['bsize']


class Fs(object):
    def __init__(self, host, fsid):
        self.host = host
        self.fsid = fsid
        self.status = Status.OK

        self.node_backends = {}

        self.stat = None

    def add_node_backend(self, nb):
        if nb.fs:
            nb.fs.remove_node_backend(nb)
        self.node_backends[nb] = nb
        nb.fs = self

    def remove_node_backend(self, nb):
        del self.node_backends[nb]

    def update_statistics(self, new_stat, collect_ts):
        if self.stat is None:
            self.stat = FsStat()
        self.stat.update(new_stat, collect_ts)

    def update_status(self):
        nbs = self.node_backends.keys()
        prev_status = self.status

        total_space = 0
        for nb in nbs:
            if nb.status not in (Status.OK, Status.BROKEN):
                continue
            total_space += nb.stat.total_space

        if total_space > self.stat.total_space:
            self.status = Status.BROKEN
        else:
            self.status = Status.OK

        if self.status != prev_status:
            logger.info('Changing status of fs {0}, affecting node backends {1}'.format(
                self.fsid, [str(nb) for nb in nbs]))
            for nb in nbs:
                nb.update_status()

    def __repr__(self):
        return '<Fs object: host={host}, fsid={fsid}>'.format(
            host=str(self.host), fsid=self.fsid)

    def __str__(self):
        return '{host}:{fsid}'.format(host=self.host.addr, fsid=self.fsid)

    def __hash__(self):
        return hash(self.__str__())

    def __eq__(self, other):
        if isinstance(other, (str, unicode)):
            return self.__str__() == other

        if isinstance(other, Fs):
            return self.host.addr == other.host.addr and self.fsid == other.fsid


class NodeBackend(object):

    ACTIVE_STATUSES = (Status.OK, Status.RO, Status.BROKEN)

    def __init__(self, node, backend_id):

        self.node = node
        self.backend_id = backend_id
        self.fs = None
        self.group = None

        self.stat = None

        self.read_only = False
        self.disabled = False
        self.start_ts = 0
        self.status = Status.INIT
        self.status_text = "Node %s is not inititalized yet" % (self.__str__())

        self.stalled = False

        self.base_path = None

    def set_group(self, group):
        self.group = group

    def remove_group(self):
        self.group = None

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
            self.stat = NodeBackendStat(self.node.stat)
        self.stat.update(new_stat, collect_ts)
        self.base_path = os.path.dirname(new_stat['backend']['config'].get('data') or
                                         new_stat['backend']['config'].get('file')) + '/'

    def update_status(self):
        if not self.stat:
            self.status = Status.INIT
            self.status_text = 'No statistics gathered for node backend {0}'.format(self.__str__())

        elif self.disabled:
            self.status = Status.STALLED
            self.status_text = 'Node backend {0} has been disabled'.format(str(self))

        elif self.stalled:
            self.status = Status.STALLED
            self.status_text = ('Statistics for node backend {} is too old: '
                                'it was gathered {} seconds ago'.format(
                                    self.__str__(), int(time.time() - self.stat.ts)))

        elif self.fs.status == Status.BROKEN:
            self.status = Status.BROKEN
            self.status_text = ("Node backends' space limit is not properly "
                                "configured on fs {0}".format(self.fs.fsid))

        elif self.read_only:
            self.status = Status.RO
            self.status_text = 'Node backend {0} is in read-only state'.format(str(self))

        else:
            self.status = Status.OK
            self.status_text = 'Node {0} is OK'.format(str(self))

        return self.status

    def update_statistics_status(self):
        if not self.stat:
            return

        self.stalled = self.stat.ts < (time.time() - NODE_BACKEND_STAT_STALE_TIMEOUT)

    @property
    def effective_space(self):

        if self.stat is None:
            return 0

        share = float(self.stat.total_space) / self.stat.vfs_total_space
        free_space_req_share = math.ceil(VFS_RESERVED_SPACE * share)

        return int(max(0, self.stat.total_space - free_space_req_share))

    @property
    def effective_free_space(self):
        if self.stat.vfs_free_space <= VFS_RESERVED_SPACE:
            return 0
        return max(
            self.stat.free_space - (self.stat.total_space - self.effective_space),
            0
        )

    def is_full(self, reserved_space=0.0):

        if self.stat is None:
            return False

        assert 0.0 <= reserved_space <= 1.0, 'Reserved space should have non-negative value lte 1.0'

        if self.stat.used_space >= self.effective_space * (1.0 - reserved_space):
            return True
        if self.effective_free_space <= 0:
            return True
        return False

    @property
    def stat_commit_errors(self):
        return (self.stat and
                self.stat.stat_commit_errors or
                0)

    def info(self):
        res = {}

        res['node'] = '{0}:{1}:{2}'.format(self.node.host, self.node.port, self.node.family)
        res['backend_id'] = self.backend_id
        res['addr'] = str(self)
        res['hostname'] = self.node.host.hostname_or_not
        res['status'] = self.status
        res['status_text'] = self.status_text
        res['dc'] = self.node.host.dc_or_not
        res['last_stat_update'] = (
            self.stat and
            datetime.datetime.fromtimestamp(self.stat.ts).strftime('%Y-%m-%d %H:%M:%S') or
            'unknown')
        if self.node.stat:
            res['tx_rate'] = self.node.stat.tx_rate
            res['rx_rate'] = self.node.stat.rx_rate
        if self.stat:
            res['free_space'] = int(self.stat.free_space)
            res['effective_space'] = self.effective_space
            res['free_effective_space'] = self.effective_free_space
            res['used_space'] = int(self.stat.used_space)
            res['total_space'] = int(self.stat.total_space)
            res['total_files'] = self.stat.files + self.stat.files_removed
            res['records_alive'] = self.stat.files
            res['records_removed'] = self.stat.files_removed
            res['records_removed_size'] = self.stat.files_removed_size
            res['fragmentation'] = self.stat.fragmentation
            res['defrag_state'] = self.stat.defrag_state
            res['want_defrag'] = self.stat.want_defrag
            res['io_blocking_size'] = self.stat.io_blocking_size
            res['io_nonblocking_size'] = self.stat.io_nonblocking_size
        if self.base_path:
            res['path'] = self.base_path

        return res

    def __repr__(self):
        return ('<Node backend object: node=%s, backend_id=%d, '
                'status=%s, read_only=%s, stat=%s>' % (
                    str(self.node), self.backend_id,
                    self.status, str(self.read_only), repr(self.stat)))

    def __str__(self):
        return '%s:%d/%d' % (self.node.host.addr, self.node.port, self.backend_id)

    def __hash__(self):
        return hash(self.__str__())

    def __eq__(self, other):
        if isinstance(other, (str, unicode)):
            return self.__str__() == other

        if isinstance(other, NodeBackend):
            return (self.node.host.addr == other.node.host.addr and
                    self.node.port == other.node.port and
                    self.backend_id == other.backend_id)


class Group(object):

    DEFAULT_NAMESPACE = 'default'
    CACHE_NAMESPACE = 'storage_cache'

    TYPE_DATA = 'data'
    TYPE_CACHE = 'cache'
    TYPE_UNMARKED = 'unmarked'

    def __init__(self, group_id, node_backends=None):
        self.group_id = group_id
        self.status = Status.INIT
        self.node_backends = []
        self.couple = None
        self.meta = None
        self.status_text = "Group %s is not inititalized yet" % (self)
        self.active_job = None

        for node_backend in node_backends or []:
            self.add_node_backend(node_backend)

    def add_node_backend(self, node_backend):
        self.node_backends.append(node_backend)
        if node_backend.group:
            node_backend.group.remove_node_backend(node_backend)
        node_backend.set_group(self)

    def remove_node_backend(self, node_backend):
        self.node_backends.remove(node_backend)
        if node_backend.group is self:
            node_backend.remove_group()

    @property
    def want_defrag(self):
        for nb in self.node_backends:
            if nb.stat and nb.stat.want_defrag > 1:
                return True
        return False

    def parse_meta(self, meta):
        if meta is None:
            self.meta = None
            return

        parsed = msgpack.unpackb(meta)
        if isinstance(parsed, tuple):
            self.meta = {'version': 1, 'couple': parsed, 'namespace': self.DEFAULT_NAMESPACE, 'frozen': False}
        elif isinstance(parsed, dict) and parsed['version'] == 2:
            self.meta = parsed
        else:
            raise Exception('Unable to parse meta')

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

    def get_stat(self):
        return reduce(lambda res, x: res + x, [nb.stat for nb in self.node_backends if nb.stat])

    def update_status_recursive(self):
        if self.couple:
            self.couple.update_status()
        else:
            self.update_status()

    @property
    def effective_space(self):
        return sum([nb.effective_space for nb in self.node_backends])

    @property
    def type(self):
        if not self.meta:
            if (CACHE_GROUP_PATH_PREFIX and
                any([nb.base_path.startswith(CACHE_GROUP_PATH_PREFIX)
                     for nb in self.node_backends])):
                return self.TYPE_UNMARKED
            return self.TYPE_DATA
        if self.meta.get('type') == self.TYPE_CACHE:
            return self.TYPE_CACHE
        return self.TYPE_DATA

    def update_status(self):
        """Updates group's own status.
        WARNING: This method should not take into consideration any of the
        groups' state coupled with itself nor any of the couple attributes,
        properties, state, etc."""

        if not self.node_backends:
            logger.info('Group {0}: no node backends, status set to INIT'.format(self.group_id))
            self.status = Status.INIT
            self.status_text = ('Group {0} is in INIT state because there is '
                                'no node backends serving this group'.format(self))
            return self.status

        if FORBIDDEN_DHT_GROUPS and len(self.node_backends) > 1:
            self.status = Status.BROKEN
            self.status_text = ('Group {} is in BROKEN state because '
                                'is has {} node backends but only 1 is allowed'.format(
                                    self.group_id, len(self.node_backends)))
            return self.status

        # node statuses should be updated before group status is set
        # statuses = tuple(nb.update_status() for nb in self.node_backends)
        statuses = tuple(nb.status for nb in self.node_backends)

        logger.info('In group {0} meta = {1}'.format(self, str(self.meta)))
        if not self.meta:
            self.status = Status.INIT
            self.status_text = ('Group {0} is in INIT state because meta key '
                                'was not read from it'.format(self))
            return self.status

        if Status.BROKEN in statuses:
            self.status = Status.BROKEN
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

        if Status.RO in statuses:
            self.status = Status.RO
            self.status_text = ('Group {0} is in Read-Only state because '
                                'there is RO node backends'.format(self))

            service_status = self.meta.get('service', {}).get('status')
            if service_status == Status.MIGRATING:
                if self.active_job and self.meta['service']['job_id'] == self.active_job['id']:
                    self.status = Status.MIGRATING
                    self.status_text = ('Group {0} is migrating, job id is {1}'.format(
                        self, self.meta['service']['job_id']))
                else:
                    self.status = Status.BAD
                    self.status_text = ('Group {0} has no active job, but marked as '
                                        'migrating by job id {1}'.format(
                                            self, self.meta['service']['job_id']))

            return self.status

        if not all([st == Status.OK for st in statuses]):
            self.status = Status.BAD
            self.status_text = ('Group {0} is in Bad state because '
                                'some node statuses are not OK'.format(self))
            return self.status

        self.status = Status.COUPLED
        self.status_text = 'Group {0} is OK'.format(self)

        return self.status

    def update_storage_group_status(self):
        if not self.meta['couple']:
            self.status = Status.INIT
            self.status_text = ('Group {0} is in INIT state because there is '
                                'no coupling info'.format(self))
            return self.status

        if not self.couple and self.meta['couple']:
            self.status = Status.BAD
            self.status_text = ('Group {0} is in Bad state because '
                                'couple was not created'.format(self))
            return self.status

        if not self.couple.check_groups(self.meta['couple']):
            self.status = Status.BAD
            self.status_text = ('Group {} is in Bad state because couple check fails'.format(self))
            return self.status

        if not self.meta.get('namespace'):
            self.status = Status.BAD
            self.status_text = ('Group {0} is in Bad state because '
                                'no namespace has been assigned to it'.format(self))
            return self.status

        if self.group_id not in self.meta['couple']:
            self.status = Status.BROKEN
            self.status_text = ('Group {0} is in BROKEN state because '
                                'its group id is missing from coupling info'.format(self))
            return self.status

    def info(self):
        g = GroupInfo(self.group_id)
        data = {'id': self.group_id,
                'status': self.status,
                'status_text': self.status_text,
                'node_backends': [nb.info() for nb in self.node_backends],
                'couple': str(self.couple) if self.couple else None}
        if self.meta:
            data['namespace'] = self.meta.get('namespace')
        if self.active_job:
            data['active_job'] = self.active_job

        g._set_raw_data(data)
        return g

    def set_active_job(self, job):
        if job is None:
            self.active_job = None
            return
        self.active_job = {
            'id': job.id,
            'type': job.type,
            'status': job.status,
        }

    def compose_cache_group_meta(self):
        return {
            'version': 2,
            'type': self.TYPE_CACHE,
            'namespace': self.CACHE_NAMESPACE,
            'couple': (self.group_id,)
        }

    @property
    def coupled_groups(self):
        if not self.couple:
            return []

        return [g for g in self.couple if g is not self]

    def __hash__(self):
        return hash(self.group_id)

    def __str__(self):
        return str(self.group_id)

    def __repr__(self):
        return '<Group object: group_id=%d, status=%s node backends=[%s], meta=%s, couple=%s>' % (self.group_id, self.status, ', '.join((repr(nb) for nb in self.node_backends)), str(self.meta), str(self.couple))

    def __eq__(self, other):
        return self.group_id == other


def status_change_log(f):
    @functools.wraps(f)
    def wrapper(self, *args, **kwargs):
        status = self.status
        new_status = f(self, *args, **kwargs)
        if status != new_status:
            logger.info('Couple {0} status updated from {1} to {2} ({3})'.format(
                self, status, new_status, self.status_text))
        return new_status
    return wrapper


class Couple(object):
    def __init__(self, groups):
        self.status = Status.INIT
        self.namespace = None
        self.groups = sorted(groups, key=lambda group: group.group_id)
        self.meta = None
        for group in self.groups:
            if group.couple:
                raise Exception('Group %s is already in couple' % (repr(group)))

            group.couple = self
        self.status_text = 'Couple {0} is not inititalized yet'.format(str(self))
        self.active_job = None

    def get_stat(self):
        try:
            return reduce(lambda res, x: res * x, [group.get_stat() for group in self.groups])
        except TypeError:
            return None

    def account_job_in_status(self):
        if self.status == Status.BAD:
            for group in self.groups:
                if group.active_job:
                    self.active_job = group.active_job
                    break

            if self.active_job and self.active_job['type'] in (JobTypes.TYPE_MOVE_JOB,
                                                               JobTypes.TYPE_RESTORE_GROUP_JOB):
                if self.active_job['status'] in (jobs.job.Job.STATUS_NEW,
                                                 jobs.job.Job.STATUS_EXECUTING):
                    self.status = Status.SERVICE_ACTIVE
                    self.status_text = 'Couple {} has active job {}'.format(
                        str(self), self.active_job['id'])
                else:
                    self.status = Status.SERVICE_STALLED
                    self.status_text = 'Couple {} has stalled job {}'.format(
                        str(self), self.active_job['id'])
        return self.status

    @status_change_log
    def update_status(self):
        statuses = [group.update_status() for group in self.groups]

        for group in self.groups:
            if not group.meta:
                self.status = Status.BAD
                self.status_text = "Couple's group {} has empty meta data".format(group)
                return self.status
            if self.namespace != group.meta.get('namespace'):
                self.status = Status.BAD
                self.status_text = "Couple's namespace does not match namespace " \
                                   "in group's meta data ('{}' != '{}')".format(
                                       self.namespace, group.meta.get('namespace'))
                return self.status

        self.active_job = None

        if any([not self.groups[0].equal_meta(group) for group in self.groups[1:]]):
            self.status = Status.BAD
            self.status_text = 'Couple {0} groups has unequal meta data'.format(str(self))
            return self.account_job_in_status()

        if any([group.meta and group.meta.get('frozen') for group in self.groups]):
            self.status = Status.FROZEN
            self.status_text = 'Couple {0} is frozen'.format(str(self))
            return self.status

        if FORBIDDEN_DC_SHARING_AMONG_GROUPS:
            # checking if any pair of groups has its node backends in same dc
            groups_dcs = []
            for group in self.groups:
                group_dcs = set()
                for nb in group.node_backends:
                    try:
                        group_dcs.add(nb.node.host.dc)
                    except CacheUpstreamError:
                        self.status_text = 'Failed to resolve dc for host {}'.format(nb.node.host)
                        self.status = Status.BAD
                        return self.status
                groups_dcs.append(group_dcs)

            dc_set = groups_dcs[0]
            for group_dcs in groups_dcs[1:]:
                if dc_set & group_dcs:
                    self.status = Status.BROKEN
                    self.status_text = 'Couple {0} has nodes sharing the same DC'.format(str(self))
                    return self.status
                dc_set = dc_set | group_dcs

        if FORBIDDEN_NS_WITHOUT_SETTINGS:
            if not infrastructure.ns_settings.get(self.namespace.id):
                self.status = Status.BROKEN
                self.status_text = ('Couple {} is assigned to a namespace {}, which is '
                                    'not set up'.format(self, self.namespace))
                return self.status

        if all([st == Status.COUPLED for st in statuses]):

            if FORBIDDEN_UNMATCHED_GROUP_TOTAL_SPACE:
                group_stats = [g.get_stat() for g in self.groups]
                total_spaces = [gs.total_space for gs in group_stats if gs]
                if any([ts != total_spaces[0] for ts in total_spaces]):
                    self.status = Status.BROKEN
                    self.status_text = ('Couple {0} has unequal total space in groups'.format(
                        str(self)))
                    return self.status

            if self.is_full():
                self.status = Status.FULL
                self.status_text = 'Couple {0} is full'.format(str(self))
            else:
                self.status = Status.OK
                self.status_text = 'Couple {0} is OK'.format(str(self))

            return self.status

        if Status.INIT in statuses:
            self.status = Status.INIT
            self.status_text = 'Couple {0} has uninitialized groups'.format(str(self))

        elif Status.BROKEN in statuses:
            self.status = Status.BROKEN
            self.status_text = 'Couple {0} has broken groups'.format(str(self))

        elif Status.BAD in statuses:

            group_status_texts = []
            for group in filter(lambda g: g.status == Status.BAD, self.groups):
                group_status_texts.append(group.status_text)
            self.status = Status.BAD
            self.status_text = 'Couple {} has bad groups: {}'.format(
                str(self), ', '.join(group_status_texts))

        elif Status.RO in statuses or Status.MIGRATING in statuses:
            self.status = Status.BAD
            self.status_text = 'Couple {0} has read-only groups'.format(str(self))

        else:
            self.status = Status.BAD
            self.status_text = 'Couple {0} is bad for some reason'.format(str(self))

        return self.account_job_in_status()

    def check_groups(self, groups):

        for group in self.groups:
            if group.meta is None or 'couple' not in group.meta or not group.meta['couple']:
                return False

            if set(groups) != set(group.meta['couple']):
                return False

        if set(groups) != set((g.group_id for g in self.groups)):
            return False

        return True

    @property
    def frozen(self):
        return any([group.meta.get('frozen') for group in self.groups])

    @property
    def closed(self):
        return self.status == Status.FULL

    def destroy(self):
        if self.namespace:
            self.namespace.remove_couple(self)
        for group in self.groups:
            group.couple = None
            group.meta = None
            group.update_status()

        couples.remove(self)
        self.groups = []
        self.status = Status.INIT

    def compose_group_meta(self, namespace, frozen):
        return {
            'version': 2,
            'couple': self.as_tuple(),
            'namespace': namespace,
            'frozen': bool(frozen),
        }

    RESERVED_SPACE_KEY = 'reserved-space-percentage'

    def is_full(self):

        ns_reserved_space = infrastructure.ns_settings.get(self.namespace.id, {}).get(self.RESERVED_SPACE_KEY, 0.0)

        # TODO: move this logic to effective_free_space property,
        #       it should handle all calculations by itself
        for group in self.groups:
            for nb in group.node_backends:
                if nb.is_full(ns_reserved_space):
                    return True

        if self.effective_free_space <= 0:
            return True

        return False

    @property
    def effective_space(self):

        groups_effective_space = min([g.effective_space for g in self.groups])

        reserved_space = infrastructure.ns_settings.get(self.namespace.id, {}).get(self.RESERVED_SPACE_KEY, 0.0)
        return int(math.floor(groups_effective_space * (1.0 - reserved_space)))

    @property
    def effective_free_space(self):
        stat = self.get_stat()
        if not stat:
            return 0
        return int(max(stat.free_space -
                       (stat.total_space - self.effective_space), 0))

    def as_tuple(self):
        return tuple(group.group_id for group in self.groups)

    def info(self):
        c = CoupleInfo(str(self))
        data = {'id': str(self),
                'couple_status': self.status,
                'couple_status_text': self.status_text,
                'tuple': self.as_tuple()}
        try:
            data['namespace'] = self.namespace.id
        except ValueError:
            pass
        stat = self.get_stat()
        if stat:
            data['free_space'] = int(stat.free_space)
            data['used_space'] = int(stat.used_space)
            try:
                data['effective_space'] = self.effective_space
                data['free_effective_space'] = self.effective_free_space
            except ValueError:
                # failed to determine couple's namespace
                data['effective_space'], data['free_effective_space'] = 0, 0

        data['groups'] = [g.info().serialize() for g in self.groups]
        data['hosts'] = {
            'primary': []
        }

        c._set_raw_data(data)
        return c

    FALLBACK_HOSTS_PER_DC = config.get('fallback_hosts_per_dc', 10)

    def couple_hosts(self):
        hosts = {'primary': [],
                 'fallback': []}
        used_hosts = set()
        used_dcs = set()

        def serialize_node(node):
            return {
                'host': node.host.hostname,
                'dc': node.host.dc,
            }

        for group in self.groups:
            for nb in group.node_backends:
                node = nb.node
                if node.host in used_hosts:
                    continue
                try:
                    hosts['primary'].append(serialize_node(node))
                except CacheUpstreamError:
                    continue
                used_hosts.add(node.host)
                used_dcs.add(node.host.dc)

        for dc in used_dcs:
            count = 0
            for node in dc_host_view[dc].by_la:
                if node.host in used_hosts:
                    continue
                try:
                    hosts['fallback'].append(serialize_node(node))
                except CacheUpstreamError:
                    continue
                used_hosts.add(node.host)
                count += 1
                if count >= self.FALLBACK_HOSTS_PER_DC:
                    break

        return hosts

    @property
    def keys_diff(self):
        group_keys = []
        for group in self.groups:
            if not len(group.node_backends):
                continue
            group_keys.append(group.get_stat().files)
        if not group_keys:
            return None
        group_keys.sort(reverse=True)
        return sum([group_keys[0] - gk for gk in group_keys[1:]])

    def __contains__(self, group):
        return group in self.groups

    def __iter__(self):
        return self.groups.__iter__()

    def __len__(self):
        return len(self.groups)

    def __str__(self):
        return ':'.join([str(group) for group in self.groups])

    def __hash__(self):
        return hash(self.__str__())

    def __eq__(self, other):
        if isinstance(other, (str, unicode)):
            return self.__str__() == other

        if isinstance(other, Couple):
            return self.groups == other.groups

    def __repr__(self):
        return '<Couple object: status=%s, groups=[%s] >' % (self.status, ', '.join([repr(g) for g in self.groups]))


class DcNodes(object):
    def __init__(self):
        self.nodes = []
        self.__by_la = None

    def append(self, node):
        self.nodes.append(node)

    @property
    def by_la(self):
        if self.__by_la is None:
            self.__by_la = sorted(self.nodes, key=lambda node: node.stat.load_average)
        return self.__by_la


class DcHostView(object):

    def __init__(self):
        self.dcs_hosts = {}

    def update(self):
        dcs_hosts = {}
        # TODO: iterate through hosts when host statistics will be moved
        # to a separate object
        hosts = set()
        for node in nodes:
            dc_hosts = dcs_hosts.setdefault(node.host.dc, DcNodes())
            if node.host in hosts:
                continue
            dc_hosts.append(node)
        self.dcs_hosts = dcs_hosts

    def __getitem__(self, key):
        return self.dcs_hosts[key]


class Namespace(object):
    def __init__(self, id):
        self.id = id
        self.couples = set()

    def add_couple(self, couple):
        if couple.namespace:
            raise ValueError('Couple {} already belongs to namespace {}, cannot be assigned to '
                             'namespace {}'.format(couple, self, couple.namespace))
        self.couples.add(couple)
        couple.namespace = self

    def remove_couple(self, couple):
        self.couples.remove(couple)
        couple.namespace = None

    def __str__(self):
        return self.id

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        if isinstance(other, types.StringTypes):
            return str(self) == other

        if isinstance(other, Namespace):
            return self.id == other.id

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return '<Namespace: id={id} >'.format(id=self.id)


hosts = Repositary(Host)
groups = Repositary(Group)
nodes = Repositary(Node)
node_backends = Repositary(NodeBackend, 'Node backend')
couples = Repositary(Couple)
namespaces = Repositary(Namespace)
fs = Repositary(Fs)

dc_host_view = DcHostView()


cache_couples = Repositary(Couple, 'Cache couple')


'''
h = hosts.add('95.108.228.31')
g = groups.add(1)
n = nodes.add(hosts['95.108.228.31'], 1025, 2)
g.add_node(n)

g2 = groups.add(2)
n2 = nodes.add(h, 1026. 2)
g2.add_node(n2)

couple = couples.add([g, g2])

print '1:2' in couples
groups[1].parse_meta({'couple': (1,2)})
groups[2].parse_meta({'couple': (1,2)})
couple.update_status()

print repr(couples)
print 1 in groups
print [g for g in couple]
couple.destroy()
print repr(couples)
'''
