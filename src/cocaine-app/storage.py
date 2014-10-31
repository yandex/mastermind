# -*- coding: utf-8 -*-
import datetime
import functools
import logging
import math
import os.path
import time
import traceback

import msgpack

import inventory
from infrastructure import infrastructure, port_to_path
from config import config


logger = logging.getLogger('mm.storage')


RPS_FORMULA_VARIANT = config.get('rps_formula', 0)
VFS_RESERVED_SPACE = config.get('reserved_space', 112742891520)  # default is 105 Gb for one vfs
NODE_BACKEND_STAT_STALE_TIMEOUT = config.get('node_backend_stat_stale_timeout', 120)


def ts_str(ts):
    return time.asctime(time.localtime(ts))


class Status(object):
    INIT = 'INIT'
    OK = 'OK'
    FULL = 'FULL'
    COUPLED = 'COUPLED'
    BAD = 'BAD'
    RO = 'RO'
    STALLED = 'STALLED'
    FROZEN = 'FROZEN'


GOOD_STATUSES = set([Status.OK, Status.FULL])
NOT_BAD_STATUSES = set([Status.OK, Status.FULL, Status.FROZEN])


class Repositary(object):
    def __init__(self, constructor):
        self.elements = {}
        self.constructor = constructor

    def add(self, *args, **kwargs):
        e = self.constructor(*args, **kwargs)
        self.elements[e] = e
        return e

    def get(self, key):
        return self.elements[key]

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
    def __init__(self, raw_stat=None, prev=None):
        self.ts = None
        if raw_stat:
            self.init(raw_stat, prev)
        else:
            self.load_average = 0.0

    def init(self, raw_stat, prev=None):
        self.ts = time.time()
        self.load_average = float(raw_stat['procfs']['vm']['la'][0]) / 100

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
    def __init__(self, node_stat, raw_stat=None, prev=None):
        self.node_stat = node_stat
        self.ts = None

        if raw_stat:
            self.init(raw_stat, prev)
        else:
            self.free_space = 0

            self.last_read, self.last_write = 0, 0
            self.read_rps, self.write_rps = 0, 0
            self.max_read_rps, self.max_write_rps = 0, 0

            self.fragmentation = 0.0
            self.files = 0
            self.files_removed = 0

            self.fsid = None
            self.defrag_state = None

            self.blob_size_limit = 0
            self.max_blob_base_size = 0
            self.blob_size = 0

    def init(self, raw_stat, prev=None):
        self.ts = time.time()

        self.last_read = raw_stat['backend']['dstat']['read_ios']
        self.last_write = raw_stat['backend']['dstat']['write_ios']

        self.vfs_total_space = raw_stat['backend']['vfs']['blocks'] * raw_stat['backend']['vfs']['bsize']
        self.vfs_free_space = raw_stat['backend']['vfs']['bavail'] * raw_stat['backend']['vfs']['bsize']
        self.vfs_used_space = self.vfs_total_space - self.vfs_free_space

        self.files = raw_stat['backend']['summary_stats']['records_total'] - raw_stat['backend']['summary_stats']['records_removed']
        self.files_removed = raw_stat['backend']['summary_stats']['records_removed']
        self.files_removed_size = raw_stat['backend']['summary_stats']['records_removed_size']
        self.fragmentation = (float(self.files_removed) /
                                 ((self.files + self.files_removed) or 1))

        self.fsid = raw_stat['backend']['vfs']['fsid']
        self.defrag_state = raw_stat['status']['defrag_state']

        self.blob_size_limit = raw_stat['backend']['config'].get('blob_size_limit', 0)
        if self.blob_size_limit > 0:
            self.total_space = self.blob_size_limit
            self.used_space = raw_stat['backend']['summary_stats'].get('base_size', 0)
            self.free_space = max(0, self.total_space - self.used_space)
        else:
            self.total_space = self.vfs_total_space
            self.free_space = self.vfs_free_space
            self.used_space = self.vfs_used_space

        if len(raw_stat['backend'].get('base_stats', {})):
            self.max_blob_base_size = max([blob_stat['base_size']
                for blob_stat in raw_stat['backend']['base_stats'].values()])
        else:
            self.max_blob_base_size = 0

        self.blob_size = raw_stat['backend']['config']['blob_size']

        if prev:
            dt = self.ts - prev.ts

            self.read_rps = (self.last_read - prev.last_read) / dt
            self.write_rps = (self.last_write - prev.last_write) / dt

            # Disk usage should be used here instead of load average
            self.max_read_rps = self.max_rps(self.read_rps, self.node_stat.load_average)
            self.max_write_rps = self.max_rps(self.write_rps, self.node_stat.load_average)

        else:
            self.read_rps = 0
            self.write_rps = 0

            # Tupical SATA HDD performance is 100 IOPS
            # It will be used as first estimation for maximum node performance
            self.max_read_rps = 100
            self.max_write_rps = 100

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
        return infrastructure.get_hostname_by_addr(self.addr)

    @property
    def dc(self):
        return infrastructure.get_dc_by_host(self.addr)

    @property
    def parents(self):
        return infrastructure.get_host_tree(
            infrastructure.get_hostname_by_addr(self.addr))

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

    def update_statistics(self, new_stat):
        self.stat = NodeStat(new_stat, self.stat)

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


class NodeBackend(object):
    def __init__(self, node, backend_id):

        self.node = node
        self.backend_id = backend_id

        self.stat = None

        self.destroyed = False
        self.read_only = False
        self.disabled = False
        self.dstat_error_code = 0
        self.status = Status.INIT
        self.status_text = "Node %s is not inititalized yet" % (self.__str__())

        self.base_path = None

    def disable(self):
        self.disabled = True

    def enable(self):
        self.disabled = False

    def update_statistics(self, new_stat):
        self.stat = NodeBackendStat(self.node.stat, new_stat, self.stat)
        self.base_path = os.path.dirname(new_stat['backend']['config'].get('data') or
                                         new_stat['backend']['config'].get('file')) + '/'

    def update_status(self):
        if self.destroyed:
            self.status = Status.BAD
            self.status_text = 'Node backend {0} is destroyed'.format(self.__str__())

        elif self.dstat_error_code != 0:
            self.status = Status.STALLED
            self.status_text = 'Node backend {0} returned error code {1}'.format(str(self), self.dstat_error_code)

        elif not self.stat:
            self.status = Status.INIT
            self.status_text = 'No statistics gathered for node backend {0}'.format(self.__str__())

        elif self.disabled:
            self.status = Status.STALLED
            self.status_text = 'Node backend {0} has been disabled'.format(str(self))

        elif self.stat.ts < (time.time() - NODE_BACKEND_STAT_STALE_TIMEOUT):
            self.status = Status.STALLED
            self.status_text = ('Statistics for node backend {0} is too old: '
                'it was gathered {1} seconds ago'.format(self.__str__(),
                    int(time.time() - self.stat.ts)))

        elif self.read_only:
            self.status = Status.RO
            self.status_text = 'Node {0} is in Read-Only state'.format(self.__str__())

        else:
            self.status = Status.OK
            self.status_text = 'Node {0} is OK'.format(self.__str__())

        return self.status

    @property
    def effective_space(self):

        if self.stat is None:
            return 0

        share = float(self.stat.total_space) / self.stat.vfs_total_space
        free_space_req_share = math.ceil(VFS_RESERVED_SPACE * share)

        return self.stat.total_space - free_space_req_share

    def is_full(self, reserved_space=0.0):

        if self.stat is None:
            return False

        assert 0.0 <= reserved_space <= 1.0, 'Reserved space should have non-negative value lte 1.0'

        if self.stat.used_space >= self.effective_space * (1.0 - reserved_space):
            return True

    def info(self):
        res = {}

        res['node'] = '{0}:{1}:{2}'.format(self.node.host, self.node.port, self.node.family)
        res['backend_id'] = self.backend_id
        res['addr'] = str(self)
        res['hostname'] = infrastructure.get_hostname_by_addr(self.node.host.addr)
        res['status'] = self.status
        res['status_text'] = self.status_text
        res['dc'] = self.node.host.dc
        res['last_stat_update'] = (self.stat and
            datetime.datetime.fromtimestamp(self.stat.ts).strftime('%Y-%m-%d %H:%M:%S') or
            'unknown')
        if self.stat:
            res['free_space'] = int(self.stat.free_space)
            res['free_effective_space'] = int(max(self.stat.free_space - (self.stat.total_space - self.effective_space), 0))
            res['used_space'] = int(self.stat.used_space)
            res['total_files'] = self.stat.files + self.stat.files_removed
            res['records_alive'] = self.stat.files
            res['records_removed'] = self.stat.files_removed
            res['fragmentation'] = self.stat.fragmentation
        if self.base_path:
            res['path'] = self.base_path

        return res

    def __repr__(self):
        if self.destroyed:
            return '<Node backend object: DESTROYED!>'

        return ('<Node backend object: node=%s, backend_id=%d, '
            'status=%s, read_only=%s, stat=%s>' % (str(self.node), self.backend_id,
                self.status, str(self.read_only), repr(self.stat)))

    def __str__(self):
        if self.destroyed:
            raise Exception('Node backend object is destroyed')

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

    def __init__(self, group_id, node_backends=None):
        self.group_id = group_id
        self.status = Status.INIT
        self.node_backends = []
        self.couple = None
        self.meta = None
        self.status_text = "Group %s is not inititalized yet" % (self.__str__())

        for node_backend in node_backends or []:
            self.add_node_backend(node_backend)

    def add_node_backend(self, node_backend):
        self.node_backends.append(node_backend)

    def has_node_backend(self, node_backend):
        return node_backend in self.node_backends

    def remove_node_backend(self, node_backend):
        self.node_backends.remove(node_backend)

    def parse_meta(self, meta):
        if meta is None:
            self.meta = None
            return

        parsed = msgpack.unpackb(meta)
        if isinstance(parsed, tuple):
            self.meta = {'version': 1, 'couple': parsed, 'namespace': self.DEFAULT_NAMESPACE}
        elif isinstance(parsed, dict) and parsed['version'] == 2:
            self.meta = parsed
        else:
            raise Exception('Unable to parse meta')

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

    def update_status(self):
        """Updates group's own status.
        WARNING: This method should not take into consideration any of the
        groups' state coupled with itself nor any of the couple attributes,
        properties, state, etc."""

        if not self.node_backends:
            self.status = Status.INIT
            self.status_text = ('Group {0} is in INIT state because there is '
                'no node backends serving this group'.format(self.__str__()))
            return self.status

        # node statuses should be updated before group status is set
        statuses = tuple(nb.update_status() for nb in self.node_backends)

        logger.info('In group {0} meta = {1}'.format(self.group_id, str(self.meta)))
        if (not self.meta) or (not 'couple' in self.meta) or (not self.meta['couple']):
            self.status = Status.INIT
            self.status_text = ('Group {0} is in INIT state because there is '
                'no coupling info'.format(self.__str__()))
            return self.status

        if Status.RO in statuses:
            self.status = Status.RO
            self.status_text = ('Group {0} is in Read-Only state because '
                'there is RO node backends'.format(self.__str__()))
            return self.status

        if not all([st == Status.OK for st in statuses]):
            self.status = Status.BAD
            self.status_text = ('Group {0} is in Bad state because '
                'some node statuses are not OK'.format(self.__str__()))
            return self.status

        if (not self.couple) and self.meta['couple']:
            self.status = Status.BAD
            self.status_text = ('Group {0} is in Bad state because '
                'couple did not created'.format(self.__str__()))
            return self.status

        elif not self.couple.check_groups(self.meta['couple']):
            self.status = Status.BAD
            self.status_text = ('Group {0} is in Bad state because '
                'couple check fails'.format(self.__str__()))
            return self.status

        elif not self.meta['namespace']:
            self.status = Status.BAD
            self.status_text = ('Group {0} is in Bad state because '
                'no namespace has been assigned to it'.format(self.__str__()))
            return self.status

        self.status = Status.COUPLED
        self.status_text = 'Group {0} is OK'.format(self.__str__())

        return self.status

    def info(self):
        res = {}

        res['id'] = self.group_id
        res['status'] = self.status
        res['status_text'] = self.status_text
        res['node_backends'] = [nb.info() for nb in self.node_backends]
        if self.couple:
            res['couples'] = self.couple.as_tuple()
        else:
            res['couples'] = None
        if self.meta:
            res['namespace'] = self.meta['namespace']

        return res

    def __hash__(self):
        return hash(self.group_id)

    def __str__(self):
        return '%d' % (self.group_id)

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
            logger.info('Couple {0} status updated from {1} to {2}'.format(
                self, status, new_status))
        return new_status
    return wrapper


class Couple(object):
    def __init__(self, groups):
        self.status = Status.INIT
        self.groups = sorted(groups, key=lambda group: group.group_id)
        self.meta = None
        for group in self.groups:
            if group.couple:
                raise Exception('Group %s is already in couple' % (repr(group)))

            group.couple = self

    def get_stat(self):
        try:
            return reduce(lambda res, x: res * x, [group.get_stat() for group in self.groups])
        except TypeError:
            return None

    @status_change_log
    def update_status(self):
        statuses = [group.update_status() for group in self.groups]

        if self.meta and self.meta.get('frozen', False):
            self.status = Status.FROZEN
            return self.status

        meta = self.groups[0].meta
        if any([meta != group.meta for group in self.groups]):
            self.status = Status.BAD
            return self.status

        if all([st == Status.COUPLED for st in statuses]):
            stats = self.get_stat()
            if self.is_full():
                self.status = Status.FULL
            else:
                self.status = Status.OK

            return self.status

        if Status.INIT in statuses:
            self.status = Status.INIT

        elif Status.BAD in statuses:
            self.status = Status.BAD

        elif Status.RO in statuses:
            self.status = Status.RO

        else:
            self.status = Status.BAD

        return self.status

    def check_groups(self, groups):

        for group in self.groups:
            if group.meta is None or not 'couple' in group.meta or not group.meta['couple']:
                return False

            if set(groups) != set(group.meta['couple']):
                return False

        if set(groups) != set((g.group_id for g in self.groups)):
            self.status = Status.BAD
            return False

        return True

    def parse_meta(self, meta):
        if meta is None:
            self.meta = None
            return

        meta = msgpack.unpackb(meta)
        if meta['version'] == 1:
            self.meta = meta
        else:
            raise ValueError('Unable to parse couple meta')

    def freeze(self):
        if not self.meta:
            self.meta = self.compose_meta()
        self.meta['frozen'] = True

    def unfreeze(self):
        if not self.meta:
            self.meta = self.compose_meta()
        self.meta['frozen'] = False

    @property
    def frozen(self):
        return self.meta and self.meta['frozen']

    @property
    def closed(self):
        return self.status == Status.FULL

    def destroy(self):
        for group in self.groups:
            group.couple = None
            group.meta = None

        couples.remove(self)
        self.groups = []
        self.status = Status.INIT

    def compose_meta(self, frozen=False):
        meta = {'version': 1}
        if frozen:
            meta['frozen'] = True
        return meta

    def compose_group_meta(self, namespace):
        return {
            'version': 2,
            'couple': self.as_tuple(),
            'namespace': namespace,
        }

    @property
    def namespace(self):
        assert self.groups, "Couple %s has empty group list (id: %s)" % (repr(self), id(self))
        available_metas = [group.meta for group in self.groups
                           if group.meta]

        if not available_metas:
            # could not read meta data from any group
            logger.error('Couple {0} has broken namespace settings'.format(self))
            raise ValueError

        assert all(['namespace' in meta
                    for meta in available_metas]), "Couple %s has broken namespace settings" % (repr(self),)
        assert all([meta['namespace'] == available_metas[0]['namespace']
                    for meta in available_metas]), "Couple %s has broken namespace settings" % (repr(self),)

        return available_metas[0]['namespace']

    RESERVED_SPACE_KEY = 'reserved-space-percentage'

    def is_full(self):

        ns_reserved_space = infrastructure.ns_settings.get(self.namespace, {}).get(self.RESERVED_SPACE_KEY, 0.0)

        for group in self.groups:
            for nb in group.node_backends:
                if nb.is_full(ns_reserved_space):
                    return True
        return False

    @property
    def effective_space(self):

        groups_effective_space = min([g.effective_space for g in self.groups])

        reserved_space = infrastructure.ns_settings.get(self.namespace, {}).get(self.RESERVED_SPACE_KEY, 0.0)
        return math.floor(groups_effective_space * (1.0 - reserved_space))

    def as_tuple(self):
        return tuple(group.group_id for group in self.groups)

    def info(self):
        res = {'couple_status': self.status,
               'id': str(self),
               'tuple': self.as_tuple()}
        try:
            res['namespace'] = self.namespace
        except ValueError:
            pass
        stat = self.get_stat()
        if stat:
            res['free_space'] = int(stat.free_space)
            try:
                res['free_effective_space'] = int(max(stat.free_space - (stat.total_space - self.effective_space), 0))
            except ValueError:
                res['free_effective_space'] = 0
            res['used_space'] = int(stat.used_space)
        return res

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
        if isinstance(other, str):
            return self.__str__() == other

        if isinstance(other, Couple):
            return self.groups == other.groups

    def __repr__(self):
        return '<Couple object: status=%s, groups=[%s] >' % (self.status, ', '.join([repr(g) for g in self.groups]))


hosts = Repositary(Host)
groups = Repositary(Group)
nodes = Repositary(Node)
node_backends = Repositary(NodeBackend)
couples = Repositary(Couple)


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
