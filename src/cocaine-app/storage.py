# -*- coding: utf-8 -*-
import datetime
import logging
import time
import traceback

import msgpack

import inventory
from infrastructure import infrastructure, port_to_path
from config import config


logger = logging.getLogger('mm.storage')


RPS_FORMULA_VARIANT = config.get('rps_formula', 0)


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


class NodeStat(object):
    def __init__(self, raw_stat=None, prev=None):

        if raw_stat:
            self.init(raw_stat, prev)
        else:
            self.free_space = 0.0
            self.rel_space = 0.0
            self.load_average = 0.0

            self.read_rps = 0
            self.write_rps = 0
            self.max_read_rps = 0
            self.max_write_rps = 0

            self.fragmentation = 0.0
            self.files = 0
            self.files_removed = 0

            self.fsid = None

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

    def init(self, raw_stat, prev=None):
        self.ts = time.time()

        self.last_read = raw_stat['storage_commands']['READ'][0] + raw_stat['proxy_commands']['READ'][0]
        self.last_write = raw_stat['storage_commands']['WRITE'][0] + raw_stat['proxy_commands']['WRITE'][0]

        self.total_space = float(raw_stat['counters']['DNET_CNTR_BLOCKS'][0]) * raw_stat['counters']['DNET_CNTR_BSIZE'][0]
        self.free_space = float(raw_stat['counters']['DNET_CNTR_BAVAIL'][0]) * raw_stat['counters']['DNET_CNTR_BSIZE'][0]
        self.used_space = self.total_space - self.free_space
        self.rel_space = float(raw_stat['counters']['DNET_CNTR_BAVAIL'][0]) / raw_stat['counters']['DNET_CNTR_BLOCKS'][0]
        self.load_average = (float(raw_stat['counters']['DNET_CNTR_DU1'][0]) / 100
                             if raw_stat['counters'].get('DNET_CNTR_DU1') else
                             float(raw_stat['counters']['DNET_CNTR_LA1'][0]) / 100)

        self.fragmentation = (float(raw_stat['counters']['DNET_CNTR_NODE_FILES_REMOVED'][0]) /
                                 ((raw_stat['counters']['DNET_CNTR_NODE_FILES'][0] +
                                   raw_stat['counters']['DNET_CNTR_NODE_FILES_REMOVED'][0]) or 1))
        self.files = raw_stat['counters']['DNET_CNTR_NODE_FILES'][0]
        self.files_removed = raw_stat['counters']['DNET_CNTR_NODE_FILES_REMOVED'][0]

        self.fsid = raw_stat['counters']['DNET_CNTR_FSID'][0]

        if prev:
            dt = self.ts - prev.ts

            self.read_rps = (self.last_read - prev.last_read) / dt
            self.write_rps = (self.last_write - prev.last_write) / dt

            # Disk usage should be used here instead of load average
            # self.max_read_rps = max(self.read_rps / max(self.load_average, 0.01), 100)
            self.max_read_rps = self.max_rps(self.read_rps, self.load_average)

            # self.max_write_rps = max(self.write_rps / max(self.load_average, 0.01), 100)
            self.max_write_rps = self.max_rps(self.write_rps, self.load_average)

        else:
            self.read_rps = 0
            self.write_rps = 0

            # Tupical SATA HDD performance is 100 IOPS
            # It will be used as first estimation for maximum node performance
            self.max_read_rps = 100
            self.max_write_rps = 100

    def __add__(self, other):
        res = NodeStat()

        res.ts = min(self.ts, other.ts)

        res.total_space = self.total_space + other.total_space
        res.free_space = self.free_space + other.free_space
        res.used_space = self.used_space + other.used_space
        res.rel_space = min(self.rel_space, other.rel_space)
        res.load_average = max(self.load_average, other.load_average)

        res.read_rps = self.read_rps + other.read_rps
        res.write_rps = self.write_rps + other.write_rps

        res.max_read_rps = self.max_read_rps + other.max_read_rps
        res.max_write_rps = self.max_write_rps + other.max_write_rps

        res.files = self.files + other.files
        res.files_removed = self.files_removed + other.files_removed
        res.fragmentation = float(res.files_removed) / (res.files_removed + res.files or 1)

        return res

    def __mul__(self, other):
        res = NodeStat()
        res.ts = min(self.ts, other.ts)

        res.total_space = min(self.total_space, other.total_space)
        res.free_space = min(self.free_space, other.free_space)
        res.used_space = min(self.used_space, other.used_space)
        res.rel_space = min(self.rel_space, other.rel_space)
        res.load_average = max(self.load_average, other.load_average)

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

        # ATTENTION: fragmentation coefficient in this case would not necessary
        # be equal to [removed keys / total keys]
        res.fragmentation = max(self.fragmentation, other.fragmentation)

        return res

    def __repr__(self):
        return ('<NodeStat object: ts=%s, write_rps=%d, max_write_rps=%d, read_rps=%d, '
                'max_read_rps=%d, total_space=%d, free_space=%d, files_removed=%s, '
                'fragmentation=%s, load_average=%s>' % (
                    ts_str(self.ts), self.write_rps, self.max_write_rps, self.read_rps,
                    self.max_read_rps, self.total_space, self.free_space, self.files_removed,
                    self.fragmentation, self.load_average))


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
    def __init__(self, host, port):
        self.host = host
        self.port = int(port)
        self.host.nodes.append(self)

        self.stat = None

        self.destroyed = False
        self.read_only = False
        self.status = Status.INIT
        self.status_text = "Node %s is not inititalized yet" % (self.__str__())

    def get_host(self):
        return self.host

    def destroy(self):
        self.destroyed = True
        self.host.nodes.remove(self)
        self.host = None
        # if there is a group using this node, remove node from the list
        # self.group.remove_node(self)

    def update_statistics(self, new_stat):
        stat = NodeStat(new_stat, self.stat)
        self.stat = stat

    def update_status(self):
        if self.destroyed:
            self.status = Status.BAD
            self.status_text = "Node %s is destroyed" % (self.__str__())

        elif not self.stat:
            self.status = Status.INIT
            self.status_text = "No statistics gathered for node %s" % (self.__str__())

        elif self.stat.ts < (time.time() - 120):
            self.status = Status.STALLED
            self.status_text = "Statistics for node %s is too old: it was gathered %d seconds ago" % (self.__str__(), int(time.time() - self.stat.ts))

        elif self.read_only:
            self.status = Status.RO
            self.status_text = "Node %s is in Read-Only state" % (self.__str__())

        else:
            self.status = Status.OK
            self.status_text = "Node %s is OK" % (self.__str__())

        return self.status

    def info(self):
        res = {}

        res['addr'] = self.__str__()
        res['hostname'] = infrastructure.get_hostname_by_addr(self.host.addr)
        res['status'] = self.status
        res['dc'] = self.host.dc
        res['last_stat_update'] = (self.stat and
            datetime.datetime.fromtimestamp(self.stat.ts).strftime('%Y-%m-%d %H:%M:%S') or
            'unknown')
        if self.stat:
            min_free_space = config['balancer_config'].get('min_free_space', 256) * 1024 * 1024
            min_free_space_rel = config['balancer_config'].get('min_free_space_relative', 0.15)

            res['free_space'] = int(self.stat.free_space)
            node_eff_space = max(min(self.stat.total_space - min_free_space,
                                     self.stat.total_space * (1 - min_free_space_rel)), 0.0)

            res['free_effective_space'] = int(max(self.stat.free_space - (self.stat.total_space - node_eff_space), 0))
            res['used_space'] = int(self.stat.used_space)
            res['total_files'] = self.stat.files + self.stat.files_removed
            res['fragmentation'] = self.stat.fragmentation
        res['path'] = port_to_path(int(res['addr'].split(':')[1]))

        return res

    def __repr__(self):
        if self.destroyed:
            return '<Node object: DESTROYED!>'

        return '<Node object: host=%s, port=%d, status=%s, read_only=%s, stat=%s>' % (str(self.host), self.port, self.status, str(self.read_only), repr(self.stat))

    def __str__(self):
        if self.destroyed:
            raise Exception('Node object is destroyed')

        return '%s:%d' % (self.host.addr, self.port)

    def __hash__(self):
        return hash(self.__str__())

    def __eq__(self, other):
        if isinstance(other, str):
            return self.__str__() == other

        if isinstance(other, Node):
            return self.host.addr == other.host.addr and self.port == other.port


class Group(object):

    DEFAULT_NAMESPACE = 'default'

    def __init__(self, group_id, nodes=None):
        self.group_id = group_id
        self.status = Status.INIT
        self.nodes = []
        self.couple = None
        self.meta = None
        self.status_text = "Group %s is not inititalized yet" % (self.__str__())

        if nodes:
            for node in nodes:
                self.add_node(node)

    def add_node(self, node):
        self.nodes.append(node)

    def has_node(self, node):
        return node in self.nodes

    def remove_node(self, node):
        self.nodes.remove(node)

    def parse_meta(self, meta):
        if meta is None:
            self.meta = None
            self.status = Status.BAD
            return

        parsed = msgpack.unpackb(meta)
        if isinstance(parsed, tuple):
            self.meta = {'version': 1, 'couple': parsed, 'namespace': self.DEFAULT_NAMESPACE}
        elif isinstance(parsed, dict) and parsed['version'] == 2:
            self.meta = parsed
        else:
            raise Exception('Unable to parse meta')

    def get_stat(self):
        return reduce(lambda res, x: res + x, [node.stat for node in self.nodes])

    def update_status_recursive(self):
        if self.couple:
            self.couple.update_status()
        else:
            self.update_status()

    def update_status(self):
        if not self.nodes:
            self.status = Status.INIT
            self.status_text = "Group %s is in INIT state because there is no nodes serving this group" % (self.__str__())

        # node statuses should be updated before group status is set
        statuses = tuple(node.update_status() for node in self.nodes)

        logger.info('In group %d meta = %s' % (self.group_id, str(self.meta)))
        if (not self.meta) or (not 'couple' in self.meta) or (not self.meta['couple']):
            self.status = Status.INIT
            self.status_text = "Group %s is in INIT state because there is no coupling info" % (self.__str__())
            return self.status

        if Status.RO in statuses:
            self.status = Status.RO
            self.status_text = "Group %s is in Read-Only state because there is RO nodes" % (self.__str__())
            return self.status

        if not all([st == Status.OK for st in statuses]):
            self.status = Status.BAD
            self.status_text = "Group %s is in Bad state because some node statuses are not OK" % (self.__str__())
            return self.status

        if (not self.couple) and self.meta['couple']:
            self.status = Status.BAD
            self.status_text = "Group %s is in Bad state because couple did not created" % (self.__str__())
            return self.status

        elif not self.couple.check_groups(self.meta['couple']):
            self.status = Status.BAD
            self.status_text = "Group %s is in Bad state because couple check fails" % (self.__str__())
            return self.status

        elif not self.meta['namespace']:
            self.status = Status.BAD
            self.status_text = "Group %s is in Bad state because no namespace has been assigned to it" % (self.__str__())
            return self.status

        elif self.meta['namespace'] != self.couple.namespace:
            self.status = Status.BAD
            self.status_text = "Group %s is in Bad state because its namespace doesn't correspond to couple namespace (%s)" % (self.__str__(), self.couple.namespace)
            return self.status

        self.status = Status.COUPLED
        self.status_text = "Group %s is OK" % (self.__str__())

        return self.status

    def info(self):
        res = {}

        res['id'] = self.group_id
        res['status'] = self.status
        res['status_text'] = self.status_text
        res['nodes'] = [n.info() for n in self.nodes]
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
        return '<Group object: group_id=%d, status=%s nodes=[%s], meta=%s, couple=%s>' % (self.group_id, self.status, ', '.join((repr(n) for n in self.nodes)), str(self.meta), str(self.couple))

    def __eq__(self, other):
        return self.group_id == other


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
            min_free_space = config['balancer_config'].get('min_free_space', 256) * 1024 * 1024
            min_rel_space = config['balancer_config'].get('min_free_space_relative', 0.15)

            stats = self.get_stat()
            if (stats and (stats.free_space < min_free_space or
                           stats.rel_space < min_rel_space)):
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
        return self.groups[0].meta and self.groups[0].meta['namespace'] or None

    def as_tuple(self):
        return tuple(group.group_id for group in self.groups)

    def info(self):
        res = {'couple_status': self.status,
               'id': str(self),
               'tuple': self.as_tuple(),
               'namespace': self.namespace}
        stat = self.get_stat()
        if stat:
            min_free_space = config['balancer_config'].get('min_free_space', 256) * 1024 * 1024
            min_free_space_rel = config['balancer_config'].get('min_free_space_relative', 0.15)

            res['free_space'] = int(stat.free_space)
            node_eff_space = max(min(stat.total_space - min_free_space,
                                     stat.total_space * (1 - min_free_space_rel)), 0.0)

            res['free_effective_space'] = int(max(stat.free_space - (stat.total_space - node_eff_space), 0))

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
couples = Repositary(Couple)


def stat_result_entry_to_dict(sre):
    cnt = sre.statistics.counters
    stat = {'group_id': sre.address.group_id,
            'addr': '{0}:{1}'.format(sre.address.host, sre.address.port)}
    stat.update(sre.statistics.counters)
    return stat


def update_statistics(stats):

    if getattr(stats, 'get', None):
        stats = [stat_result_entry_to_dict(sre) for sre in stats.get()]

    for stat in stats:
        logger.info("Stats: %s %s" % (str(stat['group_id']), stat['addr']))

        try:

            gid = stat['group_id']

            if not stat['addr'] in nodes:
                addr = stat['addr'].split(':')
                if not addr[0] in hosts:
                    host = hosts.add(addr[0])
                    logger.debug('Adding host %s' % (addr[0]))
                else:
                    host = hosts[addr[0]]

                nodes.add(host, addr[1])

            if not gid in groups:
                group = groups.add(gid)
                logger.debug('Adding group %d' % stat['group_id'])
            else:
                group = groups[gid]

            logger.info('Stats for node %s' % gid)

            node = nodes[stat['addr']]

            if not node in group.nodes:
                group.add_node(node)
                logger.debug('Adding node %d -> %s:%s' %
                              (gid, node.host.addr, node.port))


            logger.info('Updating statistics for node %s' % (str(node)))
            node.update_statistics(stat)
            logger.info('Updating status for group %d' % gid)
            groups[gid].update_status()

        except Exception as e:
            logger.error('Unable to process statictics for node %s group_id %d (%s): %s' % (stat['addr'], stat['group_id'], e, traceback.format_exc()))


'''
h = hosts.add('95.108.228.31')
g = groups.add(1)
n = nodes.add(hosts['95.108.228.31'], 1025)
g.add_node(n)

g2 = groups.add(2)
n2 = nodes.add(h, 1026)
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
