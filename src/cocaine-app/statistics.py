from collections import defaultdict
import copy
import logging

import storage
from config import config
from infrastructure import infrastructure


logger = logging.getLogger('mm.statistics')


class Statistics(object):

    def __init__(self, balancer):
        self.balancer = balancer

    @staticmethod
    def dict_keys_sum(st1, st2):
        return dict((k, st1[k] + st2[k]) for k in st1
                    if k not in ('outages',))

    @staticmethod
    def dict_keys_min(st1, st2):
        return dict((k, min(st1[k], st2[k])) for k in st1)

    @staticmethod
    def redeem_space(host_space, couple_space):
        for k in host_space:
            min_val = min(host_space[k], couple_space[k])
            host_space[k] -= min_val
            couple_space[k] -= min_val

    @staticmethod
    def account_couples(data, group):
        if group.couple:
            data['total_couples'] += 1
            if group.couple.status == storage.Status.OK:
                data['open_couples'] += 1
            elif group.couple.status == storage.Status.FULL:
                data['closed_couples'] += 1
            elif group.couple.status == storage.Status.FROZEN:
                data['frozen_couples'] += 1
            elif group.couple.status == storage.Status.BROKEN:
                data['broken_couples'] += 1
            else:
                data['bad_couples'] += 1
        else:
            data['uncoupled_groups'] += 1

    def account_memory(self, data, group, stat):
        if group.couple:
            data['free_space'] += stat.free_space
            data['total_space'] += stat.total_space
            node_eff_space = group.couple.effective_space
            data['effective_space'] += int(node_eff_space)
            data['effective_free_space'] += max(stat.free_space - (stat.total_space - int(node_eff_space)), 0)
        else:
            data['uncoupled_space'] += stat.total_space

    @staticmethod
    def account_keys(data, node_backend):
        stat = node_backend.stat
        data['total_keys'] += stat.files + stat.files_removed
        data['removed_keys'] += stat.files_removed

    def per_entity_stat(self):
        default = lambda: {
            'free_space': 0,
            'total_space': 0,
            'effective_space': 0,
            'effective_free_space': 0,
            'uncoupled_space': 0,

            'open_couples': 0,
            'frozen_couples': 0,
            'closed_couples': 0,
            'broken_couples': 0,
            'bad_couples': 0,
            'total_couples': 0,
            'uncoupled_groups': 0,

            'total_keys': 0,
            'removed_keys': 0,
        }

        by_dc = defaultdict(default)
        by_ns = defaultdict(lambda: defaultdict(default))

        dc_couple_map = defaultdict(set)
        ns_dc_couple_map = defaultdict(lambda: defaultdict(set))

        for group in sorted(storage.groups, key=lambda g: not bool(g.couple)):
            for node_backend in group.node_backends:

                couple = (group.couple
                          if group.couple else
                          str(group.group_id))

                dc = node_backend.node.host.dc
                try:
                    ns = group.couple and group.couple.namespace or None
                except ValueError as e:
                    ns = None

                if not couple in dc_couple_map[dc]:
                    self.account_couples(by_dc[dc], group)
                    dc_couple_map[dc].add(couple)
                if ns and not couple in ns_dc_couple_map[ns][dc]:
                    self.account_couples(by_ns[ns][dc], group)
                    ns_dc_couple_map[ns][dc].add(couple)

                if not node_backend.stat:
                    logger.debug('No stats available for node %s' % str(node_backend))
                    continue

                try:
                    self.account_memory(by_dc[dc], group, node_backend.stat)
                except ValueError:
                    # namespace for group couple is broken, do not try to account it
                    continue

                if ns:
                    self.account_memory(by_ns[ns][dc], group, node_backend.stat)

                self.account_keys(by_dc[dc], node_backend)
                if ns:
                    self.account_keys(by_ns[ns][dc], node_backend)

        self.count_outaged_couples(by_dc, by_ns)

        return dict(by_dc), dict((k, dict(v)) for k, v in by_ns.iteritems())

    def count_outaged_couples(self, by_dc_stats, by_ns_stats):
        default = lambda: {
            'open_couples': 0,
            'frozen_couples': 0,
            'closed_couples': 0,
            'broken_couples': 0,
            'bad_couples': 0,
            'total_couples': 0,
        }

        by_dc = defaultdict(default)
        by_ns = defaultdict(lambda: defaultdict(default))

        dcs = set(by_dc_stats.keys())
        ns_dcs = {}
        for ns, dc_stats in by_ns_stats.iteritems():
            ns_dcs[ns] = set(dc_stats.keys())

        for couple in storage.couples:
            affected_dcs = set()
            for group in couple.groups:
                for node_backend in group.node_backends:
                    affected_dcs.add(node_backend.node.host.dc)

            for dc in affected_dcs:
                by_dc[dc]['bad_couples'] += 1
                by_dc[dc]['total_couples'] += 1

            for dc in dcs - affected_dcs:
                self.account_couples(by_dc[dc], couple.groups[0])

            try:
                ns = couple.namespace
            except ValueError:
                continue

            if ns:
                for dc in affected_dcs:
                    by_ns[ns][dc]['bad_couples'] += 1
                for dc in ns_dcs[ns] - affected_dcs:
                    self.account_couples(by_ns[ns][dc], couple.groups[0])

        for dc, dc_data in by_dc_stats.iteritems():
            dc_data['outages'] = by_dc[dc]
        for ns, stats in by_ns_stats.iteritems():
            for dc, dc_data in stats.iteritems():
                dc_data['outages'] = by_ns[ns][dc]

    def total_stats(self, per_dc_stat):
        dc_stats = per_dc_stat.values()
        for dc in dc_stats:
            del dc['outages']
        return dict(reduce(self.dict_keys_sum, dc_stats))

    def get_couple_stats(self):
        open_couples = self.balancer.get_symmetric_groups(None)
        bad_couples = self.balancer.get_couples_list([{'state': 'bad'}])
        broken_couples = self.balancer.get_couples_list([{'state': 'broken'}])
        closed_couples = self.balancer.get_closed_groups(None)
        frozen_couples = self.balancer.get_frozen_groups(None)
        uncoupled_groups = self.balancer.get_empty_groups(None)

        return {'open_couples': len(open_couples),
                'frozen_couples': len(frozen_couples),
                'closed_couples': len(closed_couples),
                'bad_couples': len(bad_couples),
                'broken_couples': len(broken_couples),
                'total_couples': (len(open_couples) + len(closed_couples) +
                                  len(frozen_couples) + len(bad_couples)),
                'uncoupled_groups': len(uncoupled_groups)}

    def get_data_space(self):
        """Returns the space actually available for writing data, therefore
        excluding space occupied by replicas, accounts for groups residing
        on the same file systems, etc."""


        # TODO: Change min_free_space* usage
        host_fsid_memory_map = {}

        res = {
            'free_space': 0.0,
            'total_space': 0.0,
            'effective_space': 0.0,
            'effective_free_space': 0.0,
        }

        for group in storage.groups:
            for node_backend in group.node_backends:
                if not node_backend.stat:
                    continue

                if not (node_backend.node.host.addr, node_backend.stat.fsid) in host_fsid_memory_map:
                    host_fsid_memory_map[(node_backend.node.host.addr, node_backend.stat.fsid)] = {
                        'total_space': node_backend.stat.total_space,
                        'free_space': node_backend.stat.free_space,
                        'effective_space': node_backend.effective_space,
                        'effective_free_space': int(max(node_backend.stat.free_space - (node_backend.stat.total_space - node_backend.effective_space), 0)),
                    }

        for couple in storage.couples:

            stat = couple.get_stat()

            if not stat:
                continue

            group_top_stats = []
            for group in couple.groups:
                # space is summed through all the group node backends
                group_top_stats.append(reduce(self.dict_keys_sum,
                       [host_fsid_memory_map[(nb.node.host.addr, nb.stat.fsid)] for nb in group.node_backends if nb.stat]))

            # max space available for the couple
            couple_top_stats = reduce(self.dict_keys_min, group_top_stats)

            #increase storage stats
            res = reduce(self.dict_keys_sum, [res, couple_top_stats])

            for group in couple.groups:
                couple_top_stats_copy = copy.copy(couple_top_stats)
                for node_backend in group.node_backends:
                    # decrease filesystems space counters
                    if node_backend.stat is None:
                        continue
                    self.redeem_space(host_fsid_memory_map[(node_backend.node.host.addr, node_backend.stat.fsid)],
                                      couple_top_stats_copy)

        return res


    def get_flow_stats(self, request):

        per_dc_stat, per_ns_stat = self.per_entity_stat()

        res = self.total_stats(per_dc_stat)
        res.update({'dc': per_dc_stat,
                    'namespaces': per_ns_stat})

        res.update({'real_data': self.get_data_space()})
        res.update(self.get_couple_stats())

        return res


    def get_groups_tree(self, request):

        try:
            options = request[0]
        except (TypeError, IndexError):
            options = {}

        namespace, status = options.get('namespace', None), options.get('couple_status', None)

        tree, nodes = infrastructure.cluster_tree(namespace)

        for group in storage.groups.keys():
            if not group.node_backends:
                continue
            try:
                if namespace and (not group.couple or
                                  group.couple.namespace != namespace):
                    continue
            except ValueError:
                continue
            if status == 'UNCOUPLED' and group.couple:
                # workaround for uncoupled groups, not a real status
                continue
            elif status and status != 'UNCOUPLED' and (not group.couple or status != group.couple.status):
                continue
            if not group.node_backends:
                continue
            try:
                stat = group.get_stat()
            except TypeError:
                logger.warn('Group {0}: no node backends stat available'.format(group.group_id))
                continue
            for node_backend in group.node_backends:
                parent = node_backend.node.host.parents
                parts = [parent['name']]
                while 'parent' in parent:
                    parent = parent['parent']
                    parts.append(parent['name'])
                full_path = '|'.join(reversed(parts))
                group_parent = nodes['host'][full_path]
                groups = group_parent.setdefault('children', [])
                groups.append({'type': 'group',
                               'name': str(group),
                               'couple': group.couple and str(group.couple) or None,
                               'node_addr': '{0}:{1}'.format(node_backend.node.host, node_backend.node.port),
                               'backend_id': node_backend.backend_id,
                               'node_status': node_backend.status,
                               'couple_status': group.couple and group.couple.status or None,
                               'free_space': stat.free_space,
                               'total_space': stat.total_space,
                               'fragmentation': stat.fragmentation,
                               'status': group.couple and group.couple.status or None})

        self.__clean_tree(tree)

        return tree

    def __clean_tree(self, root):
        if root['type'] == 'host':
            return
        for child in root.get('children', [])[:]:
            self.__clean_tree(child)
            if not child.get('children'):
                root['children'].remove(child)

    def get_couple_statistics(self, request):
        group_id = int(request[0])

        if not group_id in storage.groups:
            raise ValueError('Group %d is not found' % group_id)

        group = storage.groups[group_id]

        res = {}

        if group.couple:
            res = group.couple.info()
            res['status'] = res['couple_status']
            res['stats'] = self.__stats_to_dict(group.couple.get_stat(), group.couple.effective_space)
            groups = group.couple.groups
        else:
            groups = [group]

        res['groups'] = []
        for group in groups:
            g = group.info()
            try:
                group_stat = group.get_stat()
            except TypeError:
                group_stat = None
            g['stats'] = self.__stats_to_dict(group_stat, group.effective_space)
            for nb in g['node_backends']:
                nb['stats'] = self.__stats_to_dict(storage.node_backends[nb['addr']].stat,
                    storage.node_backends[nb['addr']].effective_space)
            res['groups'].append(g)

        return res

    def __stats_to_dict(self, stat, eff_space):
        res = {}

        if not stat:
            return res

        res['total_space'] = stat.total_space
        res['free_space'] = stat.free_space

        res['free_effective_space'] = max(stat.free_space -
            (stat.total_space - eff_space), 0.0)
        res['used_space'] = stat.used_space
        res['fragmentation'] = stat.fragmentation

        return res
