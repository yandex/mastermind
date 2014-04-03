from collections import defaultdict

from cocaine.logging import Logger

import storage
from config import config
import copy


logging = Logger()


class Statistics(object):

    def __init__(self, balancer):
        self.balancer = balancer

    MIN_FREE_SPACE = config['balancer_config'].get('min_free_space', 256) * 1024 * 1024
    MIN_FREE_SPACE_REL = config['balancer_config'].get('min_free_space_relative', 0.15)

    @staticmethod
    def dict_keys_sum(st1, st2):
        return dict((k, st1[k] + st2[k]) for k in st1)

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
                if not group.couple.closed:
                    data['open_couples'] += 1
                else:
                    data['closed_couples'] += 1
            elif group.couple.status == storage.Status.FROZEN:
                data['frozen_couples'] += 1
            else:
                data['bad_couples'] += 1
        else:
            data['uncoupled_groups'] += 1

    def account_memory(self, data, group, stat):
        if group.couple:
            data['free_space'] += stat.free_space
            data['total_space'] += stat.total_space
            node_eff_space = max(min(stat.total_space - self.MIN_FREE_SPACE,
                                     stat.total_space * (1 - self.MIN_FREE_SPACE_REL)), 0.0)
            data['effective_space'] += node_eff_space
            data['effective_free_space'] += max(stat.free_space - (stat.total_space - node_eff_space), 0.0)
        else:
            data['uncoupled_space'] += stat.total_space

    def per_entity_stat(self):
        default = lambda: {
            'free_space': 0.0,
            'total_space': 0.0,
            'effective_space': 0.0,
            'effective_free_space': 0.0,
            'uncoupled_space': 0.0,

            'open_couples': 0,
            'frozen_couples': 0,
            'closed_couples': 0,
            'bad_couples': 0,
            'total_couples': 0,
            'uncoupled_groups': 0,
        }

        by_dc = defaultdict(default)
        by_ns = defaultdict(lambda: defaultdict(default))

        dc_couple_map = defaultdict(set)
        ns_dc_couple_map = defaultdict(lambda: defaultdict(set))

        host_fsid_map = defaultdict(set)
        ns_host_fsid_map = defaultdict(lambda: defaultdict(set))

        for group in sorted(storage.groups, key=lambda g: not bool(g.couple)):
            for node in group.nodes:

                couple = (group.couple
                          if group.couple else
                          str(group.group_id))

                dc = node.host.dc
                ns = group.couple and group.couple.namespace or None

                if not couple in dc_couple_map[dc]:
                    self.account_couples(by_dc[dc], group)
                    dc_couple_map[dc].add(couple)
                if ns and not couple in ns_dc_couple_map[ns][dc]:
                    self.account_couples(by_ns[ns][dc], group)
                    ns_dc_couple_map[ns][dc].add(couple)

                if not node.stat:
                    logging.debug('No stats available for node %s' % str(node))
                    continue

                if not node.stat.fsid in host_fsid_map[node.host]:
                    self.account_memory(by_dc[dc], group, node.stat)
                    host_fsid_map[node.host].add(node.stat.fsid)
                if ns and not node.stat.fsid in ns_host_fsid_map[ns][node.host]:
                    self.account_memory(by_ns[ns][dc], group, node.stat)
                    ns_host_fsid_map[ns][node.host].add(node.stat.fsid)

        return dict(by_dc), dict((k, dict(v)) for k, v in by_ns.iteritems())

    def total_stats(self, per_dc_stat):
        return dict(reduce(self.dict_keys_sum, per_dc_stat.values()))

    def get_couple_stats(self):
        symmetric_couples = self.balancer.get_symmetric_groups(None)
        bad_couples = self.balancer.get_bad_groups(None)
        closed_couples = self.balancer.get_closed_groups(None)
        frozen_couples = self.balancer.get_frozen_groups(None)
        uncoupled_groups = self.balancer.get_empty_groups(None)

        return {'open_couples': len(symmetric_couples) - len(closed_couples),
                'frozen_couples': len(frozen_couples),
                'closed_couples': len(closed_couples),
                'bad_couples': len(bad_couples),
                'total_couples': len(symmetric_couples) + len(frozen_couples) + len(bad_couples),
                'uncoupled_groups': len(uncoupled_groups)}

    def get_data_space(self):
        """Returns the space actually available for writing data, therefore
        excluding space occupied by replicas, accounts for groups residing
        on the same file systems, etc."""

        host_fsid_memory_map = {}

        res = {
            'free_space': 0.0,
            'total_space': 0.0,
            'effective_space': 0.0,
            'effective_free_space': 0.0,
        }

        for group in storage.groups:
            for node in group.nodes:
                if not node.stat:
                    continue

                if not (node.host.addr, node.stat.fsid) in host_fsid_memory_map:
                    eff_space = max(min(node.stat.total_space - self.MIN_FREE_SPACE,
                                        node.stat.total_space * (1 - self.MIN_FREE_SPACE_REL)), 0.0)
                    host_fsid_memory_map[(node.host.addr, node.stat.fsid)] = {
                        'total_space': node.stat.total_space,
                        'free_space': node.stat.free_space,
                        'effective_space': eff_space,
                        'effective_free_space': max(node.stat.free_space - (node.stat.total_space - eff_space), 0.0),
                    }

        for couple in storage.couples:

            stat = couple.get_stat()

            if not stat:
                continue

            group_top_stats = []
            for group in couple.groups:
                # space is summed through all the group nodes
                group_top_stats.append(reduce(self.dict_keys_sum,
                       [host_fsid_memory_map[(node.host.addr, node.stat.fsid)] for node in group.nodes]))

            # max space available for the couple
            couple_top_stats = reduce(self.dict_keys_min, group_top_stats)

            #increase storage stats
            res = reduce(self.dict_keys_sum, [res, couple_top_stats])

            for group in couple.groups:
                couple_top_stats_copy = copy.copy(couple_top_stats)
                for node in group.nodes:
                    # decrease filesystems space counters
                    self.redeem_space(host_fsid_memory_map[(node.host.addr, node.stat.fsid)],
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
        nodes = {}
        root = None

        try:
            hosts = []
            namespace = request[0]
        except (TypeError, IndexError):
            namespace = None

        if namespace:
            for couple in storage.couples:
                if couple.namespace == namespace:
                    hosts.extend([n.host for g in couple for n in g.nodes])
            hosts = list(set(hosts))
        else:
            hosts = storage.hosts.keys()

        for host in hosts:
            tree_node = host.parents
            new_child = None
            while True:
                type_nodes = nodes.setdefault(tree_node['type'], {})
                cur_node = type_nodes.get(tree_node['name'], {'name': tree_node['name'],
                                                              'type': tree_node['type']})

                if new_child:
                    cur_node.setdefault('children', []).append(new_child)
                    new_child = None

                if not tree_node['name'] in type_nodes:
                    type_nodes[tree_node['name']] = cur_node
                    new_child = cur_node

                if not 'parent' in tree_node:
                    if not root:
                        root = nodes[tree_node['type']]
                    break
                tree_node = tree_node['parent']

        for group in storage.groups.keys():
            if not group.nodes:
                continue
            if namespace and (not group.couple or
                              group.couple.namespace != namespace):
                continue
            stat = group.get_stat()
            for node in group.nodes:
                group_parent = nodes['host'][node.host.hostname]
                groups = group_parent.setdefault('children', [])
                groups.append({'type': 'group',
                               'name': str(group),
                               'couple': group.couple and str(group.couple) or None,
                               'couple_status': group.couple and (
                                    (group.couple.status == storage.Status.OK and
                                     group.couple.closed and 'CLOSED') or
                                    group.couple.status) or None,
                               'free_space': stat.free_space,
                               'total_space': stat.total_space,
                               'status': group.couple and group.couple.status or None})

        return {'type': 'root', 'name': 'root',
                'children': root.values()}

    def get_couple_statistics(self, request):
        group_id = int(request[0])

        if not group_id in storage.groups:
            raise ValueError('Group %d is not found' % group_id)

        group = storage.groups[group_id]

        res = {}

        if group.couple:
            res = group.couple.info()
            res['status'] = res['couple_status']
            res['stats'] = self.__stats_to_dict(group.couple.get_stat())
            groups = group.couple.groups
        else:
            groups = [group]

        res['groups'] = []
        for group in groups:
            g = group.info()
            g['stats'] = self.__stats_to_dict(group.get_stat())
            for node in g['nodes']:
                node['stats'] = self.__stats_to_dict(storage.nodes[node['addr']].stat)
            res['groups'].append(g)

        if group.couple:
            # fix used_space statistics for couple,
            # it should be min of groups used_space
            res['stats']['used_space'] = min(g['stats']['used_space'] for g in res['groups'])

        return res

    MIN_FREE_SPACE = config['balancer_config'].get('min_free_space', 256) * 1024 * 1024
    MIN_FREE_SPACE_REL = config['balancer_config'].get('min_free_space_relative', 0.15)

    def __stats_to_dict(self, stat):
        res = {}

        res['total_space'] = stat.total_space
        res['free_space'] = stat.free_space
        node_eff_space = max(min(stat.total_space - self.MIN_FREE_SPACE,
            stat.total_space * (1 - self.MIN_FREE_SPACE_REL)), 0.0)

        res['free_effective_space'] = max(stat.free_space -
            (stat.total_space - node_eff_space), 0.0)
        res['used_space'] = stat.total_space - stat.free_space

        return res
