from collections import defaultdict
import logging

from errors import CacheUpstreamError
import storage
import helpers as h
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
    def account_couples(data, group):
        if group.couple:
            data['total_couples'] += 1
            couple = group.couple
            if couple.status == storage.Status.OK:
                data['open_couples'] += 1
            elif couple.status == storage.Status.FULL:
                data['closed_couples'] += 1
            elif couple.status == storage.Status.FROZEN:
                data['frozen_couples'] += 1
            elif couple.status == storage.Status.BROKEN:
                data['broken_couples'] += 1
            else:
                data['bad_couples'] += 1
        else:
            data['uncoupled_groups'] += 1

    @staticmethod
    def account_memory(data, group, stat):
        if group.couple:
            data['free_space'] += stat.free_space
            data['total_space'] += stat.total_space
        else:
            data['uncoupled_space'] += stat.total_space

    @staticmethod
    def account_effective_memory(data, couple):
        if couple.status not in storage.GOOD_STATUSES:
            return
        try:
            stat = couple.get_stat()
        except ValueError:
            return
        if not stat:
            return

        data['effective_space'] += couple.effective_space
        data['effective_free_space'] += couple.effective_free_space

    def per_ns_statistics(self):
        ns_stats = {}
        try:
            _, per_ns_stat, _ = self.per_entity_stat()
        except Exception:
            logger.exception('Failed to calculate namespace statistics')
            return ns_stats

        for ns, stats in per_ns_stat.iteritems():
            if (stats['closed_couples'] > 0 and
                    stats['open_couples'] == 0):
                stats['is_full'] = True
            else:
                stats['is_full'] = False

        return per_ns_stat

    @staticmethod
    def account_keys(data, couple):
        stats = []
        for group in couple.groups:
            try:
                stats.append(
                    reduce(lambda res, x: res + x,
                           [nb.stat for nb in group.node_backends if nb.stat]))
            except TypeError:
                continue
        if not stats:
            return
        files_stat = max(
            stats,
            key=lambda stat: (stat.files + stat.files_removed,
                              stat.files_removed))
        data['total_keys'] += files_stat.files + files_stat.files_removed
        data['removed_keys'] += files_stat.files_removed

    def per_entity_stat(self):
        def default():
            return {
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
        by_ns = defaultdict(default)
        by_ns_dc = defaultdict(lambda: defaultdict(default))

        dc_couple_map = defaultdict(set)
        ns_couple_map = defaultdict(set)
        ns_dc_couple_map = defaultdict(lambda: defaultdict(set))

        for group in sorted(storage.groups, key=lambda g: not bool(g.couple)):

            try:

                couple = (group.couple
                          if group.couple else
                          str(group.group_id))

                ns = group.couple and group.couple.namespace.id or None

                if ns == storage.Group.CACHE_NAMESPACE:
                    continue

                for node_backend in group.node_backends:

                    try:
                        dc = node_backend.node.host.dc
                    except CacheUpstreamError:
                        continue

                    if couple not in dc_couple_map[dc]:
                        self.account_couples(by_dc[dc], group)
                        if ns:
                            self.account_keys(by_dc[dc], couple)
                            self.account_effective_memory(by_dc[dc], couple)
                        dc_couple_map[dc].add(couple)
                    if ns and couple not in ns_couple_map[ns]:
                        self.account_couples(by_ns[ns], group)
                        self.account_effective_memory(by_ns[ns], couple)
                        self.account_keys(by_ns[ns], couple)
                        ns_couple_map[ns].add(couple)
                    if ns and couple not in ns_dc_couple_map[ns][dc]:
                        self.account_couples(by_ns_dc[ns][dc], group)
                        self.account_effective_memory(by_ns_dc[ns][dc], couple)
                        self.account_keys(by_ns_dc[ns][dc], couple)
                        ns_dc_couple_map[ns][dc].add(couple)

                    if not node_backend.stat:
                        logger.debug('No stats available for node %s' % str(node_backend))
                        continue

                    self.account_memory(by_dc[dc], group, node_backend.stat)

                    if ns:
                        self.account_memory(by_ns[ns], group, node_backend.stat)
                        self.account_memory(by_ns_dc[ns][dc], group, node_backend.stat)
            except Exception:
                logger.exception(
                    'Failed to account group {} in dc and ns statistics'.format(group.group_id)
                )
                continue

        self.count_outaged_couples(by_dc, by_ns_dc)

        return (h.defaultdict_to_dict(by_dc),
                h.defaultdict_to_dict(by_ns),
                h.defaultdict_to_dict(by_ns_dc))

    def count_outaged_couples(self, by_dc_stats, by_ns_stats):
        def default():
            return {
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
                    try:
                        affected_dcs.add(node_backend.node.host.dc)
                    except CacheUpstreamError:
                        continue

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

    def total_stats(self, per_dc_stat, per_ns_stat):
        dc_stats = per_dc_stat.values()
        for dc in dc_stats:
            if 'outages' in dc:
                del dc['outages']
        # TODO: Fix for empty storage case, 'reduce' call fails
        res = dict(reduce(self.dict_keys_sum, dc_stats))
        res['real_data'] = dict(reduce(self.dict_keys_sum, per_ns_stat.itervalues()))
        return res

    def get_couple_stats(self):
        open_couples = self.balancer._good_couples()
        bad_couples = self.balancer._get_couples_list({'state': 'bad'})
        broken_couples = self.balancer._get_couples_list({'state': 'broken'})
        closed_couples = self.balancer._closed_couples()
        frozen_couples = self.balancer._frozen_couples()
        uncoupled_groups = self.balancer._empty_group_ids()

        return {'open_couples': len(open_couples),
                'frozen_couples': len(frozen_couples),
                'closed_couples': len(closed_couples),
                'bad_couples': len(bad_couples),
                'broken_couples': len(broken_couples),
                'total_couples': (len(open_couples) + len(closed_couples) +
                                  len(frozen_couples) + len(bad_couples)),
                'uncoupled_groups': len(uncoupled_groups)}

    def calculate_flow_stats(self):
        per_dc_stat, per_ns_stat, per_ns_dc_stat = self.per_entity_stat()

        res = self.total_stats(per_dc_stat, per_ns_stat)
        res.update({'dc': per_dc_stat,
                    'namespaces': per_ns_dc_stat})

        res.update(self.get_couple_stats())

        return res

    @staticmethod
    def per_key_update(dest, src):
        for key, val in dest.iteritems():
            if key not in src:
                continue
            val.update(src[key])

    @h.concurrent_handler
    def get_groups_tree(self, request):

        try:
            options = request[0]
        except (TypeError, IndexError):
            options = {}

        namespace = options.get('namespace')
        status = options.get('couple_status')

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
                try:
                    parent = node_backend.node.host.parents
                except CacheUpstreamError:
                    logger.warn('Skipping {} because of cache failure'.format(
                        node_backend.node.host))
                    continue
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

    @h.concurrent_handler
    def get_couple_statistics(self, request):
        group_id = int(request[0])

        if group_id not in storage.groups:
            raise ValueError('Group %d is not found' % group_id)

        group = storage.groups[group_id]

        res = {}

        if group.couple:
            res = group.couple.info().serialize()
            res['status'] = res['couple_status']
            res['stats'] = self.__stats_to_dict(
                group.couple.get_stat(), group.couple.effective_space)
            groups = group.couple.groups
        else:
            groups = [group]

        res['groups'] = []
        for group in groups:
            g = group.info().serialize()
            try:
                group_stat = group.get_stat()
            except TypeError:
                group_stat = None
            g['stats'] = self.__stats_to_dict(group_stat, group.effective_space)
            for nb in g['node_backends']:
                nb['stats'] = self.__stats_to_dict(
                    storage.node_backends[nb['addr']].stat,
                    storage.node_backends[nb['addr']].effective_space)
            res['groups'].append(g)

        return res

    def __stats_to_dict(self, stat, eff_space):
        res = {}

        if not stat:
            return res

        res['total_space'] = stat.total_space
        res['free_space'] = stat.free_space

        res['free_effective_space'] = max(
            stat.free_space - (stat.total_space - eff_space),
            0.0
        )
        res['used_space'] = stat.used_space
        res['fragmentation'] = stat.fragmentation

        return res
