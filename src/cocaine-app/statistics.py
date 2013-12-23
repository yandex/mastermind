from collections import defaultdict

from cocaine.logging import Logger

import storage
from config import config


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
    def reduce_top_group_stats(st1, st2):
        return dict((k, min(st1[k], st2[k])) for k in st1)

    @staticmethod
    def redeem_space(host_space, couple_space):
        for k in host_space:
            min_val = min(host_space[k], couple_space[k])
            host_space[k] -= min_val
            couple_space[k] -= min_val

    def per_dc_stat(self):
        by_dc = defaultdict(lambda: {
            'free_space': 0.0,
            'total_space': 0.0,
            'effective_space': 0.0,
            'effective_free_space': 0.0,
            'uncoupled_space': 0.0,

            'open_couples': 0,
            'frozen_couples': 0,
            'total_couples': 0,
            'uncoupled_groups': 0,
        })

        host_fsid_map = defaultdict(set)
        couple_dc_map = defaultdict(set)

        for group in sorted(storage.groups, key=lambda g: not bool(g.couple)):
            for node in group.nodes:

                couple = (group.couple
                          if group.couple else
                          str(group.group_id))

                dc = node.host.dc

                if not dc in couple_dc_map[couple]:

                    if group.couple:
                        by_dc[dc]['total_couples'] += 1
                        if group.couple.status == storage.Status.OK:
                            by_dc[dc]['open_couples'] += 1
                        elif group.couple.status == storage.Status.FROZEN:
                            by_dc[dc]['frozen_couples'] += 1
                    else:
                        by_dc[dc]['uncoupled_groups'] += 1

                    couple_dc_map[couple].add(dc)

                if not node.stat:
                    logging.debug('No stats available for node %s' % str(node))
                    continue

                if node.stat.fsid in host_fsid_map[node.host]:
                    continue

                host_fsid_map[node.host].add(node.stat.fsid)

                if group.couple:
                    by_dc[dc]['free_space'] += node.stat.free_space
                    by_dc[dc]['total_space'] += node.stat.total_space
                    node_eff_space = max(min(node.stat.total_space - self.MIN_FREE_SPACE,
                                             node.stat.total_space * (1 - self.MIN_FREE_SPACE_REL)), 0.0)
                    by_dc[dc]['effective_space'] += node_eff_space
                    by_dc[dc]['effective_free_space'] += max(node.stat.free_space - (node.stat.total_space - node_eff_space), 0.0)
                else:
                    by_dc[dc]['uncoupled_space'] += node.stat.total_space
        return dict(by_dc)

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
                'total_couples': len(symmetric_couples) + len(frozen_couples) + len(bad_couples),
                'uncoupled_groups': len(uncoupled_groups)}


    def get_flow_stats(self, request):

        # total_space = 0.0
        # free_space = 0.0
        # eff_space = 0.0
        # eff_free_space = 0.0

        # open_couples = 0
        # total_couples = 0

        # host_fsid_memory_map = {}

        # for group in storage.groups:
        #     for node in group.nodes:
        #         if not node.stat:
        #             continue

        #         if not (node.host.addr, node.stat.fsid) in host_fsid_memory_map:
        #             eff_space = max(min(node.stat.total_space - self.MIN_FREE_SPACE,
        #                                 node.stat.total_space * (1 - self.MIN_FREE_SPACE_REL)), 0.0)
        #             host_fsid_memory_map[(node.host.addr, node.stat.fsid)] = {
        #                 'total_space': node.stat.total_space,
        #                 'free_space': node.stat.free_space,
        #                 'eff_space': eff_space,
        #                 'eff_free_space': max(node.stat.free_space - (node.stat.total_space - eff_space), 0.0),
        #             }

        # logging.info('addr-fsid map: %s' % (host_fsid_memory_map,))


        # for couple in storage.couples:

        #     if couple.status == storage.Status.OK:
        #         open_couples += 1

        #     total_couples += 1

        #     stat = couple.get_stat()

        #     if not stat:
        #         logging.debug('No stats available for couple %s' % (couple,))
        #         continue

        #     group_top_stats = []
        #     for group in couple.groups:
        #         group_top_stats.append(reduce(self.dict_keys_sum,
        #                [host_fsid_memory_map[(node.host.addr, node.stat.fsid)] for node in group.nodes]))

        #     couple_top_stats = reduce(self.reduce_top_group_stats, group_top_stats)


        #     #increase storage stats
        #     total_space += couple_top_stats['total_space']
        #     free_space += couple_top_stats['free_space']
        #     eff_space += couple_top_stats['eff_space']
        #     eff_free_space += couple_top_stats['eff_free_space']

        #     for group in couple.groups:

        #         for node in group.nodes:

        #             # decrease free and total space counters of fs
        #             self.redeem_space(host_fsid_memory_map[(node.host.addr, node.stat.fsid)],
        #                               couple_top_stats)


        per_dc_stat = self.per_dc_stat()

        res = self.total_stats(per_dc_stat)
        res.update({'dc': per_dc_stat})

        res.update(self.get_couple_stats())

        return res
