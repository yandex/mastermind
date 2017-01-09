import logging
import itertools

from mastermind.utils.tree_picker import TreePicker

import infrastructure
import inventory
from jobs.job_types import JobTypes
from mastermind_core.config import config
import storage


logger = logging.getLogger('mm.lrc_builder')

LRC_CFG = config.get('lrc', {})


class LRC_8_2_2_V1_Builder(object):

    DCS_COUNT = 3
    LRC_GROUPS_PER_GROUP = 8
    TOTAL_GROUPS_COUNT = 8 + 2 + 2
    GROUPS_PER_DC = TOTAL_GROUPS_COUNT / DCS_COUNT

    DC_NODE_TYPE = inventory.get_dc_node_type()

    CFG = LRC_CFG.get('lrc-8-2-2-v1', {})

    # Builder can be configured to restrict the number
    # of selected groups for building an lrc groupset
    # per cluster node.
    # For example, it would be wise to create lrc groupset
    # from groups residing on separate hosts:
    #     "lrc": {
    #         "lrc_groups_per": {
    #             "host": 1
    #         }
    #     }
    # Any inventory cluster node type can be used as a key
    # to "lrc_groups_per" object.
    CLUSTER_NODE_LRC_GROUPS_LIMITS = CFG.get('lrc_groups_per', {})

    def __init__(self, job_processor=None):
        self.lrc_tree = None
        self.lrc_nodes = None
        self.job_processor = job_processor

    def build(self, count, mandatory_dcs=None):
        """ Create jobs for preparing at least `count` lrc groupsets

        `mandatory_dcs` optional parameter can be used to
        set certain dcs to obtain certain stripe part groups.
        E.g. if you want to use 'dc_parity' for parity parts and 'dc1'
        for parts 0, 1, 2 and 3, and do not care about dc for parts 4, 5, 6 and 7,
        you can set 'dcs' to ['dc1', None, 'dc_parity'].
        Then the builder will use supplied dcs and additionally
        choose the dc (for parts 4 to 7).

        Parameters:
            mandatory_dcs: a list of up to 3 dcs to use for placing
                lrc groups (``None`` value can be used instead of
                any dc value - such values will be replaced by
                selection algorithm)
                NOTE: each unique dc id should occur in dcs list only once

        Returns:
            a list of results where each result is one of the following:
                - job that is created for prepraing lrc groups;
                - error string with description of the exception that happened;
        """
        if not self.job_processor:
            raise RuntimeError('Builder requires job processor to build jobs')

        built_couples = 0

        selected_groups = self.select_uncoupled_groups(mandatory_dcs)

        jobs = []
        while built_couples < count:
            try:
                uncoupled_groups = next(selected_groups)
                total_space = min(
                    storage.groups[gid].node_backends[0].stat.total_space
                    for gid in uncoupled_groups
                )
                lrc_group_total_space = total_space / self.LRC_GROUPS_PER_GROUP
                logger.debug(
                    'Total space to use for groups {groups}: {total_space}'.format(
                        groups=uncoupled_groups,
                        total_space=lrc_group_total_space,
                    )
                )

                new_groups_count = len(uncoupled_groups) * self.LRC_GROUPS_PER_GROUP
                new_groups_ids = infrastructure.infrastructure.reserve_group_ids(new_groups_count)
                job = self.job_processor._create_job(
                    JobTypes.TYPE_MAKE_LRC_GROUPS_JOB,
                    params={
                        'uncoupled_groups': uncoupled_groups,
                        'lrc_groups': new_groups_ids,
                        'total_space': lrc_group_total_space,
                    },
                )

                built_couples += self.LRC_GROUPS_PER_GROUP
                jobs.append(job.dump())
            except StopIteration:
                error = 'Failed to select groups suitable for lrc groupset'
                jobs.append(error)
                break
            except Exception as e:
                logger.exception('Failed to build lrc groups')
                jobs.append(str(e))
                break

        return jobs

    def select_uncoupled_groups(self, mandatory_dcs=None, skip_groups=None):
        """ Get uncoupled groups that can be used for LRC groupset construction

        Parameters:
            mandatory_dcs: see 'build' method docs;

        Yields:
            a list of uncoupled group ids in scheme's specific order that can be used
            for LRC groupset construction;
        """

        self.lrc_tree, self.lrc_nodes = self._build_lrc_tree()

        selected_groups = self._select_groups(
            mandatory_dcs or [],
            skip_groups=skip_groups,
        )
        while True:
            groups_lists = next(selected_groups)
            uncoupled_groups = [
                group_id
                for dc_group_ids in groups_lists
                for group_id in dc_group_ids
            ]
            logger.debug(
                'Selected uncoupled groups for lrc groupsets: {}'.format(
                    uncoupled_groups
                )
            )
            yield storage.Lrc.Scheme822v1.order_groups(groups_lists)

    def _build_lrc_tree(self):
        node_types = (self.DC_NODE_TYPE,) + ('host',)
        tree, nodes = infrastructure.infrastructure.filtered_cluster_tree(node_types)

        # NOTE:
        # lrc groups that are currently being processed by running jobs
        # are not accounted here because there is no easy and
        # straightforward way to do this. This is not crucial
        # at the moment.
        lrc_types = (storage.Group.TYPE_UNCOUPLED_LRC_8_2_2_V1, storage.Group.TYPE_LRC_8_2_2_V1)
        lrc_groups = (
            group
            for group in storage.groups.keys()
            if group.type in lrc_types
        )

        # TODO: rename, nothing about "ns" here
        infrastructure.infrastructure.account_ns_groups(nodes, lrc_groups)
        infrastructure.infrastructure.update_groups_list(tree)

        return tree, nodes

    def _select_groups(self, mandatory_dcs, skip_groups=None):
        """ Get generator producing groups for lrc groupset

        Generator selects 4 groups in each of 3 dcs, total of 12 groups.
        `mandatory_dcs` parameter is described above.

        Groups are selected based on several criteria:
            1) they must have the same 'total_space';
            2) groups in each dc should be located as far as possible
            from each other (in terms of cluster nodes -- the less
            network hardware they share the better);
            3) groups in each dc should satisfy configurable lrc groups
            restrictions (["lrc"]["lrc_groups_per"][...] config sections);

        Yields:
            a list of lists, where each nested list consists of uncoupled groups
            in a certain dc that should be used for constructing lrc groupsets

        Example:
            [
                [1001, 1002, 1003, 1004],  # groups for data parts 0, 1, 4, 5
                [1005, 1006, 1007, 1008],  # groups for data parts 2, 3, 6, 7
                [1009, 1010, 1011, 1012],  # groups for l1, l2, g1, g2 parity parts
            ]
        """
        groups_by_total_space = infrastructure.infrastructure.groups_by_total_space(
            match_group_space=True,
            max_node_backends=1,
            skip_groups=skip_groups,
        )

        # TODO: move out to infrastructure
        node_types = ['root'] + inventory.get_node_types() + ['hdd']

        mandatory_dcs = mandatory_dcs[:]
        mandatory_dcs.extend([None] * (self.DCS_COUNT - len(mandatory_dcs)))

        for ts, group_ids in groups_by_total_space.iteritems():

            logger.debug(
                'Selecting among groups with total space {total_space}: {groups}'.format(
                    total_space=ts,
                    groups=group_ids,
                )
            )

            tree, nodes = infrastructure.infrastructure.filtered_cluster_tree(node_types)

            # TODO: rename, nothing about "ns" here
            infrastructure.infrastructure.account_ns_groups(
                nodes,
                (storage.groups[group_id] for group_id in group_ids)
            )
            infrastructure.infrastructure.update_groups_list(tree)

            def _pick_groups_from_dc(dc, picker, count):
                groups_ids = list(
                    itertools.islice(picker, self.GROUPS_PER_DC)
                )
                if len(groups_ids) != self.GROUPS_PER_DC:
                    # not enough groups are left in the tree
                    logger.debug(
                        'Dc {dc}: does not have '
                        'enough uncoupled groups'.format(
                            dc=dc,
                        )
                    )
                    raise StopIteration

                logger.debug(
                    'Dc {dc}: selected groups {groups}'.format(
                        dc=dc,
                        groups=groups_ids,
                    )
                )

                if not self._check_lrc_groups_restrictions(groups_ids, nodes):
                    logger.debug(
                        'Dc {dc}: no more uncoupled groups '
                        'that satisfy lrc groups restrictions are available'.format(
                            dc=dc,
                        )
                    )
                    raise StopIteration

                return groups_ids

            try:
                while True:

                    pickers = {
                        dc: self._make_tree_picker(subtree)
                        for dc, subtree in nodes[self.DC_NODE_TYPE].iteritems()
                    }

                    logger.debug('Pickers: {}'.format(pickers))

                    non_mandatory_dcs = [
                        dc
                        for dc in self._dcs_in_preferable_order()
                        if dc not in mandatory_dcs
                    ]
                    logger.debug('Mandatoty dcs: {}, non-mandatory dcs: {}'.format(
                        mandatory_dcs, non_mandatory_dcs,
                    ))
                    selected_groups = []

                    for dc in mandatory_dcs:
                        if dc:
                            # `dc` is a mandatory dc, need to find 4 suitable
                            # uncoupled groups
                            logger.debug(
                                'Dc {dc}: mandatory, picking {count} groups'.format(
                                    dc=dc,
                                    count=self.GROUPS_PER_DC,
                                )
                            )
                            group_ids = _pick_groups_from_dc(
                                dc=dc,
                                picker=pickers[dc],
                                count=self.GROUPS_PER_DC,
                            )
                        else:
                            # not mandatory dc, need to find 4 suitable
                            # uncoupled groups in any non-mandatory dc
                            group_ids = []
                            while non_mandatory_dcs:
                                dc = non_mandatory_dcs.pop(0)
                                logger.debug(
                                    'Dc {dc}: non-mandatory, picking {count} groups'.format(
                                        dc=dc,
                                        count=self.GROUPS_PER_DC,
                                    )
                                )
                                try:
                                    group_ids = _pick_groups_from_dc(
                                        dc=dc,
                                        picker=pickers[dc],
                                        count=self.GROUPS_PER_DC,
                                    )
                                    break
                                except StopIteration:
                                    continue
                            if not group_ids:
                                # no suitable dc found
                                raise StopIteration

                        selected_groups.append(group_ids)

                    yield selected_groups

                    self._account_selected_groups(tree, nodes, selected_groups)

            except StopIteration:
                # here, one of the following happened:
                # 1) some mandatory dc either doesn't have enough uncoupled groups
                #   or they don't satisfy lrc groups restrictions checks;
                # 2) non-mandatory dc with enough couples satisfying lrc groups
                #   restrictions was not found;
                logger.debug(
                    'Appropriate uncoupled groups are not found among '
                    'uncoupled groups with total space {}'.format(ts)
                )
                # continue selection with another total space uncoupled groups
                continue

    def _make_tree_picker(self, subtree):
        """ Make a TreePicker object based on dc cluster subtree.

        Provides simple selection of uncoupled groups
        that are the most distant in terms of cluster network hardware.
        """

        def convert_to_nested_list(subtree):
            nested_list = []
            src_node_iterators = [iter(subtree.get('children', []))]
            dst_nodes = [nested_list]
            while src_node_iterators:
                cur_src_node_iterator = src_node_iterators[-1]
                try:
                    src_node = next(cur_src_node_iterator)
                except StopIteration:
                    src_node_iterators.pop()
                    dst_nodes.pop()
                    continue
                new_node = []
                dst_node = dst_nodes[-1]
                dst_node.append(new_node)
                if src_node['type'] == 'hdd':
                    # groups are leaf elements
                    new_node.extend(src_node['groups'])
                else:
                    dst_nodes.append(new_node)
                    src_node_iterators.append(iter(src_node.get('children', [])))

            return nested_list

        return TreePicker(
            convert_to_nested_list(subtree),
            select=self._select_best_uncoupled_group,
        )

    def _dcs_in_preferable_order(self):
        """ Get dcs in preferable order.

        Dcs are sorted by the average number of lrc groups per host.
        """
        dc_nodes = self.lrc_nodes[self.DC_NODE_TYPE]

        def avg_lrc_groups_per_host(dc):
            return float(len(dc_nodes[dc]['groups'])) / len(dc_nodes[dc]['children'])

        return sorted(
            dc_nodes.iterkeys(),
            key=avg_lrc_groups_per_host,
        )

    def _check_lrc_groups_restrictions(self, group_ids, nodes):
        """ Check selected groups against lrc groups restrictions.

        Restrictions are configurable via "lrc_groups_per" settings,
        see "CLUSTER_NODE_LRC_GROUPS_LIMITS" attribute's description.
        """
        queue = []
        for dc_node in nodes[self.DC_NODE_TYPE].itervalues():
            queue.extend(dc_node['children'])

        group_ids = set(group_ids)

        while queue:
            node = queue.pop(0)
            if node['type'] in self.CLUSTER_NODE_LRC_GROUPS_LIMITS:
                limit = self.CLUSTER_NODE_LRC_GROUPS_LIMITS[node['type']]
                selected_groups_in_node = node['groups'] & group_ids
                if len(selected_groups_in_node) > limit:
                    logger.debug(
                        'Found {groups_in_node} groups in node {node} '
                        'of type {type}, limit is {limit} groups'.format(
                            groups_in_node=list(selected_groups_in_node),
                            node=node['full_path'],
                            type=node['type'],
                            limit=limit,
                        )
                    )
                    return False
            queue.extend(node.get('children', []))
        return True

    def _account_selected_groups(self, tree, nodes, group_ids):
        """ Change the state of internal structures
        for invariance.

        Following actions are taken:
            1) selected groups are removed from the current
            uncoupled group tree;
            2) selected groups are added to the lrc groups tree
            to keep group selection properties;
        """
        group_ids = set(
            group_id
            for dc_group_ids in group_ids
            for group_id in dc_group_ids
        )
        for hdd_node in nodes['hdd'].itervalues():
            hdd_node['groups'] = hdd_node.get('groups', set()) - group_ids

        infrastructure.infrastructure.update_groups_list(tree)

        for group_id in group_ids:
            nb = storage.groups[group_id].node_backends[0]
            host = nb.node.host
            hdd_full_path = host.full_path + '|' + str(nb.fs.fsid)
            hdd_node = self.lrc_nodes['hdd'][hdd_full_path]
            hdd_node.setdefault('groups', set()).add(group_id)

        infrastructure.infrastructure.update_groups_list(self.lrc_tree)

    def _select_best_uncoupled_group(self, group_ids):
        """ Select single best group id from `group_ids`.

        Group is selected based on the number of existing lrc groups
        on group's host -- the less the better.
        """

        def lrc_groups_on_host(group_id):
            group = storage.groups[group_id]
            host = group.node_backends[0].node.host
            host_lrc_groups_ids = self.lrc_nodes['host'][host.full_path].get('groups', [])
            return len(host_lrc_groups_ids)

        return min(
            group_ids,
            key=lrc_groups_on_host,
        )
