import bisect
import functools
import logging

from errors import CacheUpstreamError
import inventory
import storage


logger = logging.getLogger('mm.infrastructure')


class Artifacts(dict):
    def key(self):
        return self


@functools.total_ordering
class Node(object):
    """ Representation of a node in a cluster tree.

    NOTE: `full_path` was used in early version of a cluster tree, it is constructed as names of
    all node's predecessors plus name of node itself concatenated together using '|'.
    It seems redundant at the moment since almost all use cases of cluster tree utilize only
    'dc' and 'host' levels of a cluster. On 'dc' and 'host' levels any node has unique name.
    Employing just node's name instead of full path simplifies mapping Host object to a cluster
    tree node (since it does not require iterating through `parents` tree).
    Although if any of the intermediate nodes mapping is required one may revisit this decision
    and turn back to using node's full path as a mapping key. There is no guarantee that
    intermediate nodes have unique name cluster-wise.
    """

    __slots__ = (
        'parent',
        'children',
        'type',
        'name',
        'full_path',
        'artifacts',
        'groups',

        # NOTE: move to subclass?
        'addr',  # custom attribute for nodes of type 'host'
    )

    ArtifactsType = Artifacts

    def __init__(self, parent, tree_node, full_path):
        self.children = {}
        self.type = tree_node['type']
        self.name = tree_node['name']
        self.full_path = full_path

        self.parent = parent

        self.groups = {}
        self.artifacts = self.ArtifactsType()

    def find(self, type):
        """ Yield all nodes of type `type` in Node's subtree
        """
        queue = [self.children.itervalues()]

        while queue:
            try:
                node = next(queue[-1])
            except StopIteration:
                queue.pop()
                continue
            if node.type == type:
                yield node
            else:
                queue.append(node.children.itervalues())

    def sorted_subset(self, type):
        return NodesSubset(self.find(type=type))

    def key(self):
        return self.artifacts.key()

    def is_participant(self):
        return True

    def __lt__(self, other):
        return self.key() < other.key()

    def __eq__(self, other):
        return self.key() == other.key()


class RootNode(Node):
    def __init__(self):
        super(RootNode, self).__init__(
            parent=None,
            tree_node={
                'type': 'root',
                'name': '',
            },
            full_path='',
        )


class ClusterTree(object):

    NodeType = Node

    def __init__(self,
                 groups,
                 job_processor=None,
                 node_types=None,
                 on_account_group=None,
                 on_account_job=None):
        # "easy access" indexes
        # TODO: indexes for all node types?
        self.dcs = {}
        self.hosts = {}
        self.hdds = {}

        self.job_processor = job_processor

        if on_account_group:
            self._on_account_group = on_account_group
        if on_account_job:
            self._on_account_job = on_account_job

        # NOTE: inventory functions are called here and not at global module level because of
        # possible implementation errors in third-party inventory implementation
        self._node_types = list(node_types or inventory.get_node_types())
        self._dc_node_type = inventory.get_dc_node_type()

        self._root = RootNode()
        self._build_tree(groups)

        self.account_jobs()

    def _build_tree(self, groups):

        # TODO: support per-namespace cluster tree (account hosts that
        # participate in a namespace only (?))

        for host in storage.hosts.keys():
            try:
                tree_node = host.parents
            except CacheUpstreamError:
                continue

            tree_nodes = []
            cur_node = tree_node
            while 'parent' in cur_node:
                tree_nodes.append(cur_node)
                cur_node = cur_node['parent']
            tree_nodes.append(cur_node)

            node = self._root

            node_types = iter(self._node_types)
            expected_node_type = next(node_types)
            matched_types = []

            parts = []

            for tn in reversed(tree_nodes):

                parts.append(tn['name'])

                if tn['type'] != expected_node_type:
                    continue

                matched_types.append(expected_node_type)

                if tn['name'] not in node.children:
                    new_node = self.NodeType(node, tn, full_path='|'.join(parts))
                    node.children[new_node.name] = new_node

                    # filling up "easy access" indexes
                    if new_node.type == self._dc_node_type:
                        self.dcs[new_node.name] = new_node
                    elif new_node.type == 'host':
                        # custom attribute for host nodes
                        new_node.addr = host.addr
                        self.hosts[new_node.name] = new_node

                    node = new_node
                else:
                    node = node.children[tn['name']]

                try:
                    expected_node_type = next(node_types)
                except StopIteration:
                    break

            if matched_types != self._node_types:
                raise RuntimeError(
                    'Host {host}: failed to parse parent tree, matched node types: {matched}, '
                    'expected: {expected}'.format(
                        host=host,
                        matched=matched_types,
                        expected=self._node_types,
                    )
                )

        # Adding hdd level to cluster tree
        for fs in storage.fs:
            if fs.host.hostname not in self.hosts:
                # failed to fetch host tree
                continue
            host_node = self.hosts[fs.host.hostname]

            fs_id = str(fs)

            new_node = self.NodeType(
                parent=host_node,
                tree_node={
                    'type': 'hdd',
                    'name': fs_id,
                },
                full_path='{}|{}'.format(host_node.full_path, fs_id)
            )
            host_node.children[fs_id] = new_node

            self.hdds[new_node.name] = new_node

        # Filling hdd level with groups
        for group in groups:
            if len(group.node_backends) > 1:
                logger.warn('Cluster tree: group {} has unexpected number of backends: {}'.format(
                    group,
                    len(group.node_backends)
                ))
                continue

            for nb in group.node_backends:
                if not nb.fs:
                    logger.warn('Cluster tree: group {} has backend without fs: {}'.format(
                        group,
                        nb.fs
                    ))
                    continue
                fs_id = str(nb.fs)
                if fs_id not in self.hdds:
                    # failed to fetch host tree
                    continue
                self.hdds[fs_id].groups[group] = group
                self.account_group(self.hdds[fs_id], group)

    def find_jobs(self):
        raise NotImplementedError()

    def account_jobs(self):
        for job in self.find_jobs():
            self.account_job(job)

    def account_group(self, hdd_node, group):
        self._on_account_group(hdd_node, group)

    def account_job(self, job):
        self._on_account_job(job)

    def sort(self, nodes):
        for node in sorted(nodes):
            yield node

    def _on_account_group(self, hdd_node, group):
        pass

    def _on_account_job(self, job):
        pass


class NodesSubset(object):
    def __init__(self, nodes):
        self.nodes = sorted(nodes)

    def __iter__(self):
        return NodesSubsetIterator(self.nodes)

    def consume(self, node):
        self.nodes.pop(self.nodes.index(node))
        bisect.insort_right(self.nodes, node)


class NodesSubsetIterator(object):
    def __init__(self, nodes):
        self.it = iter(nodes)

    def __iter__(self):
        return self

    def next(self):
        return next(self.it)
