from functools import total_ordering


class TreePicker(object):
    """ Iterator for picking the most distant leaves in tree-like structures

    When the value is returned by the iterator, each edge forming the path
    to the corresponding leaf node gets its weight increased, therefore
    all the nodes that are in the same subtree as the selected node get
    some penalty depending on the number of edges that they share with
    that node.

    Each round the choice is being made of the leaf nodes having the least
    path weight.

    The iterator works the following way:
        1) build the tree structure based on "tree" parameter supplied
            to the "__init__" method. "Tree" should be supplied as a
            multi-level nested sequence of type "list" or "tuple" where each
            level represent parent-child relationship in the resulting tree.
            Exapmle:
                [[1, 2], [3]]
                # root node has two children nodes, we'll call them A and B;
                # child node A has two leaves, 1 and 2;
                # child node B has one leaf, 3;
        2) at this moment all leaf nodes are considered equally good
            for selection (all path weights are zero), and iteration can be started;
        3) for example, call to method "next" gets you the value of 2.
            Iterator increases weights of the edges Root->A and A->2 by 1.
            Therefore, node with value 2 has path weight of 2 (1 for Root->A and 1 for A->2),
            and node with value 1 has path weight of 1 (1 for Root->A and 0 for A->1);
            node with value 3 still has path weight of 0.
        4) the next call to "next" will return 3 because it has the least path weight
            of 0; then its path weight will be increased to 2.
        5) now the node with value 1 has the least path weight of 1, so it will be returned
            on the next call.

    NOTE: default algorithm for selecting a leaf node among those having
        the least path weight is "random choice".
    NOTE: custom algorithm for selecting preferable leaf node among those having
        the least path weight can be supplied by the user to the "__init__" method.
        It should be a callable accepting iterable as input and
        returning back some element from this iterable.
    NOTE: the value returned from the "select" is compared by identity
        to elements from candidates list (i.e. using "is" operator).
        This means that it cannot distinguish between identical objects
        (e.g. using 'int' as leaf node values may cause side-effects).
    """

    class Node(object):
        __slots__ = ['children', 'parent', 'depth']

        def __init__(self, parent=None, children=None):
            self.parent = parent
            self.children = children or []
            if parent is None:
                self.depth = 0
            else:
                self.depth = parent.depth + 1
            if self.parent:
                self.parent.children.append(self)

    @total_ordering
    class LeafNode(Node):
        __slots__ = ['value', 'path_weight']

        def __init__(self, value, parent=None):
            super(TreePicker.LeafNode, self).__init__(parent=parent)
            self.value = value
            self.path_weight = 0

        def __lt__(self, other):
            return self.path_weight < other.path_weight

        def __eq__(self, other):
            return self.path_weight == other.path_weight

    def __init__(self, tree, select=None):
        self._leaves = []

        self._root = TreePicker.Node()
        self._build_tree(tree)

        self._select = select or self._random_select

    def __iter__(self):
        return self

    def next(self):
        if not self._leaves:
            raise StopIteration

        cur_leaf = self._leaves[0]
        candidates = [
            leaf
            for leaf in self._leaves
            if leaf <= cur_leaf
        ]

        selected_leaf_value = self._select(candidate.value for candidate in candidates)

        for leaf in candidates:
            if selected_leaf_value is leaf.value:
                selected_leaf = leaf
                break
        else:
            raise RuntimeError(
                'Selected value {} does not match any of the candidate leaves'.format(
                    selected_leaf_value
                )
            )

        # process selected leaf's path back to root node
        self._update_path_weights(selected_leaf)

        self._leaves.remove(selected_leaf)

        return selected_leaf.value

    def _build_tree(self, tree):

        src_node_iterators = [iter(tree)]
        dst_nodes = [self._root]

        while src_node_iterators:
            cur_src_node_iterator = src_node_iterators[-1]
            try:
                src_node = next(cur_src_node_iterator)
            except StopIteration:
                src_node_iterators.pop()
                dst_nodes.pop()
                continue

            dst_node = dst_nodes[-1]

            if isinstance(src_node, (list, tuple)):
                # not a leaf node
                src_node_iterators.append(iter(src_node))
                new_dst_node = TreePicker.Node(parent=dst_node)
                dst_nodes.append(new_dst_node)
            else:
                # leaf node
                new_dst_leaf_node = TreePicker.LeafNode(
                    value=src_node,
                    parent=dst_node,
                )
                self._leaves.append(new_dst_leaf_node)

    def _update_path_weights(self, leaf):
        """ Update path weights of leaf nodes that share path with 'leaf'
        """

        parent = leaf
        path_weight_added = [leaf.depth]
        updated_leaf_path_nodes = set()

        def update_children_weights(node):

            nodes = [node]
            node_children_iterators = [iter(node.children)]

            while node_children_iterators:
                cur_node_children_iterator = node_children_iterators[-1]
                try:
                    next_child = next(cur_node_children_iterator)
                except StopIteration:
                    cur_node = nodes.pop()
                    node_children_iterators.pop()
                    if isinstance(cur_node, TreePicker.LeafNode):
                        cur_node.path_weight += path_weight_added[0]
                    if not nodes:
                        path_weight_added[0] -= 1
                        updated_leaf_path_nodes.add(node)
                    continue
                if next_child not in updated_leaf_path_nodes:
                    nodes.append(next_child)
                    node_children_iterators.append(iter(next_child.children))

        # path weights in a tree is updated in a bottom-top way:
        # - subtree of node 'leaf' is updated (just 'leaf' itself);
        # - 'leaf's parent subtree is updated ('leaf's first-order siblings)
        # - so on till the root
        # On each step processed leaves are added to 'updated_leaf_path_nodes'
        # to skip their processing on next step.
        while parent is not self._root:
            update_children_weights(parent)
            parent = parent.parent

        self._leaves.sort()

    def _random_select(self, candidates):
        import random
        return random.choice(list(candidates))
