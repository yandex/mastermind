import itertools

import pytest

from mastermind.utils.tree_picker import TreePicker


class TestTreePicker(object):
    def test_one_branch_tree(self):
        tp = TreePicker([1])
        assert next(tp) == 1

    def test_one_branch_multi_level_tree(self):
        tp = TreePicker([[[1]]])
        assert next(tp) == 1

    def test_one_branch_tree_exhausts(self):
        tp = TreePicker([[1]])
        assert next(tp) == 1
        with pytest.raises(StopIteration):
            next(tp)

    def test_multi_branch_tree(self):
        tp = TreePicker([[1, 1], [2, 2]])
        assert set(itertools.islice(tp, 2)) == set([1, 2])

    def test_multi_branch_tree_exhausts(self):
        tp = TreePicker([[1, 1], [2, 2]])
        assert set(itertools.islice(tp, 4)) == set([1, 2])
        with pytest.raises(StopIteration):
            next(tp)

    def test_less_dependent_branch(self):
        """ Test that less dependent leaf nodes will
        be returned earlier than more dependent ones.
              root
             /    \
           /  \    \
          /   /    / \
          1  1    2   2
        """
        tp = TreePicker([
            [
                [
                    1,
                ],
                [
                    1,
                ],
            ],
            [
                [
                    2,
                    2,
                ]
            ],
        ])
        assert sorted(itertools.islice(tp, 3)) == [1, 1, 2]

    def test_independent_branches(self):
        """ Test that elements will be preferably picked from
        independent branches>
                root
             /   |     \
           /    / \   / | \
          1    2   2  3 3 3
        """
        tp = TreePicker([
            [
                1,
            ],
            [
                2,
                2,
            ],
            [
                3,
                3,
                3,
            ],
        ])
        assert set(itertools.islice(tp, 3)) == set([1, 2, 3])

    def test_custom_select_algorithm(self):
        """ Test that elements will be picked according
        to custom select algorithm provided by user
                root
             /   |     \
           /    / \   / | \
          1    2   2  3 3 3
        """
        tp = TreePicker(
            [
                [
                    1,
                ],
                [
                    2,
                    2,
                ],
                [
                    3,
                    3,
                    3,
                ],
            ],
            select=max,
        )
        assert list(itertools.islice(tp, 3)) == [3, 2, 1]
