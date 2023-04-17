import unittest

from h3.api.basic_int import h3_get_resolution, h3_to_children, h3_to_parent

from elevations_populator.cells import (
    get_ancestors_up_to_minimum_resolution,
    get_ancestors_up_to_minimum_resolution_as_pyramid,
    get_descendents_down_to_maximum_resolution,
)


class TestGetDescendentsDownToMaximumResolution(unittest.TestCase):
    def test_with_maximum_resolution_cell(self):
        """Test that a maximum resolution cell is idempotent."""
        cell = 630949280220400639
        maximum_resolution = 12
        self.assertEqual(h3_get_resolution(cell), maximum_resolution)

        self.assertEqual(
            get_descendents_down_to_maximum_resolution(cell, maximum_resolution=maximum_resolution),
            {cell},
        )

    def test_with_resolution_11_cell(self):
        """Test that inputting a resolution 11 cell results in 7 resolution 12 cells when the maximum resolution is set
        to 12.
        """
        cell = 626445680593031167
        self.assertEqual(h3_get_resolution(cell), 11)

        descendents = get_descendents_down_to_maximum_resolution(cell, maximum_resolution=12)
        self.assertEqual(len(descendents), 7)
        self.assertEqual(descendents, h3_to_children(cell))

    def test_with_resolution_10_cell(self):
        """Test that inputting a resolution 10 cell results in 49 resolution 12 cells when the maximum resolution is set
        to 12.
        """
        cell = 621942080965672959
        self.assertEqual(h3_get_resolution(cell), 10)

        descendents = get_descendents_down_to_maximum_resolution(cell, maximum_resolution=12)
        self.assertEqual(len(descendents), 49)

        descendent_parents = {h3_to_parent(descendent) for descendent in descendents}
        self.assertEqual({h3_to_parent(descendent_parent) for descendent_parent in descendent_parents}, {cell})


class TestGetAncestorsUpToMinimumResolution(unittest.TestCase):
    def test_with_minimum_resolution_cell(self):
        """Test that a minimum resolution cell is idempotent."""
        cell = 594920487381893119
        minimum_resolution = 4
        self.assertEqual(h3_get_resolution(cell), minimum_resolution)

        ancestors = get_ancestors_up_to_minimum_resolution(cell, minimum_resolution=minimum_resolution)
        self.assertEqual(ancestors, [cell])

    def test_with_resolution_5_cell(self):
        """Test that getting the ancestors up to a minimum resolution of 4 of a resolution 5 cell results in the cell's
        parent.
        """
        cell = 599424083788038143
        self.assertEqual(h3_get_resolution(cell), 5)

        ancestors = get_ancestors_up_to_minimum_resolution(cell, minimum_resolution=4)
        self.assertEqual([h3_get_resolution(ancestor) for ancestor in ancestors], [4])
        self.assertEqual(ancestors[0], h3_to_parent(cell))

    def test_with_resolution_6_cell(self):
        """Test that getting the ancestors up to a minimum resolution of 4 of a resolution 6 cell results in the cell's
        parent and grandparent.
        """
        cell = 603927682878537727
        self.assertEqual(h3_get_resolution(cell), 6)

        ancestors = get_ancestors_up_to_minimum_resolution(cell, minimum_resolution=4)
        self.assertEqual([h3_get_resolution(ancestor) for ancestor in ancestors], [5, 4])
        self.assertEqual(ancestors[0], h3_to_parent(cell))
        self.assertEqual(ancestors[1], h3_to_parent(h3_to_parent(cell)))


class TestGetAncestorsUpToMinimumResolutionAsPyramid(unittest.TestCase):
    def test_with_resolution_12_cells_and_minimum_resolution_of_10(self):
        """Test that, given a set of resolution 12 cells, an inverse pyramid of their ancestors up to resolution 10 is
        constructed where the zeroth row comprises all the parents of the resolution 12 cells and the first row
        comprises the parents of the zeroth row.
        """
        resolution_10_grandparent = 621942081323401215
        self.assertEqual(h3_get_resolution(resolution_10_grandparent), 10)

        resolution_11_parents = h3_to_children(resolution_10_grandparent)
        self.assertTrue(all([h3_get_resolution(cell) == 11 for cell in resolution_11_parents]))

        resolution_12_cells = {child for parent in resolution_11_parents for child in h3_to_children(parent)}

        pyramid = get_ancestors_up_to_minimum_resolution_as_pyramid(resolution_12_cells, minimum_resolution=10)
        self.assertEqual(pyramid, [resolution_11_parents, {resolution_10_grandparent}])
