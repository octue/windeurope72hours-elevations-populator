import unittest

from h3.api.basic_int import h3_get_resolution, h3_to_parent

from elevations_populator.cells import (
    get_ancestors_up_to_minimum_resolution,
    get_ancestors_up_to_minimum_resolution_as_pyramid,
    get_descendents_down_to_maximum_resolution,
)


class TestGetDescendentsDownToMaximumResolution(unittest.TestCase):
    def test_with_resolution_12_cell(self):
        """Test that a resolution 12 cell is idempotent."""
        cell = 630949280220400639
        self.assertEqual(h3_get_resolution(cell), 12)
        self.assertEqual(get_descendents_down_to_maximum_resolution(cell, maximum_resolution=12), {cell})

    def test_with_resolution_11_cell(self):
        """Test that passing a resolution 11 cell results in 7 resolution 12 cells."""
        cell = 626445680593031167
        self.assertEqual(h3_get_resolution(cell), 11)

        descendents = get_descendents_down_to_maximum_resolution(cell, maximum_resolution=12)
        self.assertEqual(len(descendents), 7)

        for descendent in descendents:
            self.assertEqual(h3_get_resolution(descendent), 12)

    def test_with_resolution_10_cell(self):
        """Test that passing a resolution 10 cell results in 49 resolution 12 cells."""
        cell = 621942080965672959
        self.assertEqual(h3_get_resolution(cell), 10)

        descendents = get_descendents_down_to_maximum_resolution(cell, maximum_resolution=12)
        self.assertEqual(len(descendents), 49)

        for descendent in descendents:
            self.assertEqual(h3_get_resolution(descendent), 12)


class TestGetAncestorsUpToMinimumResolution(unittest.TestCase):
    def test_with_resolution_4_cell(self):
        """Test that getting the ancestors up to a minimum resolution of 4 of a resolution 4 cell results in the same
        resolution 4 cell.
        """
        cell = 594920487381893119
        self.assertEqual(h3_get_resolution(cell), 4)

        ancestors = get_ancestors_up_to_minimum_resolution(cell, minimum_resolution=4)
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
        resolution_12_cells = {
            630949280578134527,
            630949280578130431,
            630949280578109951,
            630949280578114047,
            630949280578122239,
            630949280578118143,
            630949280578126335,
            630949280578130943,
            630949280578135039,
            630949280578110463,
            630949280578114559,
            630949280578122751,
            630949280578118655,
            630949280578126847,
            630949280578135551,
            630949280578131455,
            630949280578110975,
            630949280578115071,
            630949280578123263,
            630949280578119167,
            630949280578127359,
            630949280578115583,
            630949280578123775,
            630949280578131967,
            630949280578136063,
            630949280578119679,
            630949280578127871,
            630949280578136575,
            630949280578132479,
            630949280578111999,
            630949280578116095,
            630949280578124287,
            630949280578120191,
            630949280578128383,
            630949280578137087,
            630949280578132991,
            630949280578112511,
            630949280578116607,
            630949280578124799,
            630949280578120703,
            630949280578128895,
            630949280578137599,
            630949280578133503,
            630949280578113023,
            630949280578117119,
            630949280578125311,
            630949280578121215,
            630949280578129407,
            630949280578111487,
        }

        pyramid = get_ancestors_up_to_minimum_resolution_as_pyramid(resolution_12_cells, minimum_resolution=10)

        self.assertEqual(
            pyramid,
            [
                {
                    626445680950743039,
                    626445680950747135,
                    626445680950751231,
                    626445680950755327,
                    626445680950759423,
                    626445680950763519,
                    626445680950767615,
                },
                {621942081323401215},
            ],
        )

        # Check that the zeroth row of the pyramid comprises resolution 11 cells only and the first row comprises
        # resolution 10 cells only.
        self.assertTrue(all([h3_get_resolution(cell) == 11 for cell in pyramid[0]]))
        self.assertTrue(all([h3_get_resolution(cell) == 10 for cell in pyramid[1]]))

        # Check that the zeroth row of the pyramid comprises all the parents of the resolution 12 cells.
        parents_of_resolution_12_cells = {h3_to_parent(cell) for cell in resolution_12_cells}
        self.assertEqual(parents_of_resolution_12_cells, pyramid[0])

        # Check that the first row of the pyramid comprises all the parents of the resolution 11 cells or, equivalently,
        # all the parents of the zeroth row of the pyramid.
        parents_of_resolution_11_cells = {h3_to_parent(cell) for cell in pyramid[0]}
        self.assertEqual(parents_of_resolution_11_cells, pyramid[1])
