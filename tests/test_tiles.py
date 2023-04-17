import unittest

from elevations_populator.tiles import get_tile_path, get_tile_reference_coordinate


class TestTiles(unittest.TestCase):
    def test_get_tile_reference_coordinate(self):
        """Test that tile reference coordinates are calculated correctly in the four latitude/longitude quadrants."""
        coordinates_and_expected_results = [
            ((0.5, 0.5), (0, 0)),
            ((0.5, -0.5), (0, -1)),
            ((-0.5, 0.5), (-1, 0)),
            ((-0.5, -0.5), (-1, -1)),
        ]

        for (latitude, longitude), expected_result in coordinates_and_expected_results:
            with self.subTest(latitude=latitude, longitude=longitude):
                tile_reference_coordinate = get_tile_reference_coordinate(latitude, longitude)
                self.assertEqual(tile_reference_coordinate, expected_result)

    def test_get_tile_path(self):
        """Test that the path of the tile containing the given latitude and longitude is constructed correctly."""
        coordinates_and_expected_paths = (
            (12, 73, "Copernicus_DSM_COG_10_N12_00_E073_00_DEM/Copernicus_DSM_COG_10_N12_00_E073_00_DEM.tif"),
            (54, -5, "Copernicus_DSM_COG_10_N54_00_W005_00_DEM/Copernicus_DSM_COG_10_N54_00_W005_00_DEM.tif"),
            (-19, 32, "Copernicus_DSM_COG_10_S19_00_E032_00_DEM/Copernicus_DSM_COG_10_S19_00_E032_00_DEM.tif"),
            (-89, -179, "Copernicus_DSM_COG_10_S89_00_W179_00_DEM/Copernicus_DSM_COG_10_S89_00_W179_00_DEM.tif"),
        )

        for latitude, longitude, expected_path in coordinates_and_expected_paths:
            with self.subTest(latitude=latitude, longitude=longitude):
                path = get_tile_path(latitude=latitude, longitude=longitude)
                self.assertEqual(path, expected_path)
