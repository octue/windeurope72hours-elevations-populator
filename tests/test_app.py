import os.path
import unittest

from elevations_populator.app import App


REPOSITORY_ROOT = os.path.dirname(os.path.dirname(__file__))


class TestApp(unittest.TestCase):
    def test_deduplicate_truncated_coordinates(self):
        """Test that truncated coordinates are deduplicated properly."""
        coordinates = [(37.234, -5.39203), (37.98, -5.2), (0.001, 73.9), (-53, 0)]
        deduplicated_coordinates = App(None)._deduplicate_truncated_coordinates(coordinates)
        self.assertEqual(deduplicated_coordinates, {(37, -5), (0, 73), (-53, 0)})

    def test_get_tile_path(self):
        """Test that the path of the tile containing the given latitude and longitude is constructed correctly."""
        coordinates_and_expected_paths = (
            (12, 73, "Copernicus_DSM_COG_10_N12_00_E073_00_DEM/Copernicus_DSM_COG_10_N12_00_E073_00_DEM.tif"),
            (54, -5, "Copernicus_DSM_COG_10_N54_00_W005_00_DEM/Copernicus_DSM_COG_10_N54_00_W005_00_DEM.tif"),
            (-19, 32, "Copernicus_DSM_COG_10_S19_00_E032_00_DEM/Copernicus_DSM_COG_10_S19_00_E032_00_DEM.tif"),
            (-89, -179, "Copernicus_DSM_COG_10_S89_00_W179_00_DEM/Copernicus_DSM_COG_10_S89_00_W179_00_DEM.tif"),
        )

        for latitude, longitude, expected_path in coordinates_and_expected_paths:
            with self.subTest(latitude=latitude, longitude=longitude, expected_path=expected_path):
                path = App(None)._get_tile_path(latitude=latitude, longitude=longitude)
                self.assertEqual(path, expected_path)
