import json
import os.path
import unittest
from unittest.mock import mock_open, patch

import rasterio
from octue import Runner

from elevations_populator.app import BUCKET_NAME, App


REPOSITORY_ROOT = os.path.dirname(os.path.dirname(__file__))
TEST_TILE_PATH = os.path.join(REPOSITORY_ROOT, "tests", "Copernicus_DSM_COG_10_N54_00_W005_00_DEM.tif")


class TestApp(unittest.TestCase):
    def test_app(self):
        """Test that the elevation at the centre-point of an H3 cell can be found and stored."""
        h3_cell = "8f1950706d34a82"
        runner = Runner(app_src=App, twine=os.path.join(REPOSITORY_ROOT, "twine.json"))

        # Mock tile download, elevation storage, and tile deletion.
        with patch("elevations_populator.app.App._store_elevations") as mock_store_elevations:
            with patch("elevations_populator.app.tempfile.NamedTemporaryFile") as mock_named_temporary_file:
                with patch("elevations_populator.app.s3.download_fileobj"):
                    with patch("builtins.open"):
                        with patch("os.remove"):
                            mock_named_temporary_file.return_value.__enter__.return_value.name = TEST_TILE_PATH
                            analysis = runner.run(input_values={"h3_cells": [h3_cell]})

        self.assertIsNone(analysis.output_values)
        self.assertTrue(mock_store_elevations.call_args[0][0][0][0], h3_cell)
        self.assertTrue(round(mock_store_elevations.call_args[0][0][0][1]), 191)

    def test_get_tile_reference_coordinate(self):
        """Test that tile coordinates are calculated correctly in the four latitude/longitude quadrants."""
        coordinates_and_expected_results = [
            ((0.5, 0.5), (0, 0)),
            ((0.5, -0.5), (0, -1)),
            ((-0.5, 0.5), (-1, 0)),
            ((-0.5, -0.5), (-1, -1)),
        ]

        app = App(None)

        for (latitude, longitude), expected_result in coordinates_and_expected_results:
            with self.subTest(latitude=latitude, longitude=longitude):
                tile_reference_coordinate = app._get_tile_reference_coordinate(latitude, longitude)
                self.assertEqual(tile_reference_coordinate, expected_result)

    def test_download_and_load_elevation_tile(self):
        """Test that elevation tiles can be downloaded and loaded correctly."""
        app = App(None)
        test_tile_s3_path = "Copernicus_DSM_COG_10_N54_00_W005_00_DEM/Copernicus_DSM_COG_10_N54_00_W005_00_DEM.tif"

        with patch("elevations_populator.app.tempfile.NamedTemporaryFile") as mock_named_temporary_file:
            with patch("elevations_populator.app.s3.download_fileobj") as mock_download_fileobj:
                with patch("builtins.open"):
                    mock_named_temporary_file.return_value.__enter__.return_value.name = TEST_TILE_PATH
                    tile = app._download_and_load_elevation_tile(latitude=54, longitude=-5)

        # Check tile has been downloaded correctly.
        self.assertEqual(mock_download_fileobj.call_args[0][0], BUCKET_NAME)
        self.assertEqual(mock_download_fileobj.call_args[0][1], test_tile_s3_path)
        self.assertEqual(app._downloaded_tiles, [TEST_TILE_PATH])

        # Check tile has been loaded successfully.
        self.assertEqual(tile.count, 1)
        self.assertEqual(tile.name, TEST_TILE_PATH)

    def test_get_elevation(self):
        """Test that an elevation can be accessed for a coordinate within a tile."""
        app = App(None)
        app._tiles = {(54, -5): rasterio.open(TEST_TILE_PATH)}
        elevation = app._get_elevation(latitude=54.21, longitude=-4.6)
        self.assertEqual(round(elevation), 191)

    def test_store_elevations(self):
        """Test that elevations are stored successfully."""
        m = mock_open(read_data=json.dumps([]))

        with patch("elevations_populator.app.open", m):
            App(None)._store_elevations([("8f1950706d34a82", 191.3)])

        self.assertEqual(m.mock_calls[6][1][0], "[")
        self.assertEqual(m.mock_calls[7][1][0], '["8f1950706d34a82"')
        self.assertEqual(m.mock_calls[8][1][0], ", 191.3")
        self.assertEqual(m.mock_calls[9][1][0], "]")
        self.assertEqual(m.mock_calls[10][1][0], "]")

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
                path = App(None)._get_tile_path(latitude=latitude, longitude=longitude)
                self.assertEqual(path, expected_path)
