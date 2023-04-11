import json
import os.path
import tempfile
import unittest
from unittest.mock import patch

import rasterio
from h3.api.basic_int import h3_get_resolution
from octue import Runner

from elevations_populator.app import BUCKET_NAME, App


REPOSITORY_ROOT = os.path.dirname(os.path.dirname(__file__))
TEST_TILE_PATH = os.path.join(REPOSITORY_ROOT, "tests", "Copernicus_DSM_COG_10_N54_00_W005_00_DEM.tif")


class TestApp(unittest.TestCase):
    def test_app(self):
        """Test that the elevation at the centre-point of an H3 cell can be found and stored."""
        h3_cell = 644460079102511746
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

    def test_get_ancestors_up_to_resolution_4_with_resolution_4_cell(self):
        cell = 594920487381893119
        self.assertEqual(h3_get_resolution(cell), 4)

        ancestors = App(None)._get_ancestors_up_to_resolution_4(cell)
        self.assertEqual(ancestors, [cell])

    def test_get_ancestors_up_to_resolution_4_with_resolution_5_cell(self):
        cell = 599424083788038143
        self.assertEqual(h3_get_resolution(cell), 5)

        ancestors = App(None)._get_ancestors_up_to_resolution_4(cell)
        self.assertEqual(len(ancestors), 1)
        self.assertEqual([h3_get_resolution(ancestor) for ancestor in ancestors], [4])

    def test_get_ancestors_up_to_resolution_4_with_resolution_6_cell(self):
        cell = 603927682878537727
        self.assertEqual(h3_get_resolution(cell), 6)

        ancestors = App(None)._get_ancestors_up_to_resolution_4(cell)
        self.assertEqual(len(ancestors), 2)
        self.assertEqual([h3_get_resolution(ancestor) for ancestor in ancestors], [5, 4])

    def test_get_resolution_12_descendents_with_resolution_12_cell(self):
        """Test that a resolution 12 cell is idempotent."""
        cell = 630949280220400639
        self.assertEqual(h3_get_resolution(cell), 12)
        self.assertEqual(App(None)._get_resolution_12_descendents(cell), {cell})

    def test_get_resolution_12_descendents_with_resolution_11_cell(self):
        """Test that passing a resolution 11 cell results in 7 resolution 12 cells."""
        cell = 626445680593031167
        self.assertEqual(h3_get_resolution(cell), 11)

        descendents = App(None)._get_resolution_12_descendents(cell)
        self.assertEqual(len(descendents), 7)

        for descendent in descendents:
            self.assertEqual(h3_get_resolution(descendent), 12)

    def test_get_resolution_12_descendents_with_resolution_10_cell(self):
        """Test that passing a resolution 10 cell results in 49 resolution 12 cells."""
        cell = 621942080965672959
        self.assertEqual(h3_get_resolution(cell), 10)

        descendents = App(None)._get_resolution_12_descendents(cell)
        self.assertEqual(len(descendents), 49)

        for descendent in descendents:
            self.assertEqual(h3_get_resolution(descendent), 12)

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
        with tempfile.NamedTemporaryFile() as temporary_file:
            App.LOCAL_STORAGE_PATH = temporary_file.name
            App(None)._store_elevations({644460079102511746: 191.3})

            with open(temporary_file.name) as f:
                self.assertEqual(json.load(f), [[644460079102511746, 191.3]])

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
