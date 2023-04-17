import json
import os.path
import tempfile
import unittest
from unittest.mock import patch

import botocore.exceptions
import numpy as np
import rasterio
from h3.api.basic_int import h3_get_resolution, h3_to_children, h3_to_parent
from octue import Runner
from octue.resources import Analysis

from elevations_populator.app import DATASET_BUCKET_NAME, App
from elevations_populator.cells import get_descendents_down_to_maximum_resolution
from elevations_populator.exceptions import DataUnavailable


REPOSITORY_ROOT = os.path.dirname(os.path.dirname(__file__))
TEST_TILE_PATH = os.path.join(REPOSITORY_ROOT, "tests", "Copernicus_DSM_COG_10_N54_00_W005_00_DEM.tif")

TWINE = os.path.join(REPOSITORY_ROOT, "twine.json")
ANALYSIS = Analysis(twine=TWINE, configuration_values={})


class TestApp(unittest.TestCase):
    def test_error_raised_if_cell_resolution_not_between_minimum_and_maximum_resolutions_inclusively(self):
        """Test that an error is raised if cells of less than the minimum resolution or more than the maximimum
        resolution are provided as inputs.
        """
        cells = [(3, 590416922114260991), (15, 644460079102511746)]
        runner = Runner(app_src=App, twine=TWINE, configuration_values={})

        for resolution, cell in cells:
            with self.subTest(resolution=resolution):
                with self.assertRaises(ValueError) as error:
                    runner.run(input_values={"h3_cells": [cell]})
                    self.assertEqual(error.exception.args[1], cell)
                    self.assertEqual(error.exception.args[2], resolution)

    def test_app(self):
        """Test that, given a resolution 11 H3 cell, the elevations of the centrepoints of the resolution 12 descendents
        of its resolution 10 parent are extracted from a satellite data tile and the average of these is used to
        calculate the elevation of the resolution 10 cell and its children (including the original resolution 11 cell).
        """
        resolution_11_cell = 626445680950767615
        self.assertEqual(h3_get_resolution(resolution_11_cell), 11)

        runner = Runner(
            app_src=App,
            twine=TWINE,
            configuration_values={"minimum_resolution": 10, "delete_downloaded_tiles_after_run": False},
        )

        # Mock tile download and elevation storage.
        with patch(
            "elevations_populator.app.App._download_and_load_elevation_tile",
            return_value=rasterio.open(TEST_TILE_PATH),
        ):
            with patch("elevations_populator.app.App._store_elevations") as mock_store_elevations:
                analysis = runner.run(input_values={"h3_cells": [resolution_11_cell]})

        # No output values are expected from the app.
        self.assertIsNone(analysis.output_values)

        elevations = mock_store_elevations.call_args[0][0]

        self.assertTrue(
            elevations,
            {
                630949280578134527: 123.45122,
                630949280578130431: 121.02042,
                630949280578109951: 122.54978,
                630949280578114047: 124.72449,
                630949280578122239: 125.50409,
                630949280578118143: 126.169914,
                630949280578126335: 120.09502,
                630949280578130943: 122.4994,
                630949280578135039: 123.46548,
                630949280578110463: 123.75406,
                630949280578114559: 123.99768,
                630949280578122751: 125.50409,
                630949280578118655: 124.92622,
                630949280578126847: 120.09502,
                630949280578135551: 125.333244,
                630949280578131455: 121.6412,
                630949280578110975: 123.46548,
                630949280578115071: 122.51705,
                630949280578123263: 126.87395,
                630949280578119167: 125.24347,
                630949280578127359: 121.568504,
                630949280578115583: 124.72449,
                630949280578123775: 126.87395,
                630949280578131967: 122.51705,
                630949280578136063: 123.46548,
                630949280578119679: 126.169914,
                630949280578127871: 121.568504,
                630949280578136575: 123.45122,
                630949280578132479: 121.02042,
                630949280578111999: 122.54978,
                630949280578116095: 122.4994,
                630949280578124287: 123.75406,
                630949280578120191: 123.46548,
                630949280578128383: 120.09502,
                630949280578137087: 121.568504,
                630949280578132991: 121.02042,
                630949280578112511: 121.6412,
                630949280578116607: 123.99768,
                630949280578124799: 125.50409,
                630949280578120703: 124.92622,
                630949280578128895: 120.09502,
                630949280578137599: 123.45122,
                630949280578133503: 121.02042,
                630949280578113023: 122.54978,
                630949280578117119: 122.51705,
                630949280578125311: 124.92622,
                630949280578121215: 125.24347,
                630949280578129407: 121.328705,
                630949280578111487: 123.75406,
                626445680950767615: 123.45519,
                626445680950747135: 123.56826,
                626445680950751231: 125.16353,
                626445680950759423: 120.69225,
                626445680950755327: 125.56292,
                626445680950763519: 121.53418,
                626445680950743039: 122.89488,
                621942081323401215: 123.26732,
            },
        )

        resolution_10_cell = h3_to_parent(resolution_11_cell)
        resolution_11_cells = h3_to_children(resolution_10_cell)
        resolution_12_cells = get_descendents_down_to_maximum_resolution(resolution_10_cell, maximum_resolution=12)

        # Check that the elevations of the original cell's parent and all its resolution 12 descendents have been
        # extracted or calculated.
        self.assertEqual(elevations.keys(), {resolution_10_cell, *resolution_11_cells, *resolution_12_cells})

        # Check that the elevation of the resolution 10 parent is the average of its resolution 11 children's
        # elevations.
        self.assertEqual(elevations[resolution_10_cell], np.mean([elevations[cell] for cell in resolution_11_cells]))

    def test_store_elevations(self):
        """Test that elevations are stored successfully."""
        with tempfile.NamedTemporaryFile() as temporary_file:
            analysis = Analysis(
                twine=TWINE,
                configuration_values={"storage_location": "local", "local_storage_path": temporary_file.name},
            )

            App(analysis)._store_elevations({644460079102511746: 191.3})

            with open(temporary_file.name) as f:
                self.assertEqual(json.load(f), [[644460079102511746, 191.3]])

    def test_download_and_load_elevation_tiles_with_non_existent_tile_results_in_null_tile(self):
        """Test that attempting to download tiles that don't exist results in a tile value of `None` being stored for
        the tile reference coordinates.
        """
        app = App(ANALYSIS)

        with patch("elevations_populator.app.App._download_and_load_elevation_tile", side_effect=DataUnavailable):
            app._download_and_load_elevation_tiles([(0, 0)])

        self.assertIsNone(app._tiles[(0, 0)])


class TestGetElevation(unittest.TestCase):
    def test_with_missing_tile_data(self):
        """Test that the elevation is given as zero if there is no tile data available for the given coordinates."""
        app = App(ANALYSIS)
        app._tiles = {(31, 2): None}
        elevation = app._get_elevation(latitude=31.21, longitude=2.5)
        self.assertEqual(elevation, 0)

    def test_with_tile_data(self):
        """Test that an elevation can be accessed for a coordinate within a tile."""
        app = App(ANALYSIS)
        app._tiles = {(54, -5): rasterio.open(TEST_TILE_PATH)}
        elevation = app._get_elevation(latitude=54.21, longitude=-4.6)
        self.assertEqual(round(elevation), 191)


class TestAddAverageElevationsForAncestorsUpToMinimumResolution(unittest.TestCase):
    def test_with_resolution_12_cells_and_minimum_resolution_of_11(self):
        """Test that, given a set of sibling resolution 12 cells and a minimum resolution of 11, the cells' parent's
        elevation is calculated as the average of their elevations.
        """
        resolution_12_cell = 630949280578134527
        self.assertEqual(h3_get_resolution(resolution_12_cell), 12)

        resolution_12_cell_parent = h3_to_parent(resolution_12_cell)
        resolution_12_cells = h3_to_children(resolution_12_cell_parent)
        resolution_12_cell_elevations = list(range(len(resolution_12_cells)))

        resolution_12_cells_and_elevations = {
            cell: elevation for cell, elevation in zip(resolution_12_cells, resolution_12_cell_elevations)
        }

        analysis = Analysis(twine=TWINE, configuration_values={"minimum_resolution": 11})

        all_elevations = App(analysis)._add_average_elevations_for_ancestors_up_to_minimum_resolution(
            resolution_12_cells_and_elevations
        )

        # The elevations dictionary should contain the elevations of the resolution 12 siblings and the elevation of
        # their parent.
        self.assertEqual(
            all_elevations,
            {
                **resolution_12_cells_and_elevations,
                resolution_12_cell_parent: np.mean(resolution_12_cell_elevations),
            },
        )

    def test_with_resolution_12_cells_and_minimum_resolution_of_10(self):
        """Test that, given the set of resolution 12 grandchild cells of a resolution 10 grandparent and a minimum
        resolution of 10, the average elevation is calculated for each parent and the single grandparent.
        """
        app = App(Analysis(twine=TWINE, configuration_values={"minimum_resolution": 10}))

        resolution_12_cell = 630949280578134527
        self.assertEqual(h3_get_resolution(resolution_12_cell), 12)

        resolution_12_cell_parent = h3_to_parent(resolution_12_cell)
        resolution_12_cell_grandparent = h3_to_parent(resolution_12_cell_parent)

        resolution_12_cells = get_descendents_down_to_maximum_resolution(
            resolution_12_cell_grandparent,
            maximum_resolution=12,
        )

        resolution_12_cells_and_elevations = {
            cell: elevation
            for cell, elevation in zip(resolution_12_cells, [1 for _ in range(len(resolution_12_cells))])
        }

        all_elevations = app._add_average_elevations_for_ancestors_up_to_minimum_resolution(
            resolution_12_cells_and_elevations
        )

        # The elevations dictionary should contain the elevations of the resolution 12 cells, the elevations of their
        # parents, and the elevation of their shared grandparent.
        self.assertEqual(
            all_elevations,
            {
                **resolution_12_cells_and_elevations,
                **{cell: 1 for cell in h3_to_children(resolution_12_cell_grandparent)},
                resolution_12_cell_grandparent: 1,
            },
        )


class TestDownloadAndLoadElevationTile(unittest.TestCase):
    def test_error_raised_if_given_coordinates_with_no_associated_tile(self):
        """Test that an error is raised if attempting to download a satellite tile that doesn't exist (i.e. for an area
        that has no data associated with it).
        """
        app = App(ANALYSIS)

        with self.assertRaises(DataUnavailable):
            with patch(
                "elevations_populator.app.s3.download_fileobj",
                side_effect=botocore.exceptions.ClientError({}, ""),
            ):
                app._download_and_load_elevation_tile(latitude=53, longitude=2)

        self.assertEqual(app._downloaded_tile_paths, [])

    def test_with_valid_coordinates(self):
        """Test that elevation tiles can be downloaded and loaded correctly."""
        app = App(ANALYSIS)
        test_tile_s3_path = "Copernicus_DSM_COG_10_N54_00_W005_00_DEM/Copernicus_DSM_COG_10_N54_00_W005_00_DEM.tif"

        with patch("elevations_populator.app.tempfile.NamedTemporaryFile") as mock_named_temporary_file:
            with patch("elevations_populator.app.s3.download_fileobj") as mock_download_fileobj:
                with patch("builtins.open"):
                    mock_named_temporary_file.return_value.__enter__.return_value.name = TEST_TILE_PATH
                    tile = app._download_and_load_elevation_tile(latitude=54, longitude=-5)

        # Check tile has been downloaded correctly.
        self.assertEqual(mock_download_fileobj.call_args[0][0], DATASET_BUCKET_NAME)
        self.assertEqual(mock_download_fileobj.call_args[0][1], test_tile_s3_path)
        self.assertEqual(app._downloaded_tile_paths, [TEST_TILE_PATH])

        # Check tile has been loaded successfully.
        self.assertEqual(tile.count, 1)
        self.assertEqual(tile.name, TEST_TILE_PATH)
