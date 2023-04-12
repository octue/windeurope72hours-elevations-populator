import os
from unittest.mock import patch

import h3
import rasterio
from octue import Runner

from elevations_populator.app import App


REPOSITORY_ROOT = os.path.dirname(os.path.dirname(__file__))
TEST_TILE_PATH = os.path.join(REPOSITORY_ROOT, "tests", "Copernicus_DSM_COG_10_N54_00_W005_00_DEM.tif")

# Avoid deleting test tile at end of app run.
runner = Runner(
    app_src=App,
    twine="../twine.json",
    configuration_values={"storage_location": "local", "delete_downloaded_tiles_after_run": False},
)

# Resolution 13 cells.
initial_h3_cell = "8d19507316da43f"
neighbours = h3.k_ring(initial_h3_cell, k=90)


with patch(
    "elevations_populator.app.App._download_and_load_elevation_tile",
    return_value=rasterio.open(TEST_TILE_PATH),
):
    analysis = runner.run(input_values={"h3_cells": [*neighbours, initial_h3_cell]})
