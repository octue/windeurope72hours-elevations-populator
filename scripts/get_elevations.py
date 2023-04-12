import os
from unittest.mock import patch

import rasterio
from h3.api.basic_int import k_ring
from octue import Runner

from elevations_populator.app import App


REPOSITORY_ROOT = os.path.dirname(os.path.dirname(__file__))
TEST_TILE_PATH = os.path.join(REPOSITORY_ROOT, "tests", "Copernicus_DSM_COG_10_N54_00_W005_00_DEM.tif")

runner = Runner(
    app_src=App,
    twine=os.path.join(REPOSITORY_ROOT, "twine.json"),
    configuration_values={
        "minimum_resolution": 12,
        "maximum_resolution": 13,
        "storage_location": "local",
        "delete_downloaded_tiles_after_run": False,
    },
)

# Resolution 13 cells.
initial_h3_cell = 635452880562529343
neighbours = k_ring(initial_h3_cell, k=90)


with patch(
    "elevations_populator.app.App._download_and_load_elevation_tile",
    return_value=rasterio.open(TEST_TILE_PATH),
):
    analysis = runner.run(input_values={"h3_cells": [*neighbours, initial_h3_cell]})
