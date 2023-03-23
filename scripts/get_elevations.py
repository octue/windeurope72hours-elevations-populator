import os
from unittest.mock import patch

import h3
from octue import Runner

from elevations_populator.app import App


REPOSITORY_ROOT = os.path.dirname(os.path.dirname(__file__))
TEST_TILE_PATH = os.path.join(REPOSITORY_ROOT, "tests", "Copernicus_DSM_COG_10_N54_00_W005_00_DEM.tif")


runner = Runner(app_src=App, twine="../twine.json")

# Resolution 13 cells.
initial_h3_cell = "8d1950706d34abf"
neighbours = h3.k_ring(initial_h3_cell, k=3)


with patch("elevations_populator.app.tempfile.NamedTemporaryFile") as mock_named_temporary_file:
    with patch("elevations_populator.app.s3.download_fileobj") as mock_download_fileobj:
        with patch("builtins.open"):
            with patch("os.remove"):
                mock_named_temporary_file.return_value.__enter__.return_value.name = TEST_TILE_PATH

                analysis = runner.run(input_values={"h3_cells": list(neighbours | {initial_h3_cell})})

a = 3
