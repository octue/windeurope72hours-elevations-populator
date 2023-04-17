import os
from unittest.mock import patch

import rasterio
from h3.api.basic_int import k_ring
from octue import Runner

from elevations_populator.app import App


REPOSITORY_ROOT = os.path.dirname(os.path.dirname(__file__))
TEST_TILE_PATH = os.path.join(REPOSITORY_ROOT, "tests", "Copernicus_DSM_COG_10_N54_00_W005_00_DEM.tif")


def run_locally_on_center_cell_and_neighbours(
    center_cell,
    number_of_neighbours=10,
    minimum_resolution=12,
    maximum_resolution=12,
    only_use_offline_test_tile=False,
):
    """Run the app locally on a center H3 cell and a number of its concentric neighbours.

    :param int center_cell:
    :param int number_of_neighbours:
    :param int minimum_resolution:
    :param int maximum_resolution:
    :param bool only_use_offline_test_tile: if `True`, run the app offline using only the satellite data tile in the `tests` directory. This only allows getting of elevations of cells whose centers fall within this tile.
    :return None:
    """
    runner = Runner(
        app_src=App,
        twine=os.path.join(REPOSITORY_ROOT, "twine.json"),
        configuration_values={
            "minimum_resolution": minimum_resolution,
            "maximum_resolution": maximum_resolution,
            "storage_location": "local",
            "delete_downloaded_tiles_after_run": not only_use_offline_test_tile,
        },
    )

    neighbours = k_ring(center_cell, k=number_of_neighbours)

    if only_use_offline_test_tile:
        with patch(
            "elevations_populator.app.App._download_and_load_elevation_tile",
            return_value=rasterio.open(TEST_TILE_PATH),
        ):
            runner.run(input_values={"h3_cells": [*neighbours, center_cell]})

    else:
        runner.run(input_values={"h3_cells": [*neighbours, center_cell]})


if __name__ == "__main__":
    center_cell = 630949280935159295

    # # Just off the UK coast
    # center_cell = geo_to_h3(53.34294215250594, 0.2669003997762649, resolution=12)

    # # North Sea
    # center_cell = geo_to_h3(53.83300832048393, 2.536378309710649, resolution=12)

    # # Caspian Sea
    # center_cell = geo_to_h3(43.00895817503546, 49.74127244039208, resolution=5)

    # # Scotland
    # center_cell = geo_to_h3(56.83541486981668, -3.6902367385843142, resolution=12)
    run_locally_on_center_cell_and_neighbours(center_cell, only_use_offline_test_tile=True)
