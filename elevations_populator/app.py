import logging
import math
import os
import tempfile

import boto3
import rasterio
from botocore import UNSIGNED
from botocore.client import Config
from h3.api.numpy_int import h3_to_geo


logger = logging.getLogger(__name__)
s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))


BUCKET_NAME = "copernicus-dem-30m"
DATAFILE_NAME_PREFIX = "Copernicus_DSM_COG"
DATAFILE_NAME_SUFFIX = "DEM"

# The resolution is 10 arcseconds for the GLO-30 dataset.
RESOLUTION = 10


class App:
    def __init__(self, analysis):
        self.analysis = analysis
        self._downloaded_files = []

    def run(self):
        try:
            logger.info("The elevations service has started.")

            # Convert the H3 cells to lat/long pairs.
            coordinates = [h3_to_geo(h3_cell) for h3_cell in self.analysis.input_values["h3_cells"]]

            # Deduplicate the truncated latitudes and longitudes so each tile is only downloaded once (consecutive tiles
            # are separated by 1 degree).
            tile_coordinates = self._deduplicate_truncated_coordinates(coordinates)

            # Download and load the required tiles.
            tiles = {
                (tile_latitude, tile_longitude): self._download_and_load_elevation_tile(
                    latitude=tile_latitude,
                    longitude=tile_longitude,
                )
                for tile_latitude, tile_longitude in tile_coordinates
            }

            elevations = [
                (h3_cell, self._get_elevation(tiles, latitude, longitude))
                for h3_cell, (latitude, longitude) in zip(self.analysis.input_values["h3_cells"], coordinates)
            ]

        finally:
            for file in self._downloaded_files:
                os.remove(file)

    def _deduplicate_truncated_coordinates(self, coordinates):
        deduplicated_truncated_coordinates = set()
        deduplicated_coordinates = []

        for latitude, longitude in coordinates:
            truncated_latitude_and_longitude = (math.trunc(latitude), math.trunc(longitude))

            if truncated_latitude_and_longitude not in deduplicated_truncated_coordinates:
                deduplicated_truncated_coordinates.add(truncated_latitude_and_longitude)
                deduplicated_coordinates.append((latitude, longitude))

        return deduplicated_coordinates

    def _download_and_load_elevation_tile(self, latitude, longitude):
        with tempfile.NamedTemporaryFile(delete=False) as temporary_file:
            with open(temporary_file.name, "wb") as f:
                s3.download_fileobj(BUCKET_NAME, self._get_datafile_name(latitude, longitude), f)

        self._downloaded_files.append(temporary_file.name)
        return rasterio.open(temporary_file.name)

    def _get_elevation(self, tiles, latitude, longitude):
        truncated_latitude = math.trunc(latitude)
        truncated_longitude = math.trunc(longitude)

        tile = tiles[(truncated_latitude, truncated_longitude)]
        band = tile.read(1)
        return band[tile.index(latitude, longitude)]

    def _get_datafile_name(self, latitude, longitude):
        # Positive latitudes are north of the equator.
        if latitude >= 0:
            latitude = f"N{math.trunc(latitude)}_00"
        else:
            latitude = f"S{math.trunc(-latitude)}_00"

        # Positive longitudes are east of the prime meridian.
        if longitude >= 0:
            longitude = f"E{math.trunc(longitude)}_00"
        else:
            longitude = f"W{math.trunc(-longitude)}_00"

        name = "_".join((DATAFILE_NAME_PREFIX, RESOLUTION, latitude, longitude, DATAFILE_NAME_SUFFIX))
        return f"{name}/{name}.tif"
