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
        self._tiles = None
        self._downloaded_files = []

    def run(self):
        try:
            logger.info("The elevations service has started.")

            # Get the latitude/longitude coordinates of the centres of the input H3 cells.
            coordinates = [h3_to_geo(h3_cell) for h3_cell in self.analysis.input_values["h3_cells"]]

            # Deduplicate the truncated latitudes and longitudes so each tile is only downloaded once (consecutive tiles
            # are separated by 1 degree).
            tile_coordinates = self._deduplicate_truncated_coordinates(coordinates)

            # Download and load the required tiles.
            self._tiles = {
                (tile_latitude, tile_longitude): self._download_and_load_elevation_tile(
                    latitude=tile_latitude,
                    longitude=tile_longitude,
                )
                for tile_latitude, tile_longitude in tile_coordinates
            }

            h3_cells_and_elevations = [
                (h3_cell, self._get_elevation(latitude, longitude))
                for h3_cell, (latitude, longitude) in zip(self.analysis.input_values["h3_cells"], coordinates)
            ]

            self._store_elevations(h3_cells_and_elevations)

        finally:
            for file in self._downloaded_files:
                os.remove(file)

    def _deduplicate_truncated_coordinates(self, coordinates):
        """Truncate the latitude and longitude coordinate and deduplicate them.

        :param iter((float, float)) coordinates: latitude/longitude pairs in decimal degrees
        :return iter((int, int)): the deduplicated truncated latitude/longitude pairs in decimal degrees
        """
        return {(math.trunc(latitude), math.trunc(longitude)) for latitude, longitude in coordinates}

    def _download_and_load_elevation_tile(self, latitude, longitude):
        """Download and load the elevation tile containing the given coordinate.

        :param float latitude: the latitude of the coordinate in decimal degrees
        :param float longitude: the longitude of the coordinate in decimal degrees
        :return rasterio.io.DatasetReader: the elevation tile as a RasterIO dataset
        """
        with tempfile.NamedTemporaryFile(delete=False) as temporary_file:
            with open(temporary_file.name, "wb") as f:
                s3.download_fileobj(BUCKET_NAME, self._get_tile_filename(latitude, longitude), f)

        self._downloaded_files.append(temporary_file.name)
        return rasterio.open(temporary_file.name)

    def _get_elevation(self, latitude, longitude):
        """Get the elevation of the given coordinate.

        :param float latitude: the latitude of the coordinate in decimal degrees
        :param float longitude: the longitude of the coordinate in decimal degrees
        :return float: the elevation of the coordinate in meters
        """
        tile = self._tiles[(math.trunc(latitude), math.trunc(longitude))]
        elevation_map = tile.read(1)
        return elevation_map[tile.index(latitude, longitude)]

    def _store_elevations(self, h3_cells_and_elevations):
        """Store the given elevations in the database.

        :param iter((float, float) h3_cells_and_elevations: the h3 cells and their elevations
        :return None:
        """
        pass

    def _get_tile_filename(self, latitude, longitude):
        """Get the filename of the tile in the GLO-30 elevation dataset whose north/south-most point is the given
        latitude and whose east/west-most point is the given longitude.

        :param int latitude: the truncated latitude of the coordinate in decimal degrees
        :param int longitude: the truncated longitude of the coordinate in decimal degrees
        :return str: the filename of the tile containing the coordinate
        """
        # Positive latitudes are north of the equator.
        if latitude >= 0:
            latitude = f"N{latitude}_00"
        else:
            latitude = f"S{-latitude}_00"

        # Positive longitudes are east of the prime meridian.
        if longitude >= 0:
            longitude = f"E{longitude}_00"
        else:
            longitude = f"W{-longitude}_00"

        name = "_".join((DATAFILE_NAME_PREFIX, RESOLUTION, latitude, longitude, DATAFILE_NAME_SUFFIX))
        return f"{name}/{name}.tif"
