import json
import logging
import math
import os
import tempfile

import boto3
import rasterio
from botocore import UNSIGNED
from botocore.client import Config
from h3 import h3_to_geo


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
        """Get the elevations of the center-points of the input H3 cells.

        :return None:
        """
        try:
            logger.info("Converting H3 cell centre-points to latitude/longitude pairs.")
            coordinates = [h3_to_geo(h3_cell) for h3_cell in self.analysis.input_values["h3_cells"]]

            # Deduplicate the coordinates of the tiles containing the coordinates so each tile is only downloaded once.
            logger.info("Determining which satellite elevation data tiles to download.")
            tile_coordinates = {
                self._get_tile_reference_coordinate(latitude, longitude) for latitude, longitude in coordinates
            }

            logger.info("Downloading and loading required satellite tiles.")
            self._tiles = {
                (tile_latitude, tile_longitude): self._download_and_load_elevation_tile(
                    latitude=tile_latitude,
                    longitude=tile_longitude,
                )
                for tile_latitude, tile_longitude in tile_coordinates
            }

            logger.info("Getting elevations from satellite tiles.")
            h3_cells_and_elevations = [
                (h3_cell, self._get_elevation(latitude, longitude))
                for h3_cell, (latitude, longitude) in zip(self.analysis.input_values["h3_cells"], coordinates)
            ]

            logger.info("Storing elevations in database.")
            self._store_elevations(h3_cells_and_elevations)

        finally:
            for file in self._downloaded_files:
                os.remove(file)

    def _get_tile_reference_coordinate(self, latitude, longitude):
        """Get the reference coordinate of the tile containing the given coordinate. A tile's reference coordinate is
        the latitude and longitude of its bottom-left point, both of which are integers.

        :param float latitude: the latitude of the coordinate (in decimal degrees) for which to get the containing tile
        :param float longitude: the longitude of the coordinate (in decimal degrees) for which to get the containing tile
        :return (int, int): the reference coordinate (in decimal degrees) of the tile containing the given coordinate
        """
        if latitude < 0:
            latitude -= 1

        if longitude < 0:
            longitude -= 1

        return (math.trunc(latitude), math.trunc(longitude))

    def _download_and_load_elevation_tile(self, latitude, longitude):
        """Download and load the elevation tile containing the given coordinate.

        :param int latitude: the latitude of the coordinate in decimal degrees
        :param int longitude: the longitude of the coordinate in decimal degrees
        :return rasterio.io.DatasetReader: the elevation tile as a RasterIO dataset
        """
        with tempfile.NamedTemporaryFile(delete=False) as temporary_file:
            with open(temporary_file.name, "wb") as f:
                s3.download_fileobj(BUCKET_NAME, self._get_tile_path(latitude, longitude), f)

        self._downloaded_files.append(temporary_file.name)
        return rasterio.open(temporary_file.name)

    def _get_elevation(self, latitude, longitude):
        """Get the elevation of the given coordinate.

        :param float latitude: the latitude of the coordinate in decimal degrees
        :param float longitude: the longitude of the coordinate in decimal degrees
        :return float: the elevation of the coordinate in meters
        """
        tile = self._tiles[self._get_tile_reference_coordinate(latitude, longitude)]
        elevation_map = tile.read(1)
        return elevation_map[tile.index(longitude, latitude)]

    def _store_elevations(self, h3_cells_and_elevations):
        """Store the given elevations in the database.

        :param iter((float, float) h3_cells_and_elevations: the h3 cells and their elevations
        :return None:
        """
        try:
            with open("local_storage.json") as f:
                persisted_data = json.load(f)
        except FileNotFoundError:
            persisted_data = []

        persisted_data.extend(h3_cells_and_elevations)

        with open("local_storage.json", "w") as f:
            json.dump(persisted_data, f)

    def _get_tile_path(self, latitude, longitude):
        """Get the path of the tile in the GLO-30 elevation dataset whose bottom-most point is the given latitude and
        whose left-most point is the given longitude.

        :param int latitude: the truncated latitude of the coordinate in decimal degrees
        :param int longitude: the truncated longitude of the coordinate in decimal degrees
        :return str: the filename of the tile containing the coordinate
        """
        # Positive latitudes are north of the equator.
        if latitude >= 0:
            latitude = f"N{latitude:02}_00"
        else:
            latitude = f"S{-latitude:02}_00"

        # Positive longitudes are east of the prime meridian.
        if longitude >= 0:
            longitude = f"E{longitude:03}_00"
        else:
            longitude = f"W{-longitude:03}_00"

        name = f"{DATAFILE_NAME_PREFIX}_{RESOLUTION}_{latitude}_{longitude}_{DATAFILE_NAME_SUFFIX}"
        return f"{name}/{name}.tif"
