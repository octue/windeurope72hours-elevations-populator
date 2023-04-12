import json
import logging
import math
import os
import tempfile

import boto3
import numpy as np
import rasterio
from botocore import UNSIGNED
from botocore.client import Config
from h3.api.basic_int import h3_get_resolution, h3_to_children, h3_to_geo, h3_to_parent


logger = logging.getLogger(__name__)
s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))


BUCKET_NAME = "copernicus-dem-30m"
DATAFILE_NAME_PREFIX = "Copernicus_DSM_COG"
DATAFILE_NAME_SUFFIX = "DEM"

# The resolution is 10 arcseconds for the GLO-30 dataset.
RESOLUTION = 10


class App:
    DELETE_DOWNLOADED_FILES_AFTER_RUN = True
    LOCAL_STORAGE_PATH = "local_storage.json"
    MINIMUM_RESOLUTION = 4
    MAXIMUM_RESOLUTION = 12

    def __init__(self, analysis):
        self.analysis = analysis
        self._tiles = None
        self._downloaded_tiles = []

    def run(self):
        """Get the elevations of the center-points of the input H3 cells.

        :return None:
        """
        try:
            self._validate_cells(self.analysis.input_values["h3_cells"])

            resolution_4_ancestors = {
                self._get_ancestors_up_to_minimum_resolution(cell)[-1]
                for cell in self.analysis.input_values["h3_cells"]
            }

            resolution_12_indexes_and_coordinates = self._get_maximum_resolution_descendent_centrepoint_coordinates(
                cells=resolution_4_ancestors
            )

            self._download_and_load_elevation_tiles(resolution_12_indexes_and_coordinates.values())

            resolution_12_descendent_centrepoint_elevations = self._get_elevations(
                cells_and_coordinates=resolution_12_indexes_and_coordinates
            )

            elevations = self._add_average_elevations_for_ancestors_up_to_minimum_resolution(
                elevations=resolution_12_descendent_centrepoint_elevations
            )

            self._store_elevations(elevations)

        finally:
            if self.DELETE_DOWNLOADED_FILES_AFTER_RUN:
                for tile in self._downloaded_tiles:
                    os.remove(tile)

    def _validate_cells(self, cells):
        """Check that all input cells are within the minimum and maximum resolutions inclusively.

        :param iter(int) cells: the cells to check
        :return None:
        """
        for cell in cells:
            resolution = h3_get_resolution(cell)

            if resolution < self.MINIMUM_RESOLUTION or resolution > self.MAXIMUM_RESOLUTION:
                raise ValueError(
                    "The H3 cells must be between resolution %d and %d. Cell %r is of resolution %r.",
                    self.MINIMUM_RESOLUTION,
                    self.MAXIMUM_RESOLUTION,
                    cell,
                    resolution,
                )

    def _get_maximum_resolution_descendent_centrepoint_coordinates(self, cells):
        """Get the centrepoint coordinates of the maximum resolution descendents of the given cells.

        :param iter(int) cells: the cells to get the maximum resolution descendent centrepoint coordinates for
        :return dict(int, tuple(float, float)): the maximum resolution descendent centrepoint coordinates
        """
        logger.info(
            "Converting centre-points of resolution %d descendents to latitude/longitude pairs.",
            self.MAXIMUM_RESOLUTION,
        )

        indexes_and_coordinates = {}

        for cell in cells:
            indexes_and_coordinates |= {
                descendent: h3_to_geo(descendent)
                for descendent in self._get_descendents_down_to_maximum_resolution(cell)
            }

        return indexes_and_coordinates

    def _download_and_load_elevation_tiles(self, coordinates):
        """Download and load the elevation tiles needed to get the elevations of the given coordinates.

        :param iter(tuple(float, float)) coordinates:
        :return None:
        """
        logger.info("Determining which satellite elevation data tiles to download.")

        # Deduplicate the coordinates of the tiles containing the coordinates so each tile is only downloaded once.
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

    def _get_elevations(self, cells_and_coordinates):
        """Get the elevation of each cell using the latitude and longitude it's mapped to.

        :param dict cells_and_coordinates: a mapping of cell index to latitude and longitude
        :return dict: a mapping of cell index to elevation
        """
        logger.info("Getting elevations for resolution %d cells from satellite tiles.", self.MAXIMUM_RESOLUTION)

        return {
            cell: self._get_elevation(latitude, longitude)
            for cell, (latitude, longitude) in cells_and_coordinates.items()
        }

    def _add_average_elevations_for_ancestors_up_to_minimum_resolution(self, elevations):
        """Calculate the average elevation for every ancestor up to the minimum resolution inclusively using each
        ancestor's immediate children's elevations and add them to the given data.

        :param dict(int, float) elevations: H3 cells mapped to their elevations
        :return dict(int, float): the input elevations dictionary with the average elevations for all ancestors up to the minimum resolution added
        """
        logger.info("Calculating average elevations for ancestor cells up to resolution %d.", self.MINIMUM_RESOLUTION)

        ancestors_pyramid = self._get_ancestors_up_to_minimum_resolution_as_pyramid(elevations.keys())

        for ancestors in ancestors_pyramid:
            for ancestor in ancestors:

                if ancestor in elevations:
                    continue

                elevations[ancestor] = np.mean([elevations[child] for child in h3_to_children(ancestor)])

        return elevations

    def _get_ancestors_up_to_minimum_resolution_as_pyramid(self, cells):
        """Get the ancestors of all the cells up to the minimum resolution as an inverted pyramid where each level of
        the pyramid contains ancestors of the same resolution. The zeroth level is the set of immediate parents and the
        final level is the set of ultimate ancestors. This format is useful when recursing down the resolutions (i.e.
        to larger and larger cells) and calculating elevations for each cell parent based on the average of its
        children.

        For example, if given a list of cells of resolution 12 and the minimum resolution is 9, the pyramid looks like
        this:

            [
                {Level 11 ancestors (most)},
                {Level 10 ancestors (fewer)},
                {Level 9 ancestors (fewest)},
            ]

        :param iter(int) cells: the cells to get the ancestors for
        :return list(set(int)): the ancestors as an inverted pyramid
        """
        pyramid = list(zip(*[self._get_ancestors_up_to_minimum_resolution(cell) for cell in cells]))

        for i, cells in enumerate(pyramid):
            pyramid[i] = set(cells)

        return pyramid

    def _store_elevations(self, h3_cells_and_elevations):
        """Store the given elevations in the database.

        :param dict(int, float) h3_cells_and_elevations: the h3 cells and their elevations
        :return None:
        """
        logger.info("Storing elevations in database.")

        try:
            with open(self.LOCAL_STORAGE_PATH) as f:
                persisted_data = json.load(f)

        except (FileNotFoundError, json.JSONDecodeError):
            persisted_data = []

        for cell, elevation in h3_cells_and_elevations.items():
            # Convert numpy float type to python float type.
            persisted_data.append([cell, float(elevation)])

        with open(self.LOCAL_STORAGE_PATH, "w") as f:
            json.dump(persisted_data, f, indent=4)

    def _get_elevation(self, latitude, longitude):
        """Get the elevation of the given coordinate.

        :param float latitude: the latitude of the coordinate in decimal degrees
        :param float longitude: the longitude of the coordinate in decimal degrees
        :return float: the elevation of the coordinate in meters
        """
        tile = self._tiles[self._get_tile_reference_coordinate(latitude, longitude)]
        elevation_map = tile.read(1)
        return elevation_map[tile.index(longitude, latitude)]

    def _get_descendents_down_to_maximum_resolution(self, cell):
        """Get all descendents of the cell down to the maximum resolution inclusively.

        :param int cell: the cell to get the descendents of
        :return set: the descendents of the cell
        """
        descendents = set()
        resolution = h3_get_resolution(cell)

        if resolution == self.MAXIMUM_RESOLUTION:
            return {cell}

        children = h3_to_children(cell)

        for child in children:
            descendents |= self._get_descendents_down_to_maximum_resolution(child)

        return descendents

    def _get_ancestors_up_to_minimum_resolution(self, cell):
        """Get the ancestors of the cell up to the minimum resolution inclusively.

        :param int cell: the cell to get the ancestors of
        :return list: the ancestors of the cell
        """
        if h3_get_resolution(cell) == self.MINIMUM_RESOLUTION:
            return [cell]

        ancestors = []

        while h3_get_resolution(cell) >= self.MINIMUM_RESOLUTION + 1:
            cell = h3_to_parent(cell)
            ancestors.append(cell)

        return ancestors

    @staticmethod
    def _get_tile_reference_coordinate(latitude, longitude):
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

        return math.trunc(latitude), math.trunc(longitude)

    def _download_and_load_elevation_tile(self, latitude, longitude):
        """Download and load the elevation tile containing the given coordinate.

        :param int latitude: the latitude of the coordinate in decimal degrees
        :param int longitude: the longitude of the coordinate in decimal degrees
        :return rasterio.io.DatasetReader: the elevation tile as a RasterIO dataset
        """
        with tempfile.NamedTemporaryFile(delete=False) as temporary_file:
            with open(temporary_file.name, "wb") as f:
                s3.download_fileobj(BUCKET_NAME, self._get_tile_path(latitude, longitude), f)

        self._downloaded_tiles.append(temporary_file.name)
        return rasterio.open(temporary_file.name)

    @staticmethod
    def _get_tile_path(latitude, longitude):
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
