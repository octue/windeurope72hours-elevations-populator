import datetime
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

from elevations_populator.storage import store_elevations_in_database, store_elevations_locally


logger = logging.getLogger(__name__)
s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))


# Constants for downloading elevation tile data from the Copernicus GLO-30 dataset.
DATASET_BUCKET_NAME = "copernicus-dem-30m"
DATASET_RESOLUTION = 10  # The resolution of the GLO-30 dataset is 10 arcseconds.
DATAFILE_NAME_PREFIX = "Copernicus_DSM_COG"
DATAFILE_NAME_SUFFIX = "DEM"


class App:
    """An app that takes H3 cell indexes, finds their elevations, and stores them locally or in a graph database.

    :param octue.resources.Analysis:
    :return None:
    """

    def __init__(self, analysis):
        self.analysis = analysis
        self.MINIMUM_RESOLUTION = self.analysis.configuration_values.get("minimum_resolution", 4)
        self.MAXIMUM_RESOLUTION = self.analysis.configuration_values.get("maximum_resolution", 12)
        self.STORAGE_LOCATION = self.analysis.configuration_values.get("storage_location", "database")
        self.LOCAL_STORAGE_PATH = self.analysis.configuration_values.get("local_storage_path")
        self.DELETE_DOWNLOADED_TILES_AFTER_RUN = self.analysis.configuration_values.get(
            "delete_downloaded_tiles_after_run",
            True,
        )

        self._tiles = None
        self._downloaded_tile_paths = []

    def run(self):
        """Carry out the following:

        1. Extract the elevations of the centrepoints of all the maximum resolution descendent cells of the minimum
           resolution ancestor of the input H3 cells using the Copernicus GLO-30 digital elevation model dataset.
        2. For each cell resolution above the maximum resolution up to and including the minimum resolution, calculate
           its elevation as the mean of its children's elevations.
        3. Store the elevations in a Neo4j graph database or locally in a JSON file.

        When storing in a graph database:
          - The node types are:
            - Cell
            - Elevation
            - Data source
          - The relationship (edge) types are:
            - Parent of (between two cell nodes)
            - Has elevation (between a cell node and an elevation node)
            - Has source (between an elevation node and a data source node)

        :return None:
        """
        try:
            self._validate_cells(self.analysis.input_values["h3_cells"])

            # Get the minimum resolution ancestors of the input cells.
            minimum_resolution_ancestors = {
                self._get_ancestors_up_to_minimum_resolution(cell)[-1]
                for cell in self.analysis.input_values["h3_cells"]
            }

            # Get the centrepoint coordinates of the maximum resolution descendents of the minimum resolution ancestors.
            maximum_resolution_cells_and_coordinates = self._get_maximum_resolution_descendent_centrepoint_coordinates(
                cells=minimum_resolution_ancestors
            )

            # Download only the satellite data elevation tiles needed.
            self._download_and_load_elevation_tiles(maximum_resolution_cells_and_coordinates.values())

            # Extract the centrepoint elevations of the maximum resolution descendents from the satellite data tiles.
            maximum_resolution_descendent_coordinates_and_elevations = self._get_elevations(
                cells_and_coordinates=maximum_resolution_cells_and_coordinates
            )

            # Calculate the average elevations of all the ancestors up to the minimum resolution ancestors and add them
            # to the set of maximum resolution cell elevations.
            cells_and_elevations = self._add_average_elevations_for_ancestors_up_to_minimum_resolution(
                cells_and_elevations=maximum_resolution_descendent_coordinates_and_elevations
            )

            # Store the elevations of all the cells between and including the maximum resolution descendents and the
            # minimum resolution ancestors.
            self._store_elevations(cells_and_elevations)

        finally:
            if self.DELETE_DOWNLOADED_TILES_AFTER_RUN:
                for tile in self._downloaded_tile_paths:
                    os.remove(tile)

    def _validate_cells(self, cells):
        """Check that the given cells are within the minimum and maximum resolutions inclusively.

        :param iter(int) cells: the indexes of the cells to check
        :raise ValueError: if any of the cells are of a resolution greater than the maximum resolution or less than the minimum resolution
        :return None:
        """
        for cell in cells:
            resolution = h3_get_resolution(cell)

            if resolution < self.MINIMUM_RESOLUTION or resolution > self.MAXIMUM_RESOLUTION:
                raise ValueError(
                    f"The H3 cells must be between resolution {self.MINIMUM_RESOLUTION} and {self.MAXIMUM_RESOLUTION}"
                    f"inclusively. Cell {cell} is of resolution {resolution}.",
                )

    def _get_ancestors_up_to_minimum_resolution(self, cell):
        """Get the ancestors of the cell up to the minimum resolution inclusively. If the cell resolution is the same
        as the minimum resolution, the cell is simply returned as a single-element list.

        :param int cell: the index of the cell to get the ancestors of
        :return list: the ancestors of the cell
        """
        if h3_get_resolution(cell) == self.MINIMUM_RESOLUTION:
            return [cell]

        ancestors = []

        while h3_get_resolution(cell) > self.MINIMUM_RESOLUTION:
            # Add the cell's parent to the list of ancestors.
            cell = h3_to_parent(cell)
            ancestors.append(cell)

        return ancestors

    def _get_maximum_resolution_descendent_centrepoint_coordinates(self, cells):
        """Get the centrepoint coordinates of the maximum resolution descendents of the given cells.

        :param iter(int) cells: the indexes of the cells to get the maximum resolution descendent centrepoint coordinates for
        :return dict(int, tuple(float, float)): the maximum resolution descendent cell indexes mapped to their centrepoint coordinates
        """
        logger.info(
            "Converting centre-points of resolution %d descendents to latitude/longitude pairs.",
            self.MAXIMUM_RESOLUTION,
        )

        # Get de-duplicated descendents.
        descendents = {
            descendent for cell in cells for descendent in self._get_descendents_down_to_maximum_resolution(cell)
        }

        return {descendent: h3_to_geo(descendent) for descendent in descendents}

    def _download_and_load_elevation_tiles(self, coordinates):
        """Download and load the elevation tiles needed to get the elevations of the given coordinates.

        :param iter(tuple(float, float)) coordinates: the (latitude, longitude) pairs to get the satellite tiles for
        :return None:
        """
        logger.info("Determining which satellite elevation data tiles to download.")

        # Deduplicate the coordinates of the tiles containing the coordinates so each tile is only downloaded once.
        tile_reference_coordinates = {self._get_tile_reference_coordinate(lat, lng) for lat, lng in coordinates}

        logger.info("Downloading and loading required satellite tiles.")

        self._tiles = {
            (tile_latitude, tile_longitude): self._download_and_load_elevation_tile(
                latitude=tile_latitude,
                longitude=tile_longitude,
            )
            for tile_latitude, tile_longitude in tile_reference_coordinates
        }

    def _get_elevations(self, cells_and_coordinates):
        """Get the elevation of each cell in meters using the coordinates it's mapped to.

        :param dict(int, tuple(float, float)) cells_and_coordinates: a mapping of cell index to latitude/longitude pair
        :return dict(int, float): a mapping of cell index to elevation in meters
        """
        logger.info("Extracting elevations for resolution %d cells from satellite tiles.", self.MAXIMUM_RESOLUTION)

        return {
            cell: self._get_elevation(latitude, longitude)
            for cell, (latitude, longitude) in cells_and_coordinates.items()
        }

    def _add_average_elevations_for_ancestors_up_to_minimum_resolution(self, cells_and_elevations):
        """Calculate the average elevation for every ancestor up to the minimum resolution inclusively using each
        ancestor's immediate children's elevations, then add them to the input dictionary.

        :param dict(int, float) cells_and_elevations: a mapping of cell index to elevation
        :return dict(int, float): the input elevations dictionary with the average elevations for all ancestors up to the minimum resolution added
        """
        logger.info("Calculating average elevations for ancestor cells up to resolution %d:", self.MINIMUM_RESOLUTION)
        ancestors_pyramid = self._get_ancestors_up_to_minimum_resolution_as_pyramid(cells_and_elevations.keys())

        for i, ancestor_level in enumerate(ancestors_pyramid):
            logger.info(" --> Resolution %d...", self.MAXIMUM_RESOLUTION - (i + 1))

            # Traverse the ancestor levels from the highest resolution to the lowest so the elevations of each
            # ancestor's direct children are always known before calculating the ancestor's own elevation.
            for ancestor in ancestor_level:
                children_elevations = [cells_and_elevations[child] for child in h3_to_children(ancestor)]
                cells_and_elevations[ancestor] = np.mean(children_elevations)

        return cells_and_elevations

    def _get_ancestors_up_to_minimum_resolution_as_pyramid(self, cells):
        """Get the ancestors of all the cells up to the minimum resolution inclusively as an inverted pyramid where each
        level of the pyramid contains ancestors of the same resolution. The zeroth level is the set of immediate parents
        and the final level is the set of minimum resolution ancestors. This format is useful when recursing down the
        resolutions (i.e. to cells of larger and larger area) to calculate the elevation of each parent based on the
        average of its children's elevations. All the given cells should be of the same resolution.

        For example, if given a list of cells of resolution 12 and the minimum resolution is 9, the pyramid looks like
        this:

            [
                {Level 11 ancestors (most)},
                {Level 10 ancestors (fewer)},
                {Level 9 ancestors (fewest)},
            ]

        :param iter(int) cells: the indexes of the cells to get the ancestors for
        :return list(set(int)): the ancestors as an inverted pyramid
        """
        pyramid = list(zip(*[self._get_ancestors_up_to_minimum_resolution(cell) for cell in cells]))

        # Deduplicate each level.
        for i, cells in enumerate(pyramid):
            pyramid[i] = set(cells)

        return pyramid

    def _store_elevations(self, cells_and_elevations):
        """Store the given elevations in the database or locally depending on the app configuration.

        :param dict(int, float) cells_and_elevations: the cell indexes mapped to their elevations
        :return None:
        """
        if self.STORAGE_LOCATION == "local":
            store_elevations_locally(
                cells_and_elevations,
                path=self.LOCAL_STORAGE_PATH or f"elevations-{datetime.datetime.now().isoformat()}.json",
            )
        else:
            store_elevations_in_database(cells_and_elevations)

    def _get_elevation(self, latitude, longitude):
        """Get the elevation of the Earth's surface at the given coordinates.

        :param float latitude: the latitude in decimal degrees
        :param float longitude: the longitude in decimal degrees
        :return float: the elevation of the coordinate in meters
        """
        tile = self._tiles[self._get_tile_reference_coordinate(latitude, longitude)]
        elevation_map = tile.read(1)
        return elevation_map[tile.index(longitude, latitude)]

    def _get_descendents_down_to_maximum_resolution(self, cell):
        """Get all descendents of the cell down to the maximum resolution inclusively. If the resolution of the cell is
        the same as the maximum resolution, the cell is simply returned as a single-element set.

        :param int cell: the index of the cell to get the descendents of
        :return set: the indexes of the descendents of the cell
        """
        descendents = set()

        if h3_get_resolution(cell) == self.MAXIMUM_RESOLUTION:
            return {cell}

        for child in h3_to_children(cell):
            descendents |= self._get_descendents_down_to_maximum_resolution(child)

        return descendents

    @staticmethod
    def _get_tile_reference_coordinate(latitude, longitude):
        """Get the reference coordinate of the tile containing the given coordinate. A tile's reference coordinate is
        the latitude and longitude of its bottom-left corner, both of which are integers.

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

        :param int latitude: the latitude of the bottom-left corner of the tile in decimal degrees
        :param int longitude: the longitude of the bottom-left corner of the tile in decimal degrees
        :return rasterio.io.DatasetReader: the elevation tile as a RasterIO dataset
        """
        with tempfile.NamedTemporaryFile(delete=False) as temporary_file:
            with open(temporary_file.name, "wb") as f:
                s3.download_fileobj(DATASET_BUCKET_NAME, self._get_tile_path(latitude, longitude), f)

        self._downloaded_tile_paths.append(temporary_file.name)
        return rasterio.open(temporary_file.name)

    @staticmethod
    def _get_tile_path(latitude, longitude):
        """Get the path of the tile within the GLO-30 elevation dataset cloud bucket whose bottom-left corner has the
        given coordinates.

        :param int latitude: the latitude of the bottom-left corner of the tile in decimal degrees
        :param int longitude: the longitude of the bottom-left corner of the tile in decimal degrees
        :return str: the path of the tile containing the coordinate
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

        name = f"{DATAFILE_NAME_PREFIX}_{DATASET_RESOLUTION}_{latitude}_{longitude}_{DATAFILE_NAME_SUFFIX}"
        return f"{name}/{name}.tif"
