# Constants for downloading elevation tile data from the Copernicus GLO-30 dataset.
import math


DATASET_RESOLUTION = 10  # The resolution of the GLO-30 dataset is 10 arcseconds.
DATAFILE_NAME_PREFIX = "Copernicus_DSM_COG"
DATAFILE_NAME_SUFFIX = "DEM"


def get_tile_reference_coordinate(latitude, longitude):
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


def get_tile_path(latitude, longitude):
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
