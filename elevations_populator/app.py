import logging
import math
import os
import tempfile

import boto3
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
            latitudes = []
            longitudes = []
            tile_latitudes = []
            tile_longitudes = []

            for h3_cell in self.analysis.input_values["h3_cells"]:
                latitude, longitude = h3_to_geo(h3_cell)

                # Positive latitudes are north of the equator.
                if latitude >= 0:
                    tile_latitude = f"N{math.trunc(latitude)}"
                else:
                    tile_latitude = f"S{math.trunc(-latitude)}"

                # Positive longitudes are east of the prime meridian.
                if longitude >= 0:
                    tile_longitude = f"E{math.trunc(longitude)}"
                else:
                    tile_longitude = f"W{math.trunc(-longitude)}"

                latitudes.append(latitude)
                longitudes.append(longitude)

                tile_latitudes.append(tile_latitude)
                tile_longitudes.append(tile_longitude)

        finally:
            for file in self._downloaded_files:
                os.remove(file)

    def _download_elevation_tile(self, latitude, longitude):
        with tempfile.NamedTemporaryFile(delete=False) as temporary_file:
            with open(temporary_file.name, "wb") as f:
                s3.download_fileobj(BUCKET_NAME, self._get_datafile_name(latitude, longitude), f)

        self._downloaded_files.append(temporary_file.name)
        return temporary_file.name

    def _get_datafile_name(self, latitude, longitude):
        name = "_".join((DATAFILE_NAME_PREFIX, RESOLUTION, latitude, longitude, DATAFILE_NAME_SUFFIX))
        return f"{name}/{name}.tif"
