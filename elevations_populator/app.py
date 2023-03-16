import logging
import os
import tempfile

import boto3
from botocore import UNSIGNED
from botocore.client import Config


logger = logging.getLogger(__name__)
s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))


BUCKET_NAME = "copernicus-dem-30m"
DATAFILE_NAME_PREFIX = "Copernicus_DSM_COG"
DATAFILE_NAME_SUFFIX = "DEM"


class App:
    def __init__(self, analysis):
        self.analysis = analysis
        self._downloaded_files = []

    def run(self):
        try:
            logger.info("The elevations service has started.")
        finally:
            for file in self._downloaded_files:
                os.remove(file)

    def _download_elevation_tile(self, northing, easting, resolution=10):
        with tempfile.NamedTemporaryFile(delete=False) as temporary_file:
            with open(temporary_file.name, "wb") as f:
                s3.download_fileobj(BUCKET_NAME, self._get_datafile_name(northing, easting, resolution), f)

        self._downloaded_files.append(temporary_file.name)
        return temporary_file.name

    def _get_datafile_name(self, northing, easting, resolution=10):
        name = "_".join((DATAFILE_NAME_PREFIX, resolution, northing, easting, DATAFILE_NAME_SUFFIX))
        return f"{name}/{name}.tif"
