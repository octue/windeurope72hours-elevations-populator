import json
import logging


logger = logging.getLogger(__name__)


def store_elevations_locally(cells_and_elevations, path):
    logger.info("Storing elevations locally at %r.", path)

    try:
        with open(path) as f:
            persisted_data = json.load(f)

    except (FileNotFoundError, json.JSONDecodeError):
        persisted_data = []

    for cell, elevation in cells_and_elevations.items():
        # Convert numpy float type to python float type.
        persisted_data.append([cell, float(elevation)])

    with open(path, "w") as f:
        json.dump(persisted_data, f, indent=4)
