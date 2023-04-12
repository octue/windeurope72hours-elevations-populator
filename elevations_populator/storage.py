import json
import logging
import os

from h3.api.basic_int import h3_get_resolution, h3_to_parent
from neo4j import GraphDatabase


logger = logging.getLogger(__name__)


DATASET_NAME = "Copernicus Digital Elevation Model GLO-30"
DATASET_URI = "s3://copernicus-dem-30m/"


def store_elevations_locally(cells_and_elevations, path):
    """Store the given elevations as a JSON file at the given local path.

    :param dict(int, float) cells_and_elevations: the h3 cells and their elevations
    :param str path: the path to save the JSON file at
    :return None:
    """
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


def store_elevations_in_database(cells_and_elevations):
    """Store the given elevations in the Neo4j graph database.

    :param dict(int, float) cells_and_elevations: the h3 cells and their elevations
    :param str path: the path to save the JSON file at
    :return None:
    """
    logger.info("Storing elevations in database.")

    driver = GraphDatabase.driver(
        uri=os.environ["NEO4J_URI"],
        auth=(os.environ["NEO4J_USERNAME"], os.environ["NEO4J_PASSWORD"]),
    )

    with driver:
        with driver.session(database="neo4j") as session:
            session.execute_write(_create_cells_and_elevations, cells_and_elevations)


def _create_cells_and_elevations(tx, cells_and_elevations):
    cells_and_elevations_query_parts = []
    cell_parent_relationships_query_parts = []

    for cell, elevation in cells_and_elevations.items():
        cells_and_elevations_query_parts.append(
            "(:Cell {index: %d, resolution: %d})-[:HAS_ELEVATION]->(:Elevation {value: %f})-[:HAS_SOURCE]->(:DataSource {name: %r, uri: %r})"
            % (cell, h3_get_resolution(cell), elevation, DATASET_NAME, DATASET_URI)
        )

        cell_parent_relationships_query_parts.append(
            """MATCH
              (child:Cell),
              (parent:Cell)
            WHERE child.index = %d
            AND parent.index = %d
            CREATE (parent)-[:PARENT_OF]->(child)
            RETURN;
            """
            % (cell, h3_to_parent(cell))
        )

    cells_and_elevations_query = "CREATE " + ", ".join(cells_and_elevations_query_parts)
    cell_parent_relationships_query = "\n".join(cell_parent_relationships_query_parts)

    tx.run(cells_and_elevations_query)
    tx.run(cell_parent_relationships_query)
