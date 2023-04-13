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
    """Create the given cells and elevations in the Neo4j graph database, connect the cells to their elevations, connect
    each cell to its parent, and connect each elevation to its data source.

    :param dict(int, float) cells_and_elevations: the h3 cells and their elevations
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
    """Construct and run the queries to create the given cells and elevations in the Neo4j graph database, connect the
    cells to their elevations, connect each cell to its parent, and connect each elevation to its data source.

    :param neo4j._sync.work.transaction.ManagedTransaction tx:
    :param dict(int, float) cells_and_elevations: the h3 cells and their elevations
    :return None:
    """
    cells_and_elevations_query_parts = []
    cell_and_parent_indexes = []

    for cell, elevation in cells_and_elevations.items():
        cells_and_elevations_query_parts.append(
            "(:Cell {index: %d, resolution: %d})-[:HAS_ELEVATION]->(:Elevation {value: %f})-[:HAS_SOURCE]"
            "->(:DataSource {name: %r, uri: %r})"
            % (cell, h3_get_resolution(cell), elevation, DATASET_NAME, DATASET_URI)
        )

        cell_and_parent_indexes.append((cell, h3_to_parent(cell)))

    cells_and_elevations_query = "CREATE " + ", ".join(cells_and_elevations_query_parts)

    cell_parent_relationships_query = """
    UNWIND $indexes AS index_pair
    MATCH
      (child:Cell),
      (parent:Cell)
    WHERE child.index = index_pair[0]
    AND parent.index = index_pair[1]
    CREATE (parent)-[:PARENT_OF]->(child)
    """

    tx.run(cells_and_elevations_query)
    tx.run(cell_parent_relationships_query, indexes=cell_and_parent_indexes)
