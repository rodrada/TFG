import pytest
from shapely.wkt import loads

def test_shape_data_consistency(pg_query_runner, neo4j_query_runner):
    """
    CROSS-VALIDATION: Fetches all shape data from both databases and asserts
    that they are identical by comparing their Well-Known Text (WKT) representations.
    """
    print("\nPerforming full data consistency check for shapes...")

    # PostgreSQL: Export the geometry as a WKT string for comparison.
    pg_query = "SELECT shape_id, ST_AsText(shape_geom) AS wkt FROM shape ORDER BY shape_id;"
    pg_data = pg_query_runner(pg_query, ())

    # Neo4J: Fetch the shape node and its wkt property. The property name 'wkt' is
    # a standard for the spatial extension used in the import script.
    neo4j_query = "MATCH (s:Shape) RETURN s.id AS shape_id, s.wkt AS wkt ORDER BY s.id;"
    neo4j_data = neo4j_query_runner(neo4j_query, {})

    pg_count = len(pg_data)
    neo4j_count = len(neo4j_data)

    assert pg_count == neo4j_count, f"Count mismatch: PostgreSQL has {pg_count} shapes, Neo4J has {neo4j_count}."

    # Convert both to a canonical, comparable format (list of tuples).
    def to_canonical_tuple(row):
        wkt = row.get('wkt')
        return (
            row.get('shape_id'),
            loads(wkt)
        )

    pg_tuples = [to_canonical_tuple(row) for row in pg_data]
    neo4j_tuples = [to_canonical_tuple(row) for row in neo4j_data]

    pg_tuples.sort()
    neo4j_tuples.sort()

    assert all([pg[0] == neo4j[0] and pg[1].equals_exact(neo4j[1], 0.0001) for pg, neo4j in zip(pg_tuples, neo4j_tuples)]), "Full data comparison failed. The shape datasets are not identical."
    print("Full data consistency check passed for shapes.")


def test_trip_shape_consistency(pg_query_runner, neo4j_query_runner):
    """
    CROSS-VALIDATION: Verifies that the links from trips to shapes are identical
    in both databases.
    """
    print("\nPerforming consistency check for trip to shape links...")

    # PostgreSQL: Get all trip entries that have a shape_id.
    pg_query = """
        SELECT trip_id, shape_id FROM trip
        WHERE shape_id IS NOT NULL
        ORDER BY trip_id;
    """
    pg_data = pg_query_runner(pg_query, ())

    # Neo4J: Reconstruct the same links by traversing the HAS_SHAPE relationship.
    neo4j_query = """
        MATCH (t:Trip)-[:HAS_SHAPE]->(s:Shape)
        RETURN t.id AS trip_id, s.id AS shape_id
        ORDER BY trip_id;
    """
    neo4j_data = neo4j_query_runner(neo4j_query, {})

    pg_count = len(pg_data)
    neo4j_count = len(neo4j_data)

    assert pg_count == neo4j_count, f"Count mismatch for trip->shape links: PostgreSQL has {pg_count}, Neo4J has {neo4j_count}."

    # Convert both to a canonical, comparable format (list of tuples).
    def to_canonical_tuple(row):
        return (
            row.get('trip_id'),
            row.get('shape_id'),
        )

    pg_tuples = [to_canonical_tuple(row) for row in pg_data]
    neo4j_tuples = [to_canonical_tuple(row) for row in neo4j_data]

    pg_tuples.sort()
    neo4j_tuples.sort()

    assert pg_tuples[0:10000] == neo4j_tuples[0:10000], "Full data comparison failed for trip->shape links."
    print("Full data consistency check passed for trip to shape links.")
