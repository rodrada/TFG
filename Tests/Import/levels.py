import pytest

def test_level_data_consistency(pg_query_runner, neo4j_query_runner):
    """
    CROSS-VALIDATION: Fetches all level data from both databases and asserts
    that they are identical row by row, including a count check.
    """
    print("\nPerforming full data consistency check for levels...")

    # PostgreSQL: Fetch all data, ordering by the primary key.
    pg_query = "SELECT * FROM level ORDER BY level_id;"
    pg_data = pg_query_runner(pg_query, ())

    # Neo4J: Fetch all Level nodes.
    neo4j_query = "MATCH (l:Level) RETURN l ORDER BY l.id;"
    neo4j_raw_data = neo4j_query_runner(neo4j_query, {})

    pg_count = len(pg_data)
    neo4j_count = len(neo4j_raw_data)

    assert pg_count == neo4j_count, f"Count mismatch: PostgreSQL has {pg_count} levels, Neo4J has {neo4j_count}."

    # Normalize Neo4J data to match the format of PostgreSQL's result.
    neo4j_data = [record['l'] for record in neo4j_raw_data]

    # Convert both to a canonical, comparable format (list of tuples).
    def to_canonical_tuple(row):
        return (
            row.get('level_id') or row.get('id'),
            # Both databases should return a standard float type
            float(row.get('level_index')) if row.get('level_index') is not None else float(row.get('index')),
            row.get('level_name') or row.get('name'),
        )

    pg_tuples = [to_canonical_tuple(row) for row in pg_data]
    neo4j_tuples = [to_canonical_tuple(row) for row in neo4j_data]

    pg_tuples.sort()
    neo4j_tuples.sort()
    
    assert pg_tuples[0:10000] == neo4j_tuples[0:10000], "Full data comparison failed. The level datasets are not identical."
    print("Full data consistency check passed for levels.")


def test_stop_level_consistency(pg_query_runner, neo4j_query_runner):
    """
    CROSS-VALIDATION: Verifies that the links from stops to levels are identical
    in both databases.
    """
    print("\nPerforming consistency check for stop to level links...")

    # PostgreSQL: Get all stop entries that have a level_id.
    pg_query = """
        SELECT stop_id, level_id FROM stop
        WHERE level_id IS NOT NULL
        ORDER BY stop_id;
    """
    pg_data = pg_query_runner(pg_query, ())

    # Neo4J: Reconstruct the same links by traversing the IN_LEVEL relationship.
    neo4j_query = """
        MATCH (s:Stop)-[:IN_LEVEL]->(l:Level)
        RETURN s.id AS stop_id, l.id AS level_id
        ORDER BY stop_id;
    """
    neo4j_data = neo4j_query_runner(neo4j_query, {})

    pg_count = len(pg_data)
    neo4j_count = len(neo4j_data)

    assert pg_count == neo4j_count, f"Count mismatch for stop->level links: PostgreSQL has {pg_count}, Neo4J has {neo4j_count}."

    # Convert both to a canonical, comparable format (list of tuples).
    def to_canonical_tuple(row):
        return (
            row.get('stop_id'),
            row.get('level_id'),
        )

    pg_tuples = [to_canonical_tuple(row) for row in pg_data]
    neo4j_tuples = [to_canonical_tuple(row) for row in neo4j_data]

    pg_tuples.sort()
    neo4j_tuples.sort()
    
    assert pg_tuples[0:10000] == neo4j_tuples[0:10000], "Full data comparison failed for stop->level links."
    print("Full data consistency check passed for stop to level links.")
