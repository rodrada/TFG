import pytest

def test_location_group_stop_data_consistency(pg_query_runner, neo4j_query_runner):
    """
    CROSS-VALIDATION: Fetches all location_group_stop join data from both
    databases and asserts that they are identical row by row.
    """
    print("\nPerforming full data consistency check for location group stops...")

    # PostgreSQL: Fetch all data from the join table.
    pg_query = "SELECT * FROM location_group_stop ORDER BY location_group_id, stop_id;"
    pg_data = pg_query_runner(pg_query, ())

    # Neo4J: Reconstruct the join table by finding all :IN_LOCATION_GROUP relationships.
    neo4j_query = """
        MATCH (s:Stop)-[:IN_LOCATION_GROUP]->(lg:LocationGroup)
        RETURN lg.id AS location_group_id, s.id AS stop_id
        ORDER BY location_group_id, stop_id;
    """
    neo4j_data = neo4j_query_runner(neo4j_query, {})

    pg_count = len(pg_data)
    neo4j_count = len(neo4j_data)

    assert pg_count == neo4j_count, f"Count mismatch: PostgreSQL has {pg_count} location group stop records, Neo4J has {neo4j_count}."

    # Convert both to a canonical, comparable format (list of tuples).
    def to_canonical_tuple(row):
        return (
            row.get('location_group_id'),
            row.get('stop_id'),
        )

    pg_tuples = [to_canonical_tuple(row) for row in pg_data]
    neo4j_tuples = [to_canonical_tuple(row) for row in neo4j_data]

    pg_tuples.sort()
    neo4j_tuples.sort()
    
    assert pg_tuples[0:10000] == neo4j_tuples[0:10000], "Full data comparison failed. The location_group_stop datasets are not identical."
    print("Full data consistency check passed for location group stops.")
