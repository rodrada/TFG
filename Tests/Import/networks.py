import pytest

def test_network_data_consistency(pg_query_runner, neo4j_query_runner):
    """
    CROSS-VALIDATION: Fetches all network data from both databases and asserts
    that they are identical row by row, including a count check.
    """
    print("\nPerforming full data consistency check for networks...")

    # PostgreSQL: Fetch all data, ordering by the primary key.
    pg_query = "SELECT * FROM network ORDER BY network_id;"
    pg_data = pg_query_runner(pg_query, ())

    # Neo4J: Fetch all Network nodes.
    neo4j_query = "MATCH (n:Network) RETURN n ORDER BY n.id;"
    neo4j_raw_data = neo4j_query_runner(neo4j_query, {})

    pg_count = len(pg_data)
    neo4j_count = len(neo4j_raw_data)

    assert pg_count == neo4j_count, f"Count mismatch: PostgreSQL has {pg_count} networks, Neo4J has {neo4j_count}."

    # Normalize Neo4J data to match the format of PostgreSQL's result.
    neo4j_data = [record['n'] for record in neo4j_raw_data]

    # Convert both to a canonical, comparable format (list of tuples).
    def to_canonical_tuple(row):
        return (
            row.get('network_id') or row.get('id'),
            row.get('network_name') or row.get('name'),
        )

    pg_tuples = [to_canonical_tuple(row) for row in pg_data]
    neo4j_tuples = [to_canonical_tuple(row) for row in neo4j_data]

    pg_tuples.sort()
    neo4j_tuples.sort()
    
    assert pg_tuples[0:10000] == neo4j_tuples[0:10000], "Full data comparison failed. The network datasets are not identical."
    print("Full data consistency check passed for networks.")
