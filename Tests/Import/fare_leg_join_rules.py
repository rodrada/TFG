import pytest

def test_fare_leg_join_rule_data_consistency(pg_query_runner, neo4j_query_runner):
    """
    CROSS-VALIDATION: Fetches all fare_leg_join_rule data from both databases
    and asserts that they are identical row by row, including a count check.
    """
    print("\nPerforming full data consistency check for fare leg join rules...")

    # PostgreSQL: Fetch all data, ordering by all columns for a stable comparison.
    pg_query = """
        SELECT *
        FROM fare_leg_join_rule
        ORDER BY from_network_id, to_network_id, from_stop_id, to_stop_id;
    """
    pg_data = pg_query_runner(pg_query, ())

    # Neo4J: Reconstruct the table by traversing from the intermediate rule node.
    neo4j_query = """
        MATCH (fljr:FareLegJoinRule)
        MATCH (fljr)-[:FROM_NETWORK]->(fn:Network)
        MATCH (fljr)-[:TO_NETWORK]->(tn:Network)
        OPTIONAL MATCH (fljr)-[:FROM_STOP]->(fs:Stop)
        OPTIONAL MATCH (fljr)-[:TO_STOP]->(ts:Stop)
        RETURN
            fn.id AS from_network_id,
            tn.id AS to_network_id,
            fs.id AS from_stop_id,
            ts.id AS to_stop_id
        ORDER BY from_network_id, to_network_id, from_stop_id, to_stop_id;
    """
    neo4j_data = neo4j_query_runner(neo4j_query, {})

    pg_count = len(pg_data)
    neo4j_count = len(neo4j_data)

    assert pg_count == neo4j_count, f"Count mismatch: PostgreSQL has {pg_count} fare leg join rules, Neo4J has {neo4j_count}."

    # Convert both to a canonical, comparable format (list of tuples)
    def to_canonical_tuple(row):
        return (
            row.get('from_network_id'),
            row.get('to_network_id'),
            row.get('from_stop_id'),
            row.get('to_stop_id'),
        )

    pg_tuples = [to_canonical_tuple(row) for row in pg_data]
    neo4j_tuples = [to_canonical_tuple(row) for row in neo4j_data]

    pg_tuples.sort()
    neo4j_tuples.sort()

    assert pg_tuples[0:10000] == neo4j_tuples[0:10000], "Full data comparison failed. The fare leg join rule datasets are not identical."
    print("Full data consistency check passed for fare leg join rules.")
