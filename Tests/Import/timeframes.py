import pytest
from conftest import to_canonical_time_str

def test_timeframe_data_consistency(pg_query_runner, neo4j_query_runner):
    """
    CROSS-VALIDATION: Fetches all timeframe data from both databases and asserts
    that they are identical row by row, including a count check.
    """
    print("\nPerforming full data consistency check for timeframes...")

    # PostgreSQL: Fetch all data, ordering by the composite unique key for stability.
    pg_query = """
        SELECT * FROM timeframe
        ORDER BY timeframe_group_id, service_id, start_time, end_time;
    """
    pg_data = pg_query_runner(pg_query, ())

    # Neo4J: Reconstruct the flat table by traversing the relationships from the Timeframe node.
    neo4j_query = """
        MATCH (t:Timeframe)-[:IN_GROUP]->(tg:TimeframeGroup)
        MATCH (t)-[:DURING]->(s:Service)
        RETURN
            tg.id AS timeframe_group_id,
            t.start_time AS start_time,
            t.end_time AS end_time,
            s.id AS service_id
        ORDER BY timeframe_group_id, service_id, start_time, end_time;
    """
    neo4j_data = neo4j_query_runner(neo4j_query, {})

    pg_count = len(pg_data)
    neo4j_count = len(neo4j_data)

    assert pg_count == neo4j_count, f"Count mismatch: PostgreSQL has {pg_count} timeframes, Neo4J has {neo4j_count}."

    # Convert both to a canonical, comparable format (list of tuples).
    def to_canonical_tuple(row):
        return (
            row.get('timeframe_group_id'),
            to_canonical_time_str(row.get('start_time')),
            to_canonical_time_str(row.get('end_time')),
            row.get('service_id'),
        )

    pg_tuples = [to_canonical_tuple(row) for row in pg_data]
    neo4j_tuples = [to_canonical_tuple(row) for row in neo4j_data]

    pg_tuples.sort()
    neo4j_tuples.sort()
    
    assert pg_tuples[0:10000] == neo4j_tuples[0:10000], "Full data comparison failed. The timeframe datasets are not identical."
    print("Full data consistency check passed for timeframes.")
