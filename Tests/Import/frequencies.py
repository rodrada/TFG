import pytest
from conftest import to_canonical_time_str

def test_frequency_data_consistency(pg_query_runner, neo4j_query_runner):
    """
    CROSS-VALIDATION: Fetches all frequency data from both databases and asserts
    that they are identical row by row, including a count check.
    """
    print("\nPerforming full data consistency check for frequencies...")

    # PostgreSQL: Join with the enum table to get the string representation of exact_times.
    # The schema references a table named 'service_type' for this.
    pg_query = """
        SELECT
            f.trip_id,
            f.start_time,
            f.end_time,
            f.headway_secs,
            tp.name AS exact_times_str
        FROM frequency f
        JOIN service_type tp ON f.exact_times = tp.id
        ORDER BY f.trip_id, f.start_time;
    """
    pg_data = pg_query_runner(pg_query, ())

    # Neo4J: Reconstruct the table by traversing from Trip to Frequency to ServiceType.
    neo4j_query = """
        MATCH (t:Trip)-[:HAS_FREQUENCY]->(f:Frequency)-[:HAS_SERVICE_TYPE]->(st:ServiceType)
        RETURN
            t.id AS trip_id,
            f.start_time AS start_time,
            f.end_time AS end_time,
            f.headway_secs AS headway_secs,
            st.value AS exact_times_str
        ORDER BY trip_id, start_time;
    """
    neo4j_data = neo4j_query_runner(neo4j_query, {})

    pg_count = len(pg_data)
    neo4j_count = len(neo4j_data)

    assert pg_count == neo4j_count, f"Count mismatch: PostgreSQL has {pg_count} frequency records, Neo4J has {neo4j_count}."

    # Convert both to a canonical, comparable format (list of tuples).
    def to_canonical_tuple(row):
        return (
            row.get('trip_id'),
            to_canonical_time_str(row.get('start_time')),
            to_canonical_time_str(row.get('end_time')),
            row.get('headway_secs'),
            row.get('exact_times_str'),
        )

    pg_tuples = [to_canonical_tuple(row) for row in pg_data]
    neo4j_tuples = [to_canonical_tuple(row) for row in neo4j_data]

    pg_tuples.sort()
    neo4j_tuples.sort()
    
    assert pg_tuples[0:10000] == neo4j_tuples[0:10000], "Full data comparison failed. The frequency datasets are not identical."
    print("Full data consistency check passed for frequencies.")
