import pytest

def test_location_group_data_consistency(pg_query_runner, neo4j_query_runner):
    """
    CROSS-VALIDATION: Fetches all location_group data from both databases and
    asserts that they are identical row by row, including a count check.
    """
    print("\nPerforming full data consistency check for location groups...")

    # PostgreSQL: Fetch all data, ordering by the primary key.
    pg_query = "SELECT * FROM location_group ORDER BY location_group_id;"
    pg_data = pg_query_runner(pg_query, ())

    # Neo4J: Fetch all LocationGroup nodes.
    neo4j_query = "MATCH (lg:LocationGroup) RETURN lg ORDER BY lg.id;"
    neo4j_raw_data = neo4j_query_runner(neo4j_query, {})

    pg_count = len(pg_data)
    neo4j_count = len(neo4j_raw_data)

    assert pg_count == neo4j_count, f"Count mismatch: PostgreSQL has {pg_count} location groups, Neo4J has {neo4j_count}."

    # Normalize Neo4J data to match the format of PostgreSQL's result.
    neo4j_data = [record['lg'] for record in neo4j_raw_data]

    # Convert both to a canonical, comparable format (list of tuples).
    def to_canonical_tuple(row):
        return (
            row.get('location_group_id') or row.get('id'),
            row.get('location_group_name') or row.get('name'),
        )

    pg_tuples = [to_canonical_tuple(row) for row in pg_data]
    neo4j_tuples = [to_canonical_tuple(row) for row in neo4j_data]

    pg_tuples.sort()
    neo4j_tuples.sort()
    
    assert pg_tuples[0:10000] == neo4j_tuples[0:10000], "Full data comparison failed. The location_group datasets are not identical."
    print("Full data consistency check passed for location groups.")


def test_stop_time_location_group_consistency(pg_query_runner, neo4j_query_runner):
    """
    CROSS-VALIDATION: Verifies that the links from stop_time to location_group
    are identical in both databases.
    """
    print("\nPerforming consistency check for stop_time to location_group links...")

    # PostgreSQL: Get all stop_time entries that have a location_group_id.
    pg_query = """
        SELECT trip_id, stop_sequence, location_group_id FROM stop_time
        WHERE location_group_id IS NOT NULL
        ORDER BY trip_id, stop_sequence;
    """
    pg_data = pg_query_runner(pg_query, ())

    # Neo4J: Reconstruct the same links by traversing the IN_LOCATION_GROUP relationship from StopTime.
    # A StopTime node is identified by its sequence and its trip.
    neo4j_query = """
        MATCH (t:Trip)<-[:PART_OF]-(st:StopTime)-[:IN_LOCATION_GROUP]->(lg:LocationGroup)
        RETURN
            t.id AS trip_id,
            st.stop_sequence AS stop_sequence,
            lg.id AS location_group_id
        ORDER BY trip_id, stop_sequence;
    """
    neo4j_data = neo4j_query_runner(neo4j_query, {})

    pg_count = len(pg_data)
    neo4j_count = len(neo4j_data)

    assert pg_count == neo4j_count, f"Count mismatch for stop_time->location_group links: PostgreSQL has {pg_count}, Neo4J has {neo4j_count}."

    # Convert both to a canonical, comparable format (list of tuples).
    def to_canonical_tuple(row):
        return (
            row.get('trip_id'),
            row.get('stop_sequence'),
            row.get('location_group_id'),
        )

    pg_tuples = [to_canonical_tuple(row) for row in pg_data]
    neo4j_tuples = [to_canonical_tuple(row) for row in neo4j_data]

    pg_tuples.sort()
    neo4j_tuples.sort()
    
    assert pg_tuples[0:10000] == neo4j_tuples[0:10000], "Full data comparison failed for stop_time->location_group links."
    print("Full data consistency check passed for stop_time to location_group links.")
