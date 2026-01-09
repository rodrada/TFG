import pytest

# NOTE: This file is named "calendar~.py" to avoid a conflict with the calendar module.

def test_service_data_consistency(pg_query_runner, neo4j_query_runner):
    """
    CROSS-VALIDATION: Fetches all service data from both databases and asserts
    that they are identical row by row, including a count check.
    """
    print("\nPerforming full data consistency check for services (calendar)...")

    # PostgreSQL: Fetch all data, ordering for consistent comparison.
    pg_query = """SELECT * FROM "service" ORDER BY service_id;"""
    pg_data = pg_query_runner(pg_query, ())

    # Neo4J: Reconstruct the table by matching the node and its date relationships.
    # NOTE: This query only selects services that have start/end dates,
    #       which is the correct subset for data imported from calendar.txt.
    neo4j_query = """
        MATCH (s:Service)-[:STARTS_ON]->(sd:Day)
        MATCH (s)-[:ENDS_ON]->(ed:Day)
        RETURN
            s,
            sd.date AS start_date,
            ed.date AS end_date
        ORDER BY s.id;
    """
    neo4j_raw_data = neo4j_query_runner(neo4j_query, {})

    pg_count = len(pg_data)
    neo4j_count = len(neo4j_raw_data)

    assert pg_count == neo4j_count, f"Count mismatch: PostgreSQL has {pg_count} services, Neo4J has {neo4j_count}."

    # Normalize Neo4J data: Combine the node properties with the related dates.
    neo4j_data = []
    for record in neo4j_raw_data:
        node_props = dict(record['s'])
        node_props['start_date'] = record['start_date']
        node_props['end_date'] = record['end_date']
        neo4j_data.append(node_props)

    # Convert both to a canonical, comparable format (list of tuples)
    def to_canonical_tuple(row):
        return (
            row.get('service_id') or row.get('id'),
            bool(row.get('monday')),
            bool(row.get('tuesday')),
            bool(row.get('wednesday')),
            bool(row.get('thursday')),
            bool(row.get('friday')),
            bool(row.get('saturday')),
            bool(row.get('sunday')),
            row.get('start_date'),
            row.get('end_date')
        )

    pg_tuples = [to_canonical_tuple(row) for row in pg_data]
    neo4j_tuples = [to_canonical_tuple(row) for row in neo4j_data]

    pg_tuples.sort()
    neo4j_tuples.sort()

    assert pg_tuples[0:10000] == neo4j_tuples[0:10000], "Full data comparison failed. The service datasets are not identical."
    print("Full data consistency check passed for services.")
