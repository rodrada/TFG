import pytest

def test_feed_data_consistency(pg_query_runner, neo4j_query_runner):
    """
    CROSS-VALIDATION: Fetches all feed data from both databases and asserts
    that they are identical row by row, including a count check.
    """
    print("\nPerforming full data consistency check for feed info...")

    # PostgreSQL: Fetch all data, ordering by the primary key for consistency.
    pg_query = "SELECT * FROM feed ORDER BY feed_publisher_name;"
    pg_data = pg_query_runner(pg_query, ())

    # Neo4J: Fetch all Feed nodes.
    neo4j_query = "MATCH (f:Feed) RETURN f ORDER BY f.publisher_name;"
    neo4j_raw_data = neo4j_query_runner(neo4j_query, {})

    pg_count = len(pg_data)
    neo4j_count = len(neo4j_raw_data)

    assert pg_count == neo4j_count, f"Count mismatch: PostgreSQL has {pg_count} feed records, Neo4J has {neo4j_count}."

    # Normalize Neo4J data to match the format of PostgreSQL's result.
    neo4j_data = [record['f'] for record in neo4j_raw_data]

    # Convert both to a canonical, comparable format (list of tuples).
    def to_canonical_tuple(row):
        return (
            row.get('feed_publisher_name') or row.get('publisher_name'),
            row.get('feed_publisher_url') or row.get('publisher_url'),
            row.get('feed_lang') or row.get('lang'),
            row.get('default_lang'), # Same name in both
            row.get('feed_start_date') or row.get('start_date'),
            row.get('feed_end_date') or row.get('end_date'),
            row.get('feed_version') or row.get('version'),
            row.get('feed_contact_email') or row.get('contact_email'),
            row.get('feed_contact_url') or row.get('contact_url'),
        )

    pg_tuples = [to_canonical_tuple(row) for row in pg_data]
    neo4j_tuples = [to_canonical_tuple(row) for row in neo4j_data]

    pg_tuples.sort()
    neo4j_tuples.sort()
    
    assert pg_tuples[0:10000] == neo4j_tuples[0:10000], "Full data comparison failed. The feed datasets are not identical."
    print("Full data consistency check passed for feed info.")
