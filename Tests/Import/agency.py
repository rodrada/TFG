import pytest

def test_agency_data_consistency(pg_query_runner, neo4j_query_runner):
    """
    CROSS-VALIDATION: Fetches all agency data from both databases and asserts
    that they are identical row by row, including a count check.
    """
    print("\nPerforming full data consistency check for agencies...")
    pg_data = pg_query_runner("SELECT * FROM agency ORDER BY agency_id;", ())
    neo4j_raw_data = neo4j_query_runner("MATCH (a:Agency) RETURN a ORDER BY a.id;", {})

    pg_count = len(pg_data)
    neo4j_count = len(neo4j_raw_data)

    assert pg_count == neo4j_count, f"Count mismatch: PostgreSQL has {pg_count} agencies, Neo4J has {neo4j_count}."

    # Normalize Neo4j data to match the format of PostgreSQL's result
    neo4j_data = [record['a'] for record in neo4j_raw_data]

    # Convert both to a canonical, comparable format (list of tuples)
    def to_canonical_tuple(row):
        return (
            row.get('agency_id') or row.get('id'),
            row.get('agency_name') or row.get('name'),
            row.get('agency_url') or row.get('url'),
            row.get('agency_timezone') or row.get('timezone'),
            row.get('agency_lang') or row.get('lang'),
            row.get('agency_phone') or row.get('phone'),
            row.get('agency_fare_url') or row.get('fare_url'),
            row.get('agency_email') or row.get('email'),
        )

    pg_tuples = [to_canonical_tuple(row) for row in pg_data]
    neo4j_tuples = [to_canonical_tuple(row) for row in neo4j_data]

    pg_tuples.sort()
    neo4j_tuples.sort()

    assert pg_tuples[0:10000] == neo4j_tuples[0:10000], "Full data comparison failed. The datasets are not identical."
    print("Full data consistency check passed.")
