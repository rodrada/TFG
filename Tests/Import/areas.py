import pytest

def test_area_data_consistency(pg_query_runner, neo4j_query_runner):
    """
    CROSS-VALIDATION: Fetches all area data from both databases and asserts
    that they are identical row by row, including a count check.
    """
    print("\nPerforming full data consistency check for areas...")
    pg_data = pg_query_runner("SELECT * FROM area ORDER BY area_id;", ())
    neo4j_raw_data = neo4j_query_runner("MATCH (a:Area) RETURN a ORDER BY a.id;", {})

    pg_count = len(pg_data)
    neo4j_count = len(neo4j_raw_data)

    assert pg_count == neo4j_count, f"Count mismatch: PostgreSQL has {pg_count} areas, Neo4J has {neo4j_count}."

    # Normalize Neo4j data to match the format of PostgreSQL's result
    neo4j_data = [record['a'] for record in neo4j_raw_data]

    # Convert both to a canonical, comparable format (list of tuples)
    def to_canonical_tuple(row):
        return (
            row.get('area_id') or row.get('id'),
            row.get('area_name') or row.get('name'),
        )

    pg_tuples = [to_canonical_tuple(row) for row in pg_data]
    neo4j_tuples = [to_canonical_tuple(row) for row in neo4j_data]

    pg_tuples.sort()
    neo4j_tuples.sort()

    assert pg_tuples == neo4j_tuples, "Full data comparison failed. The area datasets are not identical."
    print("Full data consistency check passed for areas.")

