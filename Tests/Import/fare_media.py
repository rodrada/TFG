import pytest

def test_fare_media_data_consistency(pg_query_runner, neo4j_query_runner):
    """
    CROSS-VALIDATION: Fetches all fare_media data from both databases and asserts
    that they are identical row by row, including a count check.
    """
    print("\nPerforming full data consistency check for fare media...")

    # PostgreSQL: Join with the enum table to get the string representation of the type.
    pg_query = """
        SELECT
            fm.fare_media_id,
            fm.fare_media_name,
            fmt.name AS fare_media_type_str
        FROM fare_media fm
        JOIN fare_media_type fmt ON fm.fare_media_type = fmt.id
        ORDER BY fm.fare_media_id;
    """
    pg_data = pg_query_runner(pg_query, ())

    # Neo4J: Reconstruct the table by matching the node and its HAS_TYPE relationship.
    neo4j_query = """
        MATCH (fm:FareMedia)-[:HAS_TYPE]->(fmt:FareMediaType)
        RETURN
            fm.id AS fare_media_id,
            fm.name AS fare_media_name,
            fmt.value AS fare_media_type_str
        ORDER BY fare_media_id;
    """
    neo4j_data = neo4j_query_runner(neo4j_query, {})

    pg_count = len(pg_data)
    neo4j_count = len(neo4j_data)

    assert pg_count == neo4j_count, f"Count mismatch: PostgreSQL has {pg_count} fare media, Neo4J has {neo4j_count}."

    # Convert both to a canonical, comparable format (list of tuples)
    def to_canonical_tuple(row):
        return (
            row.get('fare_media_id'),
            row.get('fare_media_name'),
            row.get('fare_media_type_str')
        )

    pg_tuples = [to_canonical_tuple(row) for row in pg_data]
    neo4j_tuples = [to_canonical_tuple(row) for row in neo4j_data]

    pg_tuples.sort()
    neo4j_tuples.sort()
    
    assert pg_tuples[0:10000] == neo4j_tuples[0:10000], "Full data comparison failed. The fare media datasets are not identical."
    print("Full data consistency check passed for fare media.")
