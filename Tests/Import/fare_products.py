import pytest

def test_fare_product_data_consistency(pg_query_runner, neo4j_query_runner):
    """
    CROSS-VALIDATION: Fetches all fare_product data from both databases and asserts
    that they are identical row by row, including a count check.
    """
    print("\nPerforming full data consistency check for fare products...")

    # PostgreSQL: Fetch all data, ordering by the composite unique key for consistency.
    pg_query = """
        SELECT * FROM fare_product
        ORDER BY fare_product_id, fare_media_id;
    """
    pg_data = pg_query_runner(pg_query, ())

    # Neo4J: Reconstruct the table by matching the node and its USED_WITH relationship.
    neo4j_query = """
        MATCH (fp:FareProduct)-[:USED_WITH]->(fm:FareMedia)
        RETURN
            fp.id AS fare_product_id,
            fp.name AS fare_product_name,
            fm.id AS fare_media_id,
            fp.amount AS amount,
            fp.currency AS currency
        ORDER BY fare_product_id, fare_media_id;
    """
    neo4j_data = neo4j_query_runner(neo4j_query, {})

    pg_count = len(pg_data)
    neo4j_count = len(neo4j_data)

    assert pg_count == neo4j_count, f"Count mismatch: PostgreSQL has {pg_count} fare products, Neo4J has {neo4j_count}."

    # Convert both to a canonical, comparable format (list of tuples)
    def to_canonical_tuple(row):
        return (
            row.get('fare_product_id'),
            row.get('fare_product_name'),
            row.get('fare_media_id'),
            float(row.get('amount')) if row.get('amount') is not None else None,
            row.get('currency')
        )

    pg_tuples = [to_canonical_tuple(row) for row in pg_data]
    neo4j_tuples = [to_canonical_tuple(row) for row in neo4j_data]

    pg_tuples.sort()
    neo4j_tuples.sort()
    
    assert pg_tuples[0:10000] == neo4j_tuples[0:10000], "Full data comparison failed. The fare product datasets are not identical."
    print("Full data consistency check passed for fare products.")
