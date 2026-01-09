import pytest

def test_fare_data_consistency(pg_query_runner, neo4j_query_runner):
    """
    CROSS-VALIDATION: Fetches all fare data from both databases and asserts
    that they are identical row by row, including a count check.
    """
    print("\nPerforming full data consistency check for fares...")

    # PostgreSQL: Join with payment_method to get the string representation for comparison.
    pg_query = """
        SELECT
            f.*,
            pm.name AS payment_method_str
        FROM fare f
        JOIN payment_method pm ON f.payment_method = pm.id
        ORDER BY f.fare_id;
    """
    pg_data = pg_query_runner(pg_query, ())

    # Neo4J: Reconstruct the table by matching relationships to PaymentMethod and Agency.
    neo4j_query = """
        MATCH (f:Fare)-[:HAS_PAYMENT_METHOD]->(pm:PaymentMethod)
        OPTIONAL MATCH (f)-[:PAID_TO]->(a:Agency)
        RETURN
            f,
            pm.value AS payment_method_str,
            a.id AS agency_id
        ORDER BY f.id;
    """
    neo4j_raw_data = neo4j_query_runner(neo4j_query, {})

    pg_count = len(pg_data)
    neo4j_count = len(neo4j_raw_data)

    assert pg_count == neo4j_count, f"Count mismatch: PostgreSQL has {pg_count} fares, Neo4J has {neo4j_count}."

    # Normalize Neo4J data: Combine the node properties with the related data.
    neo4j_data = []
    for record in neo4j_raw_data:
        node_props = dict(record['f'])
        node_props['payment_method_str'] = record['payment_method_str']
        node_props['agency_id'] = record['agency_id']
        neo4j_data.append(node_props)

    # Convert both to a canonical, comparable format (list of tuples)
    def to_canonical_tuple(row):
        return (
            row.get('fare_id') or row.get('id'),
            float(row.get('price')) if row.get('price') is not None else None,
            row.get('currency_type'),
            row.get('payment_method_str'),
            int(row.get('transfers')) if row.get('transfers') is not None else None,
            row.get('agency_id'),
            int(row.get('transfer_duration')) if row.get('transfer_duration') is not None else None,
        )

    pg_tuples = [to_canonical_tuple(row) for row in pg_data]
    neo4j_tuples = [to_canonical_tuple(row) for row in neo4j_data]

    pg_tuples.sort()
    neo4j_tuples.sort()

    assert pg_tuples[0:10000] == neo4j_tuples[0:10000], "Full data comparison failed. The fare datasets are not identical."
    print("Full data consistency check passed for fares.")
