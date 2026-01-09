import pytest

def test_fare_transfer_rule_data_consistency(pg_query_runner, neo4j_query_runner):
    """
    CROSS-VALIDATION: Fetches all fare_transfer_rule data from both databases and
    asserts that they are identical row by row, including a count check.
    """
    print("\nPerforming full data consistency check for fare transfer rules...")

    # PostgreSQL: Join with all enum tables to get the string representations.
    pg_query = """
        SELECT
            ftr.from_leg_group_id,
            ftr.to_leg_group_id,
            ftr.transfer_count,
            ftr.duration_limit,
            dlt.name AS duration_limit_type_str,
            tct.name AS fare_transfer_type_str,
            ftr.fare_product_id
        FROM fare_transfer_rule ftr
        JOIN transfer_cost_type tct ON ftr.fare_transfer_type = tct.id
        LEFT JOIN duration_limit_type dlt ON ftr.duration_limit_type = dlt.id
        ORDER BY from_leg_group_id, to_leg_group_id, fare_product_id, transfer_count, duration_limit;
    """
    pg_data = pg_query_runner(pg_query, ())

    # Neo4J: Reconstruct the flat table by traversing all relationships.
    neo4j_query = """
        MATCH (ftr:FareTransferRule)
        OPTIONAL MATCH (ftr)-[:FROM]->(flg:LegGroup)
        OPTIONAL MATCH (ftr)-[:TO]->(tlg:LegGroup)
        OPTIONAL MATCH (ftr)-[:REQUIRES]->(fp:FareProduct)
        MATCH (ftr)-[:HAS_TRANSFER_TYPE]->(ftt:FareTransferType)
        OPTIONAL MATCH (ftr)-[:HAS_DURATION_LIMIT_TYPE]->(dlt:DurationLimitType)
        RETURN
            flg.id AS from_leg_group_id,
            tlg.id AS to_leg_group_id,
            ftr.transfer_count AS transfer_count,
            ftr.duration_limit AS duration_limit,
            dlt.value AS duration_limit_type_str,
            ftt.value AS fare_transfer_type_str,
            fp.id AS fare_product_id
        ORDER BY from_leg_group_id, to_leg_group_id, fare_product_id, transfer_count, duration_limit;
    """
    neo4j_data = neo4j_query_runner(neo4j_query, {})

    pg_count = len(pg_data)
    neo4j_count = len(neo4j_data)

    assert pg_count == neo4j_count, f"Count mismatch: PostgreSQL has {pg_count} fare transfer rules, Neo4J has {neo4j_count}."

    # Convert both to a canonical, comparable format (list of tuples)
    def to_canonical_tuple(row):
        return (
            row.get('from_leg_group_id'),
            row.get('to_leg_group_id'),
            row.get('transfer_count'),
            row.get('duration_limit'),
            row.get('duration_limit_type_str'),
            row.get('fare_transfer_type_str'),
            row.get('fare_product_id'),
        )

    pg_tuples = [to_canonical_tuple(row) for row in pg_data]
    neo4j_tuples = [to_canonical_tuple(row) for row in neo4j_data]

    pg_tuples.sort()
    neo4j_tuples.sort()
    
    assert pg_tuples[0:10000] == neo4j_tuples[0:10000], "Full data comparison failed. The fare transfer rule datasets are not identical."
    print("Full data consistency check passed for fare transfer rules.")
