import pytest

def test_fare_leg_rule_data_consistency(pg_query_runner, neo4j_query_runner):
    """
    CROSS-VALIDATION: Fetches all fare_leg_rule data from both databases and
    asserts that they are identical row by row, including a count check.
    """
    print("\nPerforming full data consistency check for fare leg rules...")

    # PostgreSQL: Fetch all data, ordering by a stable set of columns for comparison.
    pg_query = """
        SELECT * FROM fare_leg_rule
        ORDER BY fare_product_id, leg_group_id, network_id, from_area_id, to_area_id,
                 from_timeframe_group_id, to_timeframe_group_id;
    """
    pg_data = pg_query_runner(pg_query, ())

    # Neo4J: Reconstruct the flat table by traversing all relationships from the FareLegRule node.
    neo4j_query = """
        MATCH (flr:FareLegRule)
        // This relationship is mandatory
        MATCH (flr)-[:REQUIRES]->(fp:FareProduct)
        // All other relationships are optional to correctly handle NULLs
        OPTIONAL MATCH (flr)-[:IN_LEG_GROUP]->(lg:LegGroup)
        OPTIONAL MATCH (flr)-[:IN_NETWORK]->(n:Network)
        OPTIONAL MATCH (flr)-[:FROM_AREA]->(fa:Area)
        OPTIONAL MATCH (flr)-[:TO_AREA]->(ta:Area)
        OPTIONAL MATCH (flr)-[:FROM_TIMEFRAME_GROUP]->(ftg:TimeframeGroup)
        OPTIONAL MATCH (flr)-[:TO_TIMEFRAME_GROUP]->(ttg:TimeframeGroup)
        RETURN
            lg.id AS leg_group_id,
            n.id AS network_id,
            fa.id AS from_area_id,
            ta.id AS to_area_id,
            ftg.id AS from_timeframe_group_id,
            ttg.id AS to_timeframe_group_id,
            fp.id AS fare_product_id,
            flr.rule_priority AS rule_priority
        ORDER BY fare_product_id, leg_group_id, network_id, from_area_id, to_area_id,
                 from_timeframe_group_id, to_timeframe_group_id;
    """
    neo4j_data = neo4j_query_runner(neo4j_query, {})

    pg_count = len(pg_data)
    neo4j_count = len(neo4j_data)

    assert pg_count == neo4j_count, f"Count mismatch: PostgreSQL has {pg_count} fare leg rules, Neo4J has {neo4j_count}."

    # Convert both to a canonical, comparable format (list of tuples)
    def to_canonical_tuple(row):
        return (
            row.get('leg_group_id'),
            row.get('network_id'),
            row.get('from_area_id'),
            row.get('to_area_id'),
            row.get('from_timeframe_group_id'),
            row.get('to_timeframe_group_id'),
            row.get('fare_product_id'),
            row.get('rule_priority'),
        )

    pg_tuples = [to_canonical_tuple(row) for row in pg_data]
    neo4j_tuples = [to_canonical_tuple(row) for row in neo4j_data]

    pg_tuples.sort()
    neo4j_tuples.sort()

    assert pg_tuples[0:10000] == neo4j_tuples[0:10000], "Full data comparison failed. The fare leg rule datasets are not identical."
    print("Full data consistency check passed for fare leg rules.")
