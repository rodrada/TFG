import pytest

def test_pathway_data_consistency(pg_query_runner, neo4j_query_runner):
    """
    CROSS-VALIDATION: Fetches all pathway data from both databases and asserts
    that they are identical row by row, including a count check.
    """
    print("\nPerforming full data consistency check for pathways...")

    # PostgreSQL: Join with the enum table to get the string representation of the mode.
    pg_query = """
        SELECT
            p.*,
            pt.name AS pathway_mode_str
        FROM pathway p
        JOIN pathway_type pt ON p.pathway_mode = pt.id
        ORDER BY p.pathway_id;
    """
    pg_data = pg_query_runner(pg_query, ())

    # Neo4J: Reconstruct the table by traversing from the Pathway node to its related nodes.
    neo4j_query = """
        MATCH (p:Pathway)-[:FROM]->(fs:Stop)
        MATCH (p)-[:TO]->(ts:Stop)
        MATCH (p)-[:HAS_PATHWAY_MODE]->(pm:PathwayMode)
        RETURN
            p,
            fs.id AS from_stop_id,
            ts.id AS to_stop_id,
            pm.value AS pathway_mode_str
        ORDER BY p.id;
    """
    neo4j_raw_data = neo4j_query_runner(neo4j_query, {})

    pg_count = len(pg_data)
    neo4j_count = len(neo4j_raw_data)

    assert pg_count == neo4j_count, f"Count mismatch: PostgreSQL has {pg_count} pathways, Neo4J has {neo4j_count}."

    # Normalize Neo4J data: Combine the node properties with the related IDs.
    neo4j_data = []
    for record in neo4j_raw_data:
        node_props = dict(record['p'])
        node_props['from_stop_id'] = record['from_stop_id']
        node_props['to_stop_id'] = record['to_stop_id']
        node_props['pathway_mode_str'] = record['pathway_mode_str']
        neo4j_data.append(node_props)

    # Convert both to a canonical, comparable format (list of tuples).
    def to_canonical_tuple(row):
        # Helper to safely convert a value to float, returning None if input is None.
        def safe_to_float(value):
            return float(value) if value is not None else None

        # Helper to safely convert a value to int, returning None if input is None.
        def safe_to_int(value):
            return int(value) if value is not None else None

        return (
            row.get('pathway_id') or row.get('id'),
            row.get('from_stop_id'),
            row.get('to_stop_id'),
            row.get('pathway_mode_str'),
            bool(row.get('is_bidirectional')),
            safe_to_float(row.get('length')),
            safe_to_int(row.get('traversal_time')),
            safe_to_int(row.get('stair_count')),
            safe_to_float(row.get('max_slope')),
            safe_to_float(row.get('min_width')),
            row.get('signposted_as'),
            row.get('reversed_signposted_as'),
        )

    pg_tuples = [to_canonical_tuple(row) for row in pg_data]
    neo4j_tuples = [to_canonical_tuple(row) for row in neo4j_data]

    pg_tuples.sort()
    neo4j_tuples.sort()
    
    assert pg_tuples[0:10000] == neo4j_tuples[0:10000], "Full data comparison failed. The pathway datasets are not identical."
    print("Full data consistency check passed for pathways.")
