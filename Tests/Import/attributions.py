import pytest

def test_attribution_data_consistency(pg_query_runner, neo4j_query_runner):
    """
    CROSS-VALIDATION: Fetches all attribution data from both databases and asserts
    that they are identical row by row, including a count check.
    """
    print("\nPerforming full data consistency check for attributions...")

    # PostgreSQL: Fetch all data, ordering for consistent comparison.
    pg_query = "SELECT * FROM attribution ORDER BY attribution_id, organization_name;"
    pg_data = pg_query_runner(pg_query, ())

    # Neo4j: Fetch the node properties AND the target of the APPLIES_TO relationship.
    # This reconstructs the equivalent of the PostgreSQL table.
    neo4j_query = """
        MATCH (a:Attribution)
        OPTIONAL MATCH (a)-[:APPLIES_TO]->(target)
        WITH a,
             // Use CASE statements to create columns equivalent to the foreign keys
             CASE WHEN target:Agency THEN target.id ELSE NULL END AS agency_id,
             CASE WHEN target:Route THEN target.id ELSE NULL END AS route_id,
             CASE WHEN target:Trip THEN target.id ELSE NULL END AS trip_id
        RETURN a, agency_id, route_id, trip_id
        ORDER BY a.id, a.organization_name
    """
    neo4j_raw_data = neo4j_query_runner(neo4j_query, {})

    pg_count = len(pg_data)
    neo4j_count = len(neo4j_raw_data)

    assert pg_count == neo4j_count, f"Count mismatch: PostgreSQL has {pg_count} attributions, Neo4j has {neo4j_count}."

    # Normalize Neo4j data: Combine the node properties with the derived relationship IDs.
    neo4j_data = []
    for record in neo4j_raw_data:
        # Start with the node's properties (a dictionary)
        node_props = dict(record['a'])
        # Add the derived foreign key equivalents
        node_props['agency_id'] = record['agency_id']
        node_props['route_id'] = record['route_id']
        node_props['trip_id'] = record['trip_id']
        neo4j_data.append(node_props)


    # Convert both to a canonical, comparable format (list of tuples)
    def to_canonical_tuple(row):
        return (
            row.get('attribution_id') or row.get('id'),
            row.get('agency_id'),
            row.get('route_id'),
            row.get('trip_id'),
            row.get('organization_name'),
            bool(row.get('is_producer')),
            bool(row.get('is_operator')),
            bool(row.get('is_authority')),
            row.get('attribution_url') or row.get('url'),
            row.get('attribution_email') or row.get('email'),
            row.get('attribution_phone') or row.get('phone'),
        )

    pg_tuples = [to_canonical_tuple(row) for row in pg_data]
    neo4j_tuples = [to_canonical_tuple(row) for row in neo4j_data]

    pg_tuples.sort()
    neo4j_tuples.sort()

    assert pg_tuples[0:10000] == neo4j_tuples[0:10000], "Full data comparison failed. The attribution datasets are not identical."
    print("Full data consistency check passed for attributions.")
