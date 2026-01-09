import pytest

def test_route_data_consistency(pg_query_runner, neo4j_query_runner):
    """
    CROSS-VALIDATION: Fetches all route data and its relationships from both
    databases and asserts that they are identical row by row.
    """
    print("\nPerforming full data consistency check for routes...")

    # PostgreSQL: Join with all enum tables to get their string representations.
    pg_query = """
        SELECT
            r.route_id,
            r.agency_id,
            r.route_short_name,
            r.route_long_name,
            r.route_desc,
            rt.name AS route_type_str,
            r.route_url,
            r.route_color,
            r.route_text_color,
            r.route_sort_order,
            cp.name AS continuous_pickup_str,
            cd.name AS continuous_drop_off_str,
            r.network_id
        FROM route r
        JOIN route_type rt ON r.route_type = rt.id
        -- LEFT JOIN for continuous statuses in case they are NULL in the source file
        LEFT JOIN continuous_status cp ON r.continuous_pickup = cp.id
        LEFT JOIN continuous_status cd ON r.continuous_drop_off = cd.id
        ORDER BY r.route_id;
    """
    pg_data = pg_query_runner(pg_query, ())

    # Neo4J: Reconstruct the full route record by traversing all relationships.
    neo4j_query = """
        MATCH (r:Route)
        // Mandatory relationships
        MATCH (r)-[:HAS_ROUTE_TYPE]->(rt:RouteType)
        // Optional relationships to correctly handle NULLs
        OPTIONAL MATCH (r)-[:OPERATED_BY]->(a:Agency)
        OPTIONAL MATCH (r)-[:HAS_CONTINUOUS_PICKUP]->(cp:ContinuousStatus)
        OPTIONAL MATCH (r)-[:HAS_CONTINUOUS_DROP_OFF]->(cd:ContinuousStatus)
        OPTIONAL MATCH (r)-[:BELONGS_TO]->(n:Network)
        RETURN
            r.id AS route_id,
            a.id AS agency_id,
            r.short_name AS route_short_name,
            r.long_name AS route_long_name,
            r.desc AS route_desc,
            rt.value AS route_type_str,
            r.url AS route_url,
            r.color AS route_color,
            r.text_color AS route_text_color,
            r.sort_order AS route_sort_order,
            cp.value AS continuous_pickup_str,
            cd.value AS continuous_drop_off_str,
            n.id AS network_id
        ORDER BY route_id;
    """
    neo4j_data = neo4j_query_runner(neo4j_query, {})

    pg_count = len(pg_data)
    neo4j_count = len(neo4j_data)

    assert pg_count == neo4j_count, f"Count mismatch: PostgreSQL has {pg_count} routes, Neo4J has {neo4j_count}."

    # Convert both to a canonical, comparable format (list of tuples).
    def to_canonical_tuple(row):
        return (
            row.get('route_id'),
            row.get('agency_id'),
            row.get('route_short_name'),
            row.get('route_long_name'),
            row.get('route_desc'),
            row.get('route_type_str'),
            row.get('route_url'),
            row.get('route_color'),
            row.get('route_text_color'),
            row.get('route_sort_order'),
            row.get('continuous_pickup_str'),
            row.get('continuous_drop_off_str'),
            row.get('network_id'),
        )

    pg_tuples = [to_canonical_tuple(row) for row in pg_data]
    neo4j_tuples = [to_canonical_tuple(row) for row in neo4j_data]

    pg_tuples.sort()
    neo4j_tuples.sort()
    
    assert pg_tuples == neo4j_tuples, "Full data comparison failed. The route datasets are not identical."
    print("Full data consistency check passed for routes.")
