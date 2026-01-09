import pytest

def test_trip_data_consistency(pg_query_runner, neo4j_query_runner):
    """
    CROSS-VALIDATION: Fetches all trip data and its relationships from both
    databases and asserts that they are identical row by row.
    """
    print("\nPerforming full data consistency check for trips...")

    # PostgreSQL: Join with all enum tables to get their string representations.
    pg_query = """
        SELECT
            t.route_id,
            t.service_id,
            t.trip_id,
            t.trip_headsign,
            t.trip_short_name,
            td.name AS direction_str,
            t.block_id,
            t.shape_id,
            ws.name AS wheelchair_accessible_str,
            bs.name AS bikes_allowed_str
        FROM trip t
        -- LEFT JOINs are crucial for fields that can be NULL
        LEFT JOIN travel_direction td ON t.direction_id = td.id
        LEFT JOIN wheelchair_status ws ON t.wheelchair_accessible = ws.id
        LEFT JOIN bicycle_status bs ON t.bikes_allowed = bs.id
        ORDER BY t.trip_id;
    """
    pg_data = pg_query_runner(pg_query, ())

    # Neo4J: Reconstruct the full trip record by traversing all relationships.
    neo4j_query = """
        MATCH (t:Trip)
        // Mandatory relationships
        MATCH (t)-[:FOLLOWS]->(r:Route)
        MATCH (t)-[:SCHEDULED_BY]->(s:Service)
        // Optional relationships to correctly handle NULLs
        OPTIONAL MATCH (t)-[:HAS_TRAVEL_DIRECTION]->(td:TravelDirection)
        OPTIONAL MATCH (t)-[:IN_TRIP_BLOCK]->(tb:TripBlock)
        OPTIONAL MATCH (t)-[:HAS_SHAPE]->(sh:Shape)
        OPTIONAL MATCH (t)-[:HAS_WHEELCHAIR_STATUS]->(ws:WheelchairStatus)
        OPTIONAL MATCH (t)-[:HAS_BICYCLE_STATUS]->(bs:BicycleStatus)
        RETURN
            r.id AS route_id,
            s.id AS service_id,
            t.id AS trip_id,
            t.headsign AS trip_headsign,
            t.short_name AS trip_short_name,
            td.value AS direction_str,
            tb.id AS block_id,
            sh.id AS shape_id,
            ws.value AS wheelchair_accessible_str,
            bs.value AS bikes_allowed_str
        ORDER BY trip_id;
    """
    neo4j_data = neo4j_query_runner(neo4j_query, {})

    pg_count = len(pg_data)
    neo4j_count = len(neo4j_data)

    assert pg_count == neo4j_count, f"Count mismatch: PostgreSQL has {pg_count} trips, Neo4J has {neo4j_count}."

    # Convert both to a canonical, comparable format (list of tuples).
    def to_canonical_tuple(row):
        return (
            row.get('route_id'),
            row.get('service_id'),
            row.get('trip_id'),
            row.get('trip_headsign'),
            row.get('trip_short_name'),
            row.get('direction_str'),
            row.get('block_id'),
            row.get('shape_id'),
            row.get('wheelchair_accessible_str'),
            row.get('bikes_allowed_str'),
        )

    pg_tuples = [to_canonical_tuple(row) for row in pg_data]
    neo4j_tuples = [to_canonical_tuple(row) for row in neo4j_data]

    pg_tuples.sort()
    neo4j_tuples.sort()

    assert pg_tuples[0:10000] == neo4j_tuples[0:10000], "Full data comparison failed. The trip datasets are not identical."
    print("Full data consistency check passed for trips.")
