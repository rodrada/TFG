import pytest

def test_stop_data_consistency(pg_query_runner, neo4j_query_runner):
    """
    CROSS-VALIDATION: Fetches all stop data and its relationships from both
    databases and asserts that they are identical row by row.
    """
    print("\nPerforming full data consistency check for stops...")

    # PostgreSQL: Join with enum tables and extract lat/lon from the geometry type.
    pg_query = """
        SELECT
            s.stop_id,
            s.stop_code,
            s.stop_name,
            s.tts_stop_name,
            s.stop_desc,
            ST_Y(s.location) AS stop_lat, -- Extract Latitude from PostGIS point
            ST_X(s.location) AS stop_lon, -- Extract Longitude from PostGIS point
            s.zone_id,
            s.stop_url,
            lt.name AS location_type_str,
            s.parent_station,
            s.stop_timezone,
            ws.name AS wheelchair_boarding_str,
            s.level_id,
            s.platform_code
        FROM stop s
        LEFT JOIN location_type lt ON s.location_type = lt.id
        LEFT JOIN wheelchair_status ws ON s.wheelchair_boarding = ws.id
        ORDER BY s.stop_id;
    """
    pg_data = pg_query_runner(pg_query, ())

    # Neo4J: Reconstruct the full stop record by traversing all relationships.
    neo4j_query = """
        MATCH (s:Stop)
        // Use OPTIONAL MATCH for all non-mandatory relationships
        OPTIONAL MATCH (s)-[:HAS_TYPE]->(lt:LocationType)
        OPTIONAL MATCH (s)-[:HAS_WHEELCHAIR_STATUS]->(ws:WheelchairStatus)
        OPTIONAL MATCH (s)-[:HAS_PARENT]->(ps:Stop)
        OPTIONAL MATCH (s)-[:IN_LEVEL]->(l:Level) // From previous level import
        OPTIONAL MATCH (s)-[:IN_ZONE]->(z:StopZone)
        RETURN
            s.id AS stop_id,
            s.code AS stop_code,
            s.name AS stop_name,
            s.tts_name AS tts_stop_name,
            s.desc AS stop_desc,
            s.latitude AS stop_lat,
            s.longitude AS stop_lon,
            z.id AS zone_id,
            s.url AS stop_url,
            lt.value AS location_type_str,
            ps.id AS parent_station,
            s.timezone AS stop_timezone,
            ws.value AS wheelchair_boarding_str,
            l.id AS level_id,
            s.platform_code AS platform_code // platform_code is assumed to be a direct property
        ORDER BY stop_id;
    """
    neo4j_data = neo4j_query_runner(neo4j_query, {})

    pg_count = len(pg_data)
    neo4j_count = len(neo4j_data)

    assert pg_count == neo4j_count, f"Count mismatch: PostgreSQL has {pg_count} stops, Neo4J has {neo4j_count}."

    # Convert both to a canonical, comparable format (list of tuples).
    def to_canonical_tuple(row):
        return (
            row.get('stop_id'),
            row.get('stop_code'),
            row.get('stop_name'),
            row.get('tts_stop_name'),
            row.get('stop_desc'),
            float(row.get('stop_lat')) if row.get('stop_lat') is not None else None,
            float(row.get('stop_lon')) if row.get('stop_lon') is not None else None,
            row.get('zone_id'),
            row.get('stop_url'),
            row.get('location_type_str'),
            row.get('parent_station'),
            row.get('stop_timezone'),
            row.get('wheelchair_boarding_str'),
            row.get('level_id'),
            row.get('platform_code'),
        )

    pg_tuples = [to_canonical_tuple(row) for row in pg_data]
    neo4j_tuples = [to_canonical_tuple(row) for row in neo4j_data]

    pg_tuples.sort()
    neo4j_tuples.sort()

    # Iterate and assert row-by-row
    # This avoids pytest.approx's issues with tuples inside lists.
    for i in range(len(pg_tuples)):
        assert pg_tuples[i] == pytest.approx(neo4j_tuples[i], abs=0.1), f"Mismatch at index {i}: {pg_tuples[i]} != {neo4j_tuples[i]}"
    
    print("Full data consistency check passed for stops.")
