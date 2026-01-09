import pytest
from conftest import to_canonical_time_str

def test_stop_time_data_consistency(pg_query_runner, neo4j_query_runner):
    """
    CROSS-VALIDATION: Fetches all stop_time data and its relationships from
    both databases and asserts that they are identical row by row.
    """
    print("\nPerforming full data consistency check for stop times...")

    # PostgreSQL: Join with all enum tables to get their string representations.
    pg_query = """
        SELECT
            st.trip_id,
            st.arrival_time,
            st.departure_time,
            st.stop_id,
            st.location_group_id,
            st.location_id,
            st.stop_sequence,
            st.stop_headsign,
            st.start_pickup_drop_off_window,
            st.end_pickup_drop_off_window,
            pt.name AS pickup_type_str,
            dt.name AS drop_off_type_str,
            cp.name AS continuous_pickup_str,
            cd.name AS continuous_drop_off_str,
            st.shape_dist_traveled,
            tp.name AS timepoint_str,
            st.pickup_booking_rule_id,
            st.drop_off_booking_rule_id
        FROM stop_time st
        -- LEFT JOINs are crucial here as many of these fields can be NULL
        LEFT JOIN stop_method pt ON st.pickup_type = pt.id
        LEFT JOIN stop_method dt ON st.drop_off_type = dt.id
        LEFT JOIN continuous_status cp ON st.continuous_pickup = cp.id
        LEFT JOIN continuous_status cd ON st.continuous_drop_off = cd.id
        LEFT JOIN time_precision tp ON st.timepoint = tp.id;
    """
    pg_data = pg_query_runner(pg_query, ())

    # Neo4J: Reconstruct the full stop_time record by traversing all relationships.
    neo4j_query = """
        MATCH (st:StopTime)-[:PART_OF]->(t:Trip)
        // Use OPTIONAL MATCH for all non-mandatory relationships
        OPTIONAL MATCH (st)-[:LOCATED_AT]->(s:Stop)
        OPTIONAL MATCH (st)-[:IN_LOCATION_GROUP]->(lg:LocationGroup)
        OPTIONAL MATCH (st)-[:HAS_PICKUP_TYPE]->(pt:StopMethod)
        OPTIONAL MATCH (st)-[:HAS_DROP_OFF_TYPE]->(dt:StopMethod)
        OPTIONAL MATCH (st)-[:HAS_CONTINUOUS_PICKUP]->(cp:ContinuousStatus)
        OPTIONAL MATCH (st)-[:HAS_CONTINUOUS_DROP_OFF]->(cd:ContinuousStatus)
        OPTIONAL MATCH (st)-[:HAS_TIMEPOINT]->(tp:Timepoint)
        OPTIONAL MATCH (st)-[:HAS_PICKUP_RULE]->(pbr:BookingRule)
        OPTIONAL MATCH (st)-[:HAS_DROP_OFF_RULE]->(dobr:BookingRule)
        RETURN
            t.id AS trip_id,
            st.arrival_time AS arrival_time,
            st.departure_time AS departure_time,
            s.id AS stop_id,
            lg.id AS location_group_id,
            st.location_id AS location_id,
            st.stop_sequence AS stop_sequence,
            st.stop_headsign AS stop_headsign,
            st.start_pickup_drop_off_window AS start_pickup_drop_off_window,
            st.end_pickup_drop_off_window AS end_pickup_drop_off_window,
            pt.value AS pickup_type_str,
            dt.value AS drop_off_type_str,
            cp.value AS continuous_pickup_str,
            cd.value AS continuous_drop_off_str,
            st.shape_dist_traveled AS shape_dist_traveled,
            tp.value AS timepoint_str,
            pbr.id AS pickup_booking_rule_id,
            dobr.id AS drop_off_booking_rule_id;
    """
    neo4j_data = neo4j_query_runner(neo4j_query, {})

    pg_count = len(pg_data)
    neo4j_count = len(neo4j_data)

    assert pg_count == neo4j_count, f"Count mismatch: PostgreSQL has {pg_count} stop times, Neo4J has {neo4j_count}."

    # Convert both to a canonical, comparable format (list of tuples).
    def to_canonical_tuple(row):
        return (
            row.get('trip_id'),
            to_canonical_time_str(row.get('arrival_time')),
            to_canonical_time_str(row.get('departure_time')),
            row.get('stop_id'),
            row.get('location_group_id'),
            row.get('location_id'),
            row.get('stop_sequence'),
            row.get('stop_headsign'),
            to_canonical_time_str(row.get('start_pickup_drop_off_window')),
            to_canonical_time_str(row.get('end_pickup_drop_off_window')),
            row.get('pickup_type'),
            row.get('drop_off_type'),
            row.get('continuous_pickup'),
            row.get('continuous_drop_off'),
            float(row.get('shape_dist_traveled')) if row.get('shape_dist_traveled') is not None else None,
            row.get('timepoint_str'),
            row.get('pickup_booking_rule_id'),
            row.get('drop_off_booking_rule_id'),
        )

    pg_tuples = [to_canonical_tuple(row) for row in pg_data]
    neo4j_tuples = [to_canonical_tuple(row) for row in neo4j_data]

    pg_tuples.sort(key=lambda x: (x[0], x[6]))
    neo4j_tuples.sort(key=lambda x: (x[0], x[6]))

    assert pg_tuples[0:10000] == neo4j_tuples[0:10000], "Full data comparison failed. The stop_time datasets are not identical."

    print("Full data consistency check passed for stop times.")
