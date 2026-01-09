
import pytest
from datetime import time
from conftest import to_canonical_time_str

def test_booking_rule_data_consistency(pg_query_runner, neo4j_query_runner):
    """
    CROSS-VALIDATION: Fetches all booking_rule data and its direct relationships
    from both databases and asserts that they are identical.
    """
    print("\nPerforming full data consistency check for booking rules...")

    # PostgreSQL: Join with booking_type to get the string representation.
    pg_query = """
        SELECT
            br.*,
            bt.name AS booking_type_str
        FROM booking_rule br
        JOIN booking_type bt ON br.booking_type = bt.id
        ORDER BY br.booking_rule_id;
    """
    pg_data = pg_query_runner(pg_query, ())

    # Neo4J: Reconstruct the table by matching relationships to BookingType and Service.
    neo4j_query = """
        MATCH (br:BookingRule)-[:HAS_BOOKING_TYPE]->(bt:BookingType)
        OPTIONAL MATCH (br)-[:PRIOR_NOTICE_FOLLOWS]->(pns:Service)
        RETURN
            br,
            bt.value AS booking_type_str,
            pns.id AS prior_notice_service_id
        ORDER BY br.id;
    """
    neo4j_raw_data = neo4j_query_runner(neo4j_query, {})

    pg_count = len(pg_data)
    neo4j_count = len(neo4j_raw_data)

    assert pg_count == neo4j_count, f"Count mismatch: PostgreSQL has {pg_count} booking rules, Neo4J has {neo4j_count}."

    # Normalize Neo4J data into a single dictionary per row.
    neo4j_data = []
    for record in neo4j_raw_data:
        node_props = dict(record['br'])
        node_props['booking_type_str'] = record['booking_type_str']
        node_props['prior_notice_service_id'] = record['prior_notice_service_id']
        neo4j_data.append(node_props)

    def to_canonical_tuple(row):
        return (
            row.get('booking_rule_id') or row.get('id'),
            row.get('booking_type_str'),
            row.get('prior_notice_duration_min'),
            row.get('prior_notice_duration_max'),
            row.get('prior_notice_last_day'),
            to_canonical_time_str(row.get('prior_notice_last_time')),
            row.get('prior_notice_start_day'),
            to_canonical_time_str(row.get('prior_notice_start_time')),
            row.get('prior_notice_service_id'),
            row.get('message'),
            row.get('pickup_message'),
            row.get('drop_off_message'),
            row.get('phone_number'),
            row.get('info_url'),
            row.get('booking_url'),
        )

    pg_tuples = [to_canonical_tuple(row) for row in pg_data]
    neo4j_tuples = [to_canonical_tuple(row) for row in neo4j_data]

    pg_tuples.sort()
    neo4j_tuples.sort()

    assert pg_tuples[0:10000] == neo4j_tuples[0:10000], "Full data comparison failed for booking_rule. The datasets are not identical."
    print("Full data consistency check passed for booking rules.")


def test_stop_time_booking_rules_consistency(pg_query_runner, neo4j_query_runner):
    """
    CROSS-VALIDATION: Verifies that the links from stop_time to booking_rule
    are identical in both databases.
    """
    print("\nPerforming consistency check for stop_time to booking_rule links...")

    # PostgreSQL: Get all stop_time entries that have a booking rule.
    pg_query = """
        SELECT trip_id, stop_sequence, pickup_booking_rule_id, drop_off_booking_rule_id
        FROM stop_time
        WHERE pickup_booking_rule_id IS NOT NULL OR drop_off_booking_rule_id IS NOT NULL
        ORDER BY trip_id, stop_sequence;
    """
    pg_data = pg_query_runner(pg_query, ())

    # Neo4J: Reconstruct the same links via relationships.
    neo4j_query = """
        MATCH (st:StopTime)-[:PART_OF]->(t:Trip)
        OPTIONAL MATCH (st)-[:HAS_PICKUP_RULE]->(pbr:BookingRule)
        OPTIONAL MATCH (st)-[:HAS_DROP_OFF_RULE]->(dobr:BookingRule)
        WITH st, t, pbr, dobr
        WHERE pbr IS NOT NULL OR dobr IS NOT NULL
        RETURN
            t.id as trip_id,
            st.stop_sequence as stop_sequence,
            pbr.id as pickup_booking_rule_id,
            dobr.id as drop_off_booking_rule_id
        ORDER BY trip_id, stop_sequence;
    """
    neo4j_data = neo4j_query_runner(neo4j_query, {})

    pg_count = len(pg_data)
    neo4j_count = len(neo4j_data)

    assert pg_count == neo4j_count, f"Count mismatch for stop_time->booking_rule links: PostgreSQL has {pg_count}, Neo4J has {neo4j_count}."

    def to_canonical_tuple(row):
        return (
            row.get('trip_id'),
            row.get('stop_sequence'),
            row.get('pickup_booking_rule_id'),
            row.get('drop_off_booking_rule_id'),
        )

    pg_tuples = [to_canonical_tuple(row) for row in pg_data]
    neo4j_tuples = [to_canonical_tuple(row) for row in neo4j_data]

    pg_tuples.sort()
    neo4j_tuples.sort()

    assert pg_tuples[0:10000] == neo4j_tuples[0:10000], "Full data comparison failed for stop_time->booking_rule links."
    print("Full data consistency check passed for stop_time to booking_rule links.")
