
// Type constraints for fields.
CREATE CONSTRAINT booking_rule_id_type FOR (br: BookingRule) REQUIRE br.id :: STRING;
CREATE CONSTRAINT booking_rule_pndmi_type FOR (br: BookingRule) REQUIRE br.prior_notice_duration_min :: INTEGER;
CREATE CONSTRAINT booking_rule_pndma_type FOR (br: BookingRule) REQUIRE br.prior_notice_duration_max :: INTEGER;
CREATE CONSTRAINT booking_rule_pnld_type FOR (br: BookingRule) REQUIRE br.prior_notice_last_day :: INTEGER;
CREATE CONSTRAINT booking_rule_pnlt_type FOR (br: BookingRule) REQUIRE br.prior_notice_last_time :: LOCAL TIME;
CREATE CONSTRAINT booking_rule_pnsd_type FOR (br: BookingRule) REQUIRE br.prior_notice_start_day :: INTEGER;
CREATE CONSTRAINT booking_rule_pnst_type FOR (br: BookingRule) REQUIRE br.prior_notice_start_time :: LOCAL TIME;
CREATE CONSTRAINT booking_rule_message_type FOR (br: BookingRule) REQUIRE br.message :: STRING;
CREATE CONSTRAINT booking_rule_pickup_message_type FOR (br: BookingRule) REQUIRE br.pickup_message :: STRING;
CREATE CONSTRAINT booking_rule_drop_off_message_type FOR (br: BookingRule) REQUIRE br.drop_off_message :: STRING;
CREATE CONSTRAINT booking_rule_phone_number_type FOR (br: BookingRule) REQUIRE br.phone_number :: STRING;
CREATE CONSTRAINT booking_rule_info_url_type FOR (br: BookingRule) REQUIRE br.info_url :: STRING;
CREATE CONSTRAINT booking_rule_booking_url_type FOR (br: BookingRule) REQUIRE br.booking_url :: STRING;

// Value constraints for fields.
CALL apoc.trigger.add('validate_booking_rule_fields','
    UNWIND $createdNodes AS node
    MATCH (node)
    WHERE node: BookingRule
    CALL apoc.util.validate(
        (node.prior_notice_last_time IS NULL) <> (node.prior_notice_last_day IS NULL),
        "Booking rule prior notice last time and last day have to be both null or not null", []
    )
    CALL apoc.util.validate(
        (node.prior_notice_start_time IS NULL) <> (node.prior_notice_start_day IS NULL),
        "Booking rule prior notice start time and start day have to be both null or not null", []
    )
    CALL apoc.util.validate(
        node.info_url IS NOT NULL AND NOT node.info_url =~ "^https?://.*$",
        "Booking rule info URL has the wrong format: %s", [node.info_url]
    )
    CALL apoc.util.validate(
        node.booking_url IS NOT NULL AND NOT node.booking_url =~ "^https?://.*$",
        "Booking rule booking URL has the wrong format: %s", [node.booking_url]
    )
    RETURN node',
    { phase: 'before' }
) YIELD name
FINISH;

// Primary key and not null constraints.
CREATE CONSTRAINT booking_rule_key FOR (br: BookingRule) REQUIRE br.id IS NODE KEY;

// Constraints for associated enums.
CREATE CONSTRAINT booking_type_value_type FOR (bt: BookingType) REQUIRE bt.value :: STRING;
CREATE CONSTRAINT booking_type_key FOR (bt: BookingType) REQUIRE bt.value IS NODE KEY;

// Load data.
LOAD CSV WITH HEADERS FROM "file:///GTFS/" + $dataset + "/booking_rules.txt" as row
CALL {
    WITH row
    OPTIONAL MATCH (pns: Service { id: row.prior_notice_service_id })
    WITH row, pns
    WHERE ((row.prior_notice_duration_min IS NOT NULL) = (row.booking_type = 1)) AND
          (row.prior_notice_duration_max IS NULL OR (row.booking_type = 1)) AND
          ((row.prior_notice_last_day IS NOT NULL) = (row.booking_type = 2)) AND
          (row.prior_notice_start_day IS NULL OR row.booking_type = 2 OR (row.booking_type = 1 AND row.prior_notice_duration_max IS NULL)) AND
          (row.prior_notice_service_id IS NULL OR (pns IS NOT NULL AND row.booking_type = 2)) AND
          row.booking_type IN ["0", "1", "2"]
    CREATE (br: BookingRule {
        id: row.booking_rule_id,
        prior_notice_duration_min: toInteger(row.prior_notice_duration_min),       // Minimum of minutes for booking before the trip starts
        prior_notice_duration_max: toInteger(row.prior_notice_duration_max),
        prior_notice_last_day: toInteger(row.prior_notice_last_day),
        prior_notice_last_time: localtime(row.prior_notice_last_time),
        prior_notice_start_day: toInteger(row.prior_notice_start_day),
        prior_notice_start_time: localtime(row.prior_notice_start_time),
        message: row.message,
        pickup_message: row.pickup_message,
        drop_off_message: row.drop_off_message,
        phone_number: row.phone_number,
        info_url: row.info_url,
        booking_url: row.booking_url
    })
    MERGE (bt: BookingType { value: CASE row.booking_type
                                    WHEN "0" THEN "Real Time"
                                    WHEN "1" THEN "Up To Same-Day"
                                    ELSE "Up To Prior Days" END })       // Value is guaranteed to be 2
    CREATE (br)-[hbt: HAS_BOOKING_TYPE]->(bt)
    CREATE (br)-[pnf: PRIOR_NOTICE_FOLLOWS]->(pns)
} IN TRANSACTIONS OF 1000 ROWS;


// Link stop times to their respective rules.
LOAD CSV WITH HEADERS FROM "file:///GTFS/" + $dataset + "/stop_times.txt" AS row
CALL {
    WITH row
    MATCH (st: StopTime { stop_sequence: row.stop_sequence })-[PART_OF]->(t: Trip { id: row.trip_id })
    OPTIONAL MATCH (pbr: BookingRule { id: row.pickup_booking_rule_id })
    OPTIONAL MATCH (dobr: BookingRule { id: row.drop_off_booking_rule_id })
    CREATE (st)-[hpr: HAS_PICKUP_RULE]->(pbr)
    CREATE (st)-[hdor: HAS_DROP_OFF_RULE]->(dobr)
} IN TRANSACTIONS OF 1000 ROWS;
