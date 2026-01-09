
// NOTE: It may seem like constraints should be created after import but, according to my tests,
//       the difference in performance is negligible.

// Type constraints for fields.
CREATE CONSTRAINT stop_time_arrival_time_type FOR (st: StopTime) REQUIRE st.arrival_time :: DURATION;
CREATE CONSTRAINT stop_time_departure_time_type FOR (st: StopTime) REQUIRE st.departure_time :: DURATION;
CREATE CONSTRAINT stop_time_stop_sequence_type FOR (st: StopTime) REQUIRE st.stop_sequence :: INTEGER;
CREATE CONSTRAINT stop_time_stop_headsign_type FOR (st: StopTime) REQUIRE st.stop_headsign :: STRING;
CREATE CONSTRAINT stop_time_start_pickup_drop_off_window_type FOR (st: StopTime) REQUIRE st.start_pickup_drop_off_window :: DURATION;
CREATE CONSTRAINT stop_time_end_pickup_drop_off_window_type FOR (st: StopTime) REQUIRE st.end_pickup_drop_off_window :: DURATION;
CREATE CONSTRAINT stop_time_shape_dist_traveled_type FOR (st: StopTime) REQUIRE st.shape_dist_traveled :: FLOAT;

// Value constraints for fields.
CALL apoc.trigger.add('validate_stop_time_fields','
    UNWIND $createdNodes AS node
    MATCH (node)
    WHERE node: StopTime
    CALL apoc.util.validate(
        node.stop_sequence < 0,
        "Stop time stop sequence must be non-negative: %d", [node.stop_sequence]
    )
    CALL apoc.util.validate(
        node.shape_dist_traveled IS NOT NULL AND node.shape_dist_traveled < 0,
        "Stop time shape distance traveled must be non-negative: %f", [node.shape_dist_traveled]
    )
    RETURN node',
    { phase: 'before' }
) YIELD name
FINISH;

// Primary key and not null constraints.
CREATE CONSTRAINT stop_time_stop_sequence_not_null FOR (st: StopTime) REQUIRE st.stop_sequence IS NOT NULL;

// Constraints for associated enums.
CREATE CONSTRAINT stop_method_value_type FOR (sm: StopMethod) REQUIRE sm.value :: STRING;
CREATE CONSTRAINT stop_method_value_key FOR (sm: StopMethod) REQUIRE sm.value IS NODE KEY;
CREATE CONSTRAINT timepoint_value_type FOR (tp: Timepoint) REQUIRE tp.value :: STRING;
CREATE CONSTRAINT timepoint_value_key FOR (tp: Timepoint) REQUIRE tp.value IS NODE KEY;

// Add trip times for each stop
LOAD CSV WITH HEADERS FROM "file:///GTFS/" + $dataset + "/stop_times.txt" AS row
CALL {           // Import progressively to prevent out-of-memory errors.
    WITH row
    MATCH (t: Trip { id: row.trip_id })
    // NOTE: This is mandatory unless location_id or location_group_id is defined.
    //       Since GeoJSON is not supported, we only check for location_group_id.
    OPTIONAL MATCH (s: Stop { id: row.stop_id })
    WITH row, t, s
    WHERE (s IS NOT NULL OR row.location_group_id IS NOT NULL) AND
          // NOTE: According to the GTFS standard, arrival time is required
          //       for the first and last stops in a trip, but it is also forbidden
          //       if pickup/drop_off windows are defined, so we impose no restrictions.
          (row.timepoint <> "1" OR (row.arrival_time IS NOT NULL AND row.departure_time IS NOT NULL)) AND
          ((row.start_pickup_drop_off_window IS NULL AND row.end_pickup_drop_off_window IS NULL) OR
           (row.arrival_time IS NULL AND row.departure_time IS NULL)) AND
          ((s IS NOT NULL) = (row.location_group_id IS NULL AND row.location_id IS NULL)) AND
          ((row.location_group_id IS NULL AND row.location_id IS NULL) OR row.start_pickup_drop_off_window IS NOT NULL) AND
          ((row.start_pickup_drop_off_window IS NULL) = (row.end_pickup_drop_off_window IS NULL)) AND
          ((row.arrival_time IS NULL AND row.departure_time IS NULL) OR row.start_pickup_drop_off_window IS NULL) AND
          (row.pickup_type IS NULL OR (row.start_pickup_drop_off_window IS NULL AND row.pickup_type IN ["0", "1", "2", "3"]) OR row.pickup_type IN ["1", "2"]) AND
          (row.drop_off_type IS NULL OR (row.start_pickup_drop_off_window IS NULL AND row.drop_off_type IN ["0", "1", "2", "3"]) OR row.drop_off_type IN ["0", "1", "2"]) AND
          (row.continuous_pickup IS NULL OR (row.start_pickup_drop_off_window IS NULL and row.continuous_pickup IN ["0", "1", "2", "3"])) AND
          (row.continuous_drop_off IS NULL OR (row.start_pickup_drop_off_window IS NULL and row.continuous_drop_off IN ["0", "1", "2", "3"])) AND
          (row.timepoint IS NULL OR row.timepoint IN ["0", "1"])
    // Split time strings into hours, minutes and seconds.
    WITH row, t, s, CASE WHEN row.arrival_time IS NULL THEN NULL ELSE split(row.arrival_time, ":") END AS atp,
                    CASE WHEN row.departure_time IS NULL THEN NULL ELSE split(row.departure_time, ":") END AS dtp,
                    CASE WHEN row.start_pickup_drop_off_window IS NULL THEN NULL ELSE split(row.start_pickup_drop_off_window, ":") END AS spdow_parts,
                    CASE WHEN row.end_pickup_drop_off_window IS NULL THEN NULL ELSE split(row.end_pickup_drop_off_window, ":") END AS epdow_parts
    // Convert time strings into seconds.
    WITH row, t, s, CASE WHEN atp IS NULL THEN NULL ELSE toInteger(atp[0]) * 3600 + toInteger(atp[1]) * 60 + toInteger(atp[2]) END AS at,
                    CASE WHEN dtp IS NULL THEN NULL ELSE toInteger(dtp[0]) * 3600 + toInteger(dtp[1]) * 60 + toInteger(dtp[2]) END AS dt,
                    CASE WHEN spdow_parts IS NULL THEN NULL ELSE toInteger(spdow_parts[0]) * 3600 + toInteger(spdow_parts[1]) * 60 + toInteger(spdow_parts[2]) END AS spdow,
                    CASE WHEN epdow_parts IS NULL THEN NULL ELSE toInteger(epdow_parts[0]) * 3600 + toInteger(epdow_parts[1]) * 60 + toInteger(epdow_parts[2]) END AS epdow
    CREATE (st: StopTime {
        arrival_time: CASE WHEN at IS NULL THEN NULL ELSE duration({seconds: at}) END,
        departure_time: CASE WHEN dt IS NULL THEN NULL ELSE duration({seconds: dt}) END,
        stop_sequence: toInteger(row.stop_sequence),
        stop_headsign: row.stop_headsign,
        start_pickup_drop_off_window: CASE WHEN spdow IS NULL THEN NULL ELSE duration({seconds: spdow}) END,
        end_pickup_drop_off_window: CASE WHEN epdow IS NULL THEN NULL ELSE duration({seconds: epdow}) END,
        shape_dist_traveled: toFloat(row.shape_dist_traveled),
        timepoint: row.timepoint,
        location_id: row.location_id    // Save this for consistency although GeoJSON is not supported.
    })
    MERGE (pt: StopMethod { value: CASE row.pickup_type
                                   WHEN "3" THEN "Must Coordinate With Driver"
                                   WHEN "2" THEN "Must Phone Agency"
                                   WHEN "1" THEN "Not Available"
                                   ELSE "Scheduled" END })
    MERGE (dot: StopMethod { value: CASE row.drop_off_type
                                    WHEN "3" THEN "Must Coordinate With Driver"
                                    WHEN "2" THEN "Must Phone Agency"
                                    WHEN "1" THEN "Not Available"
                                    ELSE "Scheduled" END })
    MERGE (cp: ContinuousStatus { value: CASE row.continuous_pickup
                                         WHEN "0" THEN "Continuous"
                                         WHEN "2" THEN "Must Phone Agency"
                                         WHEN "3" THEN "Must Coordinate With Driver"
                                         ELSE "Not Continuous" END })
    MERGE (cdo: ContinuousStatus { value: CASE row.continuous_drop_off
                                          WHEN "0" THEN "Continuous"
                                          WHEN "2" THEN "Must Phone Agency"
                                          WHEN "3" THEN "Must Coordinate With Driver"
                                          ELSE "Not Continuous" END })
    MERGE (tp: Timepoint { value: CASE row.timepoint
                                  WHEN "0" THEN "Approximate"
                                  ELSE "Exact" END })
    CREATE (st)-[p: PART_OF]->(t)
    CREATE (st)-[l: LOCATED_AT]->(s)
    CREATE (st)-[hpt: HAS_PICKUP_TYPE]->(pt)
    CREATE (st)-[hdt: HAS_DROP_OFF_TYPE]->(dot)
    CREATE (st)-[hcp: HAS_CONTINUOUS_PICKUP]->(cp)
    CREATE (st)-[hcd: HAS_CONTINUOUS_DROP_OFF]->(cdo)
    CREATE (st)-[htp: HAS_TIMEPOINT]->(tp)
} IN TRANSACTIONS OF 1000 ROWS;

CREATE INDEX idx_stop_time_stop_sequence FOR (st: StopTime) ON (st.stop_sequence);
