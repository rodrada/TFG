
// Type constraints for fields.
CREATE CONSTRAINT frequency_start_time_type FOR (f: Frequency) REQUIRE f.start_time :: DURATION;
CREATE CONSTRAINT frequency_end_time_type FOR (f: Frequency) REQUIRE f.end_time :: DURATION;
CREATE CONSTRAINT frequency_headway_secs_type FOR (f: Frequency) REQUIRE f.headway_secs :: INTEGER;

// Value constraints for fields.
CALL apoc.trigger.add('validate_frequency_fields','
    UNWIND $createdNodes AS node
    MATCH (node)
    WHERE node: Frequency
    CALL apoc.util.validate(
        node.headway_secs IS NOT NULL AND node.headway_secs <= 0,
        "Frequency headway seconds must be positive: %d", [node.headway_secs]
    )
    RETURN node',
    { phase: 'before' }
) YIELD name
FINISH;

// Constraints for associated enums.
CREATE CONSTRAINT service_type_value_type FOR (st: ServiceType) REQUIRE st.value :: STRING;
CREATE CONSTRAINT service_type_key FOR (st: ServiceType) REQUIRE st.value IS NODE KEY;

// Generate frequency nodes.
LOAD CSV WITH HEADERS FROM "file:///GTFS/" + $dataset + "/frequencies.txt" as row
CALL {
    WITH row
    MATCH (t: Trip { id: row.trip_id })
    WITH row, t, split(row.start_time, ":") AS start_time_parts, split(row.end_time, ":") AS end_time_parts
    WITH row, t, toInteger(start_time_parts[0]) * 3600 + toInteger(start_time_parts[1]) * 60 + toInteger(start_time_parts[2]) AS start_time,
                 toInteger(end_time_parts[0]) * 3600 + toInteger(end_time_parts[1]) * 60 + toInteger(end_time_parts[2]) AS end_time
    WHERE row.exact_times IS NULL OR row.exact_times IN ["0", "1"]
    CREATE (f: Frequency {
        start_time: duration({seconds: start_time}),
        end_time: duration({seconds: end_time}),
        headway_secs: toInteger(row.headway_secs)
    })
    MERGE (st: ServiceType { value: CASE row.exact_times
                                    WHEN "1" THEN "Schedule Based"
                                    ELSE "Frequency Based" END })      // Value is either NULL or 0.
    CREATE (f)-[hst: HAS_SERVICE_TYPE]->(st)             
    CREATE (t)-[hf: HAS_FREQUENCY]->(f)
} IN TRANSACTIONS OF 1000 ROWS;
