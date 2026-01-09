
// Type constraints for fields.
CREATE CONSTRAINT stop_id_type FOR (s: Stop) REQUIRE s.id :: STRING;
CREATE CONSTRAINT stop_code_type FOR (s: Stop) REQUIRE s.code :: STRING;
CREATE CONSTRAINT stop_name_type FOR (s: Stop) REQUIRE s.name :: STRING;
CREATE CONSTRAINT stop_tts_name_type FOR (s: Stop) REQUIRE s.tts_name :: STRING;
CREATE CONSTRAINT stop_desc_type FOR (s: Stop) REQUIRE s.desc :: STRING;
CREATE CONSTRAINT stop_latitude_type FOR (s: Stop) REQUIRE s.latitude :: FLOAT;
CREATE CONSTRAINT stop_longitude_type FOR (s: Stop) REQUIRE s.longitude :: FLOAT;
CREATE CONSTRAINT stop_url_type FOR (s: Stop) REQUIRE s.url :: STRING;
CREATE CONSTRAINT stop_timezone_type FOR (s: Stop) REQUIRE s.timezone :: STRING;

// Value constraints for fields.
CALL apoc.trigger.add('validate_stop_fields','
    UNWIND $createdNodes AS node
    MATCH (node)
    WHERE node: Stop
    CALL apoc.util.validate(
        node.latitude IS NOT NULL AND (node.latitude < -90.0 OR node.latitude > 90.0),
        "Stop latitude must be between -90.0 and 90.0: %f", [node.latitude]
    )
    CALL apoc.util.validate(
        node.longitude IS NOT NULL AND (node.longitude < -180.0 OR node.longitude > 180.0),
        "Stop longitude must be between -180.0 and 180.0: %f", [node.longitude]
    )
    CALL apoc.util.validate(
        node.url IS NOT NULL AND NOT node.url =~ "^https?://.*$",
        "Stop URL has the wrong format: %s", [node.url]
    )
    CALL apoc.util.validate(
        node.timezone IS NOT NULL AND NOT node.timezone =~ "^[A-Z][a-z]+/[A-Z][a-z]+(_[A-Z][a-z]+)*$",
        "Stop timezone has the wrong format: %s", [node.timezone]
    )
    RETURN node',
    { phase: 'before' }
) YIELD name
FINISH;

// Primary key and not null constraints.
CREATE CONSTRAINT stop_key FOR (s: Stop) REQUIRE s.id IS NODE KEY;

// Constraints for associated nodes.
CREATE CONSTRAINT location_type_value_type FOR (lt: LocationType) REQUIRE lt.value :: STRING;
CREATE CONSTRAINT location_type_key FOR (lt: LocationType) REQUIRE lt.value IS NODE KEY;
CREATE CONSTRAINT wheelchair_status_value_type FOR (ws: WheelchairStatus) REQUIRE ws.value :: STRING;
CREATE CONSTRAINT wheelchair_status_key FOR (ws: WheelchairStatus) REQUIRE ws.value IS NODE KEY;
CREATE CONSTRAINT stop_zone_id_type FOR (z: StopZone) REQUIRE z.id :: STRING;
CREATE CONSTRAINT stop_zone_id_key FOR (z: StopZone) REQUIRE z.id IS NODE KEY;

CALL spatial.addPointLayer("stops") YIELD node
FINISH;

// Generate one node for each stop in the graph.
LOAD CSV WITH HEADERS FROM "file:///GTFS/" + $dataset + "/stops.txt" AS row
CALL {
    WITH row
    WITH row    // Required to apply WHERE clause due to Neo4j syntax limitations
    WHERE (row.location_type IS NULL OR row.location_type IN ["0", "1", "2", "3", "4"]) AND
          (row.wheelchair_boarding IS NULL OR row.wheelchair_boarding IN ["0", "1", "2"]) AND
          (row.location_type IN ["3", "4"] OR (row.stop_name IS NOT NULL AND row.stop_lat IS NOT NULL AND row.stop_lon IS NOT NULL)) AND
          (row.location_type IS NULL OR ((NOT row.location_type IN ["1"] OR row.parent_station IS NULL) AND
                                         (NOT row.location_type IN ["2", "3", "4"] OR row.parent_station IS NOT NULL)))
    CREATE (s: Stop {
        id: row.stop_id,
        code: row.stop_code,
        name: row.stop_name,
        tts_name: row.tts_stop_name,
        desc: row.stop_desc,
        latitude: toFloat(row.stop_lat),
        longitude: toFloat(row.stop_lon),
        url: row.stop_url,
        timezone: row.stop_timezone,
        platform_code: row.platform_code
    })
    WITH row, s
    CALL spatial.addNode("stops", s) YIELD node     // Index it for spatial queries.
    WITH row, s
    MERGE (t: LocationType { value: CASE row.location_type
                                    WHEN "4" THEN "Boarding Area"
                                    WHEN "3" THEN "Generic Node"
                                    WHEN "2" THEN "Entrance/Exit"
                                    WHEN "1" THEN "Station"
                                    ELSE "Stop/Platform" END })
    MERGE (ws: WheelchairStatus { value: CASE row.wheelchair_boarding
                                         WHEN "1" THEN "Accessible"
                                         WHEN "2" THEN "Not Accessible"
                                         ELSE "Unknown" END })
    CREATE (s)-[ht: HAS_TYPE]->(t)
    CREATE (s)-[hws: HAS_WHEELCHAIR_STATUS]->(ws)
    WITH row, s
    CALL apoc.do.when(
        row.zone_id IS NOT NULL,
        "MERGE (z: StopZone { id: row.zone_id })
        CREATE (s)-[iz: IN_ZONE]->(z)
        RETURN row, s",
        "RETURN row, s",
        { row: row, s: s }
    ) YIELD value
    FINISH
} IN TRANSACTIONS OF 1000 ROWS;


// Generate relationships between parent and child stations.
// NOTE: This has to be done AFTER importing all stops to prevent parent stop matches from possibly not happening.
LOAD CSV WITH HEADERS FROM "file:///GTFS/" + $dataset + "/stops.txt" AS row
CALL {
    WITH row
    MATCH (p: Stop { id: row.parent_station })
    MATCH (s: Stop { id: row.stop_id })
    CREATE (s)-[hp: HAS_PARENT]->(p)
} IN TRANSACTIONS OF 1000 ROWS;
