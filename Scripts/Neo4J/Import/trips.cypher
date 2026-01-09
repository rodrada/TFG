
// Type constraints for fields.
CREATE CONSTRAINT trip_id_type FOR (t: Trip) REQUIRE t.id :: STRING;
CREATE CONSTRAINT trip_headsign_type FOR (t: Trip) REQUIRE t.headsign :: STRING;
CREATE CONSTRAINT trip_short_name_type FOR (t: Trip) REQUIRE t.short_name :: STRING;
CREATE CONSTRAINT trip_direction_id_type FOR (t: Trip) REQUIRE t.direction_id :: INTEGER;

// Primary key and not null constraints.
CREATE CONSTRAINT trip_key FOR (t: Trip) REQUIRE t.id IS NODE KEY;

// Constraints for associated nodes.
// WheelchairStatus constraints are already declared in stop import.
CREATE CONSTRAINT trip_block_id_type FOR (tb: TripBlock) REQUIRE tb.id :: STRING;
CREATE CONSTRAINT trip_block_id_key FOR (tb: TripBlock) REQUIRE tb.id IS NODE KEY;
CREATE CONSTRAINT bicycle_status_value_type FOR (bs: BicycleStatus) REQUIRE bs.value :: STRING;
CREATE CONSTRAINT bicycle_status_key FOR (bs: BicycleStatus) REQUIRE bs.value IS NODE KEY;
CREATE CONSTRAINT travel_direction_value_type FOR (td: TravelDirection) REQUIRE td.value :: STRING;
CREATE CONSTRAINT travel_direction_value_key FOR (td: TravelDirection) REQUIRE td.value IS NODE KEY;

// Load trips, which are instances of specific routes.
LOAD CSV WITH HEADERS FROM "file:///GTFS/" + $dataset + "/trips.txt" as row
CALL {
    WITH row
    MATCH (r: Route { id: row.route_id })
    MATCH (s: Service { id: row.service_id })
    WITH row, r, s
    WHERE (row.direction_id IS NULL OR row.direction_id IN ["0", "1"]) AND
          (row.wheelchair_accesible IS NULL OR row.wheelchair_accesible IN ["0", "1", "2"]) AND
          (row.bikes_allowed IS NULL OR row.bikes_allowed IN ["0", "1", "2"])
    CREATE (t: Trip {
        id: row.trip_id,
        headsign: row.trip_headsign,
        short_name: row.trip_short_name
    })
    MERGE (ws: WheelchairStatus { value: CASE row.wheelchair_accessible
                                         WHEN "1" THEN "Accessible"
                                         WHEN "2" THEN "Not Accessible"
                                         ELSE "Unknown" END })
    MERGE (bs: BicycleStatus { value: CASE row.bikes_allowed
                                      WHEN "1" THEN "Allowed"
                                      WHEN "2" THEN "Not Allowed"
                                      ELSE "Unknown" END })
    CREATE (t)-[fol: FOLLOWS]->(r)
    CREATE (t)-[sch: SCHEDULED_BY]->(s)
    CREATE (t)-[hws: HAS_WHEELCHAIR_STATUS]->(ws)
    CREATE (t)-[hbs: HAS_BICYCLE_STATUS]->(bs)
    WITH row, t
    CALL apoc.do.when(
        row.direction_id IS NOT NULL,
        'MERGE (td: TravelDirection { value: CASE row.direction_id
                                             WHEN "0" THEN "Outbound"
                                             ELSE "Inbound" END })
        CREATE (t)-[htd: HAS_TRAVEL_DIRECTION]->(td)
        RETURN row, t',
        'RETURN row, t',
        { row: row, t: t }
    ) YIELD value
    WITH value.row as row, value.t as t
    CALL apoc.do.when(
        row.block_id IS NOT NULL,
        'MERGE (tb: TripBlock { id: row.block_id })
        CREATE (t)-[itb: IN_TRIP_BLOCK]->(tb)
        RETURN row, t',
        'RETURN row, t',
        { row: row, t: t }
    ) YIELD value
    FINISH
} IN TRANSACTIONS OF 1000 ROWS;
