
// Type constraints for fields.
CREATE CONSTRAINT location_group_id_type FOR (lg: LocationGroup) REQUIRE lg.id :: STRING;
CREATE CONSTRAINT location_group_name_type FOR (lg: LocationGroup) REQUIRE lg.name :: STRING;

// Primary key and not null constraints.
CREATE CONSTRAINT location_group_key FOR (lg: LocationGroup) REQUIRE lg.id IS NODE KEY;

// Create location group nodes.
LOAD CSV WITH HEADERS FROM "file:///GTFS/" + $dataset + "/location_groups.txt" as row
CALL {
    WITH row
    CREATE (lg: LocationGroup {
        id: row.location_group_id,
        name: row.location_group_name
    })
} IN TRANSACTIONS OF 1000 ROWS;

// Generate relationships between stop times and their location group.
LOAD CSV WITH HEADERS FROM "file:///GTFS/" + $dataset + "/stop_times.txt" AS row
CALL {
    WITH row
    MATCH (lg: LocationGroup { id: row.location_group_id })
    WITH row, lg
    WHERE row.stop_id IS NULL AND
          row.location_id IS NULL
    MATCH (st: StopTime { stop_sequence: row.stop_sequence })-[PART_OF]->(Trip { id: row.trip_id })
    CREATE (st)-[ilg: IN_LOCATION_GROUP]->(lg)
} IN TRANSACTIONS OF 1000 ROWS;
