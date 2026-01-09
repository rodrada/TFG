
// Type constraints for fields.
CREATE CONSTRAINT area_id_type FOR (a: Area) REQUIRE a.id :: STRING;
CREATE CONSTRAINT area_name_type FOR (a: Area) REQUIRE a.name :: STRING;

// Primary key and not null constraints.
CREATE CONSTRAINT area_key FOR (a: Area) REQUIRE a.id IS NODE KEY;

// Create area nodes.
LOAD CSV WITH HEADERS FROM "file:///GTFS/" + $dataset + "/areas.txt" as row
CALL {
    WITH row
    CREATE (a: Area {
        id: row.area_id,
        name: row.area_name
    })
} IN TRANSACTIONS OF 1000 ROWS;
