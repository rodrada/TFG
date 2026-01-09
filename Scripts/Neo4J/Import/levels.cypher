
// Type constraints for fields.
CREATE CONSTRAINT level_id_type FOR (l: Level) REQUIRE l.id :: STRING;
CREATE CONSTRAINT level_index_type FOR (l: Level) REQUIRE l.index :: FLOAT;
CREATE CONSTRAINT level_name_type FOR (l: Level) REQUIRE l.name :: STRING;

// Primary key and not null constraints.
CREATE CONSTRAINT level_key FOR (l: Level) REQUIRE l.id IS NODE KEY;
CREATE CONSTRAINT level_index_not_null FOR (l: Level) REQUIRE l.index IS NOT NULL;

// Create level nodes.
LOAD CSV WITH HEADERS FROM "file:///GTFS/" + $dataset + "/levels.txt" as row
CALL {
    WITH row
    CREATE (l: Level {
        id: row.level_id,
        index: toFloat(row.level_index),
        name: row.level_name
    })
} IN TRANSACTIONS OF 1000 ROWS;

// Generate relationships between stops and their levels.
LOAD CSV WITH HEADERS FROM "file:///GTFS/" + $dataset + "/stops.txt" AS row
CALL {
    WITH row
    MATCH (s: Stop { id: row.stop_id })
    MATCH (l: Level { id: row.level_id })
    CREATE (s)-[il: IN_LEVEL]->(l)
} IN TRANSACTIONS OF 1000 ROWS;
