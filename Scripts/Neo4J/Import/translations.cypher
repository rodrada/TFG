
// Type constraints for fields.
CREATE CONSTRAINT translation_field_name_type FOR (t: Translation) REQUIRE t.field_name :: STRING;
CREATE CONSTRAINT translation_language_type FOR (t: Translation) REQUIRE t.language :: STRING;
CREATE CONSTRAINT translation_translation_type FOR (t: Translation) REQUIRE t.translation :: STRING;

// Constraints for associated enums.
CREATE CONSTRAINT translation_field_name_not_null FOR (t: Translation) REQUIRE t.field_name IS NOT NULL;
CREATE CONSTRAINT translation_language_not_null FOR (t: Translation) REQUIRE t.language IS NOT NULL;
CREATE CONSTRAINT translation_translation_not_null FOR (t: Translation) REQUIRE t.translation IS NOT NULL;

// Import "regular" translations.
LOAD CSV WITH HEADERS FROM "file:///GTFS/" + $dataset + "/translations.txt" AS row
CALL {
    WITH row
    WITH row,
         CASE row.table_name     // Dynamically map GTFS table names to node labels
             WHEN "agency" THEN { label: "Agency" }
             WHEN "stops" THEN { label: "Stop" }
             WHEN "routes" THEN { label: "Route" }
             WHEN "trips" THEN { label: "Trip" }
             WHEN "pathways" THEN { label: "Pathway" }
             WHEN "levels" THEN { label: "Level" }
             WHEN "attributions" THEN { label: "Attribution" }
             ELSE NULL
         END AS mapping
    WHERE mapping IS NOT NULL
    CALL apoc.cypher.run(
        'MATCH (e: ' + mapping.label + ') RETURN e',
        { }
    ) YIELD value
    WITH row, value.e as e
    WHERE (e.id = row.record_id OR (row.field_value IS NOT NULL AND e[row.field_name] = row.field_value))
    MERGE (e)-[ht: HAS_TRANSLATION]->(t: Translation { field_name: row.field_name, language: row.language, translation: row.translation })
} IN TRANSACTIONS OF 1000 ROWS;

// Special case for stop_times.
LOAD CSV WITH HEADERS FROM "file:///GTFS/" + $dataset + "/translations.txt" AS row
CALL {
    WITH row
    WITH row    // Required to apply WHERE clause due to Neo4j syntax limitations
    WHERE row.table_name = "stop_times"
    MATCH (st: StopTime)
    WHERE (EXISTS { (st)-[po: PART_OF]->(t: Trip { id: row.record_id }) } AND st.stop_sequence = row.record_sub_id) OR
          (row.field_value IS NOT NULL AND st[row.field_name] = row.field_value)
    MERGE (st)-[ht: HAS_TRANSLATION]->(t: Translation { field_name: row.field_name, language: row.language, translation: row.translation })
} IN TRANSACTIONS OF 1000 ROWS;

// Special case for feed_info.
LOAD CSV WITH HEADERS FROM "file:///GTFS/" + $dataset + "/translations.txt" AS row
CALL {
    WITH row
    WITH row    // Required to apply WHERE clause due to Neo4j syntax limitations
    WHERE row.table_name = "feed_info"
    MATCH (f: Feed)
    MERGE (f)-[ht: HAS_TRANSLATION]->(t: Translation { field_name: row.field_name, language: row.language, translation: row.translation })
} IN TRANSACTIONS OF 1000 ROWS;

// Index for faster queries.
CREATE INDEX translation_entity_lookup FOR (t: Translation) ON (t.field_name, t.language);
