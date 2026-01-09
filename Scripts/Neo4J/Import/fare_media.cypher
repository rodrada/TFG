
// Type constraints for fields.
CREATE CONSTRAINT fare_media_id_type FOR (fm: FareMedia) REQUIRE fm.id :: STRING;
CREATE CONSTRAINT fare_media_name_type FOR (fm: FareMedia) REQUIRE fm.name :: STRING;

// Primary key and not null constraints.
CREATE CONSTRAINT fare_media_key FOR (fm: FareMedia) REQUIRE fm.id IS NODE KEY;

// Constraints for associated enums.
CREATE CONSTRAINT fare_media_type_value_type FOR (fmt: FareMediaType) REQUIRE fmt.value :: STRING;
CREATE CONSTRAINT fare_media_type_key FOR (fmt: FareMediaType) REQUIRE fmt.value IS NODE KEY;

LOAD CSV WITH HEADERS FROM "file:///GTFS/" + $dataset + "/fare_media.txt" as row
CALL {
    WITH row
    WITH row    // Required for the WHERE clause due to Neo4j syntax limitations
    WHERE row.fare_media_type IN ["0", "1", "2", "3", "4"]
    CREATE (fm: FareMedia {
        id: row.fare_media_id,
        name: row.fare_media_name
    })
    MERGE (fmt: FareMediaType { value: CASE row.fare_media_type
                                       WHEN "0" THEN "None"
                                       WHEN "1" THEN "Physical Ticket"
                                       WHEN "2" THEN "Physical Transit Card"
                                       WHEN "3" THEN "Contactless EMV"
                                       ELSE "Mobile App" END })             // Value is guaranteed to be 4
    CREATE (fm)-[ht: HAS_TYPE]->(fmt)
} IN TRANSACTIONS OF 1000 ROWS;
