
// Generate relationships between stops and their location groups.
LOAD CSV WITH HEADERS FROM "file:///GTFS/" + $dataset + "/location_group_stops.txt" as row
CALL {
    WITH row
    MATCH (lg: LocationGroup { id: row.location_group_id })
    MATCH (s: Stop { id: row.stop_id })
    CREATE (s)-[ig: IN_LOCATION_GROUP]->(lg)
} IN TRANSACTIONS OF 1000 ROWS;
