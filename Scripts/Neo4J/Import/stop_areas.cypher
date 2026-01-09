
// Generate relationships stops and their areas.
// NOTE: PostgreSQL generates area shapes using a convex hull of the stops in the area,
//       but Neo4J Spatial does not have support for convex hulls.
//       As a result, we will not generate area shapes for Neo4J.
LOAD CSV WITH HEADERS FROM "file:///GTFS/" + $dataset + "/stop_areas.txt" as row
CALL {
    WITH row
    MATCH (a: Area { id: row.area_id })
    MATCH (s: Stop { id: row.stop_id })
    CREATE (s)-[bt: BELONGS_TO]->(a)
} IN TRANSACTIONS OF 1000 ROWS;
