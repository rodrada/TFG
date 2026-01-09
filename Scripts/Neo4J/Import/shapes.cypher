
// Type constraints for fields.
CREATE CONSTRAINT shape_id_type FOR (s: Shape) REQUIRE s.id :: STRING;

// Primary key and not null constraints.
CREATE CONSTRAINT shape_key FOR (s: Shape) REQUIRE s.id IS NODE KEY;

CALL spatial.addLayer('shapes', 'WKT', 'wkt') YIELD node
FINISH;

// Add shapes for each of the trips, batching by shape id.
CALL apoc.periodic.iterate(
  '
  LOAD CSV WITH HEADERS FROM "file:///GTFS/" + $dataset + "/shapes.txt" AS row
  WITH row.shape_id AS shape_id,
       toInteger(row.shape_pt_sequence) AS seq,
       row.shape_pt_lon AS lon,
       row.shape_pt_lat AS lat
  ORDER BY shape_id, seq
  RETURN shape_id, collect(lon + " " + lat) AS coords
  ',
  '
  WITH shape_id, "LINESTRING(" + apoc.text.join(coords, ", ") + ")" AS wkt
  CALL spatial.addWKT("shapes", wkt) YIELD node
  SET node:Shape, node.id = shape_id
  ',
  { batchSize: 1000, params: { dataset: $dataset } }
) YIELD batches
FINISH;

// Link trips to their shapes.
LOAD CSV WITH HEADERS FROM "file:///GTFS/" + $dataset + "/trips.txt" as row
CALL {
    WITH row
    MATCH (t: Trip { id: row.trip_id })
    MATCH (sh: Shape { id: row.shape_id })
    CREATE (t)-[hsh: HAS_SHAPE]->(sh)
} IN TRANSACTIONS OF 1000 ROWS;
