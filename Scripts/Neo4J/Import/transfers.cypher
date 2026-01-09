
// Type constraints for fields.
CREATE CONSTRAINT transfer_min_transfer_time_type FOR (t: Transfer) REQUIRE t.min_transfer_time :: INTEGER;

// Value constraints for fields.
CALL apoc.trigger.add('validate_transfer_fields','
    UNWIND $createdNodes AS node
    MATCH (node)
    WHERE node: Transfer
    CALL apoc.util.validate(
        node.min_transfer_time IS NOT NULL AND node.min_transfer_time < 0,
        "Transfer min transfer time must be non-negative: %d", [node.min_transfer_time]
    )
    RETURN node',
    { phase: 'before' }
) YIELD name
FINISH;

// Constraints for associated enums.
CREATE CONSTRAINT transfer_type_value_type FOR (tt: TransferType) REQUIRE tt.value :: STRING;
CREATE CONSTRAINT transfer_type_key FOR (tt: TransferType) REQUIRE tt.value IS NODE KEY;

// Generate transfer nodes.
LOAD CSV WITH HEADERS FROM "file:///GTFS/" + $dataset + "/transfers.txt" as row
CALL {
    WITH row
    OPTIONAL MATCH (fs: Stop { id: row.from_stop_id })
    OPTIONAL MATCH (ts: Stop { id: row.to_stop_id })
    OPTIONAL MATCH (ft: Trip { id: row.from_trip_id })
    OPTIONAL MATCH (tt: Trip { id: row.to_trip_id })
    OPTIONAL MATCH (fr: Route { id: row.from_route_id })
    OPTIONAL MATCH (tr: Route { id: row.to_route_id })
    WITH row, fs, ts, ft, tt, fr, tr
    WHERE (fs IS NOT NULL OR NOT row.transfer_type IN ["1", "2", "3"]) AND
          (ts IS NOT NULL OR NOT row.transfer_type IN ["1", "2", "3"]) AND
          (ft IS NOT NULL OR NOT row.transfer_type IN ["4", "5"]) AND
          (tt IS NOT NULL OR NOT row.transfer_type IN ["4", "5"]) AND
          (row.transfer_type IS NULL OR row.transfer_type IN ["0", "1", "2", "3", "4", "5"])
    CREATE (t: Transfer {
        min_transfer_time: toInteger(row.min_transfer_time)
    })
    CREATE (t)-[ffs: FROM]->(fs)
    CREATE (t)-[tts: TO]->(ts)
    CREATE (t)-[fft: FROM]->(ft)
    CREATE (t)-[ttt: TO]->(tt)
    CREATE (t)-[ffr: FROM]->(fr)
    CREATE (t)-[ttr: TO]->(tr)
    MERGE (trt: TransferType { value: CASE row.transfer_type
                                     WHEN "5" THEN "Must Re-Board"
                                     WHEN "4" THEN "In-Seat Transfer"
                                     WHEN "3" THEN "Not Between Routes"
                                     WHEN "2" THEN "Requires Time"
                                     WHEN "1" THEN "Timed"
                                     ELSE "Recommended" END })  // Value is either NULL or 0.
    CREATE (t)-[ht: HAS_TYPE]->(trt)
} IN TRANSACTIONS OF 1000 ROWS;
