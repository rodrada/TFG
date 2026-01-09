
LOAD CSV WITH HEADERS FROM "file:///GTFS/" + $dataset + "/fare_leg_join_rules.txt" as row
CALL {
    WITH row
    MATCH (fn: Network { id: row.from_network_id })
    MATCH (tn: Network { id: row.to_network_id })
    OPTIONAL MATCH (fs: Stop { id: row.from_stop_id })
    OPTIONAL MATCH (ts: Stop { id: row.to_stop_id })
    WITH row, fn, tn, fs, ts
    WHERE (row.from_stop_id IS NULL) = (row.to_stop_id IS NULL)   // Either both fields are present or none of them is.
          (row.from_stop_id IS NULL OR fs IS NOT NULL)            // Make sure the provided stop_ids are valid.
          (row.to_stop_id IS NULL OR ts IS NOT NULL)
    CREATE (fljr: FareLegJoinRule)
    CREATE (fljr)-[ffn: FROM_NETWORK]->(fn)
    CREATE (fljr)-[ttn: TO_NETWORK]->(tn)
    CREATE (fljr)-[ffs: FROM_STOP]->(fs)
    CREATE (fljr)-[tts: TO_STOP]->(ts)
} IN TRANSACTIONS OF 1000 ROWS;
