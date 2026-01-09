
// Type constraints for fields.
CREATE CONSTRAINT timeframe_start_time FOR (t: Timeframe) REQUIRE t.start_time :: LOCAL TIME;
CREATE CONSTRAINT timeframe_end_time FOR (t: Timeframe) REQUIRE t.end_time :: LOCAL TIME;

CREATE CONSTRAINT timeframe_group_id_type FOR (tg: TimeframeGroup) REQUIRE tg.id :: STRING;
CREATE CONSTRAINT timeframe_group_id_key FOR (tg: TimeframeGroup) REQUIRE tg.id IS NODE KEY;

LOAD CSV WITH HEADERS FROM "file:///GTFS/" + $dataset + "/timeframes.txt" as row
CALL {
    WITH row
    MATCH (s: Service { id: row.service_id })
    WITH row, s
    WHERE row.group_id IS NOT NULL AND
          (row.start_time IS NULL) = (row.end_time IS NULL)
    CREATE (t: Timeframe {
        start_time: localtime(row.start_time),
        end_time: localtime(row.end_time)
    })
    MERGE (tg: TimeframeGroup { id: row.timeframe_group_id })
    CREATE (t)-[ig: IN_GROUP]->(tg)
    CREATE (t)-[d: DURING]->(s)
} IN TRANSACTIONS OF 1000 ROWS;

// Indexes for faster queries.
CREATE INDEX idx_timeframe_start_time FOR (t: Timeframe) ON (t.start_time);
CREATE INDEX idx_timeframe_end_time FOR (t: Timeframe) ON (t.end_time);
