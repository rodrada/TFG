
// Type constraints for fields.
CREATE CONSTRAINT flr_rule_priority_type FOR (flr: FareLegRule) REQUIRE flr.rule_priority :: INTEGER;

// Constraints for associated groups.
CREATE CONSTRAINT flr_leg_group_id_type FOR (lg: LegGroup) REQUIRE lg.id :: STRING;
CREATE CONSTRAINT flr_leg_group_key FOR (lg: LegGroup) REQUIRE lg.id IS NODE KEY;

LOAD CSV WITH HEADERS FROM "file:///GTFS/" + $dataset + "/fare_leg_rules.txt" as row
CALL {
    WITH row
    MATCH (fp: FareProduct { id: row.fare_product_id })
    OPTIONAL MATCH (n: Network { id: row.network_id })
    OPTIONAL MATCH (fa: Area { id: row.from_area_id })
    OPTIONAL MATCH (ta: Area { id: row.to_area_id })
    OPTIONAL MATCH (ftg: TimeframeGroup { id: row.from_timeframe_group_id })
    OPTIONAL MATCH (ttg: TimeframeGroup { id: row.to_timeframe_group_id })
    CREATE (flr: FareLegRule { rule_priority: CASE WHEN row.rule_priority IS NULL THEN 0
                                                   ELSE toInteger(row.rule_priority) END })
    CREATE (flr)-[in: IN_NETWORK]->(n)
    CREATE (flr)-[ffa: FROM_AREA]->(fa)
    CREATE (flr)-[tta: TO_AREA]->(ta)
    CREATE (flr)-[fftg: FROM_TIMEFRAME_GROUP]->(ftg)
    CREATE (flr)-[tttg: TO_TIMEFRAME_GROUP]->(ttg)
    CREATE (flr)-[r: REQUIRES]->(fp)
    WITH row, flr
    WHERE row.leg_group_id IS NOT NULL
    MERGE (lg: LegGroup { id: row.leg_group_id })
    CREATE (flr)-[ilg: IN_LEG_GROUP]->(lg)
} IN TRANSACTIONS OF 1000 ROWS;
