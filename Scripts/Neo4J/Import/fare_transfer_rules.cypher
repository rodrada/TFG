
// Type constraints for fields.
CREATE CONSTRAINT ftr_transfer_count_type FOR (ftr: FareTransferRule) REQUIRE ftr.transfer_count :: INTEGER;
CREATE CONSTRAINT ftr_duration_limit_type FOR (ftr: FareTransferRule) REQUIRE ftr.duration_limit :: INTEGER;

// Value constraints for fields.
CALL apoc.trigger.add('validate_ftr_fields','
    UNWIND $createdNodes AS node
    MATCH (node)
    WHERE node: FareTransferRule
    CALL apoc.util.validate(
        node.transfer_count IS NOT NULL AND (node.transfer_count < -1 OR node.transfer_count = 0),
        "Fare transfer rule transfer count must be -1 or >=1: %d", [node.transfer_count]
    )
    CALL apoc.util.validate(
        node.duration_limit IS NOT NULL AND duration_limit <= 0,
        "Fare transfer rule duration limit must be positive: %d", [node.duration_limit]
    )
    RETURN node',
    { phase: 'before' }
) YIELD name
FINISH;

// Constraints for associated enums.
CREATE CONSTRAINT duration_limit_type_value_type FOR (dlt: DurationLimitType) REQUIRE dlt.value :: STRING;
CREATE CONSTRAINT duration_limit_type_key FOR (dlt: DurationLimitType) REQUIRE dlt.value IS NODE KEY;

CREATE CONSTRAINT fare_transfer_type_value_type FOR (ftt: FareTransferType) REQUIRE ftt.value :: STRING;
CREATE CONSTRAINT fare_transfer_type_key FOR (ftt: FareTransferType) REQUIRE ftt.value IS NODE KEY;

LOAD CSV WITH HEADERS FROM "file:///GTFS/" + $dataset + "/fare_transfer_rules.txt" as row
CALL {
    WITH row
    OPTIONAL MATCH (flg: LegGroup { id: row.from_leg_group_id })
    OPTIONAL MATCH (tlg: LegGroup { id: row.to_leg_group_id })
    OPTIONAL MATCH (fp: FareProduct { id: row.fare_product_id })
    WITH row, flg, tlg, fp
    WHERE ((row.transfer_count IS NOT NULL) = (row.from_leg_group_id = row.to_leg_group_id)) AND
          (row.duration_limit_type IS NULL) = (row.duration_limit IS NULL) AND
          (row.duration_limit_type IS NULL OR row.duration_limit_type IN ["0", "1", "2", "3"]) AND
          (row.fare_transfer_type IN ["0", "1", "2"])
    CREATE (ftr: FareTransferRule {
        transfer_count: toInteger(row.transfer_count),
        duration_limit: toInteger(row.duration_limit)
    })
    MERGE (ftt: FareTransferType { value: CASE row.fare_transfer_type
                                          WHEN "0" THEN "From-Transfer"
                                          WHEN "1" THEN "From-Transfer-To"
                                          ELSE "Transfer" END })
    CREATE (ftr)-[f: FROM]->(flg)
    CREATE (ftr)-[t: TO]->(tlg)
    CREATE (ftr)-[r: REQUIRES]->(fp)
    CREATE (ftr)-[htt: HAS_TRANSFER_TYPE]->(ftt)
    WITH row, ftr
    WHERE row.duration_limit IS NOT NULL
    MERGE (dlt: DurationLimitType { value: CASE row.duration_limit_type
                                           WHEN "0" THEN "Departure-Arrival"
                                           WHEN "1" THEN "Departure-Departure"
                                           WHEN "2" THEN "Arrival-Departure"
                                           ELSE "Arrival-Arrival" END })
    CREATE (ftr)-[hdlt: HAS_DURATION_LIMIT_TYPE]->(dlt)
} IN TRANSACTIONS OF 1000 ROWS;
