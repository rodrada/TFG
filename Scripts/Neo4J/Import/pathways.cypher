
// Type constraints for fields.
CREATE CONSTRAINT pathway_id_type FOR (p: Pathway) REQUIRE p.id :: STRING;
CREATE CONSTRAINT pathway_is_bidirectional_type FOR (p: Pathway) REQUIRE p.is_bidirectional :: BOOLEAN;
CREATE CONSTRAINT pathway_length_type FOR (p: Pathway) REQUIRE p.length :: FLOAT;
CREATE CONSTRAINT pathway_traversal_time_type FOR (p: Pathway) REQUIRE p.traversal_time :: INTEGER;
CREATE CONSTRAINT pathway_stair_count_type FOR (p: Pathway) REQUIRE p.stair_count :: INTEGER;
CREATE CONSTRAINT pathway_max_slope_type FOR (p: Pathway) REQUIRE p.max_slope :: FLOAT;
CREATE CONSTRAINT pathway_min_width_type FOR (p: Pathway) REQUIRE p.min_width :: FLOAT;
CREATE CONSTRAINT pathway_signposted_as_type FOR (p: Pathway) REQUIRE p.signposted_as :: STRING;
CREATE CONSTRAINT pathway_reversed_signposted_as_type FOR (p: Pathway) REQUIRE p.reversed_signposted_as :: STRING;

// Value constraints for fields.
CALL apoc.trigger.add('validate_pathway_fields','
    UNWIND $createdNodes AS node
    MATCH (node)
    WHERE node: Pathway
    CALL apoc.util.validate(
        node.length IS NOT NULL AND node.length < 0,
        "Pathway length must be non-negative: %f", [node.length]
    )
    CALL apoc.util.validate(
        node.traversal_time IS NOT NULL AND node.traversal_time <= 0,
        "Pathway traversal time must be positive: %d", [node.traversal_time]
    )
    CALL apoc.util.validate(
        node.stair_count IS NOT NULL AND node.stair_count = 0,
        "Pathway stair count must not be zero: %d", [node.stair_count]
    )
    CALL apoc.util.validate(
        node.min_width IS NOT NULL AND node.min_width <= 0,
        "Pathway min width must be positive: %f", [node.min_width]
    )
    RETURN node',
    { phase: 'before' }
) YIELD name
FINISH;

// Primary key and not null constraints.
CREATE CONSTRAINT pathway_key FOR (p: Pathway) REQUIRE p.id IS NODE KEY;
CREATE CONSTRAINT pathway_is_bidirectional_not_null FOR (p: Pathway) REQUIRE p.is_bidirectional IS NOT NULL;

// Constraints for associated enums.
CREATE CONSTRAINT pathway_mode_value_type FOR (pm: PathwayMode) REQUIRE pm.value :: STRING;
CREATE CONSTRAINT pathway_mode_key FOR (pm: PathwayMode) REQUIRE pm.value IS NODE KEY;

// Generate pathway nodes.
LOAD CSV WITH HEADERS FROM "file:///GTFS/" + $dataset + "/pathways.txt" as row
CALL {
    WITH row
    MATCH (fs: Stop { id: row.from_stop_id })
    MATCH (ts: Stop { id: row.to_stop_id })
    WITH row, fs, ts
    WHERE row.pathway_mode IN ["1", "2", "3", "4", "5", "6", "7"]
    CREATE (p: Pathway {
        id: row.pathway_id,
        is_bidirectional: row.is_bidirectional = "1",
        length: toFloat(row.length),
        traversal_time: toInteger(row.traversal_time),
        stair_count: toInteger(row.stair_count),
        max_slope: toFloat(row.max_slope),
        min_width: toFloat(row.min_width),
        signposted_as: row.signposted_as,
        reversed_signposted_as: row.reversed_signposted_as
    })
    CREATE (p)-[f: FROM]->(fs)
    CREATE (p)-[t: TO]->(ts)
    MERGE (pm: PathwayMode { value: CASE row.pathway_mode
                                    WHEN "1" THEN "Walkway"
                                    WHEN "2" THEN "Stairs"
                                    WHEN "3" THEN "Moving Sidewalk/Travelator"
                                    WHEN "4" THEN "Escalator"
                                    WHEN "5" THEN "Elevator"
                                    WHEN "6" THEN "Fare Gate"
                                    ELSE "Exit Gate" END })
    CREATE (p)-[hpm: HAS_PATHWAY_MODE]->(pm)
} IN TRANSACTIONS OF 1000 ROWS;
