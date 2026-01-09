
// Type constraints for fields.
CREATE CONSTRAINT attribution_id_type FOR (a: Attribution) REQUIRE a.id :: STRING;
CREATE CONSTRAINT attribution_org_name_type FOR (a: Attribution) REQUIRE a.organization_name :: STRING;
CREATE CONSTRAINT attribution_is_producer_type FOR (a: Attribution) REQUIRE a.is_producer :: BOOLEAN;
CREATE CONSTRAINT attribution_is_operator_type FOR (a: Attribution) REQUIRE a.is_operator :: BOOLEAN;
CREATE CONSTRAINT attribution_is_authority_type FOR (a: Attribution) REQUIRE a.is_authority :: BOOLEAN;
CREATE CONSTRAINT attribution_url_type FOR (a: Attribution) REQUIRE a.url :: STRING;
CREATE CONSTRAINT attribution_email_type FOR (a: Attribution) REQUIRE a.email :: STRING;
CREATE CONSTRAINT attribution_phone_type FOR (a: Attribution) REQUIRE a.phone :: STRING;

// Value constraints for fields.
CALL apoc.trigger.add('validate_attribution_fields','
    UNWIND $createdNodes AS node
    MATCH (node)
    WHERE node: Attribution
    CALL apoc.util.validate(
        NOT (node.is_producer OR node.is_operator OR node.is_authority),
        "Attribution must specify at least one role", []
    )
    CALL apoc.util.validate(
        node.url IS NOT NULL AND NOT node.url =~ "^https?://.*$",
        "Attribution URL has the wrong format: %s", [node.url]
    )
    RETURN node',
    { phase: 'before' }
) YIELD name
FINISH;

// Primary key and not null constraints.
CREATE CONSTRAINT attribution_key FOR (a: Attribution) REQUIRE a.id IS NODE KEY;
CREATE CONSTRAINT attribution_org_name_not_null FOR (a: Attribution) REQUIRE a.organization_name IS NOT NULL;

LOAD CSV WITH HEADERS FROM "file:///GTFS/" + $dataset + "/attributions.txt" as row
CALL {
    WITH row
    OPTIONAL MATCH (ag: Agency { id: row.agency_id })
    OPTIONAL MATCH (r: Route { id: row.route_id })
    OPTIONAL MATCH (t: Trip { id: row.trip_id })
    WITH row, ag, r, t
    WHERE (row.agency_id IS NULL OR ag IS NOT NULL) AND     // No invalid ids.
          (row.route_id IS NULL OR r IS NOT NULL) AND
          (row.trip_id IS NULL OR t IS NOT NULL) AND
          size([x IN [row.agency_id, row.route_id, row.trip_id] WHERE x IS NOT NULL]) <= 1 AND          // The attribution can only apply to a single element.
          toInteger(row.is_producer) + toInteger(row.is_operator) + toInteger(row.is_authority) > 0     // At least one role must be specified.
    CREATE (a: Attribution {
        id: row.attribution_id,
        organization_name: row.organization_name,
        is_producer: row.is_producer = "1",
        is_operator: row.is_operator = "1",
        is_authority: row.is_authority = "1",
        url: row.attribution_url,
        email: row.attribution_email,
        phone: row.attribution_phone
    })
    CREATE (a)-[atag: APPLIES_TO]->(ag)
    CREATE (a)-[atr: APPLIES_TO]->(r)
    CREATE (a)-[att: APPLIES_TO]->(t)
} IN TRANSACTIONS OF 1000 ROWS;
