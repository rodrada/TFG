
// Type constraints for fields.
CREATE CONSTRAINT agency_id_type FOR (a: Agency) REQUIRE a.id :: STRING;
CREATE CONSTRAINT agency_name_type FOR (a: Agency) REQUIRE a.name :: STRING;
CREATE CONSTRAINT agency_url_type FOR (a: Agency) REQUIRE a.url :: STRING;
CREATE CONSTRAINT agency_timezone_type FOR (a: Agency) REQUIRE a.timezone :: STRING;
CREATE CONSTRAINT agency_lang_type FOR (a: Agency) REQUIRE a.lang :: STRING;
CREATE CONSTRAINT agency_phone_type FOR (a: Agency) REQUIRE a.phone :: STRING;
CREATE CONSTRAINT agency_fare_url_type FOR (a: Agency) REQUIRE a.fare_url :: STRING;
CREATE CONSTRAINT agency_email_type FOR (a: Agency) REQUIRE a.email :: STRING;

// Value constraints for fields.
CALL apoc.trigger.add('validate_agency_fields','
    UNWIND $createdNodes AS node
    MATCH (node)
    WHERE node: Agency
    CALL apoc.util.validate(
        node.url IS NOT NULL AND NOT node.url =~ "^https?://.*$",
        "Agency URL has the wrong format: %s", [node.url]
    )
    CALL apoc.util.validate(
        node.lang IS NOT NULL AND NOT node.lang =~ "^[a-zA-Z]{2}$",
        "Agency lang has the wrong format: %s", [node.lang]
    )
    CALL apoc.util.validate(
        node.fare_url IS NOT NULL AND NOT node.fare_url =~ "^https?://.*$",
        "Agency fare URL has the wrong format: %s", [node.fare_url]
    )
    CALL apoc.util.validate(
        node.timezone IS NOT NULL AND NOT node.timezone =~ "^[A-Z][a-z]+/[A-Z][a-z]+(_[A-Z][a-z]+)*$",
        "Agency timezone has the wrong format: %s", [node.timezone]
    )
    CALL apoc.util.validate(
        node.email IS NOT NULL AND NOT node.email =~ "^.*@.*$",
        "Agency email has the wrong format: %s", [node.email]
    )
    RETURN node',
    { phase: 'before' }
) YIELD name
FINISH;

// Primary key and not null constraints.
CREATE CONSTRAINT agency_key FOR (a: Agency) REQUIRE a.id IS NODE KEY;
CREATE CONSTRAINT agency_name_not_null FOR (a: Agency) REQUIRE a.name IS NOT NULL;
CREATE CONSTRAINT agency_url_not_null FOR (a: Agency) REQUIRE a.url IS NOT NULL;
CREATE CONSTRAINT agency_timezone_not_null FOR (a: Agency) REQUIRE a.timezone IS NOT NULL;

// Load information about the transport agencies and create nodes.
LOAD CSV WITH HEADERS FROM "file:///GTFS/" + $dataset + "/agency.txt" as row
CALL {
    WITH row
    CREATE (a: Agency {
        id: row.agency_id,
        name: row.agency_name,
        url: row.agency_url,
        timezone: row.agency_timezone,
        lang: row.agency_lang,
        phone: row.agency_phone,
        fare_url: row.agency_fare_url,
        email: row.agency_email
    })
} IN TRANSACTIONS OF 1000 ROWS;
