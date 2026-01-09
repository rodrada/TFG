
// Type constraints for fields.
CREATE CONSTRAINT feed_publisher_name_type FOR (f: Feed) REQUIRE f.publisher_name :: STRING;
CREATE CONSTRAINT feed_publisher_url_type FOR (f: Feed) REQUIRE f.publisher_url :: STRING;
CREATE CONSTRAINT feed_lang_type FOR (f: Feed) REQUIRE f.lang :: STRING;
CREATE CONSTRAINT feed_default_lang_type FOR (f: Feed) REQUIRE f.default_lang :: STRING;
CREATE CONSTRAINT feed_start_date_type FOR (f: Feed) REQUIRE f.start_date :: DATE;
CREATE CONSTRAINT feed_end_date_type FOR (f: Feed) REQUIRE f.end_date :: DATE;
CREATE CONSTRAINT feed_version_type FOR (f: Feed) REQUIRE f.version :: STRING;
CREATE CONSTRAINT feed_contact_email_type FOR (f: Feed) REQUIRE f.contact_email :: STRING;
CREATE CONSTRAINT feed_contact_url_type FOR (f: Feed) REQUIRE f.contact_url :: STRING;

// Value constraints for fields.
CALL apoc.trigger.add('validate_feed_fields','
    UNWIND $createdNodes AS node
    MATCH (node)
    WHERE node: Feed
    CALL apoc.util.validate(
        node.publisher_url IS NOT NULL AND NOT node.publisher_url =~ "^https?://.*$",
        "Feed publisher URL has the wrong format: %s", [node.publisher_url]
    )
    CALL apoc.util.validate(
        node.lang IS NOT NULL AND NOT node.lang =~ "^[a-zA-Z]{2}$",
        "Feed lang has the wrong format: %s", [node.lang]
    )
    CALL apoc.util.validate(
        node.default_lang IS NOT NULL AND NOT node.default_lang =~ "^[a-z]{2}$",
        "Feed default lang has the wrong format: %s", [node.default_lang]
    )
    CALL apoc.util.validate(
        node.contact_url IS NOT NULL AND NOT node.contact_url =~ "^https?://.*$",
        "Feed contact URL has the wrong format: %s", [node.contact_url]
    )
    RETURN node',
    { phase: 'before' }
) YIELD name
FINISH;

// Primary key and not null constraints.
CREATE CONSTRAINT feed_publisher_name_not_null FOR (f: Feed) REQUIRE f.publisher_name IS NOT NULL;
CREATE CONSTRAINT feed_publisher_url_not_null FOR (f: Feed) REQUIRE f.publisher_url IS NOT NULL;
CREATE CONSTRAINT feed_publisher_lang_not_null FOR (f: Feed) REQUIRE f.lang IS NOT NULL;

// Generate feed nodes.
LOAD CSV WITH HEADERS FROM "file:///GTFS/" + $dataset + "/feed_info.txt" as row
CALL {
    WITH row
    CREATE (f: Feed {
        publisher_name: row.feed_publisher_name,
        publisher_url: row.feed_publisher_url,
        lang: row.feed_lang,
        default_lang: row.default_lang,
        start_date: date(row.feed_start_date),
        end_date: date(row.feed_end_date),
        version: row.feed_version,
        contact_email: row.feed_contact_email,
        contact_url: row.feed_contact_url
    })
} IN TRANSACTIONS OF 1000 ROWS;
