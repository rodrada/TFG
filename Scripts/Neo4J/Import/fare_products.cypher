
// Type constraints for fields.
CREATE CONSTRAINT fare_product_id_type FOR (fp: FareProduct) REQUIRE fp.id :: STRING;
CREATE CONSTRAINT fare_product_name_type FOR (fp: FareProduct) REQUIRE fp.name :: STRING;
CREATE CONSTRAINT fare_product_amount_type FOR (fp: FareProduct) REQUIRE fp.amount :: FLOAT;
CREATE CONSTRAINT fare_product_currency_type FOR (fp: FareProduct) REQUIRE fp.currency :: STRING;

// Value constraints for fields.
CALL apoc.trigger.add('validate_fp_fields', '
    UNWIND $createdNodes AS node
    MATCH (node)
    WHERE node: FareProduct
    CALL apoc.util.validate(
        node.currency =~ "^[a-zA-Z]{3}$",
        "Fare product currency has the wrong format: %s", [node.currency]
    )
    RETURN node',
    { phase: 'before' }
) YIELD name
FINISH;

// Primary key and not null constraints.
CREATE CONSTRAINT fare_product_id_not_null FOR (fp: FareProduct) REQUIRE fp.id IS NOT NULL;
CREATE CONSTRAINT fare_product_amount_not_null FOR (fp: FareProduct) REQUIRE fp.amount IS NOT NULL;
CREATE CONSTRAINT fare_product_currency_not_null FOR (fp: FareProduct) REQUIRE fp.currency IS NOT NULL;

LOAD CSV WITH HEADERS FROM "file:///GTFS/" + $dataset + "/fare_products.txt" as row
CALL {
    WITH row
    MATCH (fm: FareMedia { id: row.fare_media_id })
    WITH row, fm
    WHERE NOT EXISTS { (fp: FareProduct { id: row.fare_product_id })-[USED_WITH]->(fm) }
    CREATE (fp: FareProduct {
        id: row.fare_product_id,
        name: row.fare_product_name,
        amount: row.amount,
        currency: row.currency
    })
    CREATE (fp)-[uw: USED_WITH]->(fm)
} IN TRANSACTIONS OF 1000 ROWS;
