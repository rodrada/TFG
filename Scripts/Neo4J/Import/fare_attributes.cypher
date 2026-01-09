
// Type constraints for fields.
CREATE CONSTRAINT fare_id_type FOR (f: Fare) REQUIRE f.id :: STRING;
CREATE CONSTRAINT fare_price_type FOR (f: Fare) REQUIRE f.price :: FLOAT;
CREATE CONSTRAINT fare_currency_type_type FOR (f: Fare) REQUIRE f.currency_type :: STRING;
CREATE CONSTRAINT fare_transfers_type FOR (f: Fare) REQUIRE f.transfers :: INTEGER;
CREATE CONSTRAINT fare_transfer_duration_type FOR (f: Fare) REQUIRE f.transfer_duration :: INTEGER;

// Value constraints for fields.
CALL apoc.trigger.add('validate_fare_fields','
    UNWIND $createdNodes AS node
    MATCH (node)
    WHERE node: Fare
    CALL apoc.util.validate(
        node.price < 0,
        "Fare price must be non-negative: %f", [node.price]
    )
    CALL apoc.util.validate(
        NOT node.currency_type =~ "^[a-zA-Z]{3}$",
        "Fare currency type has the wrong format: %s", [node.currency_type]
    )
    CALL apoc.util.validate(
        node.transfers IS NOT NULL AND (node.transfers < 0 OR node.transfers > 2),
        "Fare transfer count must be between 0 and 2: %d", [node.transfers]
    )
    CALL apoc.util.validate(
        node.transfer_duration IS NOT NULL AND node.transfer_duration < 0,
        "Fare transfer duration must be non-negative: %d", [node.transfer_duration]
    )
    RETURN node',
    { phase: 'before' }
) YIELD name
FINISH;

// Primary key and not null constraints.
CREATE CONSTRAINT fare_key FOR (f: Fare) REQUIRE f.id IS NODE KEY;
CREATE CONSTRAINT fare_price_not_null FOR (f: Fare) REQUIRE f.price IS NOT NULL;
CREATE CONSTRAINT fare_currency_type_not_null FOR (f: Fare) REQUIRE f.currency_type IS NOT NULL;
CREATE CONSTRAINT fare_payment_method_not_null FOR (f: Fare) REQUIRE f.payment_method IS NOT NULL;

// Constraints for associated enums.
CREATE CONSTRAINT payment_method_value_type FOR (pm: PaymentMethod) REQUIRE pm.value :: STRING;
CREATE CONSTRAINT payment_method_key FOR (pm: PaymentMethod) REQUIRE pm.value IS NODE KEY;

LOAD CSV WITH HEADERS FROM "file:///GTFS/" + $dataset + "/fare_attributes.txt" as row
CALL { MATCH (a: Agency) RETURN count(a) as agency_count }
CALL {
    WITH row, agency_count
    OPTIONAL MATCH (a: Agency { id: row.agency_id })
    WITH row, agency_count, a
    WHERE row.payment_method IN ["0", "1"] AND
          (agency_count = 1 OR a IS NOT NULL)  // Either there is only one agency or it is specified
    CREATE (f: Fare {
        id: row.fare_id,
        price: toFloat(row.price),
        currency_type: row.currency_type,
        payment_method: row.payment_method,
        transfers: toInteger(row.transfers),  // Amount of transfers allowed, empty is unlimited
        transfer_duration: toInteger(row.transfer_duration)
    })
    MERGE (p: PaymentMethod { value: CASE row.payment_method
                                     WHEN "0" THEN "On Board"
                                     ELSE "Before Boarding" END })    // Value is guaranteed to be 1
    CREATE (f)-[hpm: HAS_PAYMENT_METHOD]->(p)
    CREATE (f)-[pt: PAID_TO]->(a)
} IN TRANSACTIONS OF 1000 ROWS;
