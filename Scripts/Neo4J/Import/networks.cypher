
// Type constraints for fields.
CREATE CONSTRAINT network_id_type FOR (n: Network) REQUIRE n.id :: STRING;
CREATE CONSTRAINT network_name_type FOR (n: Network) REQUIRE n.name :: STRING;

// Primary key and not null constraints.
CREATE CONSTRAINT network_key FOR (n: Network) REQUIRE n.id IS NODE KEY;

// Generate network nodes.
// Note that these nodes could have been already created while reading "routes.txt", hence the MERGE.
LOAD CSV WITH HEADERS FROM "file:///GTFS/" + $dataset + "/networks.txt" as row
CALL {
    WITH row
    MERGE (n: Network { id: row.network_id })
    SET n.name = row.network_name
} IN TRANSACTIONS OF 1000 ROWS;
