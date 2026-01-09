
// This is an independent script because route_networks.txt may not exist, and
// routes can be linked to their corresponding networks in routes.txt.
LOAD CSV WITH HEADERS FROM "file:///GTFS/" + $dataset + "/route_networks.txt" as row
CALL {
    WITH row
    MATCH (n: Network { id: row.network_id })
    MATCH (r: Route { id: row.route_id })
    CREATE (r)-[bt: BELONGS_TO]->(n)
} IN TRANSACTIONS OF 1000 ROWS;
