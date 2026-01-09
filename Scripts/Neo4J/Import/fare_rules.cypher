
LOAD CSV WITH HEADERS FROM "file:///GTFS/" + $dataset + "/fare_rules.txt" as row
CALL {
    WITH row
    MATCH (fa: Fare { id: row.fare_id })
    OPTIONAL MATCH (r: Route { id: row.route_id })
    OPTIONAL MATCH (osz: StopZone { id: row.origin_id })
    OPTIONAL MATCH (dsz: StopZone { id: row.destination_id })
    OPTIONAL MATCH (csz: StopZone { id: row.contains_id })
    CREATE (fr: FareRule)
    CREATE (fr)-[f: FOLLOWS]->(fa)
    CREATE (fr)-[aw: ASSOCIATED_WITH]->(r)
    CREATE (fr)-[fo: FROM_ORIGIN]->(osz)
    CREATE (fr)-[td: TO_DESTINATION]->(dsz)
    CREATE (fr)-[c: CONTAINING]->(csz)
} IN TRANSACTIONS OF 1000 ROWS;
