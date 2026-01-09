
// Add service exceptions.
LOAD CSV WITH HEADERS FROM "file:///GTFS/" + $dataset + "/calendar_dates.txt" as row
CALL {
    WITH row
    MERGE (d: Day { date: date(row.date) })
    MERGE (s: Service { id: row.service_id }) ON CREATE SET s.monday = false,
                                                        s.tuesday = false,
                                                        s.wednesday = false,
                                                        s.thursday = false,
                                                        s.friday = false,
                                                        s.saturday = false,
                                                        s.sunday = false
    CREATE (s)-[h: HAS_EXCEPTION { type: row.exception_type }]->(d)
} IN TRANSACTIONS OF 1000 ROWS;
