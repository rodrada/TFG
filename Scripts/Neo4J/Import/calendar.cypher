
// Type constraints for fields.
CREATE CONSTRAINT service_id_type FOR (s: Service) REQUIRE s.id :: STRING;
CREATE CONSTRAINT service_monday_type FOR (s: Service) REQUIRE s.monday :: BOOLEAN;
CREATE CONSTRAINT service_tuesday_type FOR (s: Service) REQUIRE s.tuesday :: BOOLEAN;
CREATE CONSTRAINT service_wednesday_type FOR (s: Service) REQUIRE s.wednesday :: BOOLEAN;
CREATE CONSTRAINT service_thursday_type FOR (s: Service) REQUIRE s.thursday :: BOOLEAN;
CREATE CONSTRAINT service_friday_type FOR (s: Service) REQUIRE s.friday :: BOOLEAN;
CREATE CONSTRAINT service_saturday_type FOR (s: Service) REQUIRE s.saturday :: BOOLEAN;
CREATE CONSTRAINT service_sunday_type FOR (s: Service) REQUIRE s.sunday :: BOOLEAN;

// Primary key and not null constraints.
CREATE CONSTRAINT service_key FOR (s: Service) REQUIRE s.id IS NODE KEY;
CREATE CONSTRAINT service_monday_not_null FOR (s: Service) REQUIRE s.monday IS NOT NULL;
CREATE CONSTRAINT service_tuesday_not_null FOR (s: Service) REQUIRE s.tuesday IS NOT NULL;
CREATE CONSTRAINT service_wednesday_not_null FOR (s: Service) REQUIRE s.wednesday IS NOT NULL;
CREATE CONSTRAINT service_thursday_not_null FOR (s: Service) REQUIRE s.thursday IS NOT NULL;
CREATE CONSTRAINT service_friday_not_null FOR (s: Service) REQUIRE s.friday IS NOT NULL;
CREATE CONSTRAINT service_saturday_not_null FOR (s: Service) REQUIRE s.saturday IS NOT NULL;
CREATE CONSTRAINT service_sunday_not_null FOR (s: Service) REQUIRE s.sunday IS NOT NULL;

// Constraints for associated nodes.
CREATE CONSTRAINT day_date_type FOR (d: Day) REQUIRE d.date :: DATE;
CREATE CONSTRAINT day_key FOR (d: Day) REQUIRE d.date IS NODE KEY;

// Import calendar services.
LOAD CSV WITH HEADERS FROM "file:///GTFS/" + $dataset + "/calendar.txt" as row
CALL {
    WITH row
    WITH row    // Required to apply WHERE clause due to Neo4j syntax limitations
    WHERE row.start_date =~ "\\d{8}" AND
          row.end_date =~ "\\d{8}"
    CREATE (s: Service {
        id: row.service_id,
        monday: row.monday = "1",
        tuesday: row.tuesday = "1",
        wednesday: row.wednesday = "1",
        thursday: row.thursday = "1",
        friday: row.friday = "1",
        saturday: row.saturday = "1",
        sunday: row.sunday = "1"
    })
    MERGE (st: Day { date: date(row.start_date) })
    MERGE (en: Day { date: date(row.end_date) })
    CREATE (s)-[st_r: STARTS_ON]->(st)
    CREATE (s)-[en_r: ENDS_ON]->(en)
} IN TRANSACTIONS OF 1000 ROWS;
