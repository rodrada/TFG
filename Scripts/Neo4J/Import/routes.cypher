
// Type constraints for fields.
CREATE CONSTRAINT route_id_type FOR (r: Route) REQUIRE r.id :: STRING;
CREATE CONSTRAINT route_short_name_type FOR (r: Route) REQUIRE r.short_name :: STRING;
CREATE CONSTRAINT route_long_name_type FOR (r: Route) REQUIRE r.long_name :: STRING;
CREATE CONSTRAINT route_desc_type FOR (r: Route) REQUIRE r.desc :: STRING;
CREATE CONSTRAINT route_url_type FOR (r: Route) REQUIRE r.url :: STRING;
CREATE CONSTRAINT route_color_type FOR (r: Route) REQUIRE r.color :: STRING;
CREATE CONSTRAINT route_text_color_type FOR (r: Route) REQUIRE r.text_color :: STRING;
CREATE CONSTRAINT route_sort_order_type FOR (r: Route) REQUIRE r.sort_order :: INTEGER;

// Values constraints for fields.
CALL apoc.trigger.add('validate_route_fields','
    UNWIND $createdNodes AS node
    MATCH (node)
    WHERE node: Route
    CALL apoc.util.validate(
        node.color IS NOT NULL AND NOT node.color =~ "^[A-F0-9]{6}$",
        "Route color has the wrong format: %s", [node.color]
    )
    CALL apoc.util.validate(
        node.text_color IS NOT NULL AND NOT node.text_color =~ "^[A-F0-9]{6}$",
        "Route text color has the wrong format: %s", [node.text_color]
    )
    CALL apoc.util.validate(
        node.sort_order IS NOT NULL AND node.sort_order < 0,
        "Route sort order must be non-negative: %d", [node.sort_order]
    )
    RETURN node',
    { phase: 'before' }
) YIELD name
FINISH;

// Primary key and not null constraints.
CREATE CONSTRAINT route_key FOR (r: Route) REQUIRE r.id IS NODE KEY;

// Constraints for associated enums.
CREATE CONSTRAINT route_type_value_type FOR (rt: RouteType) REQUIRE rt.value :: STRING;
CREATE CONSTRAINT route_type_key FOR (rt: RouteType) REQUIRE rt.value IS NODE KEY;
CREATE CONSTRAINT continuous_status_value_type FOR (cs: ContinuousStatus) REQUIRE cs.value :: STRING;
CREATE CONSTRAINT continuous_status_key FOR (cs: ContinuousStatus) REQUIRE cs.value IS NODE KEY;

// Import routes and link them to their agency.
LOAD CSV WITH HEADERS FROM "file:///GTFS/" + $dataset + "/routes.txt" as row
WITH row, COUNT { MATCH (ag: Agency) } as agency_count
CALL {
    WITH row, agency_count
    OPTIONAL MATCH (a: Agency { id: row.agency_id })
    WITH row, agency_count, a
    WHERE (row.route_short_name IS NOT NULL OR row.route_long_name IS NOT NULL) AND                 // We need at least one name
          (agency_count <= 1 OR a IS NOT NULL) AND                                                  // Either there is only one agency or it is specified
          (row.continuous_pickup IS NULL OR row.continuous_pickup IN ["0", "1", "2", "3"]) AND      // Enum values within range
          (row.continuous_drop_off IS NULL OR row.continuous_drop_off IN ["0", "1", "2", "3"]) AND
          row.route_type IN ["0", "1", "2", "3", "4", "5", "6", "7", "11", "12",
                             "100", "101", "102", "103", "104", "105", "106", "107", "108", "109",
                             "110", "111", "112", "113", "114", "115", "116", "117",
                             "200", "201", "202", "203", "204", "205", "206", "207", "208", "209",
                             "400", "401", "402", "403", "404", "405",
                             "700", "701", "702", "703", "704", "705", "706", "707", "708", "709",
                             "710", "711", "712", "713", "714", "715", "716",
                             "800",
                             "900", "901", "902", "903", "904", "905", "906",
                             "1000", "1100", "1200",
                             "1300", "1301", "1302", "1303", "1304", "1305", "1306", "1307",
                             "1400",
                             "1500", "1501", "1502", "1503", "1504", "1505", "1506", "1507",
                             "1700", "1702"]
    CREATE (r: Route {
        id: row.route_id,
        short_name: row.route_short_name,
        long_name: row.route_long_name,
        desc: row.route_desc,
        url: row.route_url,
        color: toUpper(row.route_color),
        text_color: toUpper(row.route_text_color),
        sort_order: toInteger(row.route_sort_order)
    })
    MERGE (rt: RouteType { value: CASE row.route_type
                                  WHEN "0" THEN "Tram"
                                  WHEN "1" THEN "Metro"
                                  WHEN "2" THEN "Rail"
                                  WHEN "3" THEN "Bus"
                                  WHEN "4" THEN "Ferry"
                                  WHEN "5" THEN "Cable Tram"
                                  WHEN "6" THEN "Aerial Lift"
                                  WHEN "7" THEN "Funicular"
                                  WHEN "11" THEN "Trolleybus"
                                  WHEN "12" THEN "Monorail"
                                  WHEN "100" THEN "Railway"
                                  WHEN "101" THEN "High Speed Rail"
                                  WHEN "102" THEN "Long Distance Train"
                                  WHEN "103" THEN "Inter Regional Rail"
                                  WHEN "104" THEN "Car Transport Rail"
                                  WHEN "105" THEN "Sleeper Rail"
                                  WHEN "106" THEN "Regional Rail"
                                  WHEN "107" THEN "Tourist Railway"
                                  WHEN "108" THEN "Rail Shuttle (Within Complex)"
                                  WHEN "109" THEN "Suburban Railway"
                                  WHEN "110" THEN "Replacement Rail"
                                  WHEN "111" THEN "Special Rail"
                                  WHEN "112" THEN "Lorry Transport Rail"
                                  WHEN "113" THEN "All Rail Services"
                                  WHEN "114" THEN "Cross-Country Rail"
                                  WHEN "115" THEN "Vehicle Transport Rail"
                                  WHEN "116" THEN "Rack and Pinion Railway"
                                  WHEN "117" THEN "Additional Rail"
                                  WHEN "200" THEN "Coach"
                                  WHEN "201" THEN "International Coach"
                                  WHEN "202" THEN "National Coach"
                                  WHEN "203" THEN "Shuttle Coach"
                                  WHEN "204" THEN "Regional Coach"
                                  WHEN "205" THEN "Special Coach"
                                  WHEN "206" THEN "Sightseeing Coach"
                                  WHEN "207" THEN "Tourist Coach"
                                  WHEN "208" THEN "Commuter Coach"
                                  WHEN "209" THEN "All Coach Services"
                                  WHEN "400" THEN "Urban Railway"
                                  WHEN "401" THEN "Metro"
                                  WHEN "402" THEN "Underground"
                                  WHEN "403" THEN "Urban Railway"
                                  WHEN "404" THEN "All Urban Railway Services"
                                  WHEN "405" THEN "Monorail"
                                  WHEN "700" THEN "Bus"
                                  WHEN "701" THEN "Regional Bus"
                                  WHEN "702" THEN "Express Bus"
                                  WHEN "703" THEN "Stopping Bus"
                                  WHEN "704" THEN "Local Bus"
                                  WHEN "705" THEN "Night Bus"
                                  WHEN "706" THEN "Post Bus"
                                  WHEN "707" THEN "Special Needs Bus"
                                  WHEN "708" THEN "Mobility Bus"
                                  WHEN "709" THEN "Mobility Bus for Registered Disabled"
                                  WHEN "710" THEN "Sightseeing Bus"
                                  WHEN "711" THEN "Shuttle Bus"
                                  WHEN "712" THEN "School Bus"
                                  WHEN "713" THEN "School and Public Service Bus"
                                  WHEN "714" THEN "Rail Replacement Bus"
                                  WHEN "715" THEN "Demand and Response Bus"
                                  WHEN "716" THEN "All Bus Services"
                                  WHEN "800" THEN "Trolleybus"
                                  WHEN "900" THEN "Tram"
                                  WHEN "901" THEN "City Tram"
                                  WHEN "902" THEN "Local Tram"
                                  WHEN "903" THEN "Regional Tram"
                                  WHEN "904" THEN "Sightseeing Tram"
                                  WHEN "905" THEN "Shuttle Tram"
                                  WHEN "906" THEN "All Tram Services"
                                  WHEN "1000" THEN "Water Transport"
                                  WHEN "1100" THEN "Air"
                                  WHEN "1200" THEN "Ferry"
                                  WHEN "1300" THEN "Aerial Lift"
                                  WHEN "1301" THEN "Telecabin"
                                  WHEN "1302" THEN "Cable Car"
                                  WHEN "1303" THEN "Elevator"
                                  WHEN "1304" THEN "Chair Lift"
                                  WHEN "1305" THEN "Drag Lift"
                                  WHEN "1306" THEN "Small Telecabin"
                                  WHEN "1307" THEN "All Telecabin Services"
                                  WHEN "1400" THEN "Funicular"
                                  WHEN "1500" THEN "Taxi"
                                  WHEN "1501" THEN "Communal Taxi"
                                  WHEN "1502" THEN "Water Taxi"
                                  WHEN "1503" THEN "Rail Taxi"
                                  WHEN "1504" THEN "Bike Taxi"
                                  WHEN "1505" THEN "Licensed Taxi"
                                  WHEN "1506" THEN "Private Hire Service Vehicle"
                                  WHEN "1507" THEN "All Taxi Services"
                                  WHEN "1700" THEN "Miscellaneous"
                                  ELSE "Horse-drawn Carriage" END })    // Value is guaranteed to be 1702.
    MERGE (cp: ContinuousStatus { value: CASE row.continuous_pickup
                                         WHEN "0" THEN "Continuous"
                                         WHEN "2" THEN "Must Phone Agency"
                                         WHEN "3" THEN "Must Coordinate With Driver"
                                         ELSE "Not Continuous" END })   // Value is guaranteed to be NULL or 1
    MERGE (cd: ContinuousStatus { value: CASE row.continuous_drop_off
                                         WHEN "0" THEN "Continuous"
                                         WHEN "2" THEN "Must Phone Agency"
                                         WHEN "3" THEN "Must Coordinate With Driver"
                                         ELSE "Not Continuous" END })     // Value is guaranteed to be NULL or 1
    CREATE (r)-[ht: HAS_ROUTE_TYPE]->(rt)
    CREATE (r)-[hcp: HAS_CONTINUOUS_PICKUP]->(cp)
    CREATE (r)-[hcd: HAS_CONTINUOUS_DROP_OFF]->(cd)
    CREATE (r)-[o: OPERATED_BY]->(a)
    WITH row, r
    WHERE row.network_id IS NOT NULL
    MERGE (n: Network { id: row.network_id })
    CREATE (r)-[bt: BELONGS_TO]->(n)
} IN TRANSACTIONS OF 1000 ROWS;
