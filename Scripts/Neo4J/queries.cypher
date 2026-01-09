
// Type constraints for fields.
CREATE CONSTRAINT cypher_query_name_type FOR (cq: CypherQuery) REQUIRE cq.name :: STRING;
CREATE CONSTRAINT cypher_query_statement_type FOR (cq: CypherQuery) REQUIRE cq.statement :: STRING;

// Primary key and not null constraints.
CREATE CONSTRAINT cypher_query_key FOR (cq: CypherQuery) REQUIRE cq.name IS NODE KEY;

////////////////////////////////////////////////////////
// Helper function that should already be in Neo4J.
////////////////////////////////////////////////////////

MERGE (:CypherQuery {
    name: 'duration_to_string',
    statement: '
WITH $duration AS d
WITH toInteger(round(d.milliseconds / 1000.0, 0, \'HALF_UP\') + 0.0000001) AS total
WITH toInteger(total / 3600) AS hh,
     toInteger((total % 3600) / 60) AS mm,
     total % 60 AS ss
RETURN
    apoc.text.lpad(toString(hh), 2, \'0\') + \':\' +
    apoc.text.lpad(toString(mm), 2, \'0\') + \':\' +
    apoc.text.lpad(toString(ss), 2, \'0\') AS hh_mm_ss
'
});

////////////////////////////////////////////////////////
// Basic queries. These are the foundation for others.
////////////////////////////////////////////////////////

// Find transport services which are active for the given date.
MERGE (:CypherQuery {
    name: 'active_services',
    statement: '
WITH date($curr_date) AS curr_date
CALL {
    WITH curr_date
    MATCH (start_day:Day)<-[:STARTS_ON]-(s:Service)-[:ENDS_ON]->(end_day:Day)
    WHERE start_day.date <= curr_date
      AND end_day.date >= curr_date
      AND (CASE curr_date.dayOfWeek
            WHEN 1 THEN s.monday
            WHEN 2 THEN s.tuesday
            WHEN 3 THEN s.wednesday
            WHEN 4 THEN s.thursday
            WHEN 5 THEN s.friday
            WHEN 6 THEN s.saturday
            WHEN 7 THEN s.sunday
          END)
    RETURN s
    UNION
    WITH curr_date
    MATCH (s:Service)-[:HAS_EXCEPTION {type: \'1\'}]->(:Day {date: curr_date})
    RETURN s
}
WITH DISTINCT s, curr_date
OPTIONAL MATCH (s)-[removedException:HAS_EXCEPTION {type: \'2\'}]->(:Day {date: curr_date})
WITH s, removedException
WHERE removedException IS NULL
RETURN s.id AS service_id
ORDER BY service_id
'
});

// Find all the departure times for a given route, stop and date.
MERGE (:CypherQuery {
    name: 'departure_times',
    statement: '
WITH date($curr_date) AS curr_date
MATCH (cq: CypherQuery {name: \'active_services\'})

WITH cq.statement as statement, curr_date
CALL apoc.cypher.run(statement, {curr_date: curr_date}) YIELD value

WITH value.service_id as service_id
MATCH (r:Route {id: $route_id})<-[:FOLLOWS]-(t:Trip)-[:SCHEDULED_BY]->(s:Service {id: service_id})
MATCH (st:Stop {id: $stop_id})<-[:LOCATED_AT]-(stt:StopTime)-[:PART_OF]->(t)
MATCH (t)-[:HAS_TRAVEL_DIRECTION]->(td:TravelDirection)

RETURN service_id, r.id AS route_id, t.id AS trip_id, td.value as direction, st.id AS stop_id, stt.departure_time as departure_time
ORDER BY departure_time ASC, direction DESC
'
});


////////////////////////////////////////////////////////
// Network inspection and visualization.
////////////////////////////////////////////////////////

// Get the amount of trips, routes and stops in the network every day.
MERGE (:CypherQuery {
    name: 'daily_status',
    statement: '
// Generate a stream of dates within the specified range.
WITH date($start_date) as start_date, date($end_date) as end_date

UNWIND range(0, duration.inDays(start_date, end_date).days) AS i
WITH start_date + duration({days: i}) AS service_date

// For each date, call the custom procedure to get all active service_ids.
MATCH (cq: CypherQuery {name: \'active_services\'})
CALL apoc.cypher.run(cq.statement, {curr_date: service_date}) YIELD value
WITH service_date, collect(value.service_id) as service_ids

// Group by service ids (every date with the same service ids will have the same exact results).
WITH collect(service_date) as service_dates, service_ids

// Find trips and trip counts for that day.
MATCH (t:Trip)-[:SCHEDULED_BY]->(s:Service)
WHERE s.id in service_ids
WITH service_dates, count(DISTINCT t) AS trip_count, collect(t) AS trips

// Find route and stop count for that day.
MATCH (r:Route)<-[:FOLLOWS]-(t:Trip)<-[:PART_OF]-(st:StopTime)-[:LOCATED_AT]->(s:Stop)
WHERE t in trips
WITH service_dates,
     trip_count,
     count(DISTINCT r) AS route_count,
     count(DISTINCT s) AS stop_count

// Order the results chronologically by service_date.
UNWIND service_dates AS service_date
RETURN service_date, trip_count as total_trips, route_count as active_routes, stop_count as active_stops
ORDER BY service_date
'
});

// Get the most important stops for a given date, according to the amount of departures.
MERGE (:CypherQuery {
    name: 'top_stops',
    statement: '
// Step 1: Find all services active on the target date.
WITH date($curr_date) AS curr_date
MATCH (cq: CypherQuery {name: \'active_services\'})
CALL apoc.cypher.run(cq.statement, {curr_date: curr_date}) YIELD value

// Step 2: Traverse from active services to find all stop times and their associated routes.
WITH value.service_id as service_id
MATCH (r:Route)<-[:FOLLOWS]-(t:Trip)-[:SCHEDULED_BY]->(s:Service {id: service_id})

WITH r, t
MATCH (stop:Stop)<-[:LOCATED_AT]-(st:StopTime)-[:PART_OF]->(t)

// Step 3: First aggregation - calculate stats for each individual physical stop node.
// We collect the routes and departure times for each unique stop.
WITH stop,
     r.short_name AS route_short_name,
     r.long_name AS route_long_name,
     st.departure_time AS departure_time

WITH stop,
     count(*) AS departures,
     min(departure_time) AS first_departure,
     max(departure_time) AS last_departure,
     // Collect the short names of all routes serving this specific stop.
     collect(DISTINCT coalesce(route_short_name, route_long_name)) AS routes

// Step 4: Final grouping - combine stops with the same name and same set of routes.
// We use apoc.coll.sort to create a canonical key for the list of routes.
WITH stop.name AS stop_name,
     departures,
     first_departure,
     last_departure,
     apoc.coll.sort(routes) AS sorted_routes
// Now group by the name and the sorted list of routes.
WITH stop_name,
     sorted_routes,
     sum(departures) AS total_departures,
     min(first_departure) AS first_departure,
     max(last_departure) AS last_departure

// Final projection to format the output as requested.
RETURN
    stop_name,
    size(sorted_routes) AS route_count,
    sorted_routes AS routes,
    total_departures,
    first_departure,
    last_departure
ORDER BY total_departures DESC, first_departure ASC, last_departure DESC, stop_name ASC
'
});

// Generate a histogram of trip start times for a given date.
MERGE (:CypherQuery {
    name: 'trip_start_time_distribution',
    statement: '
// Step 1: Find all services active on the target date.
WITH date($curr_date) AS curr_date
MATCH (cq: CypherQuery {name: \'active_services\'})
CALL apoc.cypher.run(cq.statement, {curr_date: curr_date}) YIELD value

// Step 2: For each active service, find its associated trips.
WITH value.service_id as service_id
MATCH (t:Trip)-[:SCHEDULED_BY]->(s:Service {id: service_id})

// Step 3: For each trip, find the StopTime with the minimum stop_sequence.
WITH t
MATCH (t)<-[:PART_OF]-(st:StopTime)
WITH t, st
ORDER BY st.stop_sequence ASC
WITH t, collect(st)[0].departure_time AS fsdt

// Step 4: Transform duration type into total seconds.
WITH fsdt.seconds AS total_seconds
WHERE total_seconds IS NOT NULL

// Step 5: Create buckets according to parameter size.
WITH floor(total_seconds / ($bucket_size_min * 60)) * ($bucket_size_min * 60) AS bucket_in_seconds

// Step 6: Group by the numeric bucket and count the trips.
WITH bucket_in_seconds, count(*) AS trip_count
ORDER BY bucket_in_seconds

// Step 7: Convert the aggregated seconds bucket into a formatted string.
WITH
    trip_count,
    toInteger(floor(bucket_in_seconds / 3600)) AS H,
    toInteger(floor((bucket_in_seconds % 3600) / 60)) AS M,
    toInteger(bucket_in_seconds % 60) AS S

// Final return with the clean "HH:MM:SS" format.
RETURN apoc.text.format(\'%02d:%02d:%02d\', [H, M, S]) AS time_bucket,
       trip_count
'
});

// Generate headway statistics for all routes on a given date.
MERGE (:CypherQuery {
    name: 'headway_stats',
    statement: '
// Step 1: Find all services active on the target date.
WITH date($curr_date) AS curr_date
MATCH (cq: CypherQuery {name: \'active_services\'})
CALL apoc.cypher.run(cq.statement, {curr_date: curr_date}) YIELD value

// Step 2: Find all distinct (route, stop) pairs for trips running today, and all of their departure times.
WITH value.service_id as service_id
MATCH (r:Route)<-[:FOLLOWS]-(t:Trip)-[:SCHEDULED_BY]->(ser:Service {id: service_id})
MATCH (s:Stop)<-[:LOCATED_AT]-(stt:StopTime)-[:PART_OF]->(t)
OPTIONAL MATCH (t)-[:HAS_TRAVEL_DIRECTION]->(td:TravelDirection)

WITH r, s, CASE td.value WHEN "Outbound" THEN 0 WHEN "Inbound" THEN 1 ELSE NULL END as direction_id, stt.departure_time as departure_time
ORDER BY departure_time, direction_id

WITH r, s, direction_id as direction_id, collect(departure_time) as dt
WHERE size(dt) >= 2        // A single departure is not enough to calculate headways.

WITH r, s, direction_id, [i IN range(0, size(dt) - 2) | dt[i+1] - dt[i]] as rs_headways

// Step 3: Aggregate headways per route.
WITH r, apoc.coll.flatten(collect(rs_headways)) as r_headways_merged
UNWIND r_headways_merged as r_headway

WITH r,
     duration({seconds: percentileCont(r_headway.seconds, 0.05)}) AS min_headway,
     duration({seconds: percentileCont(r_headway.seconds, 0.5)}) AS median_headway,
     duration({seconds: percentileCont(r_headway.seconds, 0.95)}) AS max_headway,
     // Adding a small value to the standard deviation avoids rounding issues.
     toInteger(round(stDev(r_headway.seconds), 0, \'HALF_UP\') + 0.0000001) AS stdev_headway

// Step 4: Convert results to strings and return them.
MATCH (cq: CypherQuery {name: \'duration_to_string\'})
CALL apoc.cypher.run(cq.statement, {duration: min_headway}) YIELD value
WITH r, value.hh_mm_ss as min_headway_str, median_headway, max_headway, stdev_headway, cq
CALL apoc.cypher.run(cq.statement, {duration: median_headway}) YIELD value
WITH r, min_headway_str, value.hh_mm_ss as median_headway_str, max_headway, stdev_headway, cq
CALL apoc.cypher.run(cq.statement, {duration: max_headway}) YIELD value
WITH r, min_headway_str, median_headway_str, value.hh_mm_ss as max_headway_str, stdev_headway

RETURN coalesce(r.short_name, r.long_name) AS route_name,
       min_headway_str AS min_headway,
       median_headway_str AS median_headway,
       max_headway_str AS max_headway,
       stdev_headway AS stddev_seconds
ORDER BY route_name, min_headway, median_headway, max_headway, stddev_seconds
'
});

////////////////////////////////////////////////////////
// Network analysis and optimization.
////////////////////////////////////////////////////////

MERGE (:CypherQuery {
    name: 'routes_by_speed',
    statement: '
// Step 1: Find length for each trip shape.
MATCH (s: Shape)
WITH s.id AS shape_id, s.wkt AS wkt
WITH shape_id, substring(wkt, 12, size(wkt)-13) AS coordsText                                       // Strip "LINESTRING(" and trailing ")"
WITH shape_id, [c IN split(coordsText, \',\') | trim(c)] AS coordStrs                               // Split into coordinate strings and trim
WITH shape_id, [s IN coordStrs | split(s, \' \')] AS pairs                                          // Split each "lon lat" into [lon, lat]
WITH shape_id, [p IN pairs | point({longitude: toFloat(p[0]), latitude: toFloat(p[1])})] AS pts     // Turn pairs into proper point objects
WITH shape_id, reduce(total = 0.0, i IN range(0, size(pts)-2) |                                     // Accumulate distances between points
                      total + point.distance(pts[i], pts[i+1])) AS length_meters

// Step 2: Find the corresponding trips and calculate their durations.
MATCH (r:Route)<-[:FOLLOWS]-(t:Trip)-[:HAS_SHAPE]->(s:Shape {id: shape_id})
MATCH (st: StopTime)-[:PART_OF]->(t)
WITH r, t, st, shape_id, length_meters
ORDER BY st.stop_sequence
WITH r, t, collect(st) as sts, shape_id, length_meters
WITH r, t, sts[-1].arrival_time - sts[0].departure_time as trip_length, shape_id, length_meters

// Step 3: Return results aggregated by route.
WITH r.id as route_id,
     coalesce(r.short_name, r.long_name) as route_name,
     COUNT(t) as trip_count,
     round(avg((length_meters / trip_length.seconds) * 3.6), 2, \'HALF_UP\') as avg_speed_kmh

RETURN route_name, trip_count, avg_speed_kmh
ORDER BY avg_speed_kmh DESC, route_name ASC
'
});

// Find the most important routes at a given time of day, according to their frequency and number of active trips.
MERGE (:CypherQuery {
    name: 'routes_by_relevance',
    statement: '
// Step 1: Find all services active on the target date.
WITH date($curr_date) AS curr_date
MATCH (cq: CypherQuery {name: \'active_services\'})
CALL apoc.cypher.run(cq.statement, {curr_date: curr_date}) YIELD value

// Step 2: For each route, find the trips that are active on the target time.
WITH value.service_id as service_id, split($curr_time, \':\') AS time_parts
WITH service_id, toInteger(time_parts[0]) * 3600 + toInteger(time_parts[1]) * 60 + toInteger(time_parts[2]) as curr_time_secs

MATCH (r:Route)<-[:FOLLOWS]-(t:Trip)-[:SCHEDULED_BY]->(ser:Service {id: service_id})
MATCH (st:StopTime)-[:PART_OF]->(t)

WITH r, t, st, curr_time_secs
ORDER BY st.stop_sequence

WITH r, t, collect(st) as sts, curr_time_secs
WHERE sts[0].departure_time.seconds <= curr_time_secs AND
      curr_time_secs <= sts[-1].arrival_time.seconds

// Step 3: For each route, count the active trips and find the average frequency.
WITH r, t, sts[0].departure_time as dt
ORDER BY id(r), dt

WITH r.id as route_id,
     coalesce(r.short_name, r.long_name) as route_name,
     COUNT(t) as active_trip_count,
     collect(dt) as dts

WITH route_id, route_name, active_trip_count, [i IN range(0, size(dts) - 2) | dts[i+1] - dts[i]] as route_headways

UNWIND CASE route_headways <> [] WHEN true THEN route_headways ELSE [null] END as rhs
WITH route_id, route_name, active_trip_count, avg(rhs) as avg_frequency
RETURN route_name, active_trip_count, avg_frequency
ORDER BY active_trip_count DESC, avg_frequency ASC, route_name ASC
'
});

// Determine which segments of the network are covered by the most routes, which might be a sign of redundant planning.
MERGE (:CypherQuery {
    name: 'overlapping_segments',
    statement: '
MATCH (s:Stop)<-[:LOCATED_AT]-(st:StopTime)-[:PART_OF]->(t:Trip)
WITH t, s, st
ORDER BY st.stop_sequence

WITH t, apoc.coll.pairsMin(collect(s)) as stop_sequences

MATCH (t:Trip)-[:FOLLOWS]->(r:Route)
UNWIND stop_sequences as stop_pair
WITH DISTINCT r, stop_pair[0] as from_stop, stop_pair[1] as to_stop

WITH from_stop, to_stop, apoc.coll.sort(collect(r.short_name)) AS routes

ORDER BY size(routes) DESC, from_stop.name, to_stop.name

RETURN
    from_stop.name AS from_stop,
    to_stop.name AS to_stop,
    size(routes) AS route_count,
    routes
'
});

////////////////////////////////////////////////////////
// User-oriented queries.
////////////////////////////////////////////////////////

// Make sure the spatial index is initialized.
// This is needed to prevent write errors when running the stops_within_distance query.
CALL spatial.withinDistance('stops', {lat: 0.0, lon: 0.0}, 1.0) YIELD node, distance
FINISH;

// NOTE: The third argument for spatial.withinDistance is distance in kilometers, so we divide by 1000.
MERGE (:CypherQuery {
    name: 'stops_within_distance',
    statement: '
CALL spatial.withinDistance(\'stops\', {lat: $origin_lat, lon: $origin_lon}, $seek_dist / 1000.0) YIELD node, distance
WITH node as stop, toInteger(distance * 1000.0) as distance, distance as distance_unrounded
RETURN stop.id as id, stop.name as name, distance
ORDER BY distance_unrounded, name
'
});

// Get the next departures for a given stop, date and time.
MERGE (:CypherQuery {
    name: 'next_departures',
    statement: '
// Step 1: Set up parameters and parse the input time into total seconds.
WITH date($curr_date) AS curr_date

// Step 2: Filter for services that are active on the target date.
MATCH (cq: CypherQuery {name: \'active_services\'})
CALL apoc.cypher.run(cq.statement, {curr_date: curr_date}) YIELD value

// Step 3: Find the starting stop and traverse to its departures and related entities.
WITH value.service_id as service_id
MATCH (stop:Stop {id: $stop_id})<-[:LOCATED_AT]-(st:StopTime)-[:PART_OF]-(t:Trip)-[:SCHEDULED_BY]->(s:Service {id: service_id})

WITH t, st
MATCH (t)-[:FOLLOWS]->(r:Route)

// Step 4: Convert departure time to seconds and filter for times after the current time.
WITH r, t, st, split($curr_time, \':\') AS time_parts
WITH r, t, st,
     toInteger(time_parts[0]) * 3600 + toInteger(time_parts[1]) * 60 + toInteger(time_parts[2]) AS current_time_seconds,
     st.departure_time.seconds AS departure_seconds
WHERE departure_seconds >= current_time_seconds

// Step 5: Order by the true departure time and format the final output.
// It is crucial to order by the original departure_seconds, not the wrapped-around time.
WITH r, t, departure_seconds
ORDER BY departure_seconds ASC, t.headsign ASC

// Calculate the wrapped-around time in seconds for a 24-hour clock.
WITH r, t, departure_seconds % 86400 AS display_seconds

// Final return with the clean "HH:MM:SS" format.
RETURN
    coalesce(r.short_name, r.long_name) AS route,
    t.headsign AS destination,
    duration({seconds: display_seconds}) AS time
'
});
