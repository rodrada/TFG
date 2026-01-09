
--------------------------------------------------------
-- Basic queries. These are the foundation for others.
--------------------------------------------------------


-- Find transport services which are active for the given date.
CREATE OR REPLACE FUNCTION active_services(curr_date DATE)
RETURNS TABLE(service_id TEXT)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT s.service_id
    FROM active_services(curr_date, curr_date) s;
END
$$;

-- Find transport services which are active for the given date range.
CREATE OR REPLACE FUNCTION active_services(start_date DATE, end_date DATE)
RETURNS TABLE(service_date DATE, service_id TEXT)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY

    WITH days_and_services AS (
        SELECT d::date as service_date, s.service_id
        FROM
            generate_series(start_date, end_date, interval '1 day') d
            JOIN service s ON
                s.start_date <= d AND
                s.end_date >= d AND
                -- Make sure the service is running on the current day of week.
                (CASE extract(dow FROM d)
                    WHEN 0 THEN sunday
                    WHEN 1 THEN monday
                    WHEN 2 THEN tuesday
                    WHEN 3 THEN wednesday
                    WHEN 4 THEN thursday
                    WHEN 5 THEN friday
                    WHEN 6 THEN saturday
                END)
    ),

    added_services AS (
        SELECT se1.date as service_date, se1.service_id
        FROM service_exception se1
        WHERE
            se1.date >= start_date AND
            se1.date <= end_date AND
            se1.exception_type = 1
    ),

    removed_services AS (
        SELECT se2.date as service_date, se2.service_id
        FROM service_exception se2
        WHERE
            se2.date >= start_date AND
            se2.date <= end_date AND
            se2.exception_type = 2
    )

    SELECT * FROM days_and_services
    UNION (SELECT * FROM added_services)
    EXCEPT (SELECT * FROM removed_services)
    ORDER BY service_date, service_id;
END
$$;

-- Find all the departure times for a given route, stop and date.
CREATE OR REPLACE FUNCTION departure_times(route_id_input TEXT, stop_id_input TEXT, curr_date DATE)
RETURNS TABLE (
    service_id TEXT,
    route_id TEXT,
    trip_id TEXT,
    direction TEXT,
    stop_id TEXT,
    departure_time INTERVAL
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY

    -- Step 1: Find all services that are active on the current date.
    WITH services_today AS (SELECT a.service_id FROM active_services(curr_date) a)

    -- Step 2: Join everything and extract the required information.
    SELECT
        t.service_id,
        t.route_id,
        t.trip_id,
        td.name as direction,
        st.stop_id,
        st.departure_time
    FROM
        stop_time st
        JOIN trip t ON t.trip_id = st.trip_id
        JOIN services_today sv ON sv.service_id = t.service_id
        JOIN travel_direction td ON td.id = t.direction_id
    WHERE
        t.route_id = route_id_input
        AND st.stop_id = stop_id_input
    ORDER BY st.departure_time ASC, direction DESC;
END
$$;

--------------------------------------------------------
-- Network inspection and visualization.
--------------------------------------------------------

-- Additional table needed for the next query.
-- Contains pairs (d, s), where d is the dates and s is the services active in those dates.
-- TODO: Optimize this, since it's very slow if a dataset contains few services but in a very long period (e.g. 100 years).
CREATE TABLE day_service_sets AS (
    -- Group dates with the same services.
    SELECT
        array_agg(DISTINCT service_date ORDER BY service_date) AS service_dates,
        service_set
    FROM (
        -- Group services by date.
        SELECT
            ds.service_date,
            array_agg(DISTINCT ds.service_id ORDER BY ds.service_id) AS service_set
        FROM (
            -- Get active services for every day in the service period.
            WITH dates AS (
                SELECT start_date as date FROM service
                UNION DISTINCT
                SELECT end_date as date FROM service
                UNION DISTINCT
                SELECT date FROM service_exception
            )

            SELECT * FROM active_services(
                (SELECT min(date) FROM dates),
                (SELECT max(date) FROM dates)
            )
        ) ds
        GROUP BY ds.service_date
    )
    GROUP BY service_set
);

CREATE INDEX idx_day_sets ON day_service_sets USING GIN (service_dates);
CREATE INDEX idx_service_sets ON day_service_sets USING GIN (service_set);

-- Get the amount of trips, routes and stops in the network every day.
CREATE OR REPLACE FUNCTION daily_status(start_date DATE, end_date DATE)
RETURNS TABLE(
    service_date DATE,
    total_trips BIGINT,
    active_routes BIGINT,
    active_stops BIGINT
)
LANGUAGE plpgsql
AS $$
DECLARE
    days date[];
BEGIN
    days := ARRAY(SELECT generate_series(start_date, end_date, interval '1 day')::date);

    RETURN QUERY
    WITH results AS (
        SELECT
            UNNEST(dss.service_dates) AS service_date,
            COUNT(DISTINCT t.trip_id) AS total_trips,
            COUNT(DISTINCT r.route_id) AS active_routes,
            COUNT(DISTINCT st.stop_id) AS active_stops
        FROM
            day_service_sets dss
            JOIN trip t ON ARRAY[t.service_id] <@ dss.service_set
            JOIN route r ON r.route_id = t.route_id
            JOIN stop_time st ON st.trip_id = t.trip_id
        WHERE
            dss.service_dates && days
        GROUP BY dss.service_dates
        ORDER BY service_date
    )

    SELECT * FROM results r WHERE r.service_date BETWEEN start_date AND end_date;
END
$$;


-- Get the most important stops for a given date, according to the amount of departures.
CREATE OR REPLACE FUNCTION top_stops(curr_date DATE)
RETURNS TABLE(
  stop_name TEXT,
  route_count INTEGER,
  routes TEXT[],
  total_departures BIGINT,
  first_departure INTERVAL,
  last_departure INTERVAL
)
LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY

    -- Step 1: Find all services that are active on the current date.
    WITH services_today AS (SELECT service_id FROM active_services(curr_date)),

    -- Step 2: Map stops to routes that serve them today.
    stops_and_routes AS (
        SELECT DISTINCT st.stop_id, t.route_id
        FROM
            stop_time st
            JOIN trip t ON st.trip_id = t.trip_id
            JOIN services_today s ON t.service_id = s.service_id
    ),

    -- Step 2: Aggregate departures per stop.
    stop_departure_stats AS (
        SELECT
            st.stop_id,
            COUNT(*)::INTEGER AS total_departures,
            MIN(st.departure_time) AS first_departure,
            MAX(st.departure_time) AS last_departure
        FROM
            stop_time st
            JOIN trip t ON st.trip_id = t.trip_id
            JOIN services_today s ON t.service_id = s.service_id
        GROUP BY st.stop_id
    ),

    -- Step 3: Combine stops with their route info and departure stats.
    stop_with_routes AS (
        SELECT
            s.stop_id,
            s.stop_name,
            ARRAY_AGG(DISTINCT COALESCE(r.route_short_name, r.route_long_name) ORDER BY COALESCE(r.route_short_name, r.route_long_name)) AS routes,
            sds.total_departures,
            sds.first_departure,
            sds.last_departure
        FROM
            stops_and_routes sar
            JOIN stop s ON s.stop_id = sar.stop_id
            JOIN route r ON r.route_id = sar.route_id
            JOIN stop_departure_stats sds ON sds.stop_id = sar.stop_id
        GROUP BY s.stop_id, s.stop_name, sds.total_departures, sds.first_departure, sds.last_departure
    )

    -- Step 4: Final grouping by name and route set.
    -- We consider all stops with the same name and routes as the same.
    SELECT
        swr.stop_name,
        CARDINALITY(swr.routes) AS route_count,
        swr.routes,
        SUM(swr.total_departures) AS total_departures,
        MIN(swr.first_departure) AS first_departure,
        MAX(swr.last_departure) AS last_departure
    FROM stop_with_routes swr
    GROUP BY swr.stop_name, swr.routes
    ORDER BY total_departures DESC, first_departure ASC, last_departure DESC, stop_name ASC;
END
$$;


-- Generate a histogram of trip start times for a given date.
CREATE OR REPLACE FUNCTION trip_start_time_distribution(curr_date DATE, bucket_size_min INT)
RETURNS TABLE(time_bucket INTERVAL, trip_count BIGINT)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY

    -- Step 1: Find all services that are active on the current date.
    WITH services_today AS (SELECT service_id FROM active_services(curr_date)),

    -- Step 2: Find the exact start time for every trip in the system.
    -- We could use stop_sequence = 1, but that may cause problems if a trip starts with stop_sequence = 0.
    trip_start_times AS (
        SELECT DISTINCT ON (trip_id)
            trip_id,
            departure_time AS start_time
        FROM stop_time
        ORDER BY trip_id, stop_sequence ASC
    )

    -- Final step: Join, filter, bucket, and count the trip departures.
    SELECT
        -- Generate the time buckets and count the amount of trips in each one.
        (FLOOR(EXTRACT(EPOCH FROM tst.start_time) / (bucket_size_min * 60)) * (bucket_size_min * interval '1 minute')) AS time_bucket,
        COUNT(*) AS trips_starting
    FROM trip AS t
         -- Join to filter for only trips running on our given date.
         JOIN services_today AS s ON t.service_id = s.service_id
         -- Join to get the pre-calculated start time for each trip.
         JOIN trip_start_times AS tst ON t.trip_id = tst.trip_id
    GROUP BY time_bucket
    ORDER BY time_bucket;
END
$$;


-- Generate headway statistics for all routes on a given date.
-- NOTE: This query is intended for datasets *without* frequencies.txt.
--       If they have it, results won't be precise.
CREATE OR REPLACE FUNCTION headway_stats(curr_date DATE)
RETURNS TABLE (
    route_name TEXT,
    min_headway INTERVAL,
    median_headway INTERVAL,
    max_headway INTERVAL,
    stddev_seconds NUMERIC
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY

    -- Step 1: Find all services that are active on the current date.
    WITH services_today AS (SELECT service_id FROM active_services(curr_date)),

    -- Step 2: Find all distinct (route, stop) pairs for trips running today.
    --         After that, calculate headways for each stop time.
    stop_headways AS (
        SELECT
            t.route_id,
            st.stop_id,
            t.direction_id,
            LEAD(st.departure_time) OVER (
                PARTITION BY t.route_id, t.direction_id, st.stop_id
                ORDER BY st.departure_time
            ) - st.departure_time AS headway
        FROM
            trip t
            JOIN services_today sv ON sv.service_id = t.service_id
            JOIN stop_time st ON st.trip_id = t.trip_id
    ),

    -- Step 3: Aggregate headways per route.
    route_agg AS (
        SELECT
            sh.route_id,
            make_interval(secs => ROUND(PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM sh.headway)))) AS min_headway,
            make_interval(secs => ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM sh.headway)))) AS median_headway,
            make_interval(secs => ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM sh.headway)))) AS max_headway,
            -- The coalesce is meant to deal with cases in which there is a single headway (STDDEV_SAMP returns NULL).
            COALESCE(STDDEV_SAMP(EXTRACT(EPOCH FROM sh.headway)), 0.0) AS stddev_secs
        FROM stop_headways sh
        GROUP BY sh.route_id
    )

    -- Step 4: Return results.
    SELECT
        COALESCE(r.route_short_name, r.route_long_name) AS route_name,
        a.min_headway,
        a.median_headway,
        a.max_headway,
        ROUND(a.stddev_secs) AS stddev_seconds
    FROM
        route r
        LEFT JOIN route_agg a USING (route_id)
    WHERE
        a.min_headway IS NOT NULL
        AND a.median_headway IS NOT NULL
        AND a.max_headway IS NOT NULL
    ORDER BY route_name, min_headway, median_headway, max_headway, stddev_seconds;
END;
$$;


--------------------------------------------------------
-- Network analysis and optimization.
--------------------------------------------------------


-- Get a ranking of the routes linked by average speed (considering the average of all of their trips).
CREATE OR REPLACE FUNCTION routes_by_speed()
RETURNS TABLE(route_name TEXT, trip_count BIGINT, avg_speed_kmh NUMERIC(5, 2), route_geom GEOMETRY, route_color CHAR(6))
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY

    -- Step 1: Calculate the length of each shape in meters using PostGIS.
    --         Use sphere instead of spheroid (false) to match Neo4J results.
    WITH shape_lengths AS (
        SELECT shape_id, shape_geom, ST_LENGTH(shape_geom::geography, false) AS length_meters
        FROM shape
    ),

    -- Step 2: Calculate the scheduled duration of each trip in seconds.
    trip_durations AS (
        SELECT DISTINCT
            trip_id,
            EXTRACT(
                EPOCH FROM (
                    LAST_VALUE(arrival_time::interval) OVER trip_window -
                    FIRST_VALUE(departure_time::interval) OVER trip_window
                )
            ) AS duration_seconds
        FROM stop_time
        WINDOW trip_window AS (
            PARTITION BY trip_id
            ORDER BY stop_sequence
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )
    )

    -- Final step: Join the data, calculate speed for each trip, and average it by route.
    SELECT
        COALESCE(r.route_short_name, r.route_long_name) AS route_name,
        COUNT(DISTINCT t.trip_id) AS trip_count,
        -- Calculate the average speed and convert it to kilometers per hour (m/s * 3.6).
        AVG((sl.length_meters / td.duration_seconds) * 3.6)::numeric(5, 2) AS avg_speed_kmh,
        -- All shapes should be very similar, so just take one of them.
        (ARRAY_AGG(sl.shape_geom))[1] AS shape_geom,
        r.route_color AS route_color
    FROM
        route AS r
        JOIN trip AS t ON r.route_id = t.route_id
        JOIN shape_lengths AS sl ON t.shape_id = sl.shape_id
        JOIN trip_durations AS td ON t.trip_id = td.trip_id
    WHERE td.duration_seconds > 0                               -- Avoid any division-by-zero errors.
    GROUP BY r.route_id, r.route_short_name, r.route_long_name
    ORDER BY avg_speed_kmh DESC, route_name;
END
$$;

-- Find the most important routes at a given time of day, according to their frequency and number of active trips.
-- TODO: Optimize this. There is no need for trip_times to be calculated for every trip, just the active ones.
CREATE OR REPLACE FUNCTION routes_by_relevance(curr_date DATE, curr_time INTERVAL)
RETURNS TABLE(route_name TEXT, active_trip_count BIGINT, avg_frequency INTERVAL, route_geom GEOMETRY)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY

    -- Step 1: Find all services that are active on the current date.
    WITH services_today AS (SELECT service_id FROM active_services(curr_date)),

    -- Step 2: Calculate the start and end time for every trip in the system.
    trip_times AS (
        SELECT DISTINCT
            trip_id,
            FIRST_VALUE(departure_time::interval) OVER trip_window AS start_time,
            LAST_VALUE(arrival_time::interval) OVER trip_window AS end_time
        FROM stop_time
        WINDOW trip_window AS (
            PARTITION BY trip_id
            ORDER BY stop_sequence
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )
    ),

    -- Step 3: Calculate the average frequency for each route.
    avg_frequencies AS (
        SELECT
            route_id,
            -- Calculate the average of all headways.
            AVG(next_trip_start_time - start_time) AS avg_headway
        FROM (
            -- Subquery to find the start time of the 'next' trip on the same route.
            SELECT
                t.route_id,
                tt.start_time,
                LEAD(tt.start_time, 1) OVER (PARTITION BY t.route_id ORDER BY tt.start_time) AS next_trip_start_time
            FROM trip AS t
                 JOIN services_today AS s ON t.service_id = s.service_id
                 JOIN trip_times AS tt ON t.trip_id = tt.trip_id
            WHERE curr_time BETWEEN tt.start_time AND tt.end_time
        ) AS trip_sequences
        -- Exclude the last trip of the day for each route, which has no 'next' trip.
        WHERE next_trip_start_time IS NOT NULL
        GROUP BY route_id
    ),

    -- Step 4: Get a representative shape for each route.
    route_shapes AS (
        SELECT DISTINCT ON (t.route_id)
            t.route_id,
            s.shape_geom
        FROM trip AS t
        JOIN shape AS s ON t.shape_id = s.shape_id
        -- It doesn't matter which shape we get, so we just take the first one.
        ORDER BY t.route_id
    )

    -- Final step: Join everything, filter for active trips, and count them per route.
    SELECT
        COALESCE(r.route_short_name, r.route_long_name) AS route_name,
        COUNT(t.trip_id) AS active_trip_count,
        DATE_TRUNC('second', h.avg_headway) AS avg_frequency,
        rs.shape_geom AS route_geom
    FROM route AS r
         JOIN trip AS t ON r.route_id = t.route_id
         JOIN services_today AS s ON t.service_id = s.service_id
         JOIN trip_times AS tt ON t.trip_id = tt.trip_id
         -- Use a LEFT JOIN in case a route has only one trip today (no headway to calculate).
         LEFT JOIN avg_frequencies h ON r.route_id = h.route_id
         -- Use a LEFT JOIN to ensure we still get routes even if they don't have a shape.
         LEFT JOIN route_shapes rs ON r.route_id = rs.route_id
    WHERE curr_time BETWEEN tt.start_time AND tt.end_time
    GROUP BY r.route_id, r.route_short_name, r.route_long_name, h.avg_headway, rs.shape_geom
    ORDER BY active_trip_count DESC, avg_frequency ASC, route_name ASC;
END
$$;

-- Determine which segments of the network are covered by the most routes, which might be a sign of redundant planning.
CREATE OR REPLACE FUNCTION overlapping_segments()
RETURNS TABLE(from_stop TEXT, to_stop TEXT, route_count BIGINT, routes TEXT[])
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY

    -- Step 1: Create a complete list of all directed segments.
    -- A segment is the connection between one stop and the very next stop on a trip.
    -- We use the LEAD() window function to find the 'next' stop_id efficiently.
    WITH trip_segments AS (
        SELECT
            trip_id,
            stop_id AS from_stop_id,
            LEAD(stop_id, 1) OVER (PARTITION BY trip_id ORDER BY stop_sequence) AS to_stop_id
        FROM stop_time
    ),

    -- Step 2: Link each segment to its route_id.
    -- This de-duplicates the data so we only have one entry per route for each segment,
    -- regardless of how many trips that route makes along that segment.
    segment_routes AS (
        SELECT DISTINCT
            ts.from_stop_id,
            ts.to_stop_id,
            t.route_id
        FROM trip_segments AS ts
            JOIN trip AS t ON ts.trip_id = t.trip_id
        WHERE ts.to_stop_id IS NOT NULL -- Exclude the last stop of each trip, which has no 'next' stop.
    )

    -- Final step: Group the segments and count the distinct routes.
    -- We also aggregate the route names into an array for easy inspection.
    SELECT
        s1.stop_name AS from_stop,
        s2.stop_name AS to_stop,
        COUNT(sr.route_id) AS route_count,
        -- Collect the short names of all overlapping routes into a sorted array.
        array_agg(r.route_short_name ORDER BY r.route_short_name) AS routes
    FROM segment_routes AS sr
         -- Join to the stops table twice to get the names for the start and end of the segment.
         JOIN stop AS s1 ON sr.from_stop_id = s1.stop_id
         JOIN stop AS s2 ON sr.to_stop_id = s2.stop_id
         -- Join to the routes table to get the route names for the array.
         JOIN route AS r ON sr.route_id = r.route_id
    GROUP BY sr.from_stop_id, sr.to_stop_id, s1.stop_name, s2.stop_name
    -- Order the results to show the most heavily overlapped segments first.
    ORDER BY route_count DESC, from_stop, to_stop;
END
$$;

-- Divide the network into hexagons and count the stops in each one.
CREATE OR REPLACE FUNCTION stop_density_heatmap(grid_size_meters INT)
RETURNS TABLE(
    stop_count BIGINT,
    hexagon_geom GEOMETRY(Polygon, 4326)
)
LANGUAGE plpgsql
AS $$
DECLARE
    -- Dynamically determine the best SRID for a meter-based projection (UTM).
    utm_srid INTEGER;
BEGIN
    -- Step 1: Find the centroid of all stops to determine the correct UTM zone.
    -- This makes the function portable to any city in the world.
    SELECT
        -- Formula for UTM Zone SRID: 32600 + zone number for Northern Hemisphere, 32700 for Southern.
        CASE
            WHEN ST_Y(centroid) > 0 THEN 32600
            ELSE 32700
        END + floor((ST_X(centroid) + 180) / 6) + 1
    INTO utm_srid
    FROM (
        SELECT ST_Centroid(ST_Collect(location)) AS centroid FROM stop
    ) AS s;

    -- Step 2: Main query using the dynamically found SRID.
    RETURN QUERY
    -- We will do all our work in the projected, meter-based system.
    WITH projected_stops AS (
        SELECT ST_Transform(location, utm_srid) AS geom
        FROM stop
    ),

    -- Step 3: Define the grid bounds in the projected system.
    bounds AS (
        SELECT ST_Expand(ST_Collect(geom), grid_size_meters) AS geom
        FROM projected_stops
    ),

    -- Step 4: Generate the hexagon grid in meters.
    hexagons AS (
        SELECT h.geom
        FROM bounds, ST_HexagonGrid(grid_size_meters, bounds.geom) AS h
    )

    -- Final step: Count stops in each hexagon and transform the grid back to lat/lon.
    SELECT
        COUNT(ps.geom) AS stop_count,
        -- Transform the result back to 4326 for easy mapping in tools like QGIS.
        ST_Transform(h.geom, 4326)::GEOMETRY(Polygon, 4326) AS hexagon_geom
    FROM
        hexagons h
        LEFT JOIN projected_stops ps ON ST_Intersects(ps.geom, h.geom)
    GROUP BY
        h.geom
    ORDER BY
        stop_count DESC;
END;
$$;

-- Determine how direct each route is, which can suggest redundant planning (too straight) or inefficiencies (too curvy).
-- TODO: Implement this in Neo4J.
CREATE OR REPLACE FUNCTION route_straightness()
RETURNS TABLE(
    route_name TEXT,
    straightness_index NUMERIC(3, 2),
    route_length_km NUMERIC(8, 2),
    direct_distance_km NUMERIC(8, 2),
    route_geom GEOMETRY
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY

    -- Step 1: Get a single, representative shape for each route and its geometry.
    WITH route_shapes AS (
        SELECT DISTINCT ON (t.route_id)
            t.route_id,
            s.shape_geom
        FROM trip t
        JOIN shape s ON t.shape_id = s.shape_id
        ORDER BY t.route_id -- It doesn't matter which shape we get.
    ),

    -- Step 2: Calculate path length and direct distance for each shape.
    shape_calcs AS (
        SELECT
            rs.route_id,
            -- Calculate the actual path length in meters.
            ST_Length(rs.shape_geom::geography, true) AS path_length_meters,
            -- Calculate the straight-line distance between start and end.
            ST_Distance(
                ST_StartPoint(rs.shape_geom)::geography,
                ST_EndPoint(rs.shape_geom)::geography,
                true
            ) AS direct_dist_meters,
            rs.shape_geom
        FROM route_shapes rs
    )

    -- Final step: Join, calculate the index, and format for output.
    SELECT
        COALESCE(r.route_short_name, r.route_long_name) AS route_name,
        (sc.direct_dist_meters / sc.path_length_meters)::NUMERIC(3, 2) AS straightness_index,
        -- Convert meters to kilometers for readability.
        (sc.path_length_meters / 1000)::NUMERIC(8, 2) AS route_length_km,
        (sc.direct_dist_meters / 1000)::NUMERIC(8, 2) AS direct_distance_km,
        sc.shape_geom
    FROM
        route r
        JOIN shape_calcs sc ON r.route_id = sc.route_id
    WHERE
        sc.direct_dist_meters > 0 -- Avoid loop routes.
    ORDER BY
        straightness_index DESC, route_name;
END;
$$;


--------------------------------------------------------
-- User-oriented queries.
--------------------------------------------------------


-- Locate stops within a given distance of a certain point.
CREATE OR REPLACE FUNCTION stops_within_distance(origin_lat FLOAT, origin_lon FLOAT, seek_dist FLOAT)
RETURNS TABLE(id TEXT, name TEXT, lat FLOAT, lon FLOAT, distance NUMERIC(8, 0), geom GEOMETRY(Point, 4326))
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    WITH user_location AS (SELECT ST_SetSRID(ST_MakePoint(origin_lon, origin_lat), 4326) AS point)
    SELECT
        stop_id,
        stop_name,
        ST_Y(location) AS lat,
        ST_X(location) AS lon,
        -- Use sphere instead of spheroid (false) to match Neo4J results.
        ST_Distance(location::geography, user_location.point::geography, false)::NUMERIC(8, 0) AS distance,
        location AS geom
    FROM stop, user_location
    WHERE
        ST_DWithin(
            location::geography,
            user_location.point::geography,
            seek_dist,
            false
        )
    -- Order by distance *before* rounding.
    ORDER BY ST_Distance(location::geography, user_location.point::geography, false), stop_name;
END
$$;

-- Get the next departures for a given stop, date and time.
CREATE OR REPLACE FUNCTION next_departures(stop_id TEXT, curr_date DATE, curr_time INTERVAL)
RETURNS TABLE(route TEXT, destination TEXT, "time" INTERVAL)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        COALESCE(r.route_short_name, r.route_long_name) AS route,
        t.trip_headsign AS destination,
        ((EXTRACT(EPOCH FROM st.departure_time) % 86400) * '1 second'::INTERVAL) AS "time"  -- Periods get converted to <24:00.
    FROM stop_time st
        JOIN trip t ON st.trip_id = t.trip_id
        JOIN route r ON t.route_id = r.route_id
    WHERE
        st.stop_id = next_departures.stop_id AND
        t.service_id IN (SELECT active_services(next_departures.curr_date)) AND
        st.departure_time >= next_departures.curr_time
    ORDER BY st.departure_time, destination;
END
$$;


--------------------------------------------------------
-- Reachability and shortest paths.
-- Without a doubt, the hardest query section.
--------------------------------------------------------


-- Materialized connection view for the CSA.
-- TODO: Fix this so that it doesn't depend on stop_sequence - 1 (it's not mandatory according to the GTFS spec)
CREATE MATERIALIZED VIEW connections AS
SELECT
    t.trip_id,
    t.service_id,
    st1.stop_id AS departure_stop_id,
    st2.stop_id AS arrival_stop_id,
    st1.departure_time,
    st2.arrival_time,
    s1.serial AS departure_stop_idx,
    s2.serial AS arrival_stop_idx
FROM
    stop_time st1
    JOIN stop_time st2 ON st1.trip_id = st2.trip_id AND st1.stop_sequence = st2.stop_sequence - 1
    JOIN stop s1 ON st1.stop_id = s1.stop_id
    JOIN stop s2 ON st2.stop_id = s2.stop_id
    JOIN trip t ON st1.trip_id = t.trip_id
ORDER BY
    st1.departure_time;

CREATE INDEX idx_connections_service_id ON connections (service_id);
CREATE INDEX idx_connections_departure_time ON connections (departure_time);

ANALYZE connections;

-- Materialized view for the neighbors of each stop.
CREATE MATERIALIZED VIEW neighbor_stops AS (
    SELECT
        s1.serial AS stop_idx_1,
        s2.serial AS stop_idx_2,
        ST_Distance(s1.location::geography, s2.location::geography, false) AS distance_meters
    FROM
        stop s1, stop s2
    WHERE
        s1.stop_id <> s2.stop_id                                                        -- Prevent self-pairs (A,A).
        AND ST_DWithin(s1.location::geography, s2.location::geography, 1000, false)     -- We consider a maximum of 1000 meters.
    ORDER BY
        s1.serial,
        s2.serial
);

CREATE INDEX idx_neighbor_stops_stop_idx_1 ON neighbor_stops USING BTREE (stop_idx_1);
CREATE INDEX idx_neighbor_stops_distance_meters ON neighbor_stops USING BTREE (distance_meters);

ANALYZE neighbor_stops;

-- Multiple types to store as much information as possible in local variables.
CREATE TYPE NEIGHBOR_ENTRY AS (
    stop_idx_2 INTEGER,
    distance_meters FLOAT
);

CREATE TYPE NEIGHBOR_TABLE AS (
    val NEIGHBOR_ENTRY[]
);

CREATE TYPE TRANSFER_ENTRY AS (
    to_stop_idx INTEGER,
    min_transfer_time_secs INTEGER
);

CREATE TYPE TRANSFER_TABLE AS (
    val TRANSFER_ENTRY[]
);

CREATE TYPE REACHABILITY_RESULT AS (
    earliest_arrival_time INTERVAL,
    previous_stop_id INTEGER,
    trip_id_used TEXT
);

-- Function that finds the minimum time required to reach each stop from a given origin stop.
-- It uses a modified CSA algorithm (CSA for trips, Dijkstra-like expansions for walking and transfers).
CREATE OR REPLACE FUNCTION earliest_arrivals(
    origin_stop_id TEXT,
    departure_date DATE,
    departure_time INTERVAL,
    max_distance_walked_meters NUMERIC = 500,
    walking_speed_mps NUMERIC = 1.4,
    destination_stop_serial INTEGER = NULL      -- Useful for ending the algorithm early if we just want to find a single shortest path.
)
RETURNS TABLE(
    stop_id TEXT,
    earliest_arrival_time INTERVAL,
    previous_stop_id TEXT,
    trip_id_used TEXT,
    stop_geom GEOMETRY(Point, 4326)
)
LANGUAGE plpgsql
AS $$
DECLARE
    idx INTEGER;
    results REACHABILITY_RESULT[];
    neighbors NEIGHBOR_TABLE[];
    transfers TRANSFER_TABLE[];
    conn RECORD;
    conf_stop RECORD;
    nei NEIGHBOR_ENTRY;
    transfer TRANSFER_ENTRY;
    last_scanned_departure_time INTERVAL;
    new_arrival_via_transfer INTERVAL;
    new_arrival_via_walk INTERVAL;
    stop_count INTEGER;
    confirmed_stop_count INTEGER;
BEGIN
    -- Step 1: Initialize data structures.
    -- Array containing results that gets built as the algorithm progresses.
    SELECT array_agg(('infinity'::INTERVAL, NULL::TEXT, NULL::TEXT)::REACHABILITY_RESULT)
    INTO results
    FROM "stop" s;

    -- Temporary table that is used for range queries (to determine stops whose optimal path has been already found).
    CREATE TEMP TABLE pqueue (
        stop_idx INTEGER,
        arrival_time INTERVAL
    ) ON COMMIT DROP;

    INSERT INTO pqueue
    SELECT s.serial, 'infinity'::INTERVAL
    FROM "stop" s;

    CREATE UNIQUE INDEX idx_pqueue_stop_idx ON pqueue USING BTREE (stop_idx);
    CREATE INDEX idx_pqueue_arrival_time ON pqueue USING BTREE (arrival_time);

    -- Counters to stop the algorithm early if possible.
    stop_count := (SELECT CARDINALITY(results));
    confirmed_stop_count := 0;

    -- We get neighbors and transfers in a single query to prevent multiple ones when the results get "confirmed".
    neighbors := (
        WITH filtered_neighbors AS (
            SELECT
                s.serial AS stop_idx_1,
                ROW(
                    COALESCE(
                        array_agg(ROW(ns.stop_idx_2, ns.distance_meters)::NEIGHBOR_ENTRY ORDER BY ns.distance_meters ASC) FILTER (WHERE ns.stop_idx_2 IS NOT NULL AND ns.distance_meters <= max_distance_walked_meters),
                        ARRAY[]::NEIGHBOR_ENTRY[]
                    )
                )::NEIGHBOR_TABLE AS arr
            FROM stop s
                 LEFT JOIN neighbor_stops ns ON s.serial = ns.stop_idx_1
            GROUP BY s.serial
            ORDER BY s.serial
        )
        SELECT array_agg(fn.arr ORDER BY fn.stop_idx_1)
        FROM filtered_neighbors fn
    );
    transfers := (
        WITH grouped_transfers AS (
            SELECT
                f.serial,
                ROW(
                    COALESCE(
                        array_agg(ROW(t.serial, min_transfer_time)::TRANSFER_ENTRY ORDER BY min_transfer_time ASC) FILTER (WHERE t.serial IS NOT NULL AND min_transfer_time IS NOT NULL),
                        ARRAY[]::TRANSFER_ENTRY[]
                    )
                )::TRANSFER_TABLE AS arr
            FROM stop f
                 LEFT JOIN "transfer" tr ON tr.from_stop_id = f.stop_id
                 LEFT JOIN stop t ON tr.to_stop_id = t.stop_id
            WHERE tr.transfer_type IS NULL OR tr.transfer_type <> 3
            GROUP BY f.serial
            ORDER BY f.serial
        )
        SELECT array_agg(gt.arr ORDER BY serial)
        FROM grouped_transfers gt
    );

    -- Set the starting condition for the origin stop
    idx := (SELECT serial FROM stop s WHERE s.stop_id = origin_stop_id);
    results[idx].earliest_arrival_time := departure_time;
    UPDATE pqueue SET arrival_time = departure_time WHERE stop_idx = idx;

    -- No results have been confirmed yet.
    last_scanned_departure_time := '-infinity'::INTERVAL;

    -- Step 2: Main loop through chronologically sorted connections
    <<outer>>
    FOR conn IN
        WITH services_today AS (SELECT * FROM active_services(departure_date))
        SELECT *
        FROM connections c
            JOIN services_today s ON c.service_id = s.service_id
        WHERE c.departure_time >= earliest_arrivals.departure_time
        -- This guarantees all nodes with an earliest_arrival_time greater than any departure time
        -- have their walking and transfer paths expanded.
        UNION
        SELECT NULL, NULL, NULL, NULL, 'infinity'::INTERVAL, NULL, NULL, NULL, NULL
        ORDER BY departure_time ASC
    LOOP
        -- Earliest arrival times are "confirmed" when they become <= the current connection's departure time.
        -- When that happens, we must scan their stops' transfers and walking paths as soon as possible.
        IF last_scanned_departure_time < conn.departure_time THEN

            FOR conf_stop IN
                -- Remove the stops from the queue after processing them.
                DELETE FROM pqueue pq WHERE pq.arrival_time <= conn.departure_time RETURNING *
            LOOP
                -- If all earliest arrival times have been found, stop the algorithm.
                -- Also stop it when there is a specific destination and it has been reached.
                confirmed_stop_count := confirmed_stop_count + 1;
                EXIT outer WHEN confirmed_stop_count = stop_count OR (destination_stop_serial IS NOT NULL AND conf_stop.stop_idx = destination_stop_serial);

                -- Iterate over nearby stops and see if any of them lead to an earlier arrival time.
                FOREACH nei IN ARRAY neighbors[conf_stop.stop_idx].val
                LOOP
                    -- Calculate arrival time at the neighbor stop.
                    new_arrival_via_walk := conf_stop.arrival_time + (nei.distance_meters / walking_speed_mps) * interval '1 second';

                    -- If the walk offers a better arrival time, update the arrival time and record the path as a walk.
                    IF results[nei.stop_idx_2].earliest_arrival_time IS NULL OR new_arrival_via_walk < results[nei.stop_idx_2].earliest_arrival_time THEN
                        results[nei.stop_idx_2] = (new_arrival_via_walk, conf_stop.stop_idx, 'Walk');
                        UPDATE pqueue SET arrival_time = new_arrival_via_walk WHERE stop_idx = nei.stop_idx_2;
                    END IF;
                END LOOP;

                -- Iterate over transfers from the confirmed stop and see if any of them lead to an earlier arrival time.
                FOREACH transfer IN ARRAY transfers[conf_stop.stop_idx].val
                LOOP
                    -- Calculate arrival time at the transfer destination
                    new_arrival_via_transfer := conf_stop.arrival_time + (transfer.min_transfer_time_secs * interval '1 second');

                    -- If the transfer offers a better arrival time, update the arrival time and record the path as a transfer.
                    IF results[transfer.to_stop_idx].earliest_arrival_time IS NULL OR new_arrival_via_transfer < results[transfer.to_stop_idx].earliest_arrival_time THEN
                        results[transfer.to_stop_idx] = (new_arrival_via_transfer, conf_stop.stop_idx, 'Transfer');
                        UPDATE pqueue SET arrival_time = new_arrival_via_transfer WHERE stop_idx = transfer.to_stop_idx;
                    END IF;
                END LOOP;

            END LOOP;

            -- Keep the last time so there are no multiple attempts if the time doesn't change.
            last_scanned_departure_time := conn.departure_time;

        END IF;

        -- Process the next regular connection (trip). We check if it's reachable from its departure stop.
        -- If the value is NULL (the stop is unreachable), this gets skipped.
        IF results[conn.departure_stop_idx].earliest_arrival_time <= conn.departure_time THEN

            -- If this connection provides an earlier arrival time at its destination
            IF results[conn.arrival_stop_idx].earliest_arrival_time IS NULL OR conn.arrival_time < results[conn.arrival_stop_idx].earliest_arrival_time THEN

                -- Update the arrival time and record the path (trip and previous stop)
                results[conn.arrival_stop_idx] = (conn.arrival_time, conn.departure_stop_idx, conn.trip_id);
                UPDATE pqueue SET arrival_time = conn.arrival_time WHERE stop_idx = conn.arrival_stop_idx;

            END IF;

        END IF;

    END LOOP;

    -- Final step: Return the results
    -- Unnest the array to return a table of reachable stops and their journey information
    RETURN QUERY
    WITH unnested_array AS (
        SELECT
            u.earliest_arrival_time,
            u.previous_stop_id,
            u.trip_id_used,
            u.ordinality
        FROM UNNEST(results) WITH ORDINALITY u
    )
    SELECT
        s.stop_id,
        date_trunc('second', u.earliest_arrival_time + INTERVAL '0.5 seconds') AS earliest_arrival_time,
        ps.stop_id AS previous_stop_id,
        u.trip_id_used,
        s.location AS stop_geom
    FROM stop s
         JOIN unnested_array u ON s.serial = u.ordinality
         LEFT JOIN stop ps ON u.previous_stop_id = ps.serial
    ORDER BY u.earliest_arrival_time, s.stop_id;
END
$$;

CREATE OR REPLACE FUNCTION shortest_path(
    origin_stop_id TEXT,
    destination_stop_id TEXT,
    departure_date DATE,
    departure_time INTERVAL,
    max_distance_walked_meters INTEGER = 500,
    walking_speed_mps NUMERIC = 1.4
)
RETURNS TABLE (
    stop_id TEXT,
    stop_name TEXT,
    trip_id TEXT,
    arrival_time INTERVAL,
    stop_geom GEOMETRY(Point, 4326)
)
LANGUAGE plpgsql
AS $$
DECLARE
    destination_stop_serial INTEGER := (SELECT s.serial FROM stop s WHERE s.stop_id = destination_stop_id);
BEGIN
    RETURN QUERY
    -- Get the results from the CSA function
    WITH RECURSIVE earliest_arrivals AS (
        SELECT *
        FROM earliest_arrivals(origin_stop_id, departure_date, departure_time, max_distance_walked_meters, walking_speed_mps, destination_stop_serial)
    ),
    path_reconstruction AS (
        -- Start the recursion from the destination stop
        SELECT
            ea.stop_id,
            ea.earliest_arrival_time,
            ea.previous_stop_id,
            ea.trip_id_used,
            ea.stop_geom
        FROM earliest_arrivals ea
        WHERE ea.stop_id = destination_stop_id

        UNION ALL

        -- Recursively join back to find the previous step in the journey
        SELECT
            prev.stop_id,
            prev.earliest_arrival_time,
            prev.previous_stop_id,
            prev.trip_id_used,
            prev.stop_geom
        FROM earliest_arrivals prev
        JOIN path_reconstruction curr ON curr.previous_stop_id = prev.stop_id
    )
    SELECT
        pr.stop_id,
        s.stop_name,
        pr.trip_id_used AS trip_id,
        pr.earliest_arrival_time AS arrival_time,
        pr.stop_geom
    FROM path_reconstruction pr
    JOIN stop s ON pr.stop_id = s.stop_id
    ORDER BY pr.earliest_arrival_time;
END
$$;
