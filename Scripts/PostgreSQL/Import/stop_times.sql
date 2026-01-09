
DROP TABLE IF EXISTS stop_method;
CREATE TABLE stop_method ( LIKE enum_table INCLUDING ALL );
INSERT INTO stop_method VALUES
    (0, 'Scheduled'),
    (1, 'Not Available'),
    (2, 'Must Phone Agency'),
    (3, 'Must Coordinate With Driver');

DROP TABLE IF EXISTS time_precision;
CREATE TABLE time_precision ( LIKE enum_table INCLUDING ALL );
INSERT INTO time_precision VALUES
    (0, 'Approximate'), 
    (1, 'Exact');

DROP TABLE IF EXISTS stop_time;
CREATE TABLE stop_time (
    trip_id TEXT,
    arrival_time TEXT,
    departure_time TEXT,
    stop_id TEXT,
    location_group_id TEXT,
    location_id TEXT,
    stop_sequence INTEGER,
    stop_headsign TEXT,
    start_pickup_drop_off_window TEXT,
    end_pickup_drop_off_window TEXT,
    pickup_type INTEGER DEFAULT 0,
    drop_off_type INTEGER DEFAULT 0,
    continuous_pickup INTEGER DEFAULT 1,
    continuous_drop_off INTEGER DEFAULT 1,
    shape_dist_traveled DOUBLE PRECISION,
    timepoint INTEGER DEFAULT 1,
    pickup_booking_rule_id TEXT,
    drop_off_booking_rule_id TEXT
);

CALL load_from_csv('stop_time', :'dataset_dir' || '/stop_times.txt');

ALTER TABLE stop_time
-- Transform times into intervals (they can be >= 24:00)
ALTER COLUMN arrival_time TYPE INTERVAL USING parse_gtfs_time(arrival_time),
ALTER COLUMN departure_time TYPE INTERVAL USING parse_gtfs_time(departure_time),
ALTER COLUMN start_pickup_drop_off_window TYPE INTERVAL USING parse_gtfs_time(start_pickup_drop_off_window),
ALTER COLUMN end_pickup_drop_off_window TYPE INTERVAL USING parse_gtfs_time(end_pickup_drop_off_window),
-- Create constraints AFTER loading data for performance reasons.
ADD CONSTRAINT chk_stop_time_stop_sequence CHECK (stop_sequence >= 0),
ADD CONSTRAINT chk_stop_time_shape_dist_traveled CHECK (shape_dist_traveled >= 0),
ADD CONSTRAINT pk_stop_time PRIMARY KEY (trip_id, stop_sequence),
ADD CONSTRAINT fk_stop_time_trip FOREIGN KEY (trip_id) REFERENCES trip(trip_id),
ADD CONSTRAINT fk_stop_time_stop FOREIGN KEY (stop_id) REFERENCES stop(stop_id),
ADD CONSTRAINT fk_stop_time_pickup_type FOREIGN KEY (pickup_type) REFERENCES stop_method(id),
ADD CONSTRAINT fk_stop_time_drop_off_type FOREIGN KEY (drop_off_type) REFERENCES stop_method(id),
ADD CONSTRAINT fk_stop_time_continuous_pickup FOREIGN KEY (continuous_pickup) REFERENCES continuous_status(id),
ADD CONSTRAINT fk_stop_time_continuous_drop_off FOREIGN KEY (continuous_drop_off) REFERENCES continuous_status(id),
ADD CONSTRAINT fk_stop_time_timepoint FOREIGN KEY (timepoint) REFERENCES time_precision(id);

-- Update stats for better query performance.
ANALYZE stop_time;
