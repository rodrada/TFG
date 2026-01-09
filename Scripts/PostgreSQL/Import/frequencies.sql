
DROP TABLE IF EXISTS service_type;
CREATE TABLE service_type ( LIKE enum_table INCLUDING ALL );
INSERT INTO service_type VALUES
    (0, 'Frequency Based'), 
    (1, 'Schedule Based');

DROP TABLE IF EXISTS frequency;
CREATE TABLE frequency (
    trip_id TEXT,
    start_time TEXT,
    end_time TEXT,
    headway_secs INTEGER,
    exact_times INTEGER DEFAULT 0
);

CALL load_from_csv('frequency', :'dataset_dir' || '/frequencies.txt');

ALTER TABLE frequency
-- Transform start and end times into intervals (they can be >= 24:00)
ALTER COLUMN start_time TYPE INTERVAL USING parse_gtfs_time(start_time),
ALTER COLUMN end_time TYPE INTERVAL USING parse_gtfs_time(end_time),
-- Create constraints AFTER loading data for performance reasons.
ADD CONSTRAINT chk_frequency_non_nulls CHECK (num_nulls(trip_id, start_time, end_time, headway_secs) = 0),
ADD CONSTRAINT chk_frequency_headway_secs CHECK (headway_secs > 0),
ADD CONSTRAINT pk_frequency PRIMARY KEY (trip_id, start_time),
ADD CONSTRAINT fk_frequency_trip_id FOREIGN KEY (trip_id) REFERENCES trip(trip_id),
ADD CONSTRAINT fk_frequency_exact_times FOREIGN KEY (exact_times) REFERENCES service_type(id);

-- Update stats for better query performance.
ANALYZE frequency;
