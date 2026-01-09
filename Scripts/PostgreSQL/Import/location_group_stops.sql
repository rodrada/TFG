
DROP TABLE IF EXISTS location_group_stop;
CREATE TABLE location_group_stop (
    location_group_id TEXT,
    stop_id TEXT
);

CALL load_from_csv('location_group_stop', :'dataset_dir' || '/location_group_stops.txt');

-- Create constraints AFTER loading data for performance reasons.
ALTER TABLE location_group_stop
ADD CONSTRAINT pk_location_group_stop PRIMARY KEY (location_group_id, stop_id),
ADD CONSTRAINT fk_location_group_stop_location_group_id FOREIGN KEY (location_group_id) REFERENCES location_group(location_group_id),
ADD CONSTRAINT fk_location_group_stop_stop_id FOREIGN KEY (stop_id) REFERENCES stop(stop_id);

-- Update stats for better query performance.
ANALYZE location_group_stop;
