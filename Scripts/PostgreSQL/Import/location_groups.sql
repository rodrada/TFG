
DROP TABLE IF EXISTS location_group;
CREATE TABLE location_group (
    location_group_id TEXT,
    location_group_name TEXT
);

CALL load_from_csv('location_group', :'dataset_dir' || '/location_groups.txt');

-- Create constraints AFTER loading data for performance reasons.
ALTER TABLE location_group
ADD CONSTRAINT pk_location_group PRIMARY KEY (location_group_id);

ALTER TABLE stop_time ADD FOREIGN KEY (location_group_id) REFERENCES location_group(location_group_id);

-- Update stats for better query performance.
ANALYZE location_group;
