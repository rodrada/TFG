
DROP TABLE IF EXISTS area;
CREATE TABLE area (
    area_id TEXT,
    area_name TEXT
);

CALL load_from_csv('area', :'dataset_dir' || '/areas.txt');

-- Create constraints AFTER loading data for performance reasons.
ALTER TABLE area
ADD CONSTRAINT pk_area PRIMARY KEY (area_id);

-- Update stats for better query performance.
ANALYZE area;
