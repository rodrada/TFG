
DROP TABLE IF EXISTS shape_temp;
CREATE TEMP TABLE shape_temp (
    shape_id TEXT,
    shape_pt_lat NUMERIC(9, 6),
    shape_pt_lon NUMERIC(9, 6),
    shape_pt_sequence INTEGER,
    shape_dist_traveled DOUBLE PRECISION
);

CALL load_from_csv('shape_temp', :'dataset_dir' || '/shapes.txt');

-- Create constraints AFTER loading data for performance reasons.
-- NOTE: Since it's just a temporary table, we do not need to check for uniqueness of shape_id just yet.
ALTER TABLE shape_temp
ADD CONSTRAINT chk_shape_pt_sequence CHECK (shape_pt_sequence >= 0),
ADD CONSTRAINT chk_shape_dist_traveled CHECK (shape_dist_traveled >= 0);

DROP TABLE IF EXISTS shape;
CREATE TABLE shape (
    shape_id TEXT,
    shape_geom GEOMETRY(LINESTRING, 4326)   -- 4326 is the SRID for lat/lon coordinates.
);

INSERT INTO shape
SELECT shape_id, ST_SetSRID(ST_LineFromMultiPoint(ST_Collect(ST_Point(shape_pt_lon, shape_pt_lat) ORDER BY shape_pt_sequence)), 4326)
FROM shape_temp
GROUP BY shape_id;

DROP TABLE shape_temp;

-- Create constraints AFTER loading data for performance reasons.
ALTER TABLE shape
ADD CONSTRAINT pk_shape PRIMARY KEY (shape_id);

ALTER TABLE trip ADD FOREIGN KEY (shape_id) REFERENCES shape(shape_id);

-- GiST index for faster geometrical queries
CREATE INDEX idx_shape_geom ON shape USING GIST(shape_geom);

-- Update stats for better query performance.
ANALYZE shape;
