
DROP TABLE IF EXISTS stop_area;
CREATE TABLE stop_area (
    area_id TEXT,
    stop_id TEXT   -- If the stop is a station (location_type = 1), then all of its platforms (location_type = 0) are part of the same area.
);

CALL load_from_csv('stop_area', :'dataset_dir' || '/stop_areas.txt');

-- Create constraints AFTER loading data for performance reasons.
ALTER TABLE stop_area
ADD CONSTRAINT pk_stop_area PRIMARY KEY (area_id, stop_id),
ADD CONSTRAINT fk_stop_area_area_id FOREIGN KEY (area_id) REFERENCES area(area_id),
ADD CONSTRAINT fk_stop_area_stop_id FOREIGN KEY (stop_id) REFERENCES stop(stop_id);

-- Make queries like finding the areas associated with a stop much faster.
-- Reverse queries (finding stops associated with an area) are already covered by the PK index.
CREATE INDEX idx_stop_area_stop_id ON stop_area(stop_id);

-- Create PostGIS geometries for areas from stop coordinates (finding the convex hull).
-- Warning: Convex hulls may include non-service areas (e.g., across a river if stops are on both sides).
ALTER TABLE area ADD COLUMN "geometry" GEOMETRY(GEOMETRY, 4326);

UPDATE area
SET "geometry" = (
    SELECT ST_ConvexHull(ST_Collect(stop.location))
    FROM stop_area
    JOIN stop ON stop_area.stop_id = stop.stop_id
    WHERE stop_area.area_id = area.area_id
);

-- Generate an index for faster geometrical queries.
CREATE INDEX idx_area_geometry ON area USING GIST("geometry");

-- Update stats for better query performance.
ANALYZE area, stop_area;
