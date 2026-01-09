
DROP TABLE IF EXISTS location_type;
CREATE TABLE location_type ( LIKE enum_table INCLUDING ALL );
INSERT INTO location_type VALUES
    (0, 'Stop/Platform'),
    (1, 'Station'),
    (2, 'Entrance/Exit'),
    (3, 'Generic Node'),
    (4, 'Boarding Area');

DROP TABLE IF EXISTS wheelchair_status;
CREATE TABLE wheelchair_status ( LIKE enum_table INCLUDING ALL );
INSERT INTO wheelchair_status VALUES
    (0, 'Unknown'),
    (1, 'Accessible'),
    (2, 'Not Accessible');

DROP TABLE IF EXISTS "stop";
CREATE TABLE stop (
    stop_id TEXT,
    stop_code TEXT,
    stop_name TEXT,
    tts_stop_name TEXT,
    stop_desc TEXT,
    stop_lat NUMERIC(8, 6),
    stop_lon NUMERIC(9, 6),
    zone_id TEXT,
    stop_url TEXT,
    location_type INTEGER DEFAULT 0,
    parent_station TEXT,
    stop_timezone TEXT,
    wheelchair_boarding INTEGER DEFAULT 0, 
    level_id TEXT,
    platform_code TEXT
);

CALL load_from_csv('stop', :'dataset_dir' || '/stops.txt');

ALTER TABLE "stop"
-- Create constraints AFTER loading data for performance reasons.
ADD CONSTRAINT chk_stop_non_nulls CHECK (num_nulls(stop_name, stop_lat, stop_lon) = 0 OR location_type IN (3, 4)),
ADD CONSTRAINT chk_stop_parent_station_required CHECK (parent_station IS NOT NULL OR location_type < 2),
ADD CONSTRAINT chk_stop_parent_station_forbidden CHECK (parent_station IS NULL OR location_type <> 1),
ADD CONSTRAINT chk_stop_valid_coordinates CHECK ((stop_lat BETWEEN -90 AND 90) AND (stop_lon BETWEEN -180 AND 180)),
ADD CONSTRAINT chk_stop_stop_url CHECK (stop_url ~ '^https?://'),
ADD CONSTRAINT pk_stop PRIMARY KEY (stop_id),
ADD CONSTRAINT fk_stop_location_type FOREIGN KEY (location_type) REFERENCES location_type(id),
ADD CONSTRAINT fk_stop_parent_station FOREIGN KEY (parent_station) REFERENCES stop(stop_id),
ADD CONSTRAINT fk_stop_wheelchair_boarding FOREIGN KEY (wheelchair_boarding) REFERENCES wheelchair_status(id),
-- Add serial (used in queries).
ADD COLUMN "serial" SERIAL,
-- Include corresponding PostGIS geometry.
ADD COLUMN "location" GEOMETRY(Point, 4326);

CREATE UNIQUE INDEX idx_stop_serial ON stop (serial);

-- Generate geometry from coordinates
UPDATE "stop" SET "location" =  ST_SetSRID(ST_Point(stop_lon, stop_lat), 4326);     -- 4326 is the SRID for lat/lon coordinates
ALTER TABLE "stop"
DROP COLUMN "stop_lat",
DROP COLUMN "stop_lon";

-- GiST index for faster geometrical queries
CREATE INDEX idx_stop_location ON stop USING GIST (location);
CREATE INDEX idx_stop_location_geography ON stop USING GIST ((location::geography));

-- Update stats for better query performance.
ANALYZE "stop";
