
DROP TABLE IF EXISTS attribution;    
CREATE TABLE attribution (
    attribution_id TEXT,
    agency_id TEXT,
    route_id TEXT,
    trip_id TEXT,
    organization_name TEXT,
    is_producer BOOLEAN DEFAULT FALSE,
    is_operator BOOLEAN DEFAULT FALSE,
    is_authority BOOLEAN DEFAULT FALSE,
    attribution_url TEXT,
    attribution_email TEXT,
    attribution_phone TEXT
);

CALL load_from_csv('attribution', :'dataset_dir' || '/attributions.txt');

-- Create constraints AFTER loading data for performance reasons.
ALTER TABLE attribution
ADD CONSTRAINT chk_attribution_organization_name CHECK (organization_name IS NOT NULL),
ADD CONSTRAINT chk_attribution_url CHECK (attribution_url ~ '^https?://'),
-- If one of the three ids is specified, the others must be empty. However, all of them can be empty too.
ADD CONSTRAINT chk_attribution_single_id CHECK (num_nonnulls(agency_id, route_id, trip_id) <= 1),
ADD CONSTRAINT chk_attribution_single_type CHECK (is_producer OR is_operator OR is_authority),
ADD CONSTRAINT fk_attribution_agency_id FOREIGN KEY (agency_id) REFERENCES agency(agency_id),
ADD CONSTRAINT fk_attribution_route_id FOREIGN KEY (route_id) REFERENCES route(route_id),
ADD CONSTRAINT fk_attribution_trip_id FOREIGN KEY (trip_id) REFERENCES trip(trip_id);

-- PK equivalent since the field may be NULL.
CREATE UNIQUE INDEX idx_attribution
    ON attribution (attribution_id)
    NULLS NOT DISTINCT;

-- Some indexes for faster joins.
CREATE INDEX idx_attribution_agency_id ON attribution(agency_id);
CREATE INDEX idx_attribution_route_id ON attribution(route_id);
CREATE INDEX idx_attribution_trip_id ON attribution(trip_id);

-- Update stats for better query performance.
ANALYZE attribution;
