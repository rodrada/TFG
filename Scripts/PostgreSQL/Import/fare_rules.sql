
DROP TABLE IF EXISTS fare_rule;
CREATE TABLE fare_rule (
    fare_id TEXT,
    route_id TEXT,
    -- The next fields refer to stop(zone_id), but that's not UNIQUE, so no foreign keys.
    origin_id TEXT,
    destination_id TEXT,
    contains_id TEXT
);

CALL load_from_csv('fare_rule', :'dataset_dir' || '/fare_rules.txt');

-- Create constraints AFTER loading data for performance reasons.
ALTER TABLE fare_rule
ADD CONSTRAINT chk_fare_rule_fare_id CHECK (fare_id IS NOT NULL),
ADD CONSTRAINT fk_fare_rule_fare_id FOREIGN KEY (fare_id) REFERENCES fare(fare_id),
ADD CONSTRAINT fk_fare_rule_route_id FOREIGN KEY (route_id) REFERENCES route(route_id);

-- PK equivalent since some of the fields can be NULL
CREATE UNIQUE INDEX idx_fare_rule
    ON fare_rule (fare_id, route_id, origin_id, destination_id, contains_id)
    NULLS NOT DISTINCT;

-- Some indexes for faster joins.
CREATE INDEX idx_fare_rule_route_id ON fare_rule (route_id);
CREATE INDEX idx_fare_rule_origin_id ON fare_rule (origin_id);
CREATE INDEX idx_fare_rule_destination_id ON fare_rule (destination_id);
CREATE INDEX idx_fare_rule_contains_id ON fare_rule (contains_id);

-- Update stats for better query performance.
ANALYZE fare_rule;
