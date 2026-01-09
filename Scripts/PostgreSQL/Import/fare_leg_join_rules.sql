
DROP TABLE IF EXISTS fare_leg_join_rule;
CREATE TABLE fare_leg_join_rule (
    from_network_id TEXT,
    to_network_id TEXT,
    from_stop_id TEXT,
    to_stop_id TEXT
);

-- Network ids can refer to either network(network_id) or route(network_id),
-- so we use a trigger to validate the data.
CREATE OR REPLACE FUNCTION fljr_validate_network_id() RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM network WHERE network_id = NEW.from_network_id
        UNION ALL
        SELECT 1 FROM route WHERE network_id = NEW.from_network_id
    ) THEN
        RAISE EXCEPTION 'Fare leg join rule: Invalid from_network_id: %', NEW.from_network_id;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM network WHERE network_id = NEW.to_network_id
        UNION ALL
        SELECT 1 FROM route WHERE network_id = NEW.to_network_id
    ) THEN
        RAISE EXCEPTION 'Fare leg join rule: Invalid to_network_id: %', NEW.to_network_id;
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER fljr_network_id_validation
BEFORE INSERT OR UPDATE ON fare_leg_join_rule
FOR EACH ROW EXECUTE FUNCTION fljr_validate_network_id();

CALL load_from_csv('fare_leg_join_rule', :'dataset_dir' || '/fare_leg_join_rules.txt');

-- Create constraints AFTER loading data for performance reasons.
ALTER TABLE fare_leg_join_rule
ADD CONSTRAINT chk_fljr_network_pair CHECK (from_network_id IS NOT NULL AND to_network_id IS NOT NULL),
ADD CONSTRAINT chk_fljr_stop_pair CHECK ((from_stop_id IS NULL) = (to_stop_id IS NULL)),     -- Either both fields are NULL or none of them is.
ADD CONSTRAINT fk_fljr_from_stop_id FOREIGN KEY (from_stop_id) REFERENCES stop(stop_id),
ADD CONSTRAINT fk_fljr_to_stop_id FOREIGN KEY (to_stop_id) REFERENCES stop(stop_id);

-- PK equivalent since some of the fields can be NULL.
CREATE UNIQUE INDEX idx_fare_leg_join_rule
    ON fare_leg_join_rule (from_network_id, to_network_id, from_stop_id, to_stop_id) 
    NULLS NOT DISTINCT;

-- Some indexes for faster joins.
CREATE INDEX idx_fljr_network_pair ON fare_leg_join_rule(from_network_id, to_network_id);
CREATE INDEX idx_fljr_from_stop_id ON fare_leg_join_rule(from_stop_id);

-- Update stats for better query performance.
ANALYZE fare_leg_join_rule;
