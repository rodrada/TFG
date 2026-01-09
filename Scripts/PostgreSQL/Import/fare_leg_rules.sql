
DROP TABLE IF EXISTS fare_leg_rule;
CREATE TABLE fare_leg_rule (
    leg_group_id TEXT,
    network_id TEXT,
    from_area_id TEXT,
    to_area_id TEXT,
    from_timeframe_group_id TEXT,
    to_timeframe_group_id TEXT,
    fare_product_id TEXT,
    rule_priority INTEGER DEFAULT 0
);

-- Network id can refer to either network(network_id) or route(network_id),
-- so we use a trigger to validate the data.
CREATE OR REPLACE FUNCTION flr_validate_network_id() RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.network_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM network WHERE network_id = NEW.network_id
        UNION ALL
        SELECT 1 FROM route WHERE network_id = NEW.network_id
    ) THEN
        RAISE EXCEPTION 'Fare leg rule: Invalid network_id: %', NEW.network_id;
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER flr_network_id_validation
BEFORE INSERT OR UPDATE ON fare_leg_rule
FOR EACH ROW EXECUTE FUNCTION flr_validate_network_id();

CALL load_from_csv('fare_leg_rule', :'dataset_dir' || '/fare_leg_rules.txt');

-- Create constraints AFTER loading data for performance reasons.
-- NOTE: Both from_timeframe_group_id and to_timeframe_group_id reference timeframe(timeframe_group_id),
--       but a foreign key constraint cannot be defined for values that may not be unique.
-- TODO: Normalize timeframes introducing timeframe_group as another table.
-- NOTE: Something similar happens with fare_product_id.
ALTER TABLE fare_leg_rule
ADD CONSTRAINT chk_flr_fare_product_id CHECK (fare_product_id IS NOT NULL),
ADD CONSTRAINT chk_flr_rule_priority CHECK (rule_priority >= 0),
ADD CONSTRAINT fk_flr_from_area_id FOREIGN KEY (from_area_id) REFERENCES area(area_id),
ADD CONSTRAINT fk_flr_to_area_id FOREIGN KEY (to_area_id) REFERENCES area(area_id);

-- PK equivalent since some of the fields can be NULL
CREATE UNIQUE INDEX idx_fare_leg_rule
    ON fare_leg_rule (network_id, from_area_id, to_area_id,
                      from_timeframe_group_id, to_timeframe_group_id, fare_product_id)
    NULLS NOT DISTINCT;

-- Some indexes for faster joins.
CREATE INDEX idx_flr_area_pair ON fare_leg_rule(from_area_id, to_area_id);
CREATE INDEX idx_flr_ftgi ON fare_leg_rule(from_timeframe_group_id);
CREATE INDEX idx_flr_ttgi ON fare_leg_rule(to_timeframe_group_id);
CREATE INDEX idx_flr_fare_product_id ON fare_leg_rule(fare_product_id);

-- Update stats for better query performance.
ANALYZE fare_leg_rule;
