    
DROP TABLE IF EXISTS duration_limit_type;
CREATE TABLE duration_limit_type ( LIKE enum_table INCLUDING ALL );
INSERT INTO duration_limit_type VALUES
    (0, 'Departure-Arrival'),
    (1, 'Departure-Departure'),
    (2, 'Arrival-Departure'),
    (3, 'Arrival-Arrival');

DROP TABLE IF EXISTS transfer_cost_type;
CREATE TABLE transfer_cost_type ( LIKE enum_table INCLUDING ALL );
INSERT INTO transfer_cost_type VALUES
    (0, 'From-Transfer'),
    (1, 'From-Transfer-To'),
    (2, 'Transfer');

DROP TABLE IF EXISTS fare_transfer_rule;
CREATE TABLE fare_transfer_rule (
    from_leg_group_id TEXT,
    to_leg_group_id TEXT,
    transfer_count INTEGER,
    duration_limit INTEGER,
    duration_limit_type INTEGER,
    fare_transfer_type INTEGER,
    fare_product_id TEXT
    -- NOTE: The field fare_product_id references fare_product(fare_product_id),
    --       but a foreign key constraint cannot be defined for values that may not be unique.
);

CALL load_from_csv('fare_transfer_rule', :'dataset_dir' || '/fare_transfer_rules.txt');

-- Create constraints AFTER loading data for performance reasons.
ALTER TABLE fare_transfer_rule
ADD CONSTRAINT chk_ftr_fare_transfer_type CHECK (fare_transfer_type IS NOT NULL),
ADD CONSTRAINT chk_ftr_transfer_count CHECK (transfer_count = -1 OR transfer_count >= 1),
ADD CONSTRAINT chk_ftr_leg_transfer_count CHECK (from_leg_group_id = to_leg_group_id AND transfer_count IS NOT NULL OR from_leg_group_id <> to_leg_group_id AND transfer_count IS NULL),
ADD CONSTRAINT chk_ftr_duration_limit CHECK (duration_limit > 0),
ADD CONSTRAINT chk_ftr_duration_limit_type CHECK ((duration_limit IS NULL) = (duration_limit_type IS NULL)),
ADD CONSTRAINT fk_ftr_duration_limit_type FOREIGN KEY (duration_limit_type) REFERENCES duration_limit_type(id),
ADD CONSTRAINT fk_ftr_fare_transfer_type FOREIGN KEY (fare_transfer_type) REFERENCES transfer_cost_type(id);

-- PK equivalent since some of the fields can be NULL
CREATE UNIQUE INDEX idx_fare_transfer_rule
    ON fare_transfer_rule (from_leg_group_id, to_leg_group_id,
                            fare_product_id, transfer_count, duration_limit)
    NULLS NOT DISTINCT;

-- Some indexes for faster joins.
CREATE INDEX idx_ftr_leg_group_pair ON fare_transfer_rule(from_leg_group_id, to_leg_group_id);
CREATE INDEX idx_ftr_fare_product_id ON fare_transfer_rule(fare_product_id);

-- Update stats for better query performance.
ANALYZE fare_transfer_rule;
