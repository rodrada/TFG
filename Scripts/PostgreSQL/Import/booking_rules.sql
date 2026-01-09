
DROP TABLE IF EXISTS booking_type;
CREATE TABLE booking_type ( LIKE enum_table INCLUDING ALL );
INSERT INTO booking_type VALUES
    (0, 'Real Time'),
    (1, 'Up To Same-Day'),
    (2, 'Up To Prior Days');

DROP TABLE IF EXISTS booking_rule;
CREATE TABLE booking_rule (
    booking_rule_id TEXT,
    booking_type INTEGER,
    prior_notice_duration_min INTEGER,
    prior_notice_duration_max INTEGER,
    prior_notice_last_day INTEGER,
    prior_notice_last_time TIME,
    prior_notice_start_day INTEGER,
    prior_notice_start_time TIME,
    prior_notice_service_id TEXT,
    message TEXT,
    pickup_message TEXT,
    drop_off_message TEXT,
    phone_number TEXT,
    info_url TEXT,
    booking_url TEXT
);

-- Service ids can refer to either service(service_id) or service_exception(service_id),
-- so we use a trigger to validate the data.
CREATE OR REPLACE FUNCTION booking_rule_validate_service_id() RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.prior_notice_service_id IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM service WHERE service_id = NEW.prior_notice_service_id
        UNION ALL
        SELECT 1 FROM service_exception WHERE service_id = NEW.prior_notice_service_id
    ) THEN
        RAISE EXCEPTION 'Booking rule: Invalid prior notice service_id: %', NEW.prior_notice_service_id;
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER booking_rule_service_id_validation
BEFORE INSERT OR UPDATE ON booking_rule
FOR EACH ROW EXECUTE FUNCTION booking_rule_validate_service_id();

CALL load_from_csv('booking_rule', :'dataset_dir' || '/booking_rules.txt');

-- Create constraints AFTER loading data for performance reasons.
ALTER TABLE booking_rule
ADD CONSTRAINT chk_booking_rule_booking_type CHECK (booking_type IS NOT NULL),
ADD CONSTRAINT chk_booking_rule_prior_notice_duration_min CHECK ((prior_notice_duration_min IS NOT NULL) = (booking_type = 1)),
ADD CONSTRAINT chk_booking_rule_prior_notice_duration_max CHECK ((prior_notice_duration_max IS NULL) OR (booking_type = 1)),
ADD CONSTRAINT chk_booking_rule_prior_notice_last_day CHECK ((prior_notice_last_day IS NOT NULL) = (booking_type = 2)),
ADD CONSTRAINT chk_booking_rule_prior_notice_last_time CHECK ((prior_notice_last_time IS NULL) = (prior_notice_last_day IS NULL)),
ADD CONSTRAINT chk_booking_rule_prior_notice_start_day CHECK ((prior_notice_start_day IS NULL) OR (booking_type = 1 AND (prior_notice_duration_max IS NULL)) OR (booking_type = 2)),
ADD CONSTRAINT chk_booking_rule_prior_notice_start_time CHECK ((prior_notice_start_time IS NULL) = (prior_notice_start_day IS NULL)),
ADD CONSTRAINT chk_booking_rule_prior_notice_service_id CHECK ((prior_notice_service_id IS NULL) OR (booking_type = 2)),
ADD CONSTRAINT chk_booking_rule_info_url CHECK (info_url ~ '^https?://'),
ADD CONSTRAINT chk_booking_rule_booking_url CHECK (booking_url ~ '^https?://'),
ADD CONSTRAINT pk_booking_rule PRIMARY KEY (booking_rule_id),
ADD CONSTRAINT fk_booking_rule_booking_type FOREIGN KEY (booking_type) REFERENCES booking_type(id);

ALTER TABLE stop_time
ADD CONSTRAINT fk_stop_time_pickup_booking_rule_id FOREIGN KEY (pickup_booking_rule_id) REFERENCES booking_rule(booking_rule_id),
ADD CONSTRAINT fk_stop_time_drop_off_booking_rule_id FOREIGN KEY (drop_off_booking_rule_id) REFERENCES booking_rule(booking_rule_id);

-- Some indexes for faster joins.
CREATE INDEX idx_booking_rule_prior_notice_service_id ON booking_rule(prior_notice_service_id);

-- Update stats for better query performance.
ANALYZE booking_rule;
