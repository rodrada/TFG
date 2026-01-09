
DROP TABLE IF EXISTS timeframe;
CREATE TABLE timeframe (
    timeframe_group_id TEXT,
    start_time TIME,
    end_time TIME,
    service_id TEXT
);

-- Service ids can refer to either service(service_id) or service_exception(service_id),
-- so we use a trigger to validate the data.
CREATE OR REPLACE FUNCTION timeframe_validate_service_id() RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM service WHERE service_id = NEW.service_id
        UNION ALL
        SELECT 1 FROM service_exception WHERE service_id = NEW.service_id
    ) THEN
        RAISE EXCEPTION 'Timeframe: Invalid service_id: %', NEW.service_id;
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER timeframe_service_id_validation
BEFORE INSERT OR UPDATE ON timeframe
FOR EACH ROW EXECUTE FUNCTION timeframe_validate_service_id();

CALL load_from_csv('timeframe', :'dataset_dir' || '/timeframes.txt');

-- Create constraints AFTER loading data for performance reasons.
ALTER TABLE timeframe
ADD CONSTRAINT chk_timeframe_non_nulls CHECK (num_nulls(timeframe_group_id, service_id) = 0),
ADD CONSTRAINT chk_timeframe_start_end_times CHECK ((start_time IS NULL) = (end_time IS NULL));     -- Either both fields are defined, or none of them is.

-- PK equivalent since some of the fields can be NULL
CREATE UNIQUE INDEX idx_timeframe
    ON timeframe (timeframe_group_id, start_time, end_time, service_id)
    NULLS NOT DISTINCT;

-- Some indexes for faster joins.
CREATE INDEX idx_timeframe_service_id ON timeframe(service_id);

-- Update stats for better query performance.
ANALYZE timeframe;
