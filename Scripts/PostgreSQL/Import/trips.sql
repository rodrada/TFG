
DROP TABLE IF EXISTS travel_direction;
CREATE TABLE travel_direction ( LIKE enum_table INCLUDING ALL );
INSERT INTO travel_direction VALUES
    (0, 'Outbound'),
    (1, 'Inbound');

DROP TABLE IF EXISTS bicycle_status;
CREATE TABLE bicycle_status ( LIKE enum_table INCLUDING ALL );
INSERT INTO bicycle_status VALUES
    (0, 'Unknown'),
    (1, 'Allowed'),
    (2, 'Not Allowed');

DROP TABLE IF EXISTS trip;
CREATE TABLE trip (
    route_id TEXT,
    service_id TEXT,
    trip_id TEXT,
    trip_headsign TEXT,
    trip_short_name TEXT,
    direction_id INTEGER,
    block_id TEXT,
    shape_id TEXT,
    wheelchair_accessible INTEGER DEFAULT 0,
    bikes_allowed INTEGER DEFAULT 0
);

-- Service ids can refer to either service(service_id) or service_exception(service_id),
-- so we use a trigger to validate the data.
CREATE OR REPLACE FUNCTION trip_validate_service_id() RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM service WHERE service_id = NEW.service_id
        UNION ALL
        SELECT 1 FROM service_exception WHERE service_id = NEW.service_id
    ) THEN
        RAISE EXCEPTION 'Trip: Invalid service_id: %', NEW.service_id;
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER trip_service_id_validation
BEFORE INSERT OR UPDATE ON trip
FOR EACH ROW EXECUTE FUNCTION trip_validate_service_id();

CALL load_from_csv('trip', :'dataset_dir' || '/trips.txt');

-- Create constraints AFTER loading data for performance reasons.
ALTER TABLE trip
ADD CONSTRAINT chk_trip_non_nulls CHECK (num_nulls(route_id, service_id) = 0),
ADD CONSTRAINT pk_trip PRIMARY KEY (trip_id),
ADD CONSTRAINT fk_trip_route_id FOREIGN KEY (route_id) REFERENCES route(route_id),
ADD CONSTRAINT fk_trip_direction_id FOREIGN KEY (direction_id) REFERENCES travel_direction(id),
ADD CONSTRAINT fk_trip_wheelchair_accessible FOREIGN KEY (wheelchair_accessible) REFERENCES wheelchair_status(id),
ADD CONSTRAINT fk_trip_bikes_allowed FOREIGN KEY (bikes_allowed) REFERENCES bicycle_status(id);

-- Update stats for better query performance.
ANALYZE trip;
