
DROP TABLE IF EXISTS payment_method;
CREATE TABLE payment_method ( LIKE enum_table INCLUDING ALL );
INSERT INTO payment_method VALUES
    (0, 'On Board'),
    (1, 'Before Boarding');

DROP TABLE IF EXISTS fare;
CREATE TABLE fare (
    fare_id TEXT,
    price NUMERIC(10, 4),
    currency_type TEXT,
    payment_method INTEGER,
    transfers NUMERIC(1, 0),
    agency_id TEXT,
    transfer_duration INTEGER
);

CALL load_from_csv('fare', :'dataset_dir' || '/fare_attributes.txt');

-- Make sure currency_type is uppercase.
-- This used to be part of a trigger, but since data is not meant to be modified once imported, it can be optimized.
UPDATE fare SET currency_type = UPPER(currency_type);

-- Check whether agency_id is present if there is more than one agency.
-- Just like in the previous case, this used to be part of a trigger, but it can be optimized.
DO $$
BEGIN
    IF (SELECT COUNT(*) FROM agency) > 1 AND
       (SELECT COUNT(*) FROM fare WHERE agency_id IS NULL) > 0 THEN
        RAISE EXCEPTION 'Fare: agency_id is mandatory when multiple agencies exist';
    END IF;
END $$;

-- Create constraints AFTER loading data for performance reasons.
ALTER TABLE fare
ADD CONSTRAINT chk_fare_fare_price CHECK (price IS NOT NULL AND price >= 0),
ADD CONSTRAINT chk_fare_fare_currency_type CHECK (currency_type IS NOT NULL AND currency_type ~ '^[A-Z]{3}$'),
ADD CONSTRAINT chk_fare_payment_method CHECK (payment_method IS NOT NULL),
ADD CONSTRAINT chk_fare_transfers CHECK (transfers BETWEEN 0 AND 2),
ADD CONSTRAINT chk_fare_transfer_duration CHECK (transfer_duration >= 0),
ADD CONSTRAINT pk_fare PRIMARY KEY (fare_id),
ADD CONSTRAINT fk_fare_payment_method FOREIGN KEY (payment_method) REFERENCES payment_method(id),
ADD CONSTRAINT fk_fare_agency_id FOREIGN KEY (agency_id) REFERENCES agency(agency_id);

-- Some indexes for faster joins.
CREATE INDEX idx_fare_agency_id ON fare(agency_id);

-- Update stats for better query performance.
ANALYZE fare;
