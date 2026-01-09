
-- Start creating tables and importing data
DROP TABLE IF EXISTS agency;
CREATE TABLE agency (
    agency_id TEXT,
    agency_name TEXT,
    agency_url TEXT,
    agency_timezone TEXT,
    agency_lang TEXT,
    agency_phone TEXT,
    agency_fare_url TEXT,
    agency_email TEXT
);

-- Convert language codes to lowercase if they aren't already
-- TODO: Replace this trigger with a simple update after loading the data.
CREATE OR REPLACE FUNCTION lowercase_agency_lang() RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    NEW.agency_lang = LOWER(NEW.agency_lang);
    RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER agency_lang_case
BEFORE INSERT OR UPDATE ON agency
FOR EACH ROW EXECUTE FUNCTION lowercase_agency_lang();

CALL load_from_csv('agency', :'dataset_dir' || '/agency.txt');

-- Create constraints AFTER loading data for performance reasons.
ALTER TABLE agency
ADD CONSTRAINT chk_agency_name CHECK (agency_name IS NOT NULL),
ADD CONSTRAINT chk_agency_url CHECK (agency_url IS NOT NULL AND agency_url ~ '^https?://'),
ADD CONSTRAINT chk_agency_timezone CHECK (agency_timezone IS NOT NULL),
ADD CONSTRAINT chk_agency_fare_url CHECK (agency_fare_url ~ '^https?://'),
ADD CONSTRAINT pk_agency PRIMARY KEY (agency_id);

-- Update stats for better query performance.
ANALYZE agency;
