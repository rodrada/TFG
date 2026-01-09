
DROP TABLE IF EXISTS exception_type;
CREATE TABLE exception_type ( LIKE enum_table INCLUDING ALL );
INSERT INTO exception_type VALUES
    (1, 'Added'),
    (2, 'Removed');

DROP TABLE IF EXISTS service_exception;
CREATE TABLE service_exception (
    service_id TEXT,                -- Can reference service(service_id) or not.
    date DATE,
    exception_type INTEGER
);

CALL load_from_csv('service_exception', :'dataset_dir' || '/calendar_dates.txt');

-- Create constraints AFTER loading data for performance reasons.
ALTER TABLE service_exception
ADD CONSTRAINT chk_service_exception_exception_type CHECK (exception_type IS NOT NULL),
ADD CONSTRAINT pk_service_exception PRIMARY KEY (service_id, date),
ADD CONSTRAINT fk_service_exception_exception_type FOREIGN KEY (exception_type) REFERENCES exception_type(id);

-- Update stats for better query performance.
ANALYZE service_exception;
