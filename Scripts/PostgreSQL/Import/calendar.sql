
DROP TABLE IF EXISTS "service";
CREATE TABLE service (
    service_id TEXT,
    monday BOOLEAN,
    tuesday BOOLEAN,
    wednesday BOOLEAN,
    thursday BOOLEAN,
    friday BOOLEAN,
    saturday BOOLEAN,
    sunday BOOLEAN,
    start_date DATE,
    end_date DATE
);

CALL load_from_csv('service', :'dataset_dir' || '/calendar.txt');

-- Create constraints AFTER loading data for performance reasons.
ALTER TABLE "service"
ADD CONSTRAINT chk_service_non_nulls CHECK (num_nulls(monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date) = 0),
ADD CONSTRAINT pk_service PRIMARY KEY (service_id);

-- Update stats for better query performance.
ANALYZE "service";
