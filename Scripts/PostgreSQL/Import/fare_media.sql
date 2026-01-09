
DROP TABLE IF EXISTS fare_media_type;
CREATE TABLE fare_media_type ( LIKE enum_table INCLUDING ALL );
INSERT INTO fare_media_type VALUES
    (0, 'None'),
    (1, 'Physical Ticket'),
    (2, 'Physical Transit Card'),
    (3, 'Contactless EMV'),
    (4, 'Mobile App');

DROP TABLE IF EXISTS fare_media;
CREATE TABLE fare_media (
    fare_media_id TEXT,
    fare_media_name TEXT,
    fare_media_type INTEGER
);

CALL load_from_csv('fare_media', :'dataset_dir' || '/fare_media.txt');

-- Create constraints AFTER loading data for performance reasons.
ALTER TABLE fare_media
ADD CONSTRAINT chk_fare_media_fare_media_type CHECK (fare_media_type IS NOT NULL),
ADD CONSTRAINT pk_fare_media PRIMARY KEY (fare_media_id),
ADD CONSTRAINT fk_fare_media_fare_media_type FOREIGN KEY (fare_media_type) REFERENCES fare_media_type(id);

-- Update stats for better query performance.
ANALYZE fare_media;
