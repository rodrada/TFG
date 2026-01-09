
DROP TABLE IF EXISTS fare_product;
CREATE TABLE fare_product (
    fare_product_id TEXT,
    fare_product_name TEXT,
    fare_media_id TEXT,
    amount NUMERIC(10, 4),
    currency TEXT
);

CALL load_from_csv('fare_product', :'dataset_dir' || '/fare_products.txt');

-- Create constraints AFTER loading data for performance reasons.
ALTER TABLE fare_product
ADD CONSTRAINT chk_fare_product_fare_product_id CHECK (fare_product_id IS NOT NULL),
ADD CONSTRAINT chk_fare_product_amount CHECK (amount IS NOT NULL),
ADD CONSTRAINT chk_fare_product_currency CHECK (currency IS NOT NULL AND currency ~ '^[A-Z]{3}$'),
ADD CONSTRAINT fk_fare_product_fare_media_id FOREIGN KEY (fare_media_id) REFERENCES fare_media(fare_media_id);

-- PK equivalent since some of the fields can be NULL
CREATE UNIQUE INDEX idx_fare_product
    ON fare_product (fare_product_id, fare_media_id) 
    NULLS NOT DISTINCT;

-- Some indexes for faster joins.
CREATE INDEX idx_fare_product_media_id ON fare_product(fare_media_id);

-- Update stats for better query performance.
ANALYZE fare_product;
