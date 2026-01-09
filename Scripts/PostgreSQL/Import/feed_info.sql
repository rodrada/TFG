
DROP TABLE IF EXISTS feed;
CREATE TABLE feed (
    feed_publisher_name TEXT,
    feed_publisher_url TEXT,
    feed_lang TEXT,
    default_lang TEXT,
    feed_start_date DATE,
    feed_end_date DATE,
    feed_version TEXT,
    feed_contact_email TEXT,
    feed_contact_url TEXT
);

CALL load_from_csv('feed', :'dataset_dir' || '/feed_info.txt');

-- Create constraints AFTER loading data for performance reasons.
ALTER TABLE feed
ADD CONSTRAINT chk_feed_feed_publisher_url CHECK (feed_publisher_url IS NOT NULL AND feed_publisher_url ~ '^https?://'),
ADD CONSTRAINT chk_feed_feed_lang CHECK (feed_lang IS NOT NULL),
ADD CONSTRAINT chk_feed_feed_contact_url CHECK (feed_contact_url ~ '^https?://'),
ADD CONSTRAINT pk_feed PRIMARY KEY (feed_publisher_name);

-- Update stats for better query performance.
ANALYZE feed;
