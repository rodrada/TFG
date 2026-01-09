
DROP TABLE IF EXISTS translation;
CREATE TABLE translation (
    table_name TEXT,
    field_name TEXT,
    language TEXT,
    translation TEXT,
    record_id TEXT,
    record_sub_id TEXT,
    field_value TEXT
);

CALL load_from_csv('translation', :'dataset_dir' || '/translations.txt');

-- Create constraints AFTER loading data for performance reasons.
ALTER TABLE translation
ADD CONSTRAINT chk_translation_non_nulls CHECK (num_nulls(table_name, field_name, language, translation) = 0),
ADD CONSTRAINT chk_translation_table_name CHECK (table_name IN ('agency', 'stops', 'routes', 'trips', 'stop_times', 'pathways', 'levels', 'feed_info', 'attributions')),
ADD CONSTRAINT chk_translation_field_info CHECK (
    (table_name = 'feed_info' AND (field_value IS NULL) AND (record_id IS NULL))
    OR
    (table_name <> 'feed_info' AND (field_value IS NULL) <> (record_id IS NULL))
),
ADD CONSTRAINT chk_translation_record_sub_id CHECK ((record_sub_id IS NOT NULL) = (table_name = 'stop_times' AND (record_id IS NOT NULL)));

-- PK equivalent since some of the fields can be NULL
CREATE UNIQUE INDEX idx_translation
    ON translation (table_name, field_name, language, record_id, field_value)
    NULLS NOT DISTINCT;

-- Some indexes for faster joins.
CREATE INDEX idx_translation_table_name ON translation(table_name);
CREATE INDEX idx_translation_field_name ON translation(field_name);
CREATE INDEX idx_translation_record_id ON translation(record_id);
CREATE INDEX idx_translation_record_sub_id ON translation(record_sub_id);
CREATE INDEX idx_translation_field_value ON translation(field_value);

-- Update stats for better query performance.
ANALYZE translation;
