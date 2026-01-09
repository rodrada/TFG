
DROP TABLE IF EXISTS level;
CREATE TABLE level (
    level_id TEXT,
    level_index DOUBLE PRECISION,
    level_name TEXT
);

CALL load_from_csv('level', :'dataset_dir' || '/levels.txt');

-- Create constraints AFTER loading data for performance reasons.
ALTER TABLE level
ADD CONSTRAINT chk_level_non_nulls CHECK (level_index IS NOT NULL),
ADD CONSTRAINT pk_level PRIMARY KEY (level_id);

ALTER TABLE "stop" ADD FOREIGN KEY (level_id) REFERENCES level(level_id);

-- Update stats for better query performance.
ANALYZE level;
