
DROP TABLE IF EXISTS pathway_type;
CREATE TABLE pathway_type ( LIKE enum_table INCLUDING ALL );
INSERT INTO pathway_type VALUES
    (1, 'Walkway'),
    (2, 'Stairs'),
    (3, 'Moving Sidewalk/Travelator'),
    (4, 'Escalator'),
    (5, 'Elevator'),
    (6, 'Fare Gate'),
    (7, 'Exit Gate');

DROP TABLE IF EXISTS pathway;
CREATE TABLE pathway (
    pathway_id TEXT,
    from_stop_id TEXT,
    to_stop_id TEXT,
    pathway_mode INTEGER,
    is_bidirectional BOOLEAN,
    length DOUBLE PRECISION,
    traversal_time INTEGER,     -- This time is in seconds. TODO: Maybe it should be stored as an INTERVAL?
    stair_count INTEGER,
    max_slope DOUBLE PRECISION,
    min_width DOUBLE PRECISION,
    signposted_as TEXT,
    reversed_signposted_as TEXT
);

CALL load_from_csv('pathway', :'dataset_dir' || '/pathways.txt');

-- Create constraints AFTER loading data for performance reasons.
ALTER TABLE pathway
ADD CONSTRAINT chk_pathway_non_nulls CHECK (num_nulls(from_stop_id, to_stop_id, pathway_mode, is_bidirectional) = 0),
ADD CONSTRAINT chk_pathway_length_non_negative CHECK (length >= 0),
ADD CONSTRAINT chk_pathway_traversal_time_positive CHECK (traversal_time > 0),
ADD CONSTRAINT chk_pathway_stair_count_non_zero CHECK (stair_count <> 0),
ADD CONSTRAINT chk_pathway_min_width_positive CHECK (min_width > 0),
ADD CONSTRAINT pk_pathway PRIMARY KEY (pathway_id),
ADD CONSTRAINT fk_pathway_from_stop_id FOREIGN KEY (from_stop_id) REFERENCES stop(stop_id),
ADD CONSTRAINT fk_pathway_to_stop_id FOREIGN KEY (to_stop_id) REFERENCES stop(stop_id),
ADD CONSTRAINT fk_pathway_pathway_mode FOREIGN KEY (pathway_mode) REFERENCES pathway_type(id);

-- Some indexes for faster joins.
CREATE INDEX idx_pathway_stop_pair ON pathway(from_stop_id, to_stop_id);

-- Update stats for better query performance.
ANALYZE pathway;
