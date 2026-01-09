
DROP TABLE IF EXISTS transfer_type;
CREATE TABLE transfer_type ( LIKE enum_table INCLUDING ALL );
INSERT INTO transfer_type VALUES
    (0, 'Recommended'),
    (1, 'Timed'),
    (2, 'Requires Time'),
    (3, 'Not Between Routes'),
    (4, 'In-Seat Transfer'),
    (5, 'Must Re-Board');

DROP TABLE IF EXISTS transfer;
CREATE TABLE transfer (
    from_stop_id TEXT,
    to_stop_id TEXT,
    from_route_id TEXT,
    to_route_id TEXT,
    from_trip_id TEXT,
    to_trip_id TEXT,
    transfer_type INTEGER DEFAULT 0,
    min_transfer_time INTEGER
);

CALL load_from_csv('transfer', :'dataset_dir' || '/transfers.txt');

-- Create constraints AFTER loading data for performance reasons.
ALTER TABLE transfer
ADD CONSTRAINT chk_transfer_stop_ids CHECK ((from_stop_id IS NOT NULL AND to_stop_id IS NOT NULL) OR NOT (transfer_type BETWEEN 1 AND 3)),
ADD CONSTRAINT chk_transfer_trip_ids CHECK ((from_trip_id IS NOT NULL AND to_trip_id IS NOT NULL) OR NOT (transfer_type BETWEEN 4 AND 5)),
ADD CONSTRAINT chk_transfer_min_transfer_time_non_negative CHECK (min_transfer_time >= 0),
ADD CONSTRAINT fk_transfer_from_stop_id FOREIGN KEY (from_stop_id) REFERENCES stop(stop_id),
ADD CONSTRAINT fk_transfer_to_stop_id FOREIGN KEY (to_stop_id) REFERENCES stop(stop_id),
ADD CONSTRAINT fk_transfer_from_route_id FOREIGN KEY (from_route_id) REFERENCES route(route_id),
ADD CONSTRAINT fk_transfer_to_route_id FOREIGN KEY (to_route_id) REFERENCES route(route_id),
ADD CONSTRAINT fk_transfer_from_trip_id FOREIGN KEY (from_trip_id) REFERENCES trip(trip_id),
ADD CONSTRAINT fk_transfer_to_trip_id FOREIGN KEY (to_trip_id) REFERENCES trip(trip_id),
ADD CONSTRAINT fk_transfer_transfer_type FOREIGN KEY (transfer_type) REFERENCES transfer_type(id);

-- PK equivalent since some of the fields can be NULL
CREATE UNIQUE INDEX idx_transfer
    ON transfer (from_stop_id, to_stop_id, from_trip_id,
                 to_trip_id, from_route_id, to_route_id)
    NULLS NOT DISTINCT;

-- Some indexes for faster joins.
CREATE INDEX idx_transfer_from_stop ON transfer(from_stop_id);
CREATE INDEX idx_transfer_to_stop ON transfer(to_stop_id);
CREATE INDEX idx_transfer_from_route ON transfer(from_route_id);
CREATE INDEX idx_transfer_to_route ON transfer(to_route_id);
CREATE INDEX idx_transfer_from_trip ON transfer(from_trip_id);
CREATE INDEX idx_transfer_to_trip ON transfer(to_trip_id);

-- Update stats for better query performance.
ANALYZE transfer;
