
DROP TABLE IF EXISTS network;
CREATE TABLE network (
    network_id TEXT,
    network_name TEXT
);

CALL load_from_csv('network', :'dataset_dir' || '/networks.txt');

-- Create constraints AFTER loading data for performance reasons.
ALTER TABLE network
ADD CONSTRAINT pk_network PRIMARY KEY (network_id);

-- Add foreign key constraint if the table is not empty.
-- TODO: Instead, we should insert all values of route(network_id) into network and then add the foreign key constraint.
DO
$$
BEGIN
   IF (SELECT COUNT(*) FROM network) > 0 THEN
      ALTER TABLE "route" ADD FOREIGN KEY (network_id) REFERENCES network(network_id);
   END IF;
END
$$;

-- Update stats for better query performance.
ANALYZE network;
