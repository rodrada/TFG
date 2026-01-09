
DROP TABLE IF EXISTS route_network;
CREATE TABLE route_network (
    network_id TEXT,
    route_id TEXT
);

CALL load_from_csv('route_network', :'dataset_dir' || '/route_networks.txt');

-- Create constraints AFTER loading data for performance reasons.
ALTER TABLE route_network
ADD CONSTRAINT pk_route_network PRIMARY KEY (route_id),
ADD CONSTRAINT fk_route_network_network_id FOREIGN KEY (network_id) REFERENCES network(network_id),
ADD CONSTRAINT fk_route_network_route_id FOREIGN KEY (route_id) REFERENCES route(route_id);

-- Move values to route since the relationship is 1:N (each route can be in a single network).
UPDATE route
SET network_id = rn.network_id
FROM route_network rn
WHERE rn.route_id = route.route_id;

DROP TABLE route_network;

-- Update stats for better query performance.
ANALYZE route;
