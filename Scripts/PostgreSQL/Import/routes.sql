
DROP TABLE IF EXISTS route_type;
CREATE TABLE route_type ( LIKE enum_table INCLUDING ALL );
INSERT INTO route_type VALUES
    (0, 'Tram'),            -- Equivalent to 900
    (1, 'Metro'),           -- Equivalent to 400
    (2, 'Rail'),            -- Equivalent to 100
    (3, 'Bus'),             -- Equivalent to 300
    (4, 'Ferry'),           -- Equivalent to 1000
    (5, 'Cable Tram'),
    (6, 'Aerial Lift'),     -- Equivalent to 1300
    (7, 'Funicular'),       -- Equivalent to 1400
    (11, 'Trolleybus'),
    (12, 'Monorail'),
    (100, 'Railway'),
    (101, 'High Speed Rail'),
    (102, 'Long Distance Train'),
    (103, 'Inter Regional Rail'),
    (104, 'Car Transport Rail'),
    (105, 'Sleeper Rail'),
    (106, 'Regional Rail'),
    (107, 'Tourist Railway'),
    (108, 'Rail Shuttle (Within Complex)'),
    (109, 'Suburban Railway'),
    (110, 'Replacement Rail'),
    (111, 'Special Rail'),
    (112, 'Lorry Transport Rail'),
    (113, 'All Rail Services'),
    (114, 'Cross-Country Rail'),
    (115, 'Vehicle Transport Rail'),
    (116, 'Rack and Pinion Railway'),
    (117, 'Additional Rail'),
    (200, 'Coach'),
    (201, 'International Coach'),
    (202, 'National Coach'),
    (203, 'Shuttle Coach'),
    (204, 'Regional Coach'),
    (205, 'Special Coach'),
    (206, 'Sightseeing Coach'),
    (207, 'Tourist Coach'),
    (208, 'Commuter Coach'),
    (209, 'All Coach Services'),
    (400, 'Urban Railway'),
    (401, 'Metro'),
    (402, 'Underground'),
    (403, 'Urban Railway'),
    (404, 'All Urban Railway Services'),
    (405, 'Monorail'),
    (700, 'Bus'),
    (701, 'Regional Bus'),
    (702, 'Express Bus'),
    (703, 'Stopping Bus'),
    (704, 'Local Bus'),
    (705, 'Night Bus'),
    (706, 'Post Bus'),
    (707, 'Special Needs Bus'),
    (708, 'Mobility Bus'),
    (709, 'Mobility Bus for Registered Disabled'),
    (710, 'Sightseeing Bus'),
    (711, 'Shuttle Bus'),
    (712, 'School Bus'),
    (713, 'School and Public Service Bus'),
    (714, 'Rail Replacement Bus'),
    (715, 'Demand and Response Bus'),
    (716, 'All Bus Services'),
    (800, 'Trolleybus'),
    (900, 'Tram'),
    (901, 'City Tram'),
    (902, 'Local Tram'),
    (903, 'Regional Tram'),
    (904, 'Sightseeing Tram'),
    (905, 'Shuttle Tram'),
    (906, 'All Tram Services'),
    (1000, 'Water Transport'),
    (1100, 'Air'),
    (1200, 'Ferry'),
    (1300, 'Aerial Lift'),
    (1301, 'Telecabin'),
    (1302, 'Cable Car'),
    (1303, 'Elevator'),
    (1304, 'Chair Lift'),
    (1305, 'Drag Lift'),
    (1306, 'Small Telecabin'),
    (1307, 'All Telecabin Services'),
    (1400, 'Funicular'),
    (1500, 'Taxi'),
    (1501, 'Communal Taxi'),
    (1502, 'Water Taxi'),
    (1503, 'Rail Taxi'),
    (1504, 'Bike Taxi'),
    (1505, 'Licensed Taxi'),
    (1506, 'Private Hire Service Vehicle'),
    (1507, 'All Taxi Services'),
    (1700, 'Miscellaneous'),
    (1702, 'Horse-drawn Carriage');

DROP TABLE IF EXISTS continuous_status;
CREATE TABLE continuous_status ( LIKE enum_table INCLUDING ALL );
INSERT INTO continuous_status VALUES
    (0, 'Continuous'),
    (1, 'Not Continuous'),
    (2, 'Must Phone Agency'),
    (3, 'Must Coordinate With Driver');

DROP TABLE IF EXISTS "route";
CREATE TABLE route (
    route_id TEXT,
    agency_id TEXT,
    route_short_name TEXT,
    route_long_name TEXT,
    route_desc TEXT,
    route_type INTEGER,
    route_url TEXT,
    route_color CHAR(6),
    route_text_color CHAR(6),
    route_sort_order INTEGER,
    continuous_pickup INTEGER DEFAULT 1,
    continuous_drop_off INTEGER DEFAULT 1,
    network_id TEXT
);

-- Convert color hex codes to uppercase if they aren't already
CREATE OR REPLACE FUNCTION uppercase_route_color() RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    NEW.route_color = UPPER(NEW.route_color);
    NEW.route_text_color = UPPER(NEW.route_text_color);
    RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER route_color_case 
BEFORE INSERT OR UPDATE ON "route"
FOR EACH ROW EXECUTE FUNCTION uppercase_route_color();

CALL load_from_csv('route', :'dataset_dir' || '/routes.txt');

-- Create constraints AFTER loading data for performance reasons.
ALTER TABLE route
ADD CONSTRAINT chk_route_non_nulls CHECK (route_type IS NOT NULL),
ADD CONSTRAINT chk_route_route_url CHECK (route_url ~ '^https?://'),
ADD CONSTRAINT chk_route_route_color CHECK (route_color ~ '^[A-F0-9]{6}$'),
ADD CONSTRAINT chk_route_text_color CHECK (route_text_color ~ '^[A-F0-9]{6}$'),
ADD CONSTRAINT pk_route PRIMARY KEY (route_id),
ADD CONSTRAINT fk_route_agency_id FOREIGN KEY (agency_id) REFERENCES agency(agency_id),
ADD CONSTRAINT fk_route_route_type FOREIGN KEY (route_type) REFERENCES route_type(id),
ADD CONSTRAINT fk_route_continuous_pickup FOREIGN KEY (continuous_pickup) REFERENCES continuous_status(id),
ADD CONSTRAINT fk_route_continuous_drop_off FOREIGN KEY (continuous_drop_off) REFERENCES continuous_status(id);

CREATE INDEX idx_route_network_id ON route(network_id);

-- Update stats for better query performance.
ANALYZE "route";
