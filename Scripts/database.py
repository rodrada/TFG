
# This file is meant to handle database connections and queries,
# and be imported from other scripts.

from contextlib import contextmanager
from neo4j import GraphDatabase
import psycopg
from psycopg.rows import dict_row

# PostgreSQL connection info.
PG_CONFIG = {
    "dbname": "gtfs",
    "user": "postgres",
    "password": "12345678",
    "host": "127.0.0.1",
    "port": "5432"
}

# Neo4J connection info.
NEO4J_CONFIG = {
    "uri": "bolt://127.0.0.1:7687",
    "user": "neo4j",
    "password": "",
    "database": "gtfs"
}

QUERY_PARAMETERS = {
    'active_services': [ 'curr_date' ],
    'daily_status': [ 'start_date', 'end_date' ],
    'departure_times': [ 'route_id', 'stop_id', 'curr_date' ],
    'headway_stats': [ 'curr_date' ],
    'next_departures': [ 'stop_id', 'curr_date', 'curr_time' ],
    'overlapping_segments': [],
    'routes_by_relevance': [ 'curr_date', 'curr_time' ],
    'routes_by_speed': [],
    'stops_within_distance': [ 'origin_lat', 'origin_lon', 'seek_dist' ],
    'top_stops': [ 'curr_date' ],
    'trip_start_time_distribution': [ 'curr_date', 'bucket_size_min' ],

    # NOTE: These queries are not available in Neo4J due to lack of library support
    #       or excessive complexity in implementation (modified CSA algorithm).
    'stop_density_heatmap': [ 'grid_size_meters' ],
    'earliest_arrivals': [ 'origin_stop_id', 'departure_date', 'departure_time' ],
    'shortest_path': [ 'origin_stop_id', 'destination_stop_id', 'departure_date', 'departure_time' ],
    'route_straightness': []
}

QUERIES = { 'postgres': {}, 'neo4j': {} }
for query_name, params in QUERY_PARAMETERS.items():
    pg_param_string = ", ".join(["%s"] * len(params))
    QUERIES['postgres'][query_name] = f"SELECT * FROM {query_name}({pg_param_string});"
    neo4j_param_string = ", ".join([f"{param}: ${param}" for param in params])
    QUERIES['neo4j'][query_name] = f"""
        MATCH (cq: CypherQuery {{name: '{query_name}'}})
        CALL apoc.cypher.run(cq.statement, {{{neo4j_param_string}}}) YIELD value
        RETURN value
    """

# Generate functions to query both PostgreSQL and Neo4J.
@contextmanager
def pg_query_runner():
    """
    Yields a FUNCTION that can execute a PostgreSQL query,
    given its SQL code and parameters (in a tuple).
    """
    conn_string = (
        f"dbname={PG_CONFIG['dbname']} user={PG_CONFIG['user']} "
        f"password={PG_CONFIG['password']} host={PG_CONFIG['host']} port={PG_CONFIG['port']}"
    )
    print("--- Initializing PostgreSQL connection ---")
    with psycopg.connect(conn_string) as conn:

        def _run_query(sql: str, params: tuple) -> list:
            try:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute(sql, params)
                    return cur.fetchall()
            except Exception as e:
                raise e
    
        yield _run_query
    print("--- PostgreSQL connection closed ---")

@contextmanager
def neo4j_query_runner():
    """
    Yields a FUNCTION that can execute a Neo4J query,
    given its Cypher code and parameters (in a dictionary).
    """
    print("--- Setting up Neo4J driver ---")
    driver = GraphDatabase.driver(
        NEO4J_CONFIG['uri'],
        auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])
    )
    
    def _run_query(cypher: str, params: dict) -> list:
        try:
            with driver.session(database=NEO4J_CONFIG['database']) as session:
                result = session.run(cypher, **params)
                return result.data()
        except Exception as e:
            raise e

    yield _run_query

    driver.close()
    print("--- Neo4J connection closed ---")

def bounding_box(pg_query_runner) -> dict:
    """Queries PostgreSQL to determine the bounding box that contains all stops."""

    sql = '''
        SELECT
            ST_YMin(bbox) AS min_lat,
            ST_YMax(bbox) AS max_lat,
            ST_XMin(bbox) AS min_lon,
            ST_XMax(bbox) AS max_lon
        FROM (
            -- ST_Extent is an aggregate function that computes the bounding box
            -- for a set of geometries.
            SELECT ST_Extent(location) AS bbox
            FROM stop
        ) AS subquery;
    '''

    pg_bbox = pg_query_runner(sql, ())[0]
    return pg_bbox

# Helper function to represent data on the map.
def focused_bounds(gdf, quantile_trim=0.05):
    """
    Calculates map bounds focused on the dense area of geometries, trimming outliers.

    Args:
        gdf (GeoDataFrame): The GeoDataFrame containing the geometries.
        quantile_trim (float): The percentage to trim from each end of the coordinate
                               distribution (e.g., 0.05 means trim the bottom 5% and top 5%).

    Returns:
        tuple: A tuple containing (min_x, max_x, min_y, max_y) for the focused view.
    """
    # Get the bounds of each individual geometry
    bounds = gdf.bounds

    # Drop any empty or invalid geometries that might result in all-zero rows
    bounds = bounds[(bounds.minx != 0) | (bounds.miny != 0) | (bounds.maxx != 0) | (bounds.maxy != 0)]

    # Calculate the quantiles for x and y coordinates
    # This effectively ignores the outlier coordinates
    min_x = bounds['minx'].quantile(quantile_trim)
    max_x = bounds['maxx'].quantile(1 - quantile_trim)
    min_y = bounds['miny'].quantile(quantile_trim)
    max_y = bounds['maxy'].quantile(1 - quantile_trim)

    return min_x, max_x, min_y, max_y
