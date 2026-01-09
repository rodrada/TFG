
from datetime import timedelta
import pytest
import random
from typing import Any, Callable
import time
import json
from neo4j.time import Duration
from hypothesis import settings

# Amount of random tests to run for each query.
RANDOM_TEST_COUNT = 30
RANDOM_SLOW_TEST_COUNT = 10
RANDOM_SEED = 42

settings.register_profile("slow", max_examples=RANDOM_SLOW_TEST_COUNT)
settings.load_profile("slow")

# This adds the parent directory (project root) to the Python path
# so that we can import modules from the 'Scripts' directory.
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Generate infrastructure for the tests: DB connections
from Scripts import database

# Re-export queries for test files.
QUERIES = database.QUERIES

# File to store execution times for all queries.
QUERY_PERFORMANCE_FILE = "query_performance.json"

# Generate fixtures to query both PostgreSQL and Neo4J from tests.
@pytest.fixture(scope="session")
def pg_query_runner():
    """Transforms the PostgreSQL query runner returned by the database module into a fixture."""
    with database.pg_query_runner() as runner:
        yield runner

@pytest.fixture(scope="session")
def neo4j_query_runner():
    """Transforms the Neo4J driver returned by the database module into a fixture."""
    with database.neo4j_query_runner() as runner:
        yield runner

@pytest.fixture(scope="session")
def bounding_box(pg_query_runner) -> dict:
    """Transforms the bounding box returned by the database module into a fixture."""
    return database.bounding_box(pg_query_runner)

@pytest.fixture(scope="session")
def execution_times():
    """Opens the query performance file as a fixture."""
    print("Loading execution times from file...")
    try:
        with open(QUERY_PERFORMANCE_FILE, "r") as f:
            execution_times = json.load(f)
    except Exception:
        print("Warning: Query performance file not found or corrupted. Starting with an empty file.")
        execution_times = {}

    yield execution_times

    print("Saving execution times to file...")
    with open(QUERY_PERFORMANCE_FILE, "w") as f:
        json.dump(execution_times, f, indent=4)

def random_point_in_bbox(bbox: dict) -> tuple[float, float]:
    """Generates a random (latitude, longitude) tuple within a given bounding box."""
    random_lat = random.uniform(bbox['min_lat'], bbox['max_lat'])
    random_lon = random.uniform(bbox['min_lon'], bbox['max_lon'])
    return (random_lat, random_lon)

def random_stop_id(pg_query_runner):
    """Fetches a random, valid stop_id from the database."""
    try:
        result = pg_query_runner("SELECT stop_id FROM stop ORDER BY RANDOM() LIMIT 1;", ())
        if result:
            return result[0]['stop_id']
    except Exception as e:
        pytest.fail(f"Could not fetch a random stop_id from the database: {e}")
    return None

@pytest.fixture(scope="session")
def service_date_range(pg_query_runner) -> dict | None:
    """Finds the earliest start_date and latest end_date in the service calendar."""
    query = "SELECT MIN(start_date), MAX(end_date) FROM service;"
    try:
        result = pg_query_runner(query, ())
        if result and result[0]['min'] and result[0]['max']:
            return {"min_date": result[0]['min'], "max_date": result[0]['max']}
    except Exception as e:
        pytest.fail(f"Could not determine service date range from database: {e}")
    return None

# TODO: We should probably consider deleted exceptions.
@pytest.fixture(scope="session")
def known_service_exception(pg_query_runner) -> dict | None:
    """Finds one example of an 'added' (type 1) service exception."""
    query = "SELECT service_id, date FROM service_exception WHERE exception_type = 1 LIMIT 1;"
    try:
        result = pg_query_runner(query, ())
        if result:
            return {"service_id": result[0]['service_id'], "date": result[0]['date']}
    except Exception:
        # It's okay if a feed has no exceptions, so we don't fail the test suite
        return None


# Run arbitrary test cases.
def run_test_case(
    pg_query_runner: Callable,
    neo4j_query_runner: Callable,
    pg_query: str,
    neo4j_query: str,
    pg_params: tuple,
    neo4j_params: dict,
    pg_extractor: Callable[[list], list],
    neo4j_extractor: Callable[[list], list],
    plausibility_checks: list[Callable[[list], None]] = [],
    result_name: str = "results",
    comparison_function: Callable[[list, list], bool] = lambda pg, neo4j: pg == neo4j
) -> tuple[list, float, float]:
    """
    Executes a query in both PostgreSQL and Neo4j and validates the results.

    This generic function handles the boilerplate of querying, extracting data,
    running plausibility checks, and performing cross-validation.

    Args:
        pg_query_runner: A callable that executes a PostgreSQL query.
        neo4j_query_runner: A callable that executes a Neo4j query.
        pg_query: The PostgreSQL query string.
        neo4j_query: The Cypher query string.
        pg_params: A tuple of parameters for the PostgreSQL query.
        neo4j_params: A dictionary of parameters for the Neo4j query.
        pg_extractor: A function to transform the raw PG result list into a
                      standardized format for comparison.
        neo4j_extractor: A function to transform the raw Neo4j result list.
        plausibility_checks: A list of functions, each performing an assertion
                             on the extracted data for a single database.
        result_name: A descriptive name for the items being tested (e.g., "stops").

    Returns:
        The extracted and cross-validated result set.
    """
    # 1. Query both databases with their specific queries and parameters
    print(f"  Querying databases with params: PostgreSQL={pg_params}, Neo4J={neo4j_params}")
    start_time = time.time()
    pg_results_raw = pg_query_runner(pg_query, pg_params)
    pg_exec_time = time.time() - start_time
    start_time = time.time()
    neo4j_results_raw = neo4j_query_runner(neo4j_query, neo4j_params)
    neo4j_exec_time = time.time() - start_time

    # 2. Extract and standardize the results using the provided extractor functions.
    pg_extracted = pg_extractor(pg_results_raw)
    neo4j_extracted = neo4j_extractor(neo4j_results_raw)

    # 3. Run all provided plausibility checks on each result set.
    if plausibility_checks:
        print(f"  Running {len(plausibility_checks)} plausibility check(s)...")
        for i, check_func in enumerate(plausibility_checks, 1):
            try:
                check_func(pg_extracted)
                check_func(neo4j_extracted)
            except AssertionError as e:
                print(f"  Plausibility check #{i} failed!")
                raise e # Re-raise the exception to fail the test
        print("  Plausibility checks passed.")

    # 4. Log the result counts.
    print(f"  PostgreSQL found {len(pg_extracted)} {result_name}.")
    print(f"  Neo4J found {len(neo4j_extracted)} {result_name}.")

    # 5. Perform the final cross-validation.
    # Sort them in case there are mismatches in collation and similar problems.
    sorted_pg_extracted = sorted(pg_extracted)
    sorted_neo4j_extracted = sorted(neo4j_extracted)
    assert comparison_function(sorted_pg_extracted, sorted_neo4j_extracted), \
        f"Result mismatch for params {neo4j_params}!\n" \
        f"PostgreSQL ({len(sorted_pg_extracted)}): {sorted_pg_extracted}\n" \
        f"Neo4J ({len(sorted_neo4j_extracted)}): {sorted_neo4j_extracted}"

    print("  Results match.")

    return (pg_extracted, pg_exec_time, neo4j_exec_time)


def to_canonical_time_str(time_val: Any) -> str:
    """
    Convert a time-like value into a canonical, zero-padded "HH:MM:SS" string.

    Supported inputs:
      * datetime.timedelta (e.g. from PostgreSQL drivers)
      * Neo4J-like Duration objects (having attributes:
            months, days, seconds, nanoseconds,
            and hours_minutes_seconds_nanoseconds)

    Notes / design choices:
      * If a Duration has a non-zero `months` component we raise TypeError
        because months are not a fixed number of seconds (ambiguous).
      * Fractional seconds (nanoseconds) are truncated toward zero (int()).
      * Negative durations are returned with a leading '-' (e.g. "-01:02:03").
    """
    if time_val is None:
        return None
    # timedelta path (PostgreSQL)
    if isinstance(time_val, timedelta):
        total_seconds = int(time_val.total_seconds())  # truncates toward zero
    else:
        # Detect Neo4J Duration-like objects by the high-level helper attribute
        hmss = getattr(time_val, "hours_minutes_seconds_nanoseconds", None)
        if hmss is not None:
            months = getattr(time_val, "months", 0)
            if months:
                raise TypeError(
                    "Cannot convert Duration with non-zero months to HH:MM:SS "
                    "(months are ambiguous)."
                )
            days = int(getattr(time_val, "days", 0))
            h_comp, m_comp, s_comp, ns_comp = hmss  # hours, minutes, seconds, nanoseconds
            # Build a float total seconds and truncate towards zero (like int(float(...)) )
            total_seconds = int(days * 86400 + h_comp * 3600 + m_comp * 60 + s_comp + ns_comp / 1e9)
        else:
            # Fallback: try to look for seconds/nanoseconds/days attributes
            if all(hasattr(time_val, attr) for attr in ("seconds", "nanoseconds", "days")):
                months = getattr(time_val, "months", 0)
                if months:
                    raise TypeError(
                        "Cannot convert Duration with non-zero months to HH:MM:SS "
                        "(months are ambiguous)."
                    )
                days = int(getattr(time_val, "days", 0))
                sec = getattr(time_val, "seconds")
                ns = getattr(time_val, "nanoseconds")
                total_seconds = int(days * 86400 + float(sec) + float(ns) / 1e9)
            else:
                raise TypeError(f"Unsupported type for time conversion: {type(time_val)}")

    # Format into HH:MM:SS (zero-padded). Use an explicit sign for negative durations.
    sign = "-" if total_seconds < 0 else ""
    abs_secs = abs(total_seconds)
    hours = abs_secs // 3600
    minutes = (abs_secs % 3600) // 60
    seconds = abs_secs % 60

    return f"{sign}{hours:02}:{minutes:02}:{seconds:02}"

def neo4j_duration_to_timedelta(neo4j_duration: Duration) -> timedelta:
    """
    Converts a neo4j.time.Duration object to a Python datetime.timedelta object.
    """
    if neo4j_duration is None:
        return None

    return timedelta(
        days=neo4j_duration.days or 0,
        seconds=neo4j_duration.seconds or 0,
        microseconds=(neo4j_duration.nanoseconds or 0) // 1000
    )

def time_str_to_seconds(time_str: str) -> int:
    """Convert a time string 'HH:MM:SS' to total seconds."""
    h, m, s = map(int, time_str.split(':'))
    return h * 3600 + m * 60 + s

