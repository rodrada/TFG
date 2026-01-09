import pytest
from conftest import run_test_case as rtc, RANDOM_TEST_COUNT, RANDOM_SEED, neo4j_duration_to_timedelta, QUERIES
from hypothesis import given, strategies as st, settings
from datetime import date, timedelta
import random

# Query statements.
SQL = QUERIES['postgres']['routes_by_relevance']
CYPHER = QUERIES['neo4j']['routes_by_relevance']

random.seed(RANDOM_SEED)

# Run test case.
def run_test_case(pg_query_runner, neo4j_query_runner, curr_date: date, curr_time: str) -> list:
    """
    Calls the generic run_test_case function with parameters for the routes_by_relevance query.
    """

    # Plausibility checks.
    def counts_are_positive(results):
        """Asserts that the active_trip_count is greater than zero."""
        for row in results:
            assert row[1] > 0, f"Found non-positive active_trip_count in row: {row}"

    def route_name_is_present(results):
        """Asserts that the route name is not null or empty."""
        for row in results:
            assert row[0] is not None and row[0].strip() != "", f"Found a row with a missing route name: {row}"

    return rtc(
        pg_query_runner,
        neo4j_query_runner,
        SQL,
        CYPHER,
        (curr_date, curr_time),
        {'curr_date': str(curr_date), 'curr_time': curr_time},
        # Extract and normalize results from PostgreSQL.
        lambda pg_results: [(
            row['route_name'],
            int(row['active_trip_count']),
            row['avg_frequency']
        ) for row in pg_results],
        # Extract and normalize results from Neo4j.
        lambda neo4j_results: [(
            res['value']['route_name'],
            int(res['value']['active_trip_count']),
            neo4j_duration_to_timedelta(res['value']['avg_frequency'])
        ) for res in neo4j_results],
        plausibility_checks=[counts_are_positive, route_name_is_present],
        result_name="routes by relevance",
        # Check if the frequencies are approximately the same (allow 1 second differences for rounding errors).
        comparison_function=lambda pg, neo4j: len(pg) == len(neo4j) and all(
            p[0] == n[0] and
            p[1] == n[1] and
            ((p[2] is None and n[2] is None) or abs(p[2] - n[2]) <= timedelta(seconds=1))
            for p, n in zip(pg, neo4j)
        )
    )

def test_random_inputs(pg_query_runner, neo4j_query_runner, service_date_range, execution_times):
    """
    CROSS-VALIDATION: Generates random inputs and asserts results are plausible and consistent.
    """
    print(f"\nRunning random input tests for 'routes_by_relevance' ({RANDOM_TEST_COUNT} iterations).")
    
    assert service_date_range is not None, "Test setup failed: Service date range could not be determined."
    min_date = service_date_range['min_date']
    max_date = service_date_range['max_date']
    date_range_days = (max_date - min_date).days

    pg_exec_times = execution_times.get('routes_by_relevance', {}).get('pg', [])
    neo4j_exec_times = execution_times.get('routes_by_relevance', {}).get('neo4j', [])

    for i in range(RANDOM_TEST_COUNT):
        random_date = min_date + timedelta(days=random.randint(0, date_range_days))
        # Generate a random time, including >24h GTFS times which are valid inputs
        random_hour = random.randint(0, 28)
        random_minute = random.randint(0, 59)
        random_second = random.randint(0, 59)
        random_time_str = f"{random_hour:02}:{random_minute:02}:{random_second:02}"

        print(f"\n[{i+1}/{RANDOM_TEST_COUNT}] Testing date: {random_date}, time: {random_time_str}")
        (_, pg_exec_time, neo4j_exec_time) = run_test_case(pg_query_runner, neo4j_query_runner, random_date, random_time_str)
        pg_exec_times.append(pg_exec_time)
        neo4j_exec_times.append(neo4j_exec_time)

    execution_times['routes_by_relevance'] = {
        'pg': pg_exec_times,
        'neo4j': neo4j_exec_times
    }
        
def test_edge_cases(pg_query_runner, neo4j_query_runner, service_date_range):
    """
    EDGE CASE ANALYSIS: Tests with dates that should yield no results.
    """
    print("\nRunning edge case analysis for 'routes_by_relevance'.")
    
    assert service_date_range is not None, "Test setup failed: Service date range could not be determined."
    
    # Test a date entirely before any services are active.
    before_date = service_date_range['min_date'] - timedelta(days=1)
    print(f"\nTesting with date before service starts: {before_date}")
    results = run_test_case(pg_query_runner, neo4j_query_runner, before_date, '12:00:00')[0]
    assert len(results) == 0, f"Expected 0 results for a date before service starts, but got {len(results)}."

    # Test a date entirely after all services have ended.
    after_date = service_date_range['max_date'] + timedelta(days=1)
    print(f"\nTesting with date after service ends: {after_date}")
    results = run_test_case(pg_query_runner, neo4j_query_runner, after_date, '12:00:00')[0]
    assert len(results) == 0, f"Expected 0 results for a date after service ends, but got {len(results)}."


@pytest.mark.hypothesis
def test_property_based(pg_query_runner, neo4j_query_runner, service_date_range):
    """
    PROPERTY-BASED TESTING: Checks the query never crashes for a variety of valid inputs.
    """
    assert service_date_range is not None, "Test setup failed: Service date range could not be determined."
    
    @given(
        curr_date=st.dates(min_value=service_date_range['min_date'], max_value=service_date_range['max_date']),
        curr_time=st.times()
    )
    @settings(deadline=None)
    def test_pbt_relevance_never_crashes(curr_date, curr_time):
        time_str = curr_time.strftime('%H:%M:%S')
        pg_query_runner(SQL, (curr_date, time_str))
        neo4j_query_runner(CYPHER, {'curr_date': str(curr_date), 'curr_time': time_str})

    test_pbt_relevance_never_crashes()