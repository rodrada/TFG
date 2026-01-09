import pytest
from conftest import run_test_case as rtc, RANDOM_TEST_COUNT, RANDOM_SEED, to_canonical_time_str, random_stop_id, QUERIES
from hypothesis import given, strategies as st, settings
from datetime import date, timedelta
import random

# Query statements.
SQL = QUERIES['postgres']['next_departures']
CYPHER = QUERIES['neo4j']['next_departures']

random.seed(RANDOM_SEED)

# Run test case.
def run_test_case(pg_query_runner, neo4j_query_runner, stop_id: str, curr_date: date, curr_time: str) -> list:
    """
    Calls the generic run_test_case function with parameters for the next_departures query.
    """

    # Plausibility checks.
    def route_is_present(results):
        """Asserts that route is not null or empty."""
        for row in results:
            assert row[0] is not None and row[0] != "", f"Missing route in row: {row}"

    return rtc(
        pg_query_runner,
        neo4j_query_runner,
        SQL,
        CYPHER,
        (stop_id, curr_date, curr_time),
        {'stop_id': stop_id, 'curr_date': str(curr_date), 'curr_time': curr_time},
        # Extract and normalize results from PostgreSQL.
        lambda pg_results: [(
            row['route'],
            row['destination'],
            to_canonical_time_str(row['time'])
        ) for row in pg_results],
        # Extract and normalize results from Neo4j.
        lambda neo4j_results: [(
            res['value']['route'],
            res['value']['destination'],
            to_canonical_time_str(res['value']['time'])
        ) for res in neo4j_results],
        plausibility_checks=[route_is_present],
        result_name="next departures"
    )

def test_random_inputs(pg_query_runner, neo4j_query_runner, service_date_range, execution_times):
    """
    CROSS-VALIDATION: Generates random inputs and asserts results are plausible and consistent.
    """
    print(f"\nRunning random input tests for 'next_departures' ({RANDOM_TEST_COUNT} iterations).")
    
    assert service_date_range is not None, "Test setup failed: Service date range could not be determined."
    min_date = service_date_range['min_date']
    max_date = service_date_range['max_date']
    date_range_days = (max_date - min_date).days

    pg_exec_times = execution_times.get('next_departures', {}).get('pg', [])
    neo4j_exec_times = execution_times.get('next_departures', {}).get('neo4j', [])

    for i in range(RANDOM_TEST_COUNT):
        stop_id = random_stop_id(pg_query_runner)
        if not stop_id:
            pytest.skip("Could not find a valid stop_id to test.")

        random_date = min_date + timedelta(days=random.randint(0, date_range_days))
        # Generate a random time, including >24h GTFS times
        random_hour = random.randint(0, 28)
        random_minute = random.randint(0, 59)
        random_second = random.randint(0, 59)
        random_time_str = f"{random_hour:02}:{random_minute:02}:{random_second:02}"

        print(f"\n[{i+1}/{RANDOM_TEST_COUNT}] Testing stop: '{stop_id}', date: {random_date}, time: {random_time_str}")
        (_, pg_exec_time, neo4j_exec_time) = run_test_case(pg_query_runner, neo4j_query_runner, stop_id, random_date, random_time_str)
        pg_exec_times.append(pg_exec_time)
        neo4j_exec_times.append(neo4j_exec_time)

    execution_times['next_departures'] = {
        'pg': pg_exec_times,
        'neo4j': neo4j_exec_times
    }
        
def test_edge_cases(pg_query_runner, neo4j_query_runner, service_date_range):
    """
    EDGE CASE ANALYSIS: Tests with tricky inputs.
    """
    print("\nRunning edge case analysis for 'next_departures'.")
    
    assert service_date_range is not None, "Test setup failed: Service date range could not be determined."
    valid_stop_id = random_stop_id(pg_query_runner)
    if not valid_stop_id:
        pytest.skip("Could not find a valid stop_id for edge case testing.")
    
    valid_date = service_date_range['min_date']

    # Test with a non-existent stop_id.
    print(f"\nTesting with non-existent stop 'invalid-stop-id'")
    results = run_test_case(pg_query_runner, neo4j_query_runner, 'invalid-stop-id', valid_date, '12:00:00')[0]
    assert len(results) == 0, f"Expected 0 results for a non-existent stop, but got {len(results)}."
    
    # Test with a date before any services are active.
    before_date = service_date_range['min_date'] - timedelta(days=1)
    print(f"\nTesting with date before service starts: {before_date}")
    results = run_test_case(pg_query_runner, neo4j_query_runner, valid_stop_id, before_date, '12:00:00')[0]
    assert len(results) == 0, f"Expected 0 results for a date before service starts, but got {len(results)}."

    # Test with a very late time, which should return few or no results.
    print(f"\nTesting with very late time '28:00:00'")
    run_test_case(pg_query_runner, neo4j_query_runner, valid_stop_id, valid_date, '28:00:00')


@pytest.mark.hypothesis
def test_property_based(pg_query_runner, neo4j_query_runner, service_date_range):
    """
    PROPERTY-BASED TESTING: Checks the query never crashes for a variety of valid inputs.
    """
    assert service_date_range is not None, "Test setup failed: Service date range could not be determined."
    
    # Use a fixed, valid stop ID for property-based testing to avoid flakiness.
    valid_stop_id = random_stop_id(pg_query_runner)
    if not valid_stop_id:
        pytest.skip("Could not find a valid stop_id for property-based testing.")

    @given(
        curr_date=st.dates(min_value=service_date_range['min_date'], max_value=service_date_range['max_date']),
        curr_time=st.times()
    )
    @settings(deadline=None)
    def test_pbt_next_departures_never_crashes(curr_date, curr_time):
        time_str = curr_time.strftime('%H:%M:%S')
        pg_query_runner(SQL, (valid_stop_id, curr_date, time_str))
        neo4j_query_runner(CYPHER, {'stop_id': valid_stop_id, 'curr_date': str(curr_date), 'curr_time': time_str})

    test_pbt_next_departures_never_crashes()