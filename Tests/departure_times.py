import pytest
from conftest import run_test_case as rtc, RANDOM_TEST_COUNT, RANDOM_SEED, to_canonical_time_str, QUERIES
from hypothesis import given, strategies as st, settings
from datetime import date, timedelta
import random

# Query statements with format specifiers to fill.
SQL = QUERIES['postgres']['departure_times']
CYPHER = QUERIES['neo4j']['departure_times']

random.seed(RANDOM_SEED)

# Helper function to get random route and stop IDs from the database.
def get_random_route_stop_pair(pg_query_runner):
    """
    Fetches a random route and a random stop that is part of that route.
    """
    # This query first finds a random trip that has at least one stop_time,
    # then gets its route_id and a random stop_id associated with that trip.
    # This ensures the route and stop are a valid pair.
    query = """
        SELECT t.route_id, st.stop_id
        FROM stop_time st
        JOIN trip t ON st.trip_id = t.trip_id
        ORDER BY RANDOM()
        LIMIT 1;
    """
    try:
        result = pg_query_runner(query, ())
        if result:
            return result[0]['route_id'], result[0]['stop_id']
    except Exception as e:
        pytest.fail(f"Could not fetch a random route/stop pair from the database: {e}")
    return None, None


# Run test case.
def run_test_case(pg_query_runner, neo4j_query_runner, route_id: str, stop_id: str, curr_date: date) -> list:
    """
    Calls the generic run_test_case function with parameters for the departure_times query.
    """

    # TODO: Add plausibility checks.

    return rtc(
        pg_query_runner,
        neo4j_query_runner,
        SQL,
        CYPHER,
        (route_id, stop_id, curr_date),
        {'route_id': route_id, 'stop_id': stop_id, 'curr_date': str(curr_date)},
        lambda pg_results: [
            (
                row['service_id'],
                row['route_id'],
                row['trip_id'],
                row['direction'],
                row['stop_id'],
                to_canonical_time_str(row['departure_time'])
            ) for row in pg_results
        ],
        lambda neo4j_results: [
            (
                res['value']['service_id'],
                res['value']['route_id'],
                res['value']['trip_id'],
                res['value']['direction'],
                res['value']['stop_id'],
                to_canonical_time_str(res['value']['departure_time'])
            ) for res in neo4j_results
        ],
        plausibility_checks=[],
        result_name="departures"
    )

def test_random_inputs(pg_query_runner, neo4j_query_runner, service_date_range, execution_times):
    """
    CROSS-VALIDATION: Generates random inputs and asserts that results are plausible and consistent between both databases.
    """
    print(f"\nRunning random input tests for 'departure_times' ({RANDOM_TEST_COUNT} iterations).")
    
    assert service_date_range is not None, "Test setup failed: Service date range could not be determined."
    start_date = service_date_range['min_date']
    end_date = service_date_range['max_date']
    date_range_days = (end_date - start_date).days

    pg_exec_times = execution_times.get('departure_times', {}).get('pg', [])
    neo4j_exec_times = execution_times.get('departure_times', {}).get('neo4j', [])

    for i in range(RANDOM_TEST_COUNT):
        route_id, stop_id = get_random_route_stop_pair(pg_query_runner)
        if not route_id or not stop_id:
            pytest.skip(f"Could not find a valid route/stop pair to test.")
            continue
            
        # Generate a random date within the valid service range.
        random_date = start_date + timedelta(days=random.randint(0, date_range_days))

        print(f"\n[{i+1}/{RANDOM_TEST_COUNT}] Testing route: '{route_id}', stop: '{stop_id}', date: {random_date}")
        (_, pg_exec_time, neo4j_exec_time) = run_test_case(pg_query_runner, neo4j_query_runner, route_id, stop_id, random_date)
        pg_exec_times.append(pg_exec_time)
        neo4j_exec_times.append(neo4j_exec_time)

    execution_times['departure_times'] = {
        'pg': pg_exec_times,
        'neo4j': neo4j_exec_times
    }

def test_edge_cases(pg_query_runner, neo4j_query_runner, service_date_range):
    """
    EDGE CASE ANALYSIS: Tests with tricky inputs.
    """
    print("\nRunning edge case analysis for 'departure_times'.")
    
    assert service_date_range is not None, "Test setup failed: Service date range could not be determined."

    # Test with a known valid route and stop, but for a date outside the service calendar.
    route_id, stop_id = get_random_route_stop_pair(pg_query_runner)
    if route_id and stop_id:
        day_before_service = service_date_range['min_date'] - timedelta(days=1)
        print(f"\nTesting with a date before any service starts ({day_before_service}) for route: '{route_id}', stop: '{stop_id}'")
        results = run_test_case(pg_query_runner, neo4j_query_runner, route_id, stop_id, day_before_service)[0]
        assert len(results) == 0, f"Unexpected results for a date before service start: {results}"

        day_after_service = service_date_range['max_date'] + timedelta(days=1)
        print(f"\nTesting with a date after all services end ({day_after_service}) for route: '{route_id}', stop: '{stop_id}'")
        results = run_test_case(pg_query_runner, neo4j_query_runner, route_id, stop_id, day_after_service)[0]
        assert len(results) == 0, f"Unexpected results for a date after service end: {results}"

    # Test with non-existent route and stop IDs
    print("\nTesting with non-existent route and stop IDs")
    results = run_test_case(pg_query_runner, neo4j_query_runner, 'non_existent_route', 'non_existent_stop', service_date_range['min_date'])[0]
    assert len(results) == 0, f"Unexpected results for non-existent route and stop IDs: {results}"


@pytest.mark.hypothesis
def test_property_based(pg_query_runner, neo4j_query_runner, service_date_range):
    """
    PROPERTY-BASED TESTING: Check properties remain true for a wide variety of inputs.
    """
    assert service_date_range is not None, "Test setup failed: Service date range could not be determined."
    
    # Use a fixed, valid route/stop pair for property-based date testing to avoid flaky tests.
    route_id, stop_id = get_random_route_stop_pair(pg_query_runner)
    if not route_id or not stop_id:
        pytest.skip("Could not find a valid route/stop pair for property-based testing.")
        return

    # Property: For any valid route, stop, and date within the service range, the query should execute without crashing.
    @given(
        curr_date=st.dates(min_value=service_date_range['min_date'], max_value=service_date_range['max_date'])
    )
    @settings(deadline=None)
    def test_pbt_departure_times_query_never_crashes(curr_date):
        pg_query_runner(SQL, (route_id, stop_id, curr_date))
        neo4j_query_runner(CYPHER, {'route_id': route_id, 'stop_id': stop_id, 'curr_date': str(curr_date)})

    test_pbt_departure_times_query_never_crashes()
