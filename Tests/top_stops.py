import pytest
from conftest import run_test_case as rtc, RANDOM_TEST_COUNT, RANDOM_SEED, to_canonical_time_str, QUERIES
from hypothesis import given, strategies as st, settings
from datetime import date, timedelta
import random

# Query statements with format specifiers to fill.
SQL = QUERIES['postgres']['top_stops']
CYPHER = QUERIES['neo4j']['top_stops']

random.seed(RANDOM_SEED)

# Run test case.
def run_test_case(pg_query_runner, neo4j_query_runner, curr_date: date) -> list:
    """
    Calls the generic run_test_case function with parameters for the top_stops query.
    """

    # Plausibility checks.
    def has_internal_consistency(results):
        """Asserts that route_count matches len(routes) and times are logical."""
        for row in results:
            stop_name, route_count, routes, total_departures, first_dep, last_dep = row
            assert route_count == len(routes), f"Inconsistency in '{stop_name}': route_count is {route_count} but routes list has {len(routes)} items."
            assert first_dep is None or first_dep <= last_dep, f"Inconsistency in '{stop_name}': first_departure '{first_dep}' is after last_departure '{last_dep}'."

    def counts_are_positive(results):
        """Asserts that count columns are greater than zero."""
        for row in results:
            # Indices 1 (route_count) and 3 (total_departures)
            assert row[1] > 0 and row[3] > 0, f"Found non-positive count in row: {row}"

    return rtc(
        pg_query_runner,
        neo4j_query_runner,
        SQL,
        CYPHER,
        (curr_date,),
        {'curr_date': str(curr_date)},
        # Extract and normalize results from PostgreSQL.
        lambda pg_results: [(
            row['stop_name'],
            row['route_count'],
            sorted(row['routes']),
            row['total_departures'],
            to_canonical_time_str(row['first_departure']),
            to_canonical_time_str(row['last_departure'])
        ) for row in pg_results],
        # Extract and normalize results from Neo4j.
        lambda neo4j_results: [(
            res['value']['stop_name'],
            res['value']['route_count'],
            sorted(res['value']['routes']),
            res['value']['total_departures'],
            to_canonical_time_str(res['value']['first_departure']),
            to_canonical_time_str(res['value']['last_departure'])
        ) for res in neo4j_results],
        plausibility_checks=[has_internal_consistency, counts_are_positive],
        result_name="top stops"
    )

def test_random_inputs(pg_query_runner, neo4j_query_runner, service_date_range, execution_times):
    """
    CROSS-VALIDATION: Generates random dates and asserts results are plausible and consistent.
    """
    print(f"\nRunning random input tests for 'top_stops' ({RANDOM_TEST_COUNT} iterations).")

    assert service_date_range is not None, "Test setup failed: Service date range could not be determined."
    min_date = service_date_range['min_date']
    max_date = service_date_range['max_date']
    date_range_days = (max_date - min_date).days

    pg_exec_times = execution_times.get('top_stops', {}).get('pg', [])
    neo4j_exec_times = execution_times.get('top_stops', {}).get('neo4j', [])

    for i in range(RANDOM_TEST_COUNT):
        random_date = min_date + timedelta(days=random.randint(0, date_range_days))
        print(f"\n[{i+1}/{RANDOM_TEST_COUNT}] Testing date: {random_date}")
        (_, pg_exec_time, neo4j_exec_time) = run_test_case(pg_query_runner, neo4j_query_runner, random_date)
        pg_exec_times.append(pg_exec_time)
        neo4j_exec_times.append(neo4j_exec_time)

    execution_times['top_stops'] = {
        'pg': pg_exec_times,
        'neo4j': neo4j_exec_times
    }

def test_edge_cases(pg_query_runner, neo4j_query_runner, service_date_range):
    """
    EDGE CASE ANALYSIS: Tests with dates that should yield no results.
    """
    print("\nRunning edge case analysis for 'top_stops'.")

    assert service_date_range is not None, "Test setup failed: Service date range could not be determined."

    # Test a date entirely before any services are active.
    before_date = service_date_range['min_date'] - timedelta(days=30)
    print(f"\nTesting a date entirely before service starts: {before_date}")
    results = run_test_case(pg_query_runner, neo4j_query_runner, before_date)[0]
    assert len(results) == 0, f"Expected 0 results for a date before service starts, but got {len(results)}."

    # Test a date entirely after all services have ended.
    after_date = service_date_range['max_date'] + timedelta(days=30)
    print(f"\nTesting a date entirely after service ends: {after_date}")
    results = run_test_case(pg_query_runner, neo4j_query_runner, after_date)[0]
    assert len(results) == 0, f"Expected 0 results for a date after service ends, but got {len(results)}."

@pytest.mark.hypothesis
def test_property_based(pg_query_runner, neo4j_query_runner, service_date_range):
    """
    PROPERTY-BASED TESTING: Checks the query never crashes for any valid date.
    """
    assert service_date_range is not None, "Test setup failed: Service date range could not be determined."

    @given(
        curr_date=st.dates(min_value=service_date_range['min_date'], max_value=service_date_range['max_date'])
    )
    @settings(deadline=None)
    def test_pbt_top_stops_never_crashes(curr_date):
         # Property: For any date within the service range, the query should execute without crashing.
        pg_query_runner(SQL, (curr_date,))
        neo4j_query_runner(CYPHER, {'curr_date': str(curr_date)})

    test_pbt_top_stops_never_crashes()