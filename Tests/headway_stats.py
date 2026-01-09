import pytest
from conftest import run_test_case as rtc, RANDOM_SLOW_TEST_COUNT, RANDOM_SEED, to_canonical_time_str, time_str_to_seconds, QUERIES
from hypothesis import given, strategies as st, settings
from datetime import date, timedelta
import random

# Query statements with format specifiers to fill.
SQL = QUERIES['postgres']['headway_stats']
CYPHER = QUERIES['neo4j']['headway_stats']

random.seed(RANDOM_SEED)

# Run test case.
def run_test_case(pg_query_runner, neo4j_query_runner, curr_date: date) -> list:
    """
    Calls the generic run_test_case function with parameters for the headway_stats query.
    """

    # Plausibility checks.
    def stats_are_logical(results):
        """Asserts that for each row, min_headway <= median_headway <= max_headway."""
        for row in results:
            route, min_h, med_h, max_h, _ = row
            assert min_h <= med_h, f"Inconsistency in route '{route}': min_headway '{min_h}' is greater than median_headway '{med_h}'."
            assert med_h <= max_h, f"Inconsistency in route '{route}': median_headway '{med_h}' is greater than max_headway '{max_h}'."

    def values_are_non_negative(results):
        """Asserts that stddev_seconds is not negative."""
        for row in results:
            assert row[4] >= 0, f"Found negative stddev_seconds in row: {row}"

    return rtc(
        pg_query_runner,
        neo4j_query_runner,
        SQL,
        CYPHER,
        (curr_date,),
        {'curr_date': str(curr_date)},
        # Extract and normalize results from PostgreSQL.
        lambda pg_results: [(
            row['route_name'],
            to_canonical_time_str(row['min_headway']),
            to_canonical_time_str(row['median_headway']),
            to_canonical_time_str(row['max_headway']),
            int(row['stddev_seconds'])
        ) for row in pg_results],
        # Extract and normalize results from Neo4j.
        lambda neo4j_results: [(
            res['value']['route_name'],
            res['value']['min_headway'],
            res['value']['median_headway'],
            res['value']['max_headway'],
            int(res['value']['stddev_seconds'])
        ) for res in neo4j_results],
        plausibility_checks=[stats_are_logical, values_are_non_negative],
        # Allow a 1 second difference for rounding errors in headways.
        comparison_function=lambda pg, neo4j: len(pg) == len(neo4j) and all(
            p[0] == n[0] and
            abs(time_str_to_seconds(p[1]) - time_str_to_seconds(n[1])) <= 1 and
            abs(time_str_to_seconds(p[2]) - time_str_to_seconds(n[2])) <= 1 and
            abs(time_str_to_seconds(p[3]) - time_str_to_seconds(n[3])) <= 1 and
            p[4] == n[4]
            for p, n in zip(pg, neo4j)
        )
    )

def test_random_inputs(pg_query_runner, neo4j_query_runner, service_date_range, execution_times):
    """
    CROSS-VALIDATION: Generates random dates and asserts results are plausible and consistent.
    """
    print(f"\nRunning random input tests for 'headway_stats' ({RANDOM_SLOW_TEST_COUNT} iterations).")
    
    assert service_date_range is not None, "Test setup failed: Service date range could not be determined."
    min_date = service_date_range['min_date']
    max_date = service_date_range['max_date']
    date_range_days = (max_date - min_date).days

    pg_exec_times = execution_times.get('headway_stats', {}).get('pg', [])
    neo4j_exec_times = execution_times.get('headway_stats', {}).get('neo4j', [])

    for i in range(RANDOM_SLOW_TEST_COUNT):
        random_date = min_date + timedelta(days=random.randint(0, date_range_days))
        print(f"\n[{i+1}/{RANDOM_SLOW_TEST_COUNT}] Testing date: {random_date}")
        (_, pg_exec_time, neo4j_exec_time) = run_test_case(pg_query_runner, neo4j_query_runner, random_date)
        pg_exec_times.append(pg_exec_time)
        neo4j_exec_times.append(neo4j_exec_time)

    execution_times['headway_stats'] = {
        'pg': pg_exec_times,
        'neo4j': neo4j_exec_times
    }
        
def test_edge_cases(pg_query_runner, neo4j_query_runner, service_date_range):
    """
    EDGE CASE ANALYSIS: Tests with dates that should yield no results.
    """
    print("\nRunning edge case analysis for 'headway_stats'.")
    
    assert service_date_range is not None, "Test setup failed: Service date range could not be determined."
    
    # Test the date before any services are active.
    before_date = service_date_range['min_date'] - timedelta(days=1)
    print(f"\nTesting a date entirely before service starts: {before_date}")
    results = run_test_case(pg_query_runner, neo4j_query_runner, before_date)[0]
    assert len(results) == 0, f"Expected 0 results for a date before service starts, but got {len(results)}."

    # Test the date after all services have ended.
    after_date = service_date_range['max_date'] + timedelta(days=1)
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
    def test_pbt_headway_stats_never_crashes(curr_date):
         # Property: For any date within the service range, the query should execute without crashing.
        pg_query_runner(SQL, (curr_date,))
        neo4j_query_runner(CYPHER, {'curr_date': str(curr_date)})

    test_pbt_headway_stats_never_crashes()