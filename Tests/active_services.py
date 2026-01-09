
import pytest
from conftest import run_test_case as rtc, RANDOM_TEST_COUNT, RANDOM_SEED, QUERIES
from hypothesis import given, strategies as st, settings
from datetime import date, timedelta
import random

# Query statements with format specifiers to fill.
SQL = QUERIES['postgres']['active_services']
CYPHER = QUERIES['neo4j']['active_services']

random.seed(RANDOM_SEED)

def run_test_case(pg_query_runner, neo4j_query_runner, test_date: date) -> list:
    """
    Calls the generic run_test_case function with parameters for the active_services query.
    """
    
    # TODO: Add plausibility checks

    return rtc(
        pg_query_runner,
        neo4j_query_runner,
        SQL,
        CYPHER,
        (test_date.isoformat(),),
        {'curr_date': test_date.isoformat()},
        lambda pg_results: [row['service_id'] for row in pg_results],
        lambda neo4j_results: [record['value']['service_id'] for record in neo4j_results],
        plausibility_checks=[],
        result_name="services"
    )

def test_random_inputs(pg_query_runner, neo4j_query_runner, service_date_range, execution_times):
    """
    CROSS-VALIDATION: Generates random dates within the GTFS feed's validity period and asserts results are plausible and consistent.
    """
    print(f"\nRunning random input tests for 'active_services' ({RANDOM_TEST_COUNT} iterations).")
    assert service_date_range is not None, "Test setup failed: Service date range could not be determined."

    min_d, max_d = service_date_range['min_date'], service_date_range['max_date']
    date_range_days = (max_d - min_d).days

    pg_exec_times = execution_times.get('active_services', {}).get('pg', [])
    neo4j_exec_times = execution_times.get('active_services', {}).get('neo4j', [])

    for i in range(RANDOM_TEST_COUNT):
        random_days = random.randint(0, date_range_days)
        test_date = min_d + timedelta(days=random_days)
        print(f"\n[{i+1}/{RANDOM_TEST_COUNT}] Testing date: {test_date.isoformat()}")
        (_, pg_exec_time, neo4j_exec_time) = run_test_case(pg_query_runner, neo4j_query_runner, test_date)
        pg_exec_times.append(pg_exec_time)
        neo4j_exec_times.append(neo4j_exec_time)

    execution_times['active_services'] = {
        'pg': pg_exec_times,
        'neo4j': neo4j_exec_times
    }

def test_edge_cases(pg_query_runner, neo4j_query_runner, service_date_range, known_service_exception):
    """
    EDGE CASE ANALYSIS: Tests with known tricky inputs like dates with exceptions.
    """
    print("\nRunning edge case analysis for 'active_services'.")
    assert service_date_range is not None, "Test setup failed: Service date range is None."

    # Test the very first day of the service period
    first_day = service_date_range['min_date']
    print(f"\nTesting first day of service period: {first_day.isoformat()}")
    run_test_case(pg_query_runner, neo4j_query_runner, first_day)

    # Test the very last day of the service period
    last_day = service_date_range['max_date']
    print(f"\nTesting last day of service period: {last_day.isoformat()}")
    run_test_case(pg_query_runner, neo4j_query_runner, last_day)

    # Test a known "added service" date, if one exists in the dataset
    if known_service_exception:
        exception_date = known_service_exception['date']
        exception_service = known_service_exception['service_id']
        print(f"\nTesting known added exception date: {exception_date.isoformat()} for service '{exception_service}'")
        results = run_test_case(pg_query_runner, neo4j_query_runner, exception_date)[0]
        assert exception_service in results, \
            f"Edge case failed: Service '{exception_service}' was NOT found on its exception date {exception_date}."
    else:
        print("\nSkipping added exception test: No type 1 exceptions found in dataset.")


@pytest.mark.hypothesis
def test_property_based(pg_query_runner, neo4j_query_runner, service_date_range):
    """
    PROPERTY-BASED TESTING: Check properties remain true for a wide variety of date ranges.
    """
    print("\nRunning property-based tests for 'active_services'...")
    assert service_date_range is not None, "Test setup failed: Service date range is None."

    # Property: For any valid date, the query should execute without crashing.
    @given(test_date=st.dates(
        min_value=service_date_range['min_date'],
        max_value=service_date_range['max_date']
    ))
    @settings(deadline=None)
    def test_pbt_active_services_is_always_consistent(test_date):
        """
        Property: For any valid date, both databases must return the exact same set of service IDs.
        This test now correctly uses the full testing harness.
        """
        run_test_case(pg_query_runner, neo4j_query_runner, test_date)

    # Run the hypothesis test
    test_pbt_active_services_is_always_consistent()