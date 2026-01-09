import pytest
from conftest import run_test_case as rtc, RANDOM_TEST_COUNT, RANDOM_SEED, to_canonical_time_str, QUERIES
from hypothesis import given, strategies as st, settings
from datetime import date, timedelta
import random

# Test parameters.
MIN_BUCKET_SIZE_MIN = 5
DEFAULT_BUCKET_SIZE_MIN = 15
MAX_BUCKET_SIZE_MIN = 60

# Query statements with format specifiers to fill.
SQL = QUERIES['postgres']['trip_start_time_distribution']
CYPHER = QUERIES['neo4j']['trip_start_time_distribution']

random.seed(RANDOM_SEED)

# Run test case.
def run_test_case(pg_query_runner, neo4j_query_runner, curr_date: date, bucket_size_min: int) -> list:
    """
    Calls the generic run_test_case function with parameters for the trip_start_time_distribution query.
    """

    # Plausibility checks.
    def is_sorted_by_time_bucket(results):
        """Asserts that results are sorted chronologically by the time bucket."""
        time_buckets = [row[0] for row in results]
        assert time_buckets == sorted(time_buckets), "Results are not sorted by time_bucket."

    def counts_are_positive(results):
        """Asserts that the trip_count for any bucket is greater than zero."""
        for row in results:
            assert row[1] > 0, f"Found non-positive trip count in row: {row}"

    return rtc(
        pg_query_runner,
        neo4j_query_runner,
        SQL,
        CYPHER,
        (curr_date, bucket_size_min),
        {'curr_date': str(curr_date), 'bucket_size_min': bucket_size_min},
        # Extract and normalize results from PostgreSQL.
        lambda pg_results: [(
            to_canonical_time_str(row['time_bucket']),
            row['trip_count']
        ) for row in pg_results],
        # Extract and normalize results from Neo4j.
        lambda neo4j_results: [(
            res['value']['time_bucket'],
            res['value']['trip_count']
        ) for res in neo4j_results],
        plausibility_checks=[is_sorted_by_time_bucket, counts_are_positive],
        result_name="buckets"
    )

def test_random_inputs(pg_query_runner, neo4j_query_runner, service_date_range, execution_times):
    """
    CROSS-VALIDATION: Generates random dates and asserts results are plausible and consistent.
    """
    print(f"\nRunning random input tests for 'trip_start_time_distribution' ({RANDOM_TEST_COUNT} iterations).")
    
    assert service_date_range is not None, "Test setup failed: Service date range could not be determined."
    min_date = service_date_range['min_date']
    max_date = service_date_range['max_date']
    date_range_days = (max_date - min_date).days

    pg_exec_times = execution_times.get('trip_start_time_distribution', {}).get('pg', [])
    neo4j_exec_times = execution_times.get('trip_start_time_distribution', {}).get('neo4j', [])

    for i in range(RANDOM_TEST_COUNT):
        random_date = min_date + timedelta(days=random.randint(0, date_range_days))
        random_bucket_size_min = random.randint(MIN_BUCKET_SIZE_MIN, MAX_BUCKET_SIZE_MIN)
        print(f"\n[{i+1}/{RANDOM_TEST_COUNT}] Testing date: {random_date}")
        (_, pg_exec_time, neo4j_exec_time) = run_test_case(pg_query_runner, neo4j_query_runner, random_date, random_bucket_size_min)
        pg_exec_times.append(pg_exec_time)
        neo4j_exec_times.append(neo4j_exec_time)

    execution_times['trip_start_time_distribution'] = {
        'pg': pg_exec_times,
        'neo4j': neo4j_exec_times
    }
        
def test_edge_cases(pg_query_runner, neo4j_query_runner, service_date_range):
    """
    EDGE CASE ANALYSIS: Tests with dates that should yield no results.
    """
    print("\nRunning edge case analysis for 'trip_start_time_distribution'.")
    
    assert service_date_range is not None, "Test setup failed: Service date range could not be determined."
    
    # Test a date entirely before any services are active.
    before_date = service_date_range['min_date'] - timedelta(days=30)
    print(f"\nTesting a date entirely before service starts: {before_date}")
    results = run_test_case(pg_query_runner, neo4j_query_runner, before_date, DEFAULT_BUCKET_SIZE_MIN)[0]
    assert len(results) == 0, f"Expected 0 results for a date before service starts, but got {len(results)}."

    # Test a date entirely after all services have ended.
    after_date = service_date_range['max_date'] + timedelta(days=30)
    print(f"\nTesting a date entirely after service ends: {after_date}")
    results = run_test_case(pg_query_runner, neo4j_query_runner, after_date, DEFAULT_BUCKET_SIZE_MIN)[0]
    assert len(results) == 0, f"Expected 0 results for a date after service ends, but got {len(results)}."

    # Test with a bucket size of 1.
    date_range_days = (service_date_range['max_date'] - service_date_range['min_date']).days
    random_date = service_date_range['min_date'] + timedelta(days=random.randint(0, date_range_days))
    print(f"\nTesting with a bucket size of 1")
    results = run_test_case(pg_query_runner, neo4j_query_runner, random_date, 1)

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
    def test_pbt_distribution_never_crashes(curr_date):
         # Property: For any date within the service range, the query should execute without crashing.
        pg_query_runner(SQL, (curr_date, DEFAULT_BUCKET_SIZE_MIN))
        neo4j_query_runner(CYPHER, {'curr_date': str(curr_date), 'bucket_size_min': DEFAULT_BUCKET_SIZE_MIN})

    test_pbt_distribution_never_crashes()