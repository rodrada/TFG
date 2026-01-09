import pytest
from conftest import run_test_case as rtc, QUERIES
from hypothesis import given, strategies as st, settings, assume
from datetime import date, timedelta

# Query statements with format specifiers to fill.
SQL = QUERIES['postgres']['daily_status']
CYPHER = QUERIES['neo4j']['daily_status']

# Run test case.
def run_test_case(pg_query_runner, neo4j_query_runner, start_date: date, end_date: date) -> list:
    """
    Calls the generic run_test_case function with parameters for the daily_status query.
    """

    # Plausibility checks.
    def counts_are_non_negative(results):
        """
        Asserts that all count columns (total_trips, active_routes, active_stops) are >= 0.
        """
        for row in results:
            # row[0] is the date, row[1:] are the counts.
            assert all(count >= 0 for count in row[1:]), f"Found a negative count in row: {row}"

    return rtc(
        pg_query_runner,
        neo4j_query_runner,
        SQL,
        CYPHER,
        (start_date, end_date),
        {'start_date': str(start_date), 'end_date': str(end_date)},
        # Extract results from PostgreSQL into a canonical form.
        lambda pg_results: [(row['service_date'], row['total_trips'], row['active_routes'], row['active_stops']) for row in pg_results],
        # Extract results from Neo4j into a canonical form, ensuring date is a Python date object.
        lambda neo4j_results: [(res['value']['service_date'], res['value']['total_trips'], res['value']['active_routes'], res['value']['active_stops']) for res in neo4j_results],
        plausibility_checks=[counts_are_non_negative],
        result_name="daily statuses"
    )

# NOTE: Since we check if queries for subranges are consistent with the full range, we only need to cross-validate the whole range.
def test_cross_validation(pg_query_runner, neo4j_query_runner, service_date_range, execution_times):
    """
    CROSS-VALIDATION: Tests if results are the same in a query for the whole service period.
    """
    print(f"\nRunning cross-validation for 'daily_status'.")

    assert service_date_range is not None, "Test setup failed: Service date range could not be determined."
    min_date = service_date_range['min_date']
    max_date = service_date_range['max_date']

    pg_exec_times = execution_times.get('daily_status', {}).get('pg', [])
    neo4j_exec_times = execution_times.get('daily_status', {}).get('neo4j', [])

    (_, pg_exec_time, neo4j_exec_time) = run_test_case(pg_query_runner, neo4j_query_runner, min_date, max_date)

    pg_exec_times.append(pg_exec_time)
    neo4j_exec_times.append(neo4j_exec_time)

    execution_times['daily_status'] = {
        'pg': pg_exec_times,
        'neo4j': neo4j_exec_times
    }

def test_edge_cases(pg_query_runner, neo4j_query_runner, service_date_range):
    """
    EDGE CASE ANALYSIS: Tests with tricky date range inputs.
    """
    print("\nRunning edge case analysis for 'daily_status'.")

    assert service_date_range is not None, "Test setup failed: Service date range could not be determined."

    # Test a single-day range.
    single_date = service_date_range['min_date'] + timedelta(days=10)
    print(f"\nTesting a single-day range: {single_date}")
    results = run_test_case(pg_query_runner, neo4j_query_runner, single_date, single_date)[0]
    assert len(results) <= 1, f"Expected 0 or 1 result for a single-day range, but got {len(results)}."

    # Test an inverted date range (start_date > end_date).
    start_date = service_date_range['max_date']
    end_date = service_date_range['min_date']
    print(f"\nTesting an inverted range: start={start_date}, end={end_date}")
    results = run_test_case(pg_query_runner, neo4j_query_runner, start_date, end_date)[0]
    assert len(results) == 0, f"Expected 0 results for an inverted date range, but got {len(results)}."

    # Test a date range entirely before any services are active.
    before_start = service_date_range['min_date'] - timedelta(days=30)
    before_end = service_date_range['min_date'] - timedelta(days=15)
    print(f"\nTesting a range entirely before service starts: {before_start} to {before_end}")
    results = run_test_case(pg_query_runner, neo4j_query_runner, before_start, before_end)[0]
    assert len(results) == 0, f"Expected 0 results for a date range before service starts, but got {len(results)}."

    # Test a date range entirely after all services have ended.
    after_start = service_date_range['max_date'] + timedelta(days=15)
    after_end = service_date_range['max_date'] + timedelta(days=30)
    print(f"\nTesting a range entirely after service ends: {after_start} to {after_end}")
    results = run_test_case(pg_query_runner, neo4j_query_runner, after_start, after_end)[0]
    assert len(results) == 0, f"Expected 0 results for a date range after service ends, but got {len(results)}."


@pytest.mark.hypothesis
def test_property_based(pg_query_runner, neo4j_query_runner, service_date_range):
    """
    PROPERTY-BASED TESTING: Check properties remain true for a wide variety of date ranges.
    """
    assert service_date_range is not None, "Test setup failed: Service date range could not be determined."

    # Run full-range queries
    full_pg = pg_query_runner(SQL, (service_date_range['min_date'], service_date_range['max_date']))
    full_neo = neo4j_query_runner(CYPHER, {
        'start_date': str(service_date_range['min_date']),
        'end_date': str(service_date_range['max_date'])
    })

    # Property: For any two dates within the service range, the query should execute without crashing,
    # even if the start date is after the end date.
    @given(
        start_date=st.dates(min_value=service_date_range['min_date'], max_value=service_date_range['max_date']),
        end_date=st.dates(min_value=service_date_range['min_date'], max_value=service_date_range['max_date'])
    )
    @settings(deadline=None)
    def test_pbt_daily_status_never_crashes(start_date, end_date):
        pg_query_runner(SQL, (start_date, end_date))
        neo4j_query_runner(CYPHER, {'start_date': str(start_date), 'end_date': str(end_date)})

    # Property: Subrange queries should return the same results as the full range query filtered to that subrange.
    @given(
        sub_start=st.dates(min_value=service_date_range['min_date'], max_value=service_date_range['max_date']),
        sub_end=st.dates(min_value=service_date_range['min_date'], max_value=service_date_range['max_date'])
    )
    @settings(deadline=None)
    def test_pbt_subrange_consistency(sub_start, sub_end):
        assume(sub_start <= sub_end)

        # Run subrange queries
        sub_pg = pg_query_runner(SQL, (sub_start, sub_end))
        sub_neo = neo4j_query_runner(CYPHER, {'start_date': str(sub_start), 'end_date': str(sub_end)})

        # Filter the full-range results to the subrange
        filtered_pg = [r for r in full_pg if sub_start <= r['service_date'] <= sub_end]
        filtered_neo = [r for r in full_neo if sub_start <= r['value']['service_date'] <= sub_end]

        # Check if the property holds.
        assert sub_pg == filtered_pg, \
            f"Postgres subrange mismatch for {sub_start}–{sub_end}"

        assert sub_neo == filtered_neo, \
            f"Neo4j subrange mismatch for {sub_start}–{sub_end}"

    test_pbt_daily_status_never_crashes()
    test_pbt_subrange_consistency()
