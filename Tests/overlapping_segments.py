
from conftest import run_test_case as rtc, QUERIES

# Query statements.
SQL = QUERIES['postgres']['overlapping_segments']
CYPHER = QUERIES['neo4j']['overlapping_segments']

# Run test case.
def run_test_case(pg_query_runner, neo4j_query_runner) -> list:
    """
    Calls the generic run_test_case function with parameters for the overlapping_segments query.
    """

    # Plausibility checks.
    def has_internal_consistency(results):
        """Asserts that route_count matches len(routes)."""
        for row in results:
            from_stop, to_stop, route_count, routes = row
            assert route_count == len(routes), f"Inconsistency in '{from_stop}' -> '{to_stop}': route_count is {route_count} but routes list has {len(routes)} items."

    return rtc(
        pg_query_runner,
        neo4j_query_runner,
        SQL,
        CYPHER,
        (), # No parameters for PostgreSQL
        {}, # No parameters for Neo4j
        # Extract and normalize results from PostgreSQL.
        lambda pg_results: [(
            row['from_stop'],
            row['to_stop'],
            row['route_count'],
            sorted(row['routes'])
        ) for row in pg_results],
        # Extract and normalize results from Neo4j.
        lambda neo4j_results: [(
            res['value']['from_stop'],
            res['value']['to_stop'],
            res['value']['route_count'],
            sorted(res['value']['routes'])
        ) for res in neo4j_results],
        plausibility_checks=[has_internal_consistency],
        result_name="overlapping segments",
        comparison_function=lambda pg, neo4j: sorted(pg) == sorted(neo4j)
    )

def test_cross_validation(pg_query_runner, neo4j_query_runner, execution_times):
    """
    CROSS-VALIDATION: Runs the query on both databases and asserts that the results
    are identical and plausible. Since the query has no parameters, this is a single,
    comprehensive test.
    """
    print(f"\nRunning cross-validation for 'overlapping_segments'.")

    pg_exec_times = execution_times.get('overlapping_segments', {}).get('pg', [])
    neo4j_exec_times = execution_times.get('overlapping_segments', {}).get('neo4j', [])

    (_, pg_exec_time, neo4j_exec_time) = run_test_case(pg_query_runner, neo4j_query_runner)
    pg_exec_times.append(pg_exec_time)
    neo4j_exec_times.append(neo4j_exec_time)

    execution_times['overlapping_segments'] = {
        'pg': pg_exec_times,
        'neo4j': neo4j_exec_times
    }
