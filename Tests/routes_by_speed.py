import pytest
from conftest import run_test_case as rtc, QUERIES

# Query statements.
SQL = QUERIES['postgres']['routes_by_speed']
CYPHER = QUERIES['neo4j']['routes_by_speed']

# Run test case.
def run_test_case(pg_query_runner, neo4j_query_runner) -> list:
    """
    Calls the generic run_test_case function with parameters for the routes_by_speed query.
    """

    # Plausibility checks.
    def values_are_positive(results):
        """Asserts that trip_count and avg_speed_kmh are greater than zero."""
        for row in results:
            route_name, trip_count, avg_speed = row
            assert trip_count > 0, f"Found non-positive trip_count for route '{route_name}': {trip_count}"
            assert avg_speed > 0, f"Found non-positive avg_speed_kmh for route '{route_name}': {avg_speed}"

    def route_name_is_present(results):
        """Asserts that the route name is not null or empty."""
        for row in results:
            assert row[0] is not None and row[0].strip() != "", f"Found a row with a missing route name: {row}"

    # NOTE: This is required because the 1 km/h differences change the sort order of outputs.
    #       The idea if results are the same if, when sorted a specific way, they are the same.
    def comparison(pg, neo4j):
        """Compares the results of the query."""
        sorted_pg = sorted(pg, key=lambda x: (x[0], x[1], x[2]))
        sorted_neo4j = sorted(neo4j, key=lambda x: (x[0], x[1], x[2]))

        return len(sorted_pg) == len(sorted_neo4j) and all(
            p[0] == n[0] and
            p[1] == n[1] and
            abs(p[2] - n[2]) <= 1     # Allow a 1 km/h margin for minor calculation errors.
            for p, n in zip(sorted_pg, sorted_neo4j)
        )

    return rtc(
        pg_query_runner,
        neo4j_query_runner,
        SQL,
        CYPHER,
        (), # No parameters for PostgreSQL
        {}, # No parameters for Neo4j
        # Extract and normalize results from PostgreSQL.
        # Cast NUMERIC (Decimal) to float for consistent comparison.
        lambda pg_results: [(
            row['route_name'],
            int(row['trip_count']),
            float(row['avg_speed_kmh'])
        ) for row in pg_results],
        # Extract and normalize results from Neo4j.
        lambda neo4j_results: [(
            res['value']['route_name'],
            int(res['value']['trip_count']),
            float(res['value']['avg_speed_kmh'])
        ) for res in neo4j_results],
        plausibility_checks=[values_are_positive, route_name_is_present],
        result_name="results",
        comparison_function=comparison
    )

def test_cross_validation(pg_query_runner, neo4j_query_runner, execution_times):
    """
    CROSS-VALIDATION: Runs the query on both databases and asserts that the results
    are identical and plausible. Since the query has no parameters, this is a single,
    comprehensive test.
    """
    print(f"\nRunning cross-validation for 'routes_by_speed'.")

    pg_exec_times = execution_times.get('routes_by_speed', {}).get('pg', [])
    neo4j_exec_times = execution_times.get('routes_by_speed', {}).get('neo4j', [])

    (_, pg_exec_time, neo4j_exec_time) = run_test_case(pg_query_runner, neo4j_query_runner)
    pg_exec_times.append(pg_exec_time)
    neo4j_exec_times.append(neo4j_exec_time)

    execution_times['routes_by_speed'] = {
        'pg': pg_exec_times,
        'neo4j': neo4j_exec_times
    }
