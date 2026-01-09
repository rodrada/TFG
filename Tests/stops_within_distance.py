
import pytest
from conftest import random_point_in_bbox, run_test_case as rtc, RANDOM_TEST_COUNT, RANDOM_SEED, QUERIES
from hypothesis import given, strategies as st, settings
import random

# Test parameters.
MAX_SEARCH_DISTANCE_METERS = 1000

# Query statements.
SQL = QUERIES['postgres']['stops_within_distance']
CYPHER = QUERIES['neo4j']['stops_within_distance']

random.seed(RANDOM_SEED)

# Run test case.
def run_test_case(pg_query_runner, neo4j_query_runner, origin_lat: float, origin_lon: float, seek_dist: int) -> list:
    """
    Calls the generic run_test_case function with parameters for the stops_within_distance query.
    """

    # Plausibility checks.
    def in_range(results):
        """
        Asserts that all distances in the results are positive and under the search distance.
        """
        assert [x for x in results if x[2] >= 0 and x[2] <= seek_dist] == results

    return rtc(
        pg_query_runner,
        neo4j_query_runner,
        SQL,
        CYPHER,
        (origin_lat, origin_lon, seek_dist),
        {'origin_lat': origin_lat, 'origin_lon': origin_lon, 'seek_dist': seek_dist},
        lambda pg_results: [(row['id'], row['name'], int(row['distance'])) for row in pg_results],
        lambda neo4j_results: [(record['value']['id'], record['value']['name'], record['value']['distance']) for record in neo4j_results],
        plausibility_checks=[in_range],
        result_name="stops",
        # Check if the distances are approximately the same (allow 1 meter differences for rounding errors).
        comparison_function=lambda pg, neo4j: len(pg) == len(neo4j) and all(
            p[0] == n[0] and
            p[1] == n[1] and
            abs(p[2] - n[2]) <= 1
            for p, n in zip(pg, neo4j)
        )
    )

def test_random_inputs(pg_query_runner, neo4j_query_runner, bounding_box, execution_times):
    """
    CROSS-VALIDATION: Generates random points and asserts that results are plausible and consistent between both databases.
    """
    print(f"\nRunning random input tests for 'stops_within_distance' ({RANDOM_TEST_COUNT} iterations).")

    assert bounding_box is not None, "Test setup failed: Bounding box could not be determined."

    pg_exec_times = execution_times.get('stops_within_distance', {}).get('pg', [])
    neo4j_exec_times = execution_times.get('stops_within_distance', {}).get('neo4j', [])

    for i in range(RANDOM_TEST_COUNT):
        lat, lon = random_point_in_bbox(bounding_box)
        distance = random.randint(0, MAX_SEARCH_DISTANCE_METERS)
        print(f"\n[{i+1}/{RANDOM_TEST_COUNT}] Testing point: (lat={lat:.4f}, lon={lon:.4f}), distance={distance}m")
        (_, pg_exec_time, neo4j_exec_time) = run_test_case(pg_query_runner, neo4j_query_runner, origin_lat=lat, origin_lon=lon, seek_dist=distance)
        pg_exec_times.append(pg_exec_time)
        neo4j_exec_times.append(neo4j_exec_time)

    execution_times['stops_within_distance'] = {
        'pg': pg_exec_times,
        'neo4j': neo4j_exec_times
    }

def test_edge_cases(pg_query_runner, neo4j_query_runner, bounding_box):
    """
    EDGE CASE ANALYSIS: Tests with tricky inputs.
    """

    print("\nRunning edge case analysis for 'stops_within_distance'.")

    assert bounding_box is not None, "Test setup failed: Bounding box is None."

    print(f"\nTesting the North Pole (lat=90.0, lon=0.0), distance={MAX_SEARCH_DISTANCE_METERS}m")
    results = run_test_case(pg_query_runner, neo4j_query_runner, origin_lat=90.0, origin_lon=0.0, seek_dist=MAX_SEARCH_DISTANCE_METERS)[0]

    assert len(results) == 0, f"Unexpected result for North Pole, results were non-empty: {results}"

    print(f"\nTesting the South Pole (lat=-90.0, lon=0.0), distance={MAX_SEARCH_DISTANCE_METERS}m")
    results = run_test_case(pg_query_runner, neo4j_query_runner, origin_lat=-90.0, origin_lon=0.0, seek_dist=MAX_SEARCH_DISTANCE_METERS)[0]

    assert len(results) == 0, f"Unexpected result for South Pole, results were non-empty: {results}"

    print("\nTesting null search distances (distance=0m)")
    for i in range(RANDOM_TEST_COUNT):
        lat, lon = random_point_in_bbox(bounding_box)
        print(f"\n[{i+1}/{RANDOM_TEST_COUNT}] Testing point: (lat={lat:.4f}, lon={lon:.4f}), distance=0m")
        results = run_test_case(pg_query_runner, neo4j_query_runner, origin_lat=lat, origin_lon=lon, seek_dist=0)[0]

        assert len(results) == 0, f"Unexpected result for null search distance, results were non-empty: {results}"

    outside_lat = bounding_box['max_lat'] + 10.0
    outside_lon = bounding_box['max_lon'] + 10.0
    distance = 5000
    print(f"\nTesting point well outside the bounding box (lat={outside_lat:.4f}, lon={outside_lon:.4f}), distance={distance}m")
    results = run_test_case(pg_query_runner, neo4j_query_runner, origin_lat=outside_lat, origin_lon=outside_lon, seek_dist=distance)[0]

    assert len(results) == 0, f"Unexpected result for point (lat={outside_lat:.4f}, lon={outside_lon:.4f}), results were non-empty: {results}"


@pytest.mark.hypothesis
def test_property_based(pg_query_runner, neo4j_query_runner):
    """
    PROPERTY-BASED TESTING: Check properties remain true for a wide variety of inputs.
    """

    @given(
        lat=st.floats(min_value=-90.0, max_value=90.0),
        lon=st.floats(min_value=-180.0, max_value=180.0),
        distance=st.integers(min_value=0, max_value=MAX_SEARCH_DISTANCE_METERS)
    )
    @settings(deadline=None)
    # Property: For any valid coordinate, the query should execute without crashing.
    def test_pbt_distance_query_never_crashes(lat, lon, distance):
        pg_query_runner(SQL, (lat, lon, distance))
        neo4j_query_runner(CYPHER, {'origin_lat': lat, 'origin_lon': lon, 'seek_dist': distance})

    test_pbt_distance_query_never_crashes()
