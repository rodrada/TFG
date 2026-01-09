import pytest

def test_transfer_data_consistency(pg_query_runner, neo4j_query_runner):
    """
    CROSS-VALIDATION: Fetches all transfer data from both databases and asserts
    that they are identical row by row, including a count check.
    """
    print("\nPerforming full data consistency check for transfers...")

    # PostgreSQL: Join with the enum table to get the string representation of the type.
    pg_query = """
        SELECT
            t.*,
            tt.name AS transfer_type_str
        FROM transfer t
        LEFT JOIN transfer_type tt ON t.transfer_type = tt.id
        ORDER BY from_stop_id, to_stop_id, from_route_id, to_route_id, from_trip_id, to_trip_id;
    """
    pg_data = pg_query_runner(pg_query, ())

    # Neo4J: Reconstruct the flat table by traversing all relationships from the Transfer node.
    neo4j_query = """
        MATCH (t:Transfer)
        // Use OPTIONAL MATCH for all relationships as they can be NULL
        OPTIONAL MATCH (t)-[:FROM]->(fs:Stop)
        OPTIONAL MATCH (t)-[:TO]->(ts:Stop)
        OPTIONAL MATCH (t)-[:FROM]->(fr:Route)
        OPTIONAL MATCH (t)-[:TO]->(tr:Route)
        OPTIONAL MATCH (t)-[:FROM]->(ft:Trip)
        OPTIONAL MATCH (t)-[:TO]->(tt:Trip)
        OPTIONAL MATCH (t)-[:HAS_TYPE]->(trt:TransferType)
        RETURN
            fs.id AS from_stop_id,
            ts.id AS to_stop_id,
            fr.id AS from_route_id,
            tr.id AS to_route_id,
            ft.id AS from_trip_id,
            tt.id AS to_trip_id,
            trt.value AS transfer_type_str,
            t.min_transfer_time
        ORDER BY from_stop_id, to_stop_id, from_route_id, to_route_id, from_trip_id, to_trip_id;
    """
    neo4j_data = neo4j_query_runner(neo4j_query, {})

    pg_count = len(pg_data)
    neo4j_count = len(neo4j_data)

    assert pg_count == neo4j_count, f"Count mismatch: PostgreSQL has {pg_count} transfers, Neo4J has {neo4j_count}."

    # Convert both to a canonical, comparable format (list of tuples).
    def to_canonical_tuple(row):
        return (
            row.get('from_stop_id'),
            row.get('to_stop_id'),
            row.get('from_route_id'),
            row.get('to_route_id'),
            row.get('from_trip_id'),
            row.get('to_trip_id'),
            row.get('transfer_type_str'),
            row.get('min_transfer_time'),
        )

    pg_tuples = [to_canonical_tuple(row) for row in pg_data]
    neo4j_tuples = [to_canonical_tuple(row) for row in neo4j_data]

    pg_tuples.sort()
    neo4j_tuples.sort()
    
    assert pg_tuples[0:10000] == neo4j_tuples[0:10000], "Full data comparison failed. The transfer datasets are not identical."
    print("Full data consistency check passed for transfers.")
