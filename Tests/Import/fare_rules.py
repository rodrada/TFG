import pytest

def test_fare_rule_data_consistency(pg_query_runner, neo4j_query_runner):
    """
    CROSS-VALIDATION: Fetches all fare_rule data from both databases and asserts
    that they are identical row by row, including a count check.
    """
    print("\nPerforming full data consistency check for fare rules...")

    # PostgreSQL: Fetch all data, ordering by the composite unique key for a stable comparison.
    pg_query = """
        SELECT * FROM fare_rule
        ORDER BY fare_id, route_id, origin_id, destination_id, contains_id;
    """
    pg_data = pg_query_runner(pg_query, ())

    # Neo4J: Reconstruct the flat table by traversing all relationships from the FareRule node.
    neo4j_query = """
        MATCH (fr:FareRule)-[:FOLLOWS]->(fa:Fare)
        OPTIONAL MATCH (fr)-[:ASSOCIATED_WITH]->(r:Route)
        OPTIONAL MATCH (fr)-[:FROM_ORIGIN]->(osz:StopZone)
        OPTIONAL MATCH (fr)-[:TO_DESTINATION]->(dsz:StopZone)
        OPTIONAL MATCH (fr)-[:CONTAINING]->(csz:StopZone)
        RETURN
            fa.id AS fare_id,
            r.id AS route_id,
            osz.id AS origin_id,
            dsz.id AS destination_id,
            csz.id AS contains_id
        ORDER BY fare_id, route_id, origin_id, destination_id, contains_id;
    """
    neo4j_data = neo4j_query_runner(neo4j_query, {})

    pg_count = len(pg_data)
    neo4j_count = len(neo4j_data)

    assert pg_count == neo4j_count, f"Count mismatch: PostgreSQL has {pg_count} fare rules, Neo4J has {neo4j_count}."

    # Convert both to a canonical, comparable format (list of tuples)
    def to_canonical_tuple(row):
        return (
            row.get('fare_id'),
            row.get('route_id'),
            row.get('origin_id'),
            row.get('destination_id'),
            row.get('contains_id'),
        )

    pg_tuples = [to_canonical_tuple(row) for row in pg_data]
    neo4j_tuples = [to_canonical_tuple(row) for row in neo4j_data]

    pg_tuples.sort()
    neo4j_tuples.sort()
    
    assert pg_tuples[0:10000] == neo4j_tuples[0:10000], "Full data comparison failed. The fare rule datasets are not identical."
    print("Full data consistency check passed for fare rules.")
