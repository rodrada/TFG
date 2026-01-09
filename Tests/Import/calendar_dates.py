import pytest

def test_service_exception_data_consistency(pg_query_runner, neo4j_query_runner):
    """
    CROSS-VALIDATION: Fetches all service exception data from both databases and
    asserts that they are identical row by row, including a count check.
    """
    print("\nPerforming full data consistency check for service exceptions (calendar_dates)...")

    # PostgreSQL: Join with the enum table to get the string representation of the type.
    pg_query = """
        SELECT
            se.service_id,
            se.date,
            et.name AS exception_type_str
        FROM service_exception se
        JOIN exception_type et ON se.exception_type = et.id
        ORDER BY se.service_id, se.date;
    """
    pg_data = pg_query_runner(pg_query, ())

    # Neo4J: Find the relationship and its properties to reconstruct the table.
    neo4j_query = """
        MATCH (s:Service)-[h:HAS_EXCEPTION]->(d:Day)
        RETURN
            s.id AS service_id,
            d.date AS date,
            // Convert the numeric type property to its string equivalent
            CASE toInteger(h.type)
                WHEN 1 THEN 'Added'
                WHEN 2 THEN 'Removed'
            END AS exception_type_str
        ORDER BY service_id, date;
    """
    neo4j_data = neo4j_query_runner(neo4j_query, {})

    pg_count = len(pg_data)
    neo4j_count = len(neo4j_data)

    assert pg_count == neo4j_count, f"Count mismatch: PostgreSQL has {pg_count} service exceptions, Neo4J has {neo4j_count}."

    # Convert both to a canonical, comparable format (list of tuples)
    def to_canonical_tuple(row):
        # The database drivers should return comparable date objects
        return (
            row.get('service_id'),
            row.get('date'),
            row.get('exception_type_str'),
        )

    pg_tuples = [to_canonical_tuple(row) for row in pg_data]
    neo4j_tuples = [to_canonical_tuple(row) for row in neo4j_data]

    pg_tuples.sort()
    neo4j_tuples.sort()

    assert pg_tuples[0:10000] == neo4j_tuples[0:10000], "Full data comparison failed. The service exception datasets are not identical."
    print("Full data consistency check passed for service exceptions.")
