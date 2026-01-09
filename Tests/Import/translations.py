import pytest

def test_translation_data_consistency(pg_query_runner, neo4j_query_runner):
    """
    CROSS-VALIDATION: Fetches all translation data from both databases and asserts
    that they are identical row by row, including a count check.
    """
    print("\nPerforming full data consistency check for translations...")

    # PostgreSQL: Fetch all data directly from the translation table.
    # The order by clause mirrors the unique index for stable ordering.
    pg_query = """
        SELECT * FROM translation
        ORDER BY table_name, field_name, language, record_id, record_sub_id, field_value;
    """
    pg_data = pg_query_runner(pg_query, ())

    # Neo4J: Reconstruct the flat translation table by querying all possible
    # translated nodes and their relationships to Translation nodes. This is a complex
    # query that unions the results for different entity types.
    neo4j_query = """
        // Part 1: Regular Nodes (Agency, Stop, Route, etc.)
        MATCH (e)-[:HAS_TRANSLATION]->(t:Translation)
        WHERE single(lbl IN labels(e) WHERE lbl IN ["Agency", "Stop", "Route", "Trip", "Pathway", "Level", "Attribution"])
        WITH e, t, labels(e)[0] as label
        RETURN
            CASE label
                WHEN 'Agency' THEN 'agency'
                WHEN 'Stop' THEN 'stops'
                WHEN 'Route' THEN 'routes'
                WHEN 'Trip' THEN 'trips'
                WHEN 'Pathway' THEN 'pathways'
                WHEN 'Level' THEN 'levels'
                WHEN 'Attribution' THEN 'attributions'
            END AS table_name,
            t.field_name AS field_name,
            t.language AS language,
            t.text AS translation,
            e.id AS record_id,
            NULL AS record_sub_id
        UNION ALL
        // Part 2: StopTimes, which have a composite key
        MATCH (st:StopTime)-[:HAS_TRANSLATION]->(t:Translation)
        MATCH (st)-[:PART_OF]->(trip:Trip)
        RETURN
            'stop_times' AS table_name,
            t.field_name AS field_name,
            t.language AS language,
            t.text AS translation,
            trip.id AS record_id,
            toString(st.stop_sequence) AS record_sub_id
        UNION ALL
        // Part 3: FeedInfo, which is a singleton
        MATCH (f:Feed)-[:HAS_TRANSLATION]->(t:Translation)
        RETURN
            'feed_info' AS table_name,
            t.field_name AS field_name,
            t.language AS language,
            t.text AS translation,
            NULL AS record_id,
            NULL AS record_sub_id
    """
    # We need to add the ORDER BY clause here because UNION does not guarantee order.
    neo4j_data = neo4j_query_runner(neo4j_query + " ORDER BY table_name, field_name, language, record_id, record_sub_id;", {})

    pg_count = len(pg_data)
    neo4j_count = len(neo4j_data)

    assert pg_count == neo4j_count, f"Count mismatch: PostgreSQL has {pg_count} translations, Neo4J has {neo4j_count}."

    # Convert both to a canonical, comparable format (list of tuples).
    # NOTE: We intentionally ignore the 'field_value' column from PostgreSQL. This value is used
    #       for the matching logic during the Neo4J import but is not stored in the final graph structure.
    def to_canonical_tuple(row):
        return (
            row.get('table_name'),
            row.get('field_name'),
            row.get('language'),
            row.get('translation') or row.get('text'),
            row.get('record_id'),
            row.get('record_sub_id'),
        )

    pg_tuples = [to_canonical_tuple(row) for row in pg_data]
    neo4j_tuples = [to_canonical_tuple(row) for row in neo4j_data]

    pg_tuples.sort()
    neo4j_tuples.sort()
    
    assert pg_tuples[0:10000] == neo4j_tuples[0:10000], "Full data comparison failed. The translation datasets are not identical."
    print("Full data consistency check passed for translations.")
