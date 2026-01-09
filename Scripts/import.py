#!/usr/bin/env python3

import argparse
import sys
from pathlib import Path
import logging

from database import NEO4J_CONFIG, PG_CONFIG

try:
    from neo4j import GraphDatabase, exceptions as neo4j_exceptions
    import psycopg
    from psycopg import sql
except ImportError as e:
    print(f"Error: A required library is not installed. Please install it using 'pip install neo4j psycopg'. Missing: {e.name}", file=sys.stderr)
    sys.exit(1)

# Ignore deprecated method warnings from Neo4J (mostly linked to apoc.trigger.add).
logging.getLogger("neo4j").setLevel(logging.ERROR)    

def execute_neo4j_commands(commands, dataset_name):
    """Connects to Neo4J and executes a series of Cypher commands."""
    try:
        with GraphDatabase.driver(NEO4J_CONFIG['uri'], auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password'])) as driver:
            # Check for connectivity
            driver.verify_connectivity()
            
            # Neo4j 5+ requires system commands to be run against the 'system' database
            with driver.session(database="system") as session:
                # Using a transaction to ensure all setup commands succeed or fail together
                session.execute_write(lambda tx: tx.run("CREATE OR REPLACE DATABASE gtfs WAIT;").consume())
            
            # Now connect to the newly created 'gtfs' database
            with driver.session(database=NEO4J_CONFIG['database']) as session:
                for command_part in commands:
                    # NOTE: The Neo4J driver does not support multi-statement queries directly.
                    #       We split by semicolon, but this is a simplistic approach.
                    #       Complex scripts with semicolons in strings might require more robust parsing.
                    statements = [s.strip() for s in command_part.split(';') if s.strip()]
                    for statement in statements:
                        # Substitute the parameter placeholder
                        param_statement = statement.replace('$dataset', f'"{dataset_name}"')
                        session.run(param_statement)

            print("Neo4J import executed successfully.")
            
    except neo4j_exceptions.AuthError:
        print("Error: Neo4J authentication failed. Please check your credentials.", file=sys.stderr)
        sys.exit(1)
    except neo4j_exceptions.ServiceUnavailable:
        print(f"Error: Could not connect to Neo4J at {NEO4J_CONFIG['uri']}. Is the database running and the port accessible?", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected Neo4J error occurred: {e}", file=sys.stderr)
        sys.exit(1)

def execute_postgres_commands(commands, dataset_name):
    """Connects to PostgreSQL and executes a series of SQL commands."""
    conn_info = f"host={PG_CONFIG['host']} port={PG_CONFIG['port']} user={PG_CONFIG['user']} password={PG_CONFIG['password']}"
    
    try:
        # Connect to the default 'postgres' database to manage the 'gtfs' database
        with psycopg.connect(conn_info, dbname="postgres", autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("DROP DATABASE IF EXISTS gtfs;")
                cur.execute("CREATE DATABASE gtfs;")
                cur.execute("ALTER SYSTEM SET max_wal_size = '4GB';")
                
        
        # Now connect to the newly created 'gtfs' database
        with psycopg.connect(conn_info, dbname=PG_CONFIG['dbname']) as conn:
            with conn.cursor() as cur:
                for command_part in commands:
                    # Replace the psql \set variable. 
                    # A better approach would be to adapt SQL scripts to use session variables or function parameters.
                    command = command_part.replace(":'dataset_dir'", f"'/var/lib/postgresql/import/GTFS/{dataset_name}'")
                    cur.execute(command)
                    conn.commit()

        print("PostgreSQL import executed successfully.")

    except psycopg.OperationalError as e:
        print(f"Error: Could not connect to PostgreSQL. Is the database running and the port accessible? Details: {e}", file=sys.stderr)
        sys.exit(1)
    except psycopg.Error as e:
        print(f"A PostgreSQL error occurred: {e}", file=sys.stderr)
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Import a GTFS dataset into Neo4J or PostgreSQL.")
    parser.add_argument("dataset", help="The name of the GTFS dataset directory.")
    parser.add_argument("dbms", choices=['neo4j', 'postgres'], help="The target database management system.")

    args = parser.parse_args()

    dataset = args.dataset
    dbms = args.dbms.lower()

    script_dir = Path(__file__).parent.resolve()
    root_dir = script_dir.parent
    dataset_dir = root_dir / "Datasets" / "GTFS" / dataset

    if not dataset_dir.is_dir():
        print(f"Error: Dataset directory not found at '{dataset_dir}'", file=sys.stderr)
        sys.exit(1)

    command_string_parts = []
    file_extension = ""

    # Choose script directory and file extension based on DBMS.
    if dbms == "neo4j":
        import_dir = script_dir / "Neo4J" / "Import"
        file_extension = "cypher"
    elif dbms == "postgres":
        import_dir = script_dir / "PostgreSQL" / "Import"
        file_extension = "sql"
        # Initial setup commands.
        # NOTE: You can add "LOAD 'auto_explain';" to the command string if you want to debug execution times.
        command_string_parts.append("""
            SET client_min_messages = 'warning';
            CREATE EXTENSION IF NOT EXISTS postgis;
            CREATE EXTENSION IF NOT EXISTS pgrouting;
            SELECT pg_reload_conf();
        """)
        try:
            import_script = (import_dir / "base.sql").read_text()
            command_string_parts.append(import_script)
        except FileNotFoundError:
            print(f"Error: PostgreSQL script 'base.sql' not found.", file=sys.stderr)
            exit(1)

    # List of files contained by a GTFS dataset by the order in which they must be imported.
    file_list = [
        "agency",
        "stops",
        "routes",
        "calendar",
        "calendar_dates",    # NOTE: This is not mandatory
        "trips",
        "stop_times",

        # Completely optional files
        "networks",
        "timeframes",
        "fare_media",
        "fare_products",
        "areas",
        "attributions",
        "booking_rules",
        "fare_attributes",
        "fare_leg_join_rules",
        "fare_leg_rules",
        "fare_rules",
        "fare_transfer_rules",
        "feed_info",
        "frequencies",
        "levels",
        "location_groups",
        "location_group_stops",
        "route_networks",
        "pathways",
        "shapes",
        "stop_areas",
        "transfers",
        "translations"
    ]

    base_files = [
        "agency",
        "routes",
        "trips",
        "stops",
        "stop_times"
    ]

    print("Checking for files:\n")

    # Make sure base files are present first.
    for file in base_files:
        if not (dataset_dir / f"{file}.txt").is_file():
            print(f"\nError: Missing fundamental files in the GTFS standard: {file}.txt\n", file=sys.stderr)
            sys.exit(1)

    # Handle the special case for services.
    if not (dataset_dir / "calendar.txt").is_file() and not (dataset_dir / "calendar_dates.txt").is_file():
        print("\nError: Missing calendar.txt or calendar_dates.txt. At least one of them is required.", file=sys.stderr)
        sys.exit(1)

    # Iterate over all files and compose the command string.
    for file in file_list:
        file_exists = (dataset_dir / f"{file}.txt").is_file()

        if not file_exists:
            print(f"{file}: no")
        else:
            print(f"{file}: yes")

        if file_exists or dbms == "postgres":
            try:
                import_script = (import_dir / f"{file}.{file_extension}").read_text()
                command_string_parts.append(import_script)
            except FileNotFoundError:
                print(f"Warning: GTFS files exist but script '{file}.{file_extension}' not found.", file=sys.stderr)

    # Append predefined queries to the command string.
    queries_script_path = (script_dir / "Neo4J" / "queries.cypher") if dbms == "neo4j" else (script_dir / "PostgreSQL" / "queries.sql")
    if queries_script_path.is_file():
        command_string_parts.append(queries_script_path.read_text())

    # Launch the commands.
    print("\nStarting import...\n")

    if dbms == "neo4j":
        execute_neo4j_commands(command_string_parts, dataset)
    elif dbms == "postgres":
        execute_postgres_commands(command_string_parts, dataset)

    print("Import finished.")

if __name__ == "__main__":
    main()
