#!/usr/bin/env python3

import json
import subprocess
from subprocess import CalledProcessError
import time
import os
import pathlib

CACHE_FILE = 'import_times.json'
SCRIPT_DIR = pathlib.Path(__file__).parent.resolve()

def load_cache():
    """Loads the cache from a .json file if it exists."""
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_cache(cache):
    """Saves the cache to a .json file."""
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache, f, indent=4)

def run_import_scripts(dataset_name: str):
    """
    Executes the import scripts for a given dataset.
    """
    print("  Running import scripts...")

    print(f"  Running PostgreSQL import script for {dataset_name}...")
    postgres_start_time = time.time()
    import_command = ["python", SCRIPT_DIR / "import.py", dataset_name, "postgres"]
    subprocess.run(import_command, check=True)
    postgres_end_time = time.time()
    postgres_import_time = postgres_end_time - postgres_start_time

    print(f"  Running Neo4J import script for {dataset_name}...")
    neo4j_start_time = time.time()
    import_command = ["python", SCRIPT_DIR / "import.py", dataset_name, "neo4j"]
    subprocess.run(import_command, check=True)
    neo4j_end_time = time.time()
    neo4j_import_time = neo4j_end_time - neo4j_start_time

    return (postgres_import_time, neo4j_import_time)

def main():
    """
    Main function to run the process.
    """
    arguments_list = ['MadridEMT', 'Prague', 'HongKong', 'Bogota', 'Singapore', 'GaliciaBus', 'Munich', 'Moscow', 'NYCSubway', 'Belgrade']
    cache = load_cache()

    for arg in arguments_list:
        if arg in cache:
            print(f"Result for '{arg}' found in cache.")
            postgres_import_time = cache[arg]['postgres_import_time']
            neo4j_import_time = cache[arg]['neo4j_import_time']
        else:
            try:
                print(f"Result for '{arg}' not in cache. Executing imports.")
                (postgres_import_time, neo4j_import_time) = run_import_scripts(arg)            
                cache[arg] = {
                    'postgres_import_time': postgres_import_time,
                    'neo4j_import_time': neo4j_import_time
                }
                print(f"Dataset: {arg}, PostgreSQL Import Time: {postgres_import_time:.4f} seconds, Neo4J Import Time: {neo4j_import_time:.4f} seconds\n")
                save_cache(cache)
                print(f"Result for '{arg}' cached.")
            except CalledProcessError as e:
                print(f"Error executing imports for '{arg}': {e}")
                postgres_import_time = None
                neo4j_import_time = None

if __name__ == "__main__":
    main()
