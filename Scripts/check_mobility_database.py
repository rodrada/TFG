#!/usr/bin/env python3
import os
import requests
import zipfile
import subprocess
import time
import tempfile
import pathlib
import json

# --- Configuration ---
TOKEN_URL = "https://api.mobilitydatabase.org/v1/tokens"
CATALOG_URL = "https://api.mobilitydatabase.org/v1/gtfs_feeds"
DATASET_QUERY_FORMAT_URL = "https://api.mobilitydatabase.org/v1/gtfs_feeds/mdb-{dataset_id}/datasets"
DATASET_RETRIEVE_FORMAT_URL = "https://api.mobilitydatabase.org/v1/datasets/{dataset_name}"
REFRESH_TOKEN_ENV_VAR = "MOBILITY_DB_REFRESH_TOKEN"
MAX_TEST_DATASET_SIZE_MB = 30
MAX_FILE_LINE_COUNT = 3000
SCRIPT_DIR = pathlib.Path(__file__).parent.resolve()
CACHE_FILE = SCRIPT_DIR / "mdb_dataset_cache.json"
TEMP_BASE_DIR = SCRIPT_DIR / "../Datasets/GTFS"

def load_cache() -> dict:
    """Loads the processed datasets cache from a JSON file."""
    if not CACHE_FILE.exists():
        return {}
    with open(CACHE_FILE, 'r') as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            print("Warning: Cache file is corrupted. Starting with an empty cache.")
            return {}

def save_cache(cache: dict):
    """Saves the processed datasets cache to a JSON file."""
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache, f, indent=4)

def get_refresh_token() -> str:
    """
    Retrieves the Mobility Database Refresh Token from environment variables.
    Exits with an error if the token is not found.
    """
    refresh_token = os.environ.get(REFRESH_TOKEN_ENV_VAR)
    if not refresh_token:
        print(f"Error: Refresh Token not found.")
        print(f"Please set the '{REFRESH_TOKEN_ENV_VAR}' environment variable.")
        exit(1)
    return refresh_token


def get_access_token(refresh_token: str) -> str | None:
    """
    Uses the Refresh Token to generate a short-lived Access Token.
    """
    print("Generating a new Access Token...")
    headers = {"Content-Type": "application/json"}
    payload = {"refresh_token": refresh_token}
    try:
        response = requests.post(TOKEN_URL, headers=headers, json=payload)
        response.raise_for_status()
        access_token = response.json().get("access_token")
        if access_token:
            print("Successfully obtained Access Token.")
            return access_token
        else:
            print("Error: Could not find 'access_token' in the authentication response.")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error generating Access Token: {e}")
        return None


def get_datasets(access_token: str) -> list:
    """
    Fetches the list of all available GTFS datasets from the Mobility Database.
    """
    print("Fetching dataset catalog from Mobility Database...")
    headers = {"Authorization": f"Bearer {access_token}"}
    try:
        response = requests.get(CATALOG_URL, headers=headers)
        response.raise_for_status()
        sources = response.json()
        print(f"Found {len(sources)} total datasets in the catalog.")
        return sources
    except requests.exceptions.RequestException as e:
        print(f"Error fetching dataset catalog: {e}")
        return []

def download_and_unzip(dataset_url: str, target_path: str) -> str:
    """
    Downloads a GTFS zip file and unzips it into the target path.
    """
    print(f"  Downloading from {dataset_url}...")
    zip_path = os.path.join(target_path, "dataset.zip")
    with requests.get(dataset_url, stream=True) as r:
        r.raise_for_status()
        with open(zip_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    print("  Download complete. Unzipping...")
    unzip_dir = target_path
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(unzip_dir)
    print("  Unzip complete.")
    return unzip_dir


def run_import_scripts(dataset_dir: str):
    """
    Executes the import scripts for a given dataset.
    """
    print("  Running import scripts...")

    print(f"  Running preprocessing script for {dataset_dir}...")
    preprocessing_command = ["python", SCRIPT_DIR / "process_dataset.py", dataset_dir]
    subprocess.run(preprocessing_command, check=True)

    print(f"  Running PostgreSQL import script for {dataset_dir}...")
    postgres_start_time = time.time()
    import_command = ["python", SCRIPT_DIR / "import.py", os.path.basename(dataset_dir), "postgres"]
    subprocess.run(import_command, check=True)
    postgres_end_time = time.time()
    postgres_import_time = postgres_end_time - postgres_start_time

    print(f"  Running Neo4J import script for {dataset_dir}...")
    neo4j_start_time = time.time()
    import_command = ["python", SCRIPT_DIR / "import.py", os.path.basename(dataset_dir), "neo4j"]
    subprocess.run(import_command, check=True)
    neo4j_end_time = time.time()
    neo4j_import_time = neo4j_end_time - neo4j_start_time

    return (postgres_import_time, neo4j_import_time)


def run_tests(dataset_dir: str):
    """
    Executes the test suite for a given dataset.
    """
    print("  Running pytest suite...")

    import_test_files = []
    for filename in os.listdir(dataset_dir):
        if filename.lower().endswith('.txt'):
            filepath = os.path.join(dataset_dir, filename)

            # Use subprocess to run 'wc -l'
            result = subprocess.run(['wc', '-l', filepath], check=True, capture_output=True, text=True)

            # The output of 'wc -l' is "  <line_count> <filename>"
            line_count_str = result.stdout.strip().split()[0]
            line_count = int(line_count_str)

            if line_count <= MAX_FILE_LINE_COUNT:
                if filename.lower() == 'calendar.txt':  # This is a workaround for Python not allowing you to have a module named calendar.py.
                    import_test_files.append("Tests/Import/calendar~.py")
                else:
                    import_test_files.append("Tests/Import/" + filename.lower().replace(".txt", ".py"))

    if import_test_files:
        subprocess.run("pytest -vv " + " ".join(import_test_files) + " >> test_results.txt", shell=True, check=True)
    subprocess.run("pytest -vv Tests/*.py >> test_results.txt", shell=True, check=True)

def main():
    """Main orchestrator function."""
    cache = load_cache()
    refresh_token = get_refresh_token()
    access_token = get_access_token(refresh_token)
    if not access_token:
        print("Could not obtain an Access Token. Exiting.")
        return

    datasets = sorted(get_datasets(access_token), key=lambda x: x['id'])
    if not datasets:
        print("No datasets found or API call failed. Exiting.")
        return

    os.makedirs(TEMP_BASE_DIR, exist_ok=True)

    for i, dataset in enumerate(datasets):

        # Skip datasets that are not GTFS, if they exist
        if 'gtfs' != dataset.get('data_type'):
            continue

        dataset_name = dataset.get('provider', 'Unknown')
        mdb_source_id = dataset.get('id', 'Unknown')
        latest_dataset = dataset.get('latest_dataset') or {}
        latest_url = latest_dataset.get('hosted_url', None)
        latest_unzipped_size = latest_dataset.get('unzipped_folder_size_mb', None)

        print(f"\n--- [{i+1}/{len(datasets)}] Processing: {dataset_name} (ID: {mdb_source_id}) ---")

        if not latest_url:
            print("  Skipping: No 'latest' download URL available.")
            continue

        if not latest_unzipped_size:
            print("  Skipping: Couldn't determine unzipped size.")
            continue

        if latest_unzipped_size > MAX_TEST_DATASET_SIZE_MB:
            print("  Skipping: Dataset is too large for testing.")
            continue

        if mdb_source_id not in cache:
            cache[mdb_source_id] = {}

        if cache.get(mdb_source_id).get('success', False):
            print("  Skipping: Dataset already processed.")
            continue

        with tempfile.TemporaryDirectory(dir=TEMP_BASE_DIR) as temp_dir_path:
            os.chmod(temp_dir_path, 0o755)
            try:
                unzipped_path = download_and_unzip(latest_url, temp_dir_path)
                (postgres_import_time, neo4j_import_time) = run_import_scripts(unzipped_path)
                run_tests(unzipped_path)
                print(f"--- SUCCESS: Completed import and testing for {dataset_name} ---")
            except Exception as e:
                print(f"An error occurred while processing {dataset_name}: {e}")
                cache[mdb_source_id] = {
                    'name': dataset_name,
                    'id': latest_dataset['id'],
                    'success': False,
                    'cause': str(e),
                    'validation_report': latest_dataset.get('validation_report', {})
                }
                save_cache(cache)
                print(f"--- FAILED: Skipping to next dataset ---")
                continue

        # Get countries in the dataset.
        countries = set(map(lambda x: x['country_code'], dataset.get('locations', [])))

        cache[mdb_source_id] = {
            'name': dataset_name,
            'id': latest_dataset['id'],
            'success': True,
            'countries': list(countries),
            'official': dataset.get('official', False),
            'bounding_box': latest_dataset.get('bounding_box', {}),
            'postgres_import_time': postgres_import_time,
            'neo4j_import_time': neo4j_import_time,
            'validation_report': latest_dataset.get('validation_report', {})
        }
        save_cache(cache)

    print("\nScript finished.")


if __name__ == "__main__":
    main()
