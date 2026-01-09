
# Efficient querying of geospatial networks: A comparison between Object-Relational and Graph databases.

![Python](https://img.shields.io/badge/Python-3.13-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-PostGIS-336791)
![Neo4j](https://img.shields.io/badge/Neo4j-Spatial-008CC1)
![Docker](https://img.shields.io/badge/Docker-Enabled-2496ED)
![License](https://img.shields.io/badge/License-GPLv3-green)

> **Bachelor's Thesis (Trabajo de Fin de Grado)**  
> **Author:** Daniel Ramos Rodríguez  
> **University:** Universidad de Santiago de Compostela (USC)  
> **Date:** February 2026

## 📋 About The Project

This project explores the management and analysis of geospatial public transport networks by comparing two distinct database paradigms: **Object-Relational (PostgreSQL)** and **Graph-Oriented (Neo4j)**.

Using the **GTFS (General Transit Feed Specification)** standard as the data source, the system models transport networks (stops, routes, trips, schedules) to execute complex geospatial and temporal queries. The project includes a robust pipeline for data ingestion, validation, benchmarking, and visualization.

### Key Features
*   **GTFS Import Pipeline:** Automated scripts to clean, validate, and import CSV GTFS data into both DBs.
*   **Dual Modeling:**
    *   *PostgreSQL:* Relational schema with PostGIS extensions for spatial calculations.
    *   *Neo4j:* Graph topology with nodes/relationships and Neo4j Spatial/APOC.
*   **Query Catalog:** A suite of predefined queries (e.g., headway and connectivity analysis, statistics generation, heatmaps) implemented in both SQL and Cypher.
*   **Hybrid Routing:** Implementation of a custom connection-scan/Dijkstra hybrid algorithm for reachability analysis.
*   **Visualization:** Integration with **Folium** (Python) for interactive HTML maps and **QGIS** for desktop GIS analysis.
*   **Automated Testing:** Cross-validation system using `pytest` to ensure result consistency between the two database engines.

## 🏗 Architecture

The system follows a modular architecture:

1.  **Control Layer:** Python 3.13 scripts utilizing `psycopg` and `neo4j-driver` to orchestrate data flow and benchmarks.
2.  **Persistence Layer:** Dockerized instances of:
    *   **PostgreSQL 17** (with `postgis` and `pgrouting`).
    *   **Neo4j 5** (Enterprise, with `apoc` and `neo4j-spatial`).
3.  **Presentation Layer:** Output as JSON metrics, PNG charts, and HTML interactive maps.

## 🛠️ Built With

*   **Languages:** Python 3.13, PL/pgSQL, Cypher.
*   **Databases:** PostgreSQL, Neo4j.
*   **Libraries:**
    *   `pandas` (Data processing)
    *   `pytest` (Testing)
    *   `folium` (Visualization)
    *   `hypothesis` (Property-based testing)
*   **Tools:** Docker, QGIS.

## 🚀 Getting Started

### Prerequisites

*   **Linux OS** (Recommended for native Docker support).
*   **Docker Engine** (v28.1.1+).
*   **Python** (v3.13+).
*   **Git**.
*   **Hardware:** Recommended 16GB RAM and SSD (for large datasets).

### Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/rodrada/TFG.git
    cd TFG
    ```

2.  **Set up the Python environment:**
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt
    ```

3.  **Prepare a Dataset:**
    Download a GTFS zip (e.g., from Mobility Database), extract it into `Datasets/GTFS/<CityName>`, and run the pre-processor:
    ```bash
    # Example for Singapore
    ./process_dataset.py ../Datasets/GTFS/Singapore
    ```

## 💻 Usage

### 1. Launch Databases
Start the Docker containers for the database engines using the provided helper scripts.

```bash
# Start PostgreSQL
./Scripts/PostgreSQL/launch.sh

# Start Neo4j
./Scripts/Neo4j/launch.sh
```

### 2. Import Data
Run the import orchestrator. This creates the schema and loads the data.

```bash
# Usage: ./import.sh <Dataset_Folder_Name> <Database_Engine>
./import.sh Singapore neo4j
./import.sh Singapore postgres
```

### 3. Execution & Visualization
You can run specific analysis scripts to generate maps:

```bash
python Scripts/shortest_path_interactive_map.py --origin-stop-id "123" --destination-stop-id "456" --output map.html
```

Or connect via **QGIS** to the PostgreSQL instance (`localhost:5432`) to launch custom queries and visualize geometric results.

## 🧪 Testing

The project uses `pytest` for rigorous testing. The strategy includes **Cross-Validation** (ensuring PostgreSQL and Neo4j return identical results for the same query), **Edge Case Analysis**, and **Property-Based Testing**.

To run the full test suite:
```bash
pytest -v Tests/*.py Tests/Import/*.py
```

## 📊 Benchmarking Results

Based on the study conducted in this thesis:

*   **Import Speed:** PostgreSQL is approximately **13x faster** than Neo4j for importing raw GTFS data.
*   **Query Performance:**
    *   **PostgreSQL** excels at aggregation queries involving massive amounts of data (e.g., overlapping segments).
    *   **Neo4j** offers superior performance for deep traversals and pathfinding when starting from a limited set of nodes (e.g., finding next departures).
*   **Development:** PostgreSQL offered easier modeling (due to GTFS being relational), while Neo4j offered more concise query syntax (Cypher).

## 📂 Project Structure

```text
TFG/
├── Datasets/          # Raw GTFS data
├── Scripts/           # Source code
│   ├── Neo4J/         # Cypher queries, import logic, launch scripts
│   ├── PostgreSQL/    # SQL queries, schema definitions, launch scripts
│   ├── database.py    # DB Connection abstractions
│   └── ...            # Visualization and utility scripts
├── Tests/             # Pytest modules
├── Volumes/           # Docker persistence volumes
├── requirements.txt   # Python dependencies
└── README.md
```

## 📄 License

*   **Code:** Distributed under the **GPLv3 License**. See `LICENSE` for more information.
*   **Documentation:** Licensed under Creative Commons Attribution-ShareAlike 4.0.

## 🎓 Acknowledgments

*   **Tutor:** José Ramón Ríos Viqueira.
*   **Institution:** Escuela Técnica Superior de Ingeniería (ETSE), Universidade de Santiago de Compostela.
