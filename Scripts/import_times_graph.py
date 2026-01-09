#!/usr/bin/env python3

import argparse
import json
import sys
import numpy as np
import pathlib
import os

try:
    import matplotlib.pyplot as plt
except ImportError as e:
    print(f"Error: A required library is not installed. Please install it using 'pip install matplotlib'. Missing: {e.name}", file=sys.stderr)
    sys.exit(1)

def main():
    """
    Main function to parse arguments, read the JSON data,
    and generate a comparative bar chart.
    """
    parser = argparse.ArgumentParser(description="Generate a bar chart comparing PostgreSQL and Neo4j import times from a JSON file.")
    parser.add_argument("--input", required=True, help="Path to the input JSON file with timing data.")
    parser.add_argument("--output", required=True, help="Path to save the generated bar chart image.")
    parser.add_argument("--postgres-color", type=str, default="#2f6792", help="Color for the PostgreSQL bars.")
    parser.add_argument("--neo4j-color", type=str, default="#e87722", help="Color for the Neo4j bars.")
    args = parser.parse_args()

    # Load the data from the JSON file
    with open(args.input, 'r') as f:
        data = json.load(f)

    # Prepare data for plotting
    dataset_names = list(data.keys())
    postgres_times = [data[ds]['postgres_import_time'] for ds in dataset_names]
    neo4j_times = [data[ds]['neo4j_import_time'] for ds in dataset_names]

    dataset_sizes_mb = []
    for ds in dataset_names:
        # Check dataset directory size
        dataset_dir = pathlib.Path(__file__).parent / "../Datasets/GTFS" / ds
        dataset_size = 0
        for _, _, files in os.walk(dataset_dir):
            for file in files:
                fp = os.path.join(dataset_dir, file)
                # Skip symbolic links
                if not os.path.islink(fp):
                    dataset_size += os.path.getsize(fp)
        dataset_sizes_mb.append(dataset_size / 1024 / 1024)

    # Data sorting
    zipped_data = zip(dataset_names, postgres_times, neo4j_times, dataset_sizes_mb)
    sorted_data = sorted(zipped_data, key=lambda x: x[3])
    dataset_names, postgres_times, neo4j_times, dataset_sizes_mb = zip(*sorted_data)

    # Create dataset labels (name + size)
    dataset_labels = [f"{ds}\n({size:.0f} MB)" for ds, size in zip(dataset_names, dataset_sizes_mb)]

    # Speedup calculation
    speedups = [n_time / p_time for p_time, n_time in zip(postgres_times, neo4j_times)]
    average_speedup = np.mean(speedups)

    # Plotting
    x = np.arange(len(dataset_names))  # The horizontal locations for the groups
    bar_width = 0.35

    fig, ax = plt.subplots(figsize=(14, 10))

    # Create the vertical bars
    pg_bars = ax.bar(x - bar_width / 2, postgres_times, bar_width, label='PostgreSQL', color=args.postgres_color)
    neo4j_bars = ax.bar(x + bar_width / 2, neo4j_times, bar_width, label='Neo4J', color=args.neo4j_color)

    # Chart formatting
    ax.set_ylabel('Import Time (seconds, log scale)', fontsize=12, labelpad=10)
    ax.set_xlabel('Data Source', fontsize=12, labelpad=10)
    ax.set_title(
        'Database Import Performance: PostgreSQL vs. Neo4J\n'
        f'(PostgreSQL is {average_speedup:.2f}x faster on average)',
        fontsize=16,
        pad=20
    )
    ax.set_yscale('log')    # Set the y-axis to a logarithmic scale

    ax.set_xticks(x)
    ax.set_xticklabels(dataset_labels, rotation=0, ha='center')  # Set ticks and rotate for readability
    ax.legend(fontsize=12)

    # Add data labels to the end of each bar for clarity
    ax.bar_label(pg_bars, padding=3, fmt='%.0f s')
    ax.bar_label(neo4j_bars, padding=3, fmt='%.0f s')

    ax.grid(axis='x', linestyle='--', alpha=0.7)

    plt.tight_layout()
    plt.savefig(args.output)
    print(f"Bar chart saved to {args.output}")

if __name__ == "__main__":
    main()