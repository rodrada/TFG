#!/usr/bin/env python3

import argparse
import json
import sys
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import ScalarFormatter

def main():
    """
    Main function to parse arguments, read the JSON data,
    and generate a comparative bar chart for query performance.
    """
    parser = argparse.ArgumentParser(description="Generate a bar chart comparing PostgreSQL and Neo4j query performance.")
    parser.add_argument("--input", required=True, help="Path to the input JSON file (query_performance.json).")
    parser.add_argument("--output", required=True, help="Path to save the generated bar chart image.")
    parser.add_argument("--postgres-color", type=str, default="#2f6792", help="Color for the PostgreSQL bars.")
    parser.add_argument("--neo4j-color", type=str, default="#e87722", help="Color for the Neo4j bars.")
    parser.add_argument("--linthresh", type=float, default=0.1, 
                        help="For symlog scale: The range [0, linthresh] will be plotted linearly. Values above this are log.")
    args = parser.parse_args()

    # Load the data from the JSON file
    try:
        with open(args.input, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: File {args.input} not found.", file=sys.stderr)
        sys.exit(1)

    # Process data: Calculate mean execution time for each metric
    metric_names = []
    postgres_means = []
    neo4j_means = []

    # Iterate through the dictionary keys
    for metric, times in data.items():
        metric_names.append(metric)
        # Calculate average time from the list of runs
        pg_mean = np.mean(times['pg'])
        neo_mean = np.mean(times['neo4j'])
        
        postgres_means.append(pg_mean)
        neo4j_means.append(neo_mean)

    # Sort data by Neo4J execution time.
    zipped_data = zip(metric_names, postgres_means, neo4j_means)
    sorted_data = sorted(zipped_data, key=lambda x: x[2]) 
    metric_names, postgres_means, neo4j_means = zip(*sorted_data)

    # Formatting labels: Replace underscores with spaces and Title Case
    plot_labels = [m.replace('_', ' ').title() for m in metric_names]

    # Plotting
    x = np.arange(len(metric_names))  # The horizontal locations for the groups
    bar_width = 0.35

    fig, ax = plt.subplots(figsize=(14, 10))

    # Create the vertical bars
    pg_bars = ax.bar(x - bar_width / 2, postgres_means, bar_width, label='PostgreSQL', color=args.postgres_color)
    neo4j_bars = ax.bar(x + bar_width / 2, neo4j_means, bar_width, label='Neo4J', color=args.neo4j_color)

    # Symlog allows a linear plot around zero, and log plot for larger numbers
    # linthresh=0.1 means: 0 to 0.1 is linear, 0.1 to infinity is log
    ax.set_yscale('symlog', linthresh=args.linthresh)
    scale_label = f"Symlog Scale (Linear < {args.linthresh}s)"
    
    # Force y-axis to look normal (avoiding 10^x labels for small numbers)
    formatter = ScalarFormatter()
    formatter.set_scientific(False)
    ax.yaxis.set_major_formatter(formatter)

    # Chart formatting
    ax.set_ylabel(f'Avg Execution Time (seconds, {scale_label})', fontsize=12, labelpad=10)
    ax.set_xlabel('Query Type', fontsize=12, labelpad=10)
    
    title_text = 'Query Performance: PostgreSQL vs. Neo4J'
        
    ax.set_title(title_text, fontsize=16, pad=20)
    
    ax.set_xticks(x)
    ax.set_xticklabels(plot_labels, rotation=45, ha='right')  # Rotate labels for readability
    ax.legend(fontsize=12)

    # Add data labels to the end of each bar
    # Using %.3f or %.4f because query times can be very small (ms)
    ax.bar_label(pg_bars, padding=3, fmt='%.3f s', fontsize=9)
    ax.bar_label(neo4j_bars, padding=3, fmt='%.3f s', fontsize=9)

    ax.grid(axis='x', linestyle='--', alpha=0.7)
    ax.grid(axis='y', linestyle=':', alpha=0.3, which='both') # Light horizontal grid for log scale

    plt.tight_layout()
    plt.savefig(args.output)
    print(f"Bar chart saved to {args.output}")

if __name__ == "__main__":
    main()
