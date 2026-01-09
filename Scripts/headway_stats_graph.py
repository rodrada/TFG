#!/usr/bin/env python3

import argparse
import sys
from datetime import datetime
import numpy as np
import random  # Import the random module for sampling
from database import pg_query_runner, QUERIES
from colors import ACCENT_PRIMARY, ACCENT_SECONDARY, ACCENT_TERTIARY

try:
    import matplotlib.pyplot as plt
except ImportError as e:
    print(f"Error: A required library is not installed. Please install it using 'pip install matplotlib'. Missing: {e.name}", file=sys.stderr)
    sys.exit(1)

def main():
    """
    Main function to parse arguments, connect to PostgreSQL,
    run the query and plot the headway statistics.
    """
    parser = argparse.ArgumentParser(description="Connect to PostgreSQL, get headway stats for a given date, and produce a horizontal bar chart.")
    parser.add_argument("--date", required=True, help="Date for the query in YYYY-MM-DD format.")
    parser.add_argument("--min-color", type=str, default=ACCENT_TERTIARY, help="Color for the minimum headway segment.")
    parser.add_argument("--median-color", type=str, default=ACCENT_PRIMARY, help="Color for the median headway segment.")
    parser.add_argument("--max-color", type=str, default=ACCENT_SECONDARY, help="Color for the maximum headway segment.")
    parser.add_argument("--output", type=str, required=True, help="Path to save the bar chart.")
    parser.add_argument("--num-routes", type=int, default=15, help="Number of representative routes to display.")
    args = parser.parse_args()

    with pg_query_runner() as runner:
        results = runner(
            QUERIES['postgres']['headway_stats'],
            (args.date,)
        )
        
        # Combine the results into a single list to keep data aligned
        # No need to process into separate lists yet
        all_data = [
            {
                "route_name": item['route_name'],
                "min": item['min_headway'].total_seconds() / 60,
                "median": item['median_headway'].total_seconds() / 60,
                "max": item['max_headway'].total_seconds() / 60,
            }
            for item in results
        ]

        # Prefilter for routes with a good degree of variability
        interesting_data = [
            d for d in all_data if (d['max'] - d['min']) > 3
        ]

        # Take a random sample if the number of routes exceeds the desired amount
        if len(interesting_data) > args.num_routes:
            sampled_data = random.sample(interesting_data, args.num_routes)
        else:
            sampled_data = interesting_data

        # Sort the random sample by max headway
        sampled_data.sort(key=lambda item: item['max'], reverse=True)
        
        # Unpack the final data into lists for plotting
        route_names = [item['route_name'] for item in sampled_data]
        min_headways = np.array([item['min'] for item in sampled_data])
        median_headways = np.array([item['median'] for item in sampled_data])
        max_headways = np.array([item['max'] for item in sampled_data])

        fig, ax = plt.subplots(figsize=(12, 10))

        y = np.arange(len(route_names))  # The vertical locations for the routes

        # 1. Draw the horizontal lines connecting min and max
        ax.hlines(y=y, xmin=min_headways, xmax=max_headways, color='gray', alpha=0.7, linewidth=1, label='Headway range')

        # 2. Draw the scatter points for min, max, and average
        # We use the same color for min and max for a cleaner look
        ax.scatter(min_headways, y, color=args.min_color, s=60, label='Minimum headway (5th percentile)', zorder=3)
        ax.scatter(median_headways, y, color=args.median_color, s=100, label='Median headway (50th percentile)', zorder=3)
        ax.scatter(max_headways, y, color=args.max_color, s=60, label='Maximum headway (95th percentile)', zorder=3)

        # Standard formatting
        ax.set_ylabel("Route name")
        ax.set_xlabel("Headway (minutes)")

        date_obj = datetime.strptime(args.date, '%Y-%m-%d')
        formatted_date_string = date_obj.strftime('%A, %Y-%m-%d')
        
        ax.set_title(f"Headway statistics by route for {formatted_date_string} (Random sample)")
        
        ax.set_yticks(y)
        ax.set_yticklabels(route_names)
        ax.legend(bbox_to_anchor=(1, 0), loc='lower right')

        ax.invert_yaxis()  # Puts the route with the longest max headway at the top
        ax.grid(axis='x', linestyle='--', alpha=0.7)
        
        plt.tight_layout()
        plt.savefig(args.output)
        print(f"Bar chart saved to {args.output}")

if __name__ == "__main__":
    main()