#!/usr/bin/env python3

import argparse
import sys
from datetime import datetime, timedelta

from database import neo4j_query_runner, QUERIES
from colors import ACCENT_PRIMARY

try:
    import matplotlib.pyplot as plt
except ImportError as e:
    print(f"Error: A required library is not installed. Please install it using 'pip install matplotlib'. Missing: {e.name}", file=sys.stderr)
    sys.exit(1)

def main():
    """
    Main function to parse arguments, connect to Neo4J,
    run the query and plot the results.
    """
    parser = argparse.ArgumentParser(description="Connect to Neo4J, find trip start times for a given date, and produce a histogram with the output.")
    parser.add_argument("--date", required=True, help="Date for the query in YYYY-MM-DD format.")
    parser.add_argument("--bucket-size", type=int, required=True, help="Bucket size in minutes for the histogram.")
    parser.add_argument("--output", type=str, required=True, help="Path to save the histogram.")
    parser.add_argument("--color", default=ACCENT_PRIMARY, help="Override the default bar color for the histogram.")
    args = parser.parse_args()

    with neo4j_query_runner() as runner:
        results = runner(
            QUERIES['neo4j']['trip_start_time_distribution'],
            {"curr_date": args.date, "bucket_size_min": args.bucket_size}
        )

        time_buckets = [item['value']['time_bucket'][:5] for item in results]
        trip_counts = [item['value']['trip_count'] for item in results]

        fig, ax = plt.subplots(figsize=(15, 8))

        # Plot the bars with the left edge aligned to the x-coordinate
        # We use range(len(time_buckets)) as the x-coordinates (0, 1, 2, ...)
        ax.bar(range(len(time_buckets)), trip_counts, width=1.0, align='edge', color=args.color, edgecolor='black', linewidth=0.5)

        # Create the tick positions and labels for the bar EDGES
        # We need one more tick than there are bars.
        tick_positions = range(len(time_buckets) + 1)

        # The labels will be the start time of each bucket...
        tick_labels = time_buckets
        # ...plus one final label for the end time of the last bucket.
        if time_buckets:
            last_bucket_start = time_buckets[-1]

            # Step 1: Manually parse the time string by splitting it.
            parts = last_bucket_start.split(':')
            hours = int(parts[0])
            minutes = int(parts[1])

            # Step 2: Convert the entire time to a single unit (total minutes).
            total_minutes = (hours * 60) + minutes

            # Step 3: Add the bucket size.
            new_total_minutes = total_minutes + args.bucket_size

            # Step 4: Convert the new total minutes back into HH, MM, SS format.
            new_h = int(new_total_minutes // 60)
            new_m = int(new_total_minutes % 60)

            final_label = f"{new_h:02d}:{new_m:02d}"
            tick_labels.append(final_label)

        # Apply the new ticks and labels to the x-axis
        ax.set_xticks(tick_positions)
        ax.set_xticklabels(tick_labels, rotation=90)

        # Standard formatting
        ax.set_xlabel("Time bucket")
        ax.set_ylabel("Number of trips")

        # Parse the date string to a datetime object to find the day of the week
        date_obj = datetime.strptime(args.date, '%Y-%m-%d')
        # Format the date to include the full day name (e.g., "Monday, 2023-10-27")
        formatted_date_string = date_obj.strftime('%A, %Y-%m-%d')

        ax.set_title(f"Trip start time distribution on {formatted_date_string} (Bucket size: {args.bucket_size} minutes)")

        # Set the x-axis limits to perfectly match the bars
        ax.set_xlim(0, len(time_buckets))

        plt.tight_layout()
        plt.savefig(args.output)
        print(f"Histogram saved to {args.output}")

if __name__ == "__main__":
    main()
