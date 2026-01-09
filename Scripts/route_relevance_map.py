#!/usr/bin/env python3

import argparse
import sys
from datetime import datetime
from database import pg_query_runner, QUERIES, focused_bounds

import cmocean
import pandas as pd

# Library imports with user-friendly error messages
try:
    import matplotlib.pyplot as plt
    from matplotlib.colors import Normalize
    from matplotlib.cm import ScalarMappable
except ImportError as e:
    print(f"Error: A required library is not installed. Please install it using 'pip install matplotlib'. Missing: {e.name}", file=sys.stderr)
    sys.exit(1)

try:
    import geopandas as gpd
    from shapely import wkb
except ImportError as e:
    print(f"Error: A required library is not installed. Please install it using 'pip install geopandas'. Missing: {e.name}", file=sys.stderr)
    sys.exit(1)

try:
    import contextily as cx
except ImportError as e:
    print(f"Error: A required library is not installed. Please install it using 'pip install contextily'. Missing: {e.name}", file=sys.stderr)
    sys.exit(1)

def main():
    """
    Main function to query for route relevance and generate a map.
    """
    parser = argparse.ArgumentParser(
        description="Generates a map of currently active routes, colored by relevance (number of active trips)."
    )
    parser.add_argument("--date", type=str, required=True, help="The date to query for, in YYYY-MM-DD format.")
    parser.add_argument("--time", type=str, required=True, help="The time to query for, in HH:MM:SS format.")
    parser.add_argument("--output", type=str, required=True, help="Path to save the output map image (e.g., 'relevance_map.png').")
    args = parser.parse_args()

    # Validate inputs and query the database
    try:
        # Validate date and time formats before querying
        curr_date = datetime.strptime(args.date, '%Y-%m-%d').date()
        curr_time = datetime.strptime(args.time, '%H:%M:%S').time()
    except ValueError as e:
        print(f"Error: Invalid date or time format. {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Querying database for route relevance on {curr_date} at {curr_time}...")
    with pg_query_runner() as runner:
        results = runner(
            QUERIES['postgres']['routes_by_relevance'],
            (curr_date, curr_time)
        )

    if not results:
        print(f"No active routes were found for the specified date and time.")
        return

    print(f"Found {len(results)} active routes to plot.")

    # Prepare data for plotting
    # Load the PostGIS geometry into Shapely objects
    for row in results:
        if row['route_geom']:
            row['route_geom'] = wkb.loads(row['route_geom'], hex=True)

    # Create a GeoDataFrame
    gdf = gpd.GeoDataFrame(
        results,
        geometry='route_geom',
        crs="EPSG:4326"
    )

    # Drop routes that might not have a geometry
    gdf = gdf.dropna(subset=['route_geom'])

    # Project to Web Mercator for compatibility with web map tiles
    gdf_plot = gdf.to_crs(epsg=3857)
    gdf_plot['active_trip_count'] = pd.to_numeric(gdf_plot['active_trip_count'])

    # Dynamically calculate line widths based on trip count.
    # This maps the number of trips to a visual range (e.g., 0.5 to 4 points).
    min_width, max_width = 0.5, 4
    min_trips = gdf_plot['active_trip_count'].min()
    max_trips = gdf_plot['active_trip_count'].max()

    # Avoid division by zero if all routes have the same count
    if max_trips == min_trips:
        gdf_plot['linewidth'] = (min_width + max_width) / 2
    else:
        gdf_plot['linewidth'] = gdf_plot['active_trip_count'].apply(
            lambda count: min_width + (count - min_trips) * (max_width - min_width) / (max_trips - min_trips)
        )

    # Plot the data
    fig, ax = plt.subplots(figsize=(15, 15))

    # Define the colormap and normalization based on active trip count
    cmap = cmocean.cm.deep_r
    vmin = gdf_plot['active_trip_count'].min()
    vmax = gdf_plot['active_trip_count'].max()

    # Loop through the sorted GeoDataFrame to plot each line individually
    # This gives us control over color, linewidth, and transparency for each route.
    for _, row in gdf_plot.iterrows():
        gpd.GeoSeries(row['route_geom']).plot(
            ax=ax,
            color=cmap((row['active_trip_count'] - vmin) / (vmax - vmin) if (vmax - vmin) > 0 else 0.5),
            linewidth=row['linewidth'],
            alpha=0.75,  # Use transparency to handle overlapping routes
            zorder=2
        )

    # Calculate and apply focused bounds to zoom in on the core network
    if not gdf_plot.empty:
        print("Calculating focused map bounds to ignore outliers...")
        min_x, max_x, min_y, max_y = focused_bounds(gdf_plot)
        ax.set_xlim(min_x, max_x)
        ax.set_ylim(min_y, max_y)

    # Add the basemap
    cx.add_basemap(ax, source=cx.providers.CartoDB.DarkMatter, zorder=1)

    # Finalize the plot with title and legend
    ax.set_title(f"Transit Route Relevance at {curr_time} on {curr_date}", fontsize=20, color='white')
    ax.set_axis_off()

    # Create a colorbar legend that is styled for a dark background
    norm = Normalize(vmin=vmin, vmax=vmax)
    sm = ScalarMappable(cmap=cmap, norm=norm)
    cbar = fig.colorbar(sm, ax=ax, orientation='horizontal', pad=0.02, shrink=0.6)
    cbar.set_label("Number of Active Trips on Route", fontsize=12, color='white')
    cbar.ax.xaxis.set_tick_params(color='white')
    plt.setp(plt.getp(cbar.ax.axes, 'xticklabels'), color='white')

    # Set the figure background color and save with higher quality
    fig.patch.set_facecolor('black')
    plt.tight_layout()
    plt.savefig(args.output, dpi=200, bbox_inches='tight', facecolor='black')
    print(f"\nMap successfully saved to {args.output}")

if __name__ == "__main__":
    main()
