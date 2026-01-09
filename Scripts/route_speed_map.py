#!/usr/bin/env python3

import argparse
import sys
from database import pg_query_runner, QUERIES, focused_bounds

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
    Main function to query for route speeds and generate a map.
    """
    parser = argparse.ArgumentParser(
        description="Generates a map of public transit routes colored by average speed."
    )
    parser.add_argument("--output", type=str, required=True, help="Path to save the output map image (e.g., 'route_speed_map.png').")
    args = parser.parse_args()

    # Query the database
    print("Querying the database for routes and average speeds...")
    with pg_query_runner() as runner:
        results = runner(QUERIES['postgres']['routes_by_speed'], ())

    if not results:
        print("No route data was returned from the database.")
        return

    print(f"Found {len(results)} routes to plot.")

    # Prepare data for plotting
    # The geometry from PostGIS is often returned as a WKB hex string or bytes
    for row in results:
        # The 'route_geom' is loaded into a Shapely geometry object
        row['route_geom'] = wkb.loads(row['route_geom'], hex=True)

    # Create a GeoDataFrame from the query results
    gdf = gpd.GeoDataFrame(
        results,
        geometry='route_geom',
        crs="EPSG:4326"  # Assuming GTFS data is in WGS84
    )
    
    # Convert speed to numeric for plotting
    gdf['avg_speed_kmh'] = gdf['avg_speed_kmh'].astype(float)
    
    # Project to Web Mercator (EPSG:3857) for compatibility with web map tiles
    gdf_plot = gdf.to_crs(epsg=3857)

    # Plot the data
    fig, ax = plt.subplots(figsize=(15, 15))

    # Define the colormap
    cmap = 'viridis'
    vmin = gdf_plot['avg_speed_kmh'].min()
    vmax = gdf_plot['avg_speed_kmh'].max()

    # Plot the routes, coloring them by average speed
    gdf_plot.plot(
        ax=ax,
        column='avg_speed_kmh',
        cmap=cmap,
        linewidth=2,
        legend=False, # We will create a custom legend
        zorder=2
    )

    print("Calculating focused map bounds to ignore outliers...")
    min_x, max_x, min_y, max_y = focused_bounds(gdf_plot)
    ax.set_xlim(min_x, max_x)
    ax.set_ylim(min_y, max_y)

    # Add map background
    cx.add_basemap(ax, source=cx.providers.CartoDB.Positron, zorder=1)

    # Finalize the plot with title and legend
    ax.set_title("Average Speed of Public Transit Routes", fontsize=20)
    ax.set_axis_off()

    # Create a colorbar as a legend
    norm = Normalize(vmin=vmin, vmax=vmax)
    sm = ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])    # We have to set a dummy array for this to work
    cbar = fig.colorbar(sm, ax=ax, orientation='horizontal', pad=0.02, shrink=0.5)
    cbar.set_label("Average Speed (km/h)", fontsize=12)

    plt.tight_layout()
    plt.savefig(args.output, dpi=150, bbox_inches='tight')
    print(f"\nMap successfully saved to {args.output}")


if __name__ == "__main__":
    main()
