#!/usr/bin/env python3

import argparse
import sys
from database import pg_query_runner, QUERIES

# Third-party library imports with user-friendly error messages
try:
    import pandas as pd
    import geopandas as gpd
    from shapely import wkb
except ImportError as e:
    print(f"Error: A required library is not installed. Please install it using 'pip install geopandas pandas shapely'. Missing: {e.name}", file=sys.stderr)
    sys.exit(1)

try:
    import folium
except ImportError as e:
    print(f"Error: A required library is not installed. Please install it using 'pip install folium'. Missing: {e.name}", file=sys.stderr)
    sys.exit(1)


def main():
    """
    Main function to run the routes_by_speed query and plot results
    on an interactive map.
    """
    parser = argparse.ArgumentParser(
        description="Generates an interactive map of public transit routes, showing trip count and average speed."
    )
    parser.add_argument("--output", type=str, default="route_speed_map.html", help="Path to save the output map HTML file.")
    args = parser.parse_args()

    # Fetch data using the query runner
    all_routes = []
    print("Fetching route data from the database...")
    with pg_query_runner() as runner:
        results = runner(QUERIES['postgres']['routes_by_speed'], ())
        for route in results:
            all_routes.append(route)

    if not all_routes:
        print("No data was returned from the query to plot.")
        return

    print(f"Successfully loaded {len(all_routes)} routes.")

    # Prepare the data for plotting
    # 1. Create a standard Pandas DataFrame first
    df = pd.DataFrame(all_routes)

    # The 'avg_speed_kmh' column is of type Decimal, which is not JSON serializable.
    df['avg_speed_kmh'] = df['avg_speed_kmh'].astype(float)

    # 2. Convert the WKB geometry into a GeoSeries for GeoPandas
    #    The `wkb.loads` function from shapely decodes the binary format.
    geometry = gpd.GeoSeries([wkb.loads(geom) for geom in df['route_geom']], crs="EPSG:4326")

    # 3. Create the final GeoDataFrame
    gdf = gpd.GeoDataFrame(df, geometry=geometry)


    # Create an interactive map
    if gdf.empty:
        print("GeoDataFrame is empty. Cannot create a map.")
        return

    # Center the map on the approximate center of all routes
    map_center = [gdf.union_all().centroid.y, gdf.union_all().centroid.x]
    m = folium.Map(location=map_center, zoom_start=12, tiles="CartoDB positron")

    # Add routes to the map
    # Create a GeoJson layer with tooltips
    geojson_layer = folium.GeoJson(
        gdf,
        # Style the lines on the map
        style_function=lambda feature: {
            'color': f"#{feature['properties']['route_color'] or '228be6'}",
            'weight': 3,
            'opacity': 0.75
        },
        # Create a tooltip that appears on hover
        tooltip=folium.GeoJsonTooltip(
            fields=['route_name', 'trip_count', 'avg_speed_kmh'],
            aliases=['Route:', 'Total Trips:', 'Avg. Speed (km/h):'],
            sticky=True  # The tooltip will follow the mouse
        )
    ).add_to(m)

    # Fit the map's view to the bounds of the routes
    m.fit_bounds(geojson_layer.get_bounds())

    # Save the map to an HTML file
    m.save(args.output)
    print(f"\nInteractive map successfully saved to {args.output}")


if __name__ == "__main__":
    main()
