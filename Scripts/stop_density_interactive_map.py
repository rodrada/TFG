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
    Main function to run the stop_density_heatmap query and plot results
    on an interactive map.
    """
    parser = argparse.ArgumentParser(
        description="Generates an interactive heatmap of public transit stop density."
    )
    parser.add_argument("--grid_size", type=int, required=True, help="The size of the hexagon grid cells in meters.")
    parser.add_argument("--output", type=str, default="stop_density_heatmap.html", help="Path to save the output map HTML file.")
    args = parser.parse_args()

    # Fetch data using the query runner
    heatmap_data = []
    print(f"Fetching stop density data using a {args.grid_size}m grid...")
    with pg_query_runner() as runner:
        heatmap_data = runner(QUERIES['postgres']['stop_density_heatmap'], (args.grid_size,))

    if not heatmap_data:
        print("No data was returned from the query to plot.")
        return

    # Prepare the data for plotting
    df = pd.DataFrame(heatmap_data)

    # Convert the WKB geometry into a GeoSeries for GeoPandas
    geometry = gpd.GeoSeries([wkb.loads(geom) for geom in df['hexagon_geom']], crs="EPSG:4326")

    gdf = gpd.GeoDataFrame(df, geometry=geometry)

    # Filter out hexagons with no stops. They are returned by the query's
    # LEFT JOIN but are not useful for a heatmap visualization.
    gdf = gdf[gdf['stop_count'] > 0].copy()

    if gdf.empty:
        print("No stops were found. Cannot generate a map.")
        return

    print(f"Successfully loaded {len(gdf)} hexagons with stops.")

    # Create an interactive map
    map_center = [gdf.union_all().centroid.y, gdf.union_all().centroid.x]
    m = folium.Map(location=map_center, zoom_start=12, tiles="CartoDB.Positron")

    # Create and add the Choropleth (Heatmap) Layer

    # First, reset the index to turn it into a column that Folium can find.
    # This creates a new column, typically named 'index'.
    gdf.reset_index(inplace=True)

    folium.Choropleth(
        geo_data=gdf,
        name='Stop Density',
        data=gdf,
        columns=['index', 'stop_count'], # Use index for key, 'stop_count' for value
        key_on='feature.id',                    # Link to the feature's ID (which is the index)
        fill_color='YlOrRd',
        fill_opacity=0.3,
        line_opacity=0.1,
        legend_name='Number of Stops per Hexagon',
        highlight=True, # Adds a nice highlight effect on hover
    ).add_to(m)

    # Add a separate layer for tooltips for a better user experience
    tooltip = folium.GeoJsonTooltip(
        fields=['stop_count'],
        aliases=['Stops in this area:'],
        sticky=True
    )
    folium.GeoJson(
        gdf,
        style_function=lambda x: {'fillOpacity': 0, 'weight': 0}, # Invisible layer
        tooltip=tooltip
    ).add_to(m)

    # Fit the map's view to the bounds of the hexagons
    m.fit_bounds(gdf.total_bounds.tolist())

    # Save the map to an HTML file
    m.save(args.output)
    print(f"\nInteractive heatmap successfully saved to {args.output}")

if __name__ == "__main__":
    main()
