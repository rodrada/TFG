#!/usr/bin/env python3

import argparse
import sys
from database import pg_query_runner, QUERIES

# Third-party library imports with user-friendly error messages
try:
    import pandas as pd
    import geopandas as gpd
    from shapely import wkb
    from shapely.geometry import LineString
except ImportError as e:
    print(f"Error: A required library is not installed. Please install it using 'pip install geopandas pandas shapely'. Missing: {e.name}", file=sys.stderr)
    sys.exit(1)

try:
    import matplotlib.colors as colors
    from matplotlib.colors import LinearSegmentedColormap, TwoSlopeNorm
except ImportError as e:
    print(f"Error: A required library is not installed. Please install it using 'pip install matplotlib'. Missing: {e.name}", file=sys.stderr)
    sys.exit(1)

try:
    import folium
    from folium.features import DivIcon
    from folium import FeatureGroup, LayerControl
except ImportError as e:
    print(f"Error: A required library is not installed. Please install it using 'pip install folium'. Missing: {e.name}", file=sys.stderr)
    sys.exit(1)

def main():
    """
    Main function to run the route_straightness query and plot results
    on an interactive map.
    """
    parser = argparse.ArgumentParser(
        description="Generates an interactive map comparing actual route paths to straight lines."
    )
    parser.add_argument("--num_routes", type=int, default=10, help="Number of random routes to display. Default is 10.")
    parser.add_argument("--output", type=str, default="route_straightness_map.html", help="Path to save the output map HTML file.")
    args = parser.parse_args()

    # Fetch data from the database.
    all_routes = []
    print("Fetching route straightness data from the database...")
    with pg_query_runner() as runner:
        all_routes = runner(QUERIES['postgres']['route_straightness'], ())

    if not all_routes:
        print("No data was returned from the query to plot.")
        return

    print(f"Successfully loaded {len(all_routes)} routes.")

    # Prepare the data for plotting.
    df = pd.DataFrame(all_routes)

    # FIX: Convert Decimal types to float for JSON compatibility
    for col in ['straightness_index', 'route_length_km', 'direct_distance_km']:
        df[col] = df[col].astype(float)

    # Convert the WKB geometry into a GeoSeries for GeoPandas
    geometry = gpd.GeoSeries([wkb.loads(geom) for geom in df['route_geom']], crs="EPSG:4326")
    gdf = gpd.GeoDataFrame(df, geometry=geometry)

    # Sample the data and generate unique colors
    if 0 < args.num_routes < len(gdf):
        print(f"Randomly selecting {args.num_routes} out of {len(gdf)} routes to display.")
        gdf = gdf.sample(n=args.num_routes)

    # Define the "sweet spot" center for the straightness index.
    SWEET_SPOT_CENTER = 0.75

    # 1. Create a custom colormap from a list of colors.
    custom_cmap = LinearSegmentedColormap.from_list(
        "custom_rgy", ["#d73027", "#1a9850", "#fee08b"]  # Red, Green, Yellow
    )

    # 2. Create a special "two-slope" normalizer.
    # This forces the colormap to pivot around our sweet spot value.
    # Values from min->center will span Red->Green.
    # Values from center->max will span Green->Yellow.
    norm = TwoSlopeNorm(
        vcenter=SWEET_SPOT_CENTER,
        vmin=gdf['straightness_index'].min(),
        vmax=1.0
    )

    # 3. Apply the custom colormap and normalizer to the data.
    gdf['color'] = gdf['straightness_index'].apply(lambda x: colors.to_hex(custom_cmap(norm(x))))

    # Create an interactive map.
    map_center = [gdf.union_all().centroid.y, gdf.union_all().centroid.x]
    m = folium.Map(location=map_center, zoom_start=12, tiles="CartoDB.Positron")

    # Iterate through each route to add custom layers
    # Iterate through each route to add custom layers into toggleable groups
    for _, route in gdf.iterrows():
        # Create a FeatureGroup for this specific route.
        # The name is what will appear in the layer control panel.
        route_group = FeatureGroup(name=f"Route: {route['route_name']}", show=True)

        route_color = route['color']
        tooltip_text = (
            f"<b>Route:</b> {route['route_name']}<br>"
            f"<b>Straightness Index:</b> {route['straightness_index']:.2f}<br>"
            f"<b>Route Length:</b> {route['route_length_km']:.2f} km<br>"
            f"<b>Direct Distance:</b> {route['direct_distance_km']:.2f} km"
        )

        # 1. Add the ACTUAL route path to the group
        folium.GeoJson(
            route.geometry,
            style_function=lambda x, color=route_color: {'color': color, 'weight': 4, 'opacity': 0.8},
            tooltip=tooltip_text
        ).add_to(route_group)

        # 2. Create and add the STRAIGHT line path to the group
        start_point = route.geometry.coords[0]
        end_point = route.geometry.coords[-1]
        straight_line = LineString([start_point, end_point])
        folium.GeoJson(
            straight_line,
            style_function=lambda x, color=route_color: {'color': color, 'weight': 2, 'dashArray': '5, 10'}
        ).add_to(route_group)

        # 3. Add the straightness index value as a text label to the group
        midpoint = straight_line.centroid
        folium.Marker(
            location=[midpoint.y, midpoint.x],
            icon=DivIcon(
                icon_size=(150, 36),
                icon_anchor=(75, 18),
                html=(
                    f'<div style="font-size: 10pt; font-weight: bold; color: {route_color}; border: 1px solid {route_color}; '
                    f'background-color: rgba(255,255,255,0.9); border-radius: 5px; '
                    f'padding: 2px 5px; text-align: center;">'
                    f'{route["straightness_index"]:.2f}'
                    f'</div>'
                )
            )
        ).add_to(route_group)

        # Finally, add the entire group of layers to the main map
        route_group.add_to(m)

    # Add a layer control panel to the map to toggle routes on/off.
    LayerControl().add_to(m)

    # Fit the map's view to the bounds of all routes.
    m.fit_bounds(gdf.total_bounds.tolist())

    # Save the map to an HTML file.
    m.save(args.output)
    print(f"\nInteractive map successfully saved to {args.output}")

if __name__ == "__main__":
    main()
