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

def format_timedelta_hms(td):
    """Formats a pandas Timedelta object into an HH:MM:SS string."""
    if pd.isna(td):
        return ""
    total_seconds = int(td.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

def main():
    """
    Main function to run the shortest_path query and plot the resulting
    path on an interactive map.
    """
    parser = argparse.ArgumentParser(
        description="Generates an interactive map of the shortest path between two public transit stops."
    )
    parser.add_argument("--origin-stop-id", type=str, required=True, help="The ID of the starting stop.")
    parser.add_argument("--destination-stop-id", type=str, required=True, help="The ID of the destination stop.")
    parser.add_argument("--date", type=str, required=True, help="The departure date in YYYY-MM-DD format.")
    parser.add_argument("--time", type=str, required=True, help="The departure time in HH:MI:SS format.")
    parser.add_argument("--output", type=str, default="shortest_path_map.html", help="Path to save the output map HTML file.")
    args = parser.parse_args()

    # Fetch data using the query runner
    path_data = []
    print(f"Fetching shortest path from '{args.origin_stop_id}' to '{args.destination_stop_id}' on {args.date} at {args.time}...")
    with pg_query_runner() as runner:
        params = (args.origin_stop_id, args.destination_stop_id, args.date, args.time)
        path_data = runner(QUERIES['postgres']['shortest_path'], params)

    if not path_data:
        print("No path was found between the specified origin and destination.", file=sys.stderr)
        return

    # Prepare the data for plotting
    df = pd.DataFrame(path_data)
    df['arrival_time'] = pd.to_timedelta(df['arrival_time'])

    # Convert the WKB geometry into a GeoSeries for GeoPandas
    geometry = gpd.GeoSeries([wkb.loads(geom, hex=True) for geom in df['stop_geom']], crs="EPSG:4326")
    gdf = gpd.GeoDataFrame(df, geometry=geometry)

    # Define a color palette for up to 5 unique transit trips
    # Get all unique string values from the trip_id column.
    unique_trip_ids = gdf['trip_id'].unique()

    # Filter this list to exclude the special labels for walking and transfers.
    transit_trips = [trip for trip in unique_trip_ids if trip and trip not in ['Walk', 'Transfer']]

    # A list of 5 distinct colors for transit lines.
    trip_colors = ['#e7298a', '#1a9e77', '#984ea3', '#7570b3', '#e41a1c']

    # Create a mapping from each transit trip_id to a color.
    trip_color_map = {
        trip_id: trip_colors[i % len(trip_colors)]
        for i, trip_id in enumerate(transit_trips)
    }

    print(f"Successfully loaded a path with {len(gdf)} stops.")

    # Extract the origin and destination stops for special markers
    origin_stop = gdf.iloc[0]
    destination_stop = gdf.iloc[-1]

    # Create an interactive map centered on the origin stop
    map_center = [origin_stop.geometry.y, origin_stop.geometry.x]
    m = folium.Map(location=map_center, zoom_start=13, tiles="CartoDB.Positron")

    # 1. Draw path segments with appropriate colors
    # We loop through the path from the first stop to the second-to-last stop.
    for i in range(len(gdf) - 1):
        start_stop = gdf.iloc[i]
        end_stop = gdf.iloc[i+1]
        travel_mode = end_stop['trip_id']
    
        # Determine the color and style for this segment
        if travel_mode == 'Transfer':
            color = '#d95f02'       # Orange
            tooltip = 'Transfer'
            line_style = {'dashArray': '10, 5', 'weight': 5}
        
        elif travel_mode == 'Walk':
            color = '#3388ff'       # Deep blue
            tooltip = 'Walking'
            line_style = {'dashArray': '5, 5', 'weight': 4}
        else:
            color = trip_color_map.get(travel_mode, '#808080')
            tooltip = f"Trip: {travel_mode}"
            line_style = {'weight': 6}

        # Draw the PolyLine for this specific segment
        folium.PolyLine(
            locations=[
                (start_stop.geometry.y, start_stop.geometry.x),
                (end_stop.geometry.y, end_stop.geometry.x)
            ],
            color=color,
            tooltip=tooltip,
            **line_style
        ).add_to(m)


    # 2. Add Google Maps-style markers for start, end, and stops

    # Start marker: A filled blue circle with a white inner dot, mimicking Google Maps.
    folium.CircleMarker(
        location=[origin_stop.geometry.y, origin_stop.geometry.x],
        radius=10,
        color='#1c52c7', # Border color
        weight=3,
        fill=True,
        fill_color='white',
        fill_opacity=1,
        popup=folium.Popup(f"<b>Origin:</b><br>{origin_stop['stop_name']}<br><b>Departure:</b><br>{args.time}", show=True)
    ).add_to(m)
    folium.CircleMarker(
        location=[origin_stop.geometry.y, origin_stop.geometry.x],
        radius=4,
        color='#1c52c7', # Inner dot color
        fill=True,
        fill_color='#1c52c7',
        fill_opacity=1
    ).add_to(m)

    # Destination marker: A standard red "pin" icon.
    folium.Marker(
        location=[destination_stop.geometry.y, destination_stop.geometry.x],
        popup=folium.Popup(f"<b>Destination:</b><br>{destination_stop['stop_name']}<br><b>Arrival:</b><br>{format_timedelta_hms(destination_stop['arrival_time'])}", show=True),
        icon=folium.Icon(color='red', icon_color='white', icon='info-circle', prefix='fa')
    ).add_to(m)

    # 3. Add intermediate stops markers.
    for i, stop in gdf.iloc[1:-1].iterrows():
        folium.CircleMarker(
            location=(stop.geometry.y, stop.geometry.x),
            radius=4,
            color='white',
            weight=2,
            fill=True,
            fill_color='black',
            fill_opacity=1.0,
            tooltip=f"""
                <b>Stop:</b> {stop['stop_name']}<br>
                <b>Arrival:</b> {format_timedelta_hms(stop['arrival_time'])}<br>
                <b>Arrived via:</b> {stop['trip_id']}
            """
        ).add_to(m)

    # Fit the map's view to the bounds of the entire path
    bounds = gdf.total_bounds
    m.fit_bounds([[bounds[1], bounds[0]], [bounds[3], bounds[2]]])

    # Save the map to an HTML file
    m.save(args.output)
    print(f"\nInteractive shortest path map successfully saved to {args.output}")

if __name__ == "__main__":
    main()
