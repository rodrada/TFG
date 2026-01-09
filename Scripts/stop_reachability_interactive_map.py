#!/usr/bin/env python3

import argparse
import sys
import math
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
    import branca.element
except ImportError as e:
    print(f"Error: A required library is not installed. Please install it using 'pip install folium'. Missing: {e.name}", file=sys.stderr)
    sys.exit(1)


def get_interval_and_color(td):
    """
    Categorizes a timedelta into a 5-minute interval up to 60 minutes
    and assigns a corresponding color from a green-to-red gradient.

    Args:
        td (pd.Timedelta): The travel time.

    Returns:
        tuple: A tuple containing the interval label (str) and the color (str),
               or (None, None) if outside the 0-60 minute range.
    """
    if pd.isnull(td) or td.total_seconds() <= 0:
        return None, None

    total_minutes = td.total_seconds() / 60.0

    if total_minutes > 60:
        return None, None

    # Gradient from Green -> Yellow -> Red -> Purple for 12 intervals (60min / 5min)
    colors = [
        '#2ca02c', '#59b32a', '#86c52a', '#b3d729', '#e0e928', '#ffff26',
        '#ffd724', '#ffae22', '#ff8620', '#ff5d1e', '#ff341c', '#ff001a'
    ]

    # Determine the interval bin (1-12)
    interval_idx = math.ceil(total_minutes / 5) - 1
    interval_idx = max(0, interval_idx) # Ensure index is not negative

    upper_bound = (interval_idx + 1) * 5
    lower_bound = upper_bound - 5
    label = f"{lower_bound:02d}-{upper_bound:02d} minutes"

    return label, colors[interval_idx]


def main():
    """
    Main function to run the earliest_arrivals query and plot the results
    on an interactive map.
    """
    parser = argparse.ArgumentParser(
        description="Generates an interactive map of reachable public transit stops."
    )
    parser.add_argument("--origin-stop-id", type=str, required=True, help="The ID of the starting stop.")
    parser.add_argument("--date", type=str, required=True, help="The departure date in YYYY-MM-DD format.")
    parser.add_argument("--time", type=str, required=True, help="The departure time in HH:MI:SS format.")
    parser.add_argument("--output", type=str, default="reachability_map.html", help="Path to save the output map HTML file.")
    parser.add_argument("--sample-percentage", type=int, default=100, choices=range(1, 101), metavar="[1-100]", help="Randomly downsample stops to the given percentage (e.g., 50 for 50%%). Default: 100.")
    args = parser.parse_args()

    # Convert user's departure time string to a timedelta for calculation
    departure_timedelta = pd.to_timedelta(args.time)

    # Fetch data using the query runner
    reachable_stops_data = []
    print(f"Fetching reachable stops from '{args.origin_stop_id}' on {args.date} at {args.time}...")
    with pg_query_runner() as runner:
        params = (args.origin_stop_id, args.date, args.time)
        reachable_stops_data = runner(QUERIES['postgres']['earliest_arrivals'], params)

    if not reachable_stops_data:
        print("No data was returned from the query to plot.")
        return

    # Prepare the data for plotting
    df = pd.DataFrame(reachable_stops_data)

    # Convert arrival times to Timedelta objects (representing time from midnight)
    df['earliest_arrival_time'] = pd.to_timedelta(df['earliest_arrival_time'])

    # Calculate the actual travel duration by subtracting the departure time
    df['travel_duration'] = df['earliest_arrival_time'] - departure_timedelta

    # Convert the WKB geometry into a GeoSeries for GeoPandas
    geometry = gpd.GeoSeries([wkb.loads(geom, hex=True) for geom in df['stop_geom']], crs="EPSG:4326")
    gdf = gpd.GeoDataFrame(df, geometry=geometry)

    print(f"Successfully loaded {len(gdf)} reachable stops.")

    # The origin stop has a travel duration of zero. Find and store it.
    origin_stop_row = gdf[gdf['travel_duration'].dt.total_seconds() == 0]
    if origin_stop_row.empty:
        print("Error: Origin stop not found in the results. Cannot proceed.", file=sys.stderr)
        return
    
    origin_stop = origin_stop_row.iloc[0]

    # Create a lookup map of {stop_id: (lat, lon)} for all stops.
    # This is needed to efficiently draw the path segments later.
    stop_coords = {row['stop_id']: (row.geometry.y, row.geometry.x) for _, row in gdf.iterrows()}

    # Assign interval labels and colors to each stop
    gdf[['interval', 'color']] = gdf['travel_duration'].apply(
        lambda x: pd.Series(get_interval_and_color(x))
    )
    # Drop stops that are outside our 60-minute visualization window
    gdf.dropna(subset=['interval', 'color'], inplace=True)

    if gdf.empty:
        print("No reachable stops were found within the 60-minute window.")
        return

    # Downsample the data if requested
    if args.sample_percentage < 100:
        original_count = len(gdf)
        # Use the sample() method to get a random fraction of the data
        # random_state ensures the "random" sample is the same every time for consistency
        gdf = gdf.sample(frac=(args.sample_percentage / 100.0), random_state=42)
        print(f"Downsampling to {args.sample_percentage}%. Displaying {len(gdf)} of {original_count} stops.")

    # Create an interactive map centered on the origin stop
    map_center = [origin_stop.geometry.y, origin_stop.geometry.x]
    m = folium.Map(location=map_center, zoom_start=13, tiles="CartoDB.Positron")

    # Add a special marker for the origin stop
    folium.Marker(
        location=map_center,
        popup=f"<b>Origin Stop:</b><br>{origin_stop['stop_id']}",
        icon=folium.Icon(color='darkblue', icon_color='white', icon='search', prefix='fa')
    ).add_to(m)

    # Create a feature group for each time interval for better layer management
    intervals = sorted(gdf['interval'].unique())
    feature_groups = {interval: folium.FeatureGroup(name=interval) for interval in intervals}

    # Add stops to the appropriate feature group
    for _, row in gdf.iterrows():
        interval_group = feature_groups[row['interval']]
        folium.CircleMarker(
            location=(row.geometry.y, row.geometry.x),
            radius=5,
            color=row['color'],
            fill=True,
            fill_color=row['color'],
            fill_opacity=0.7,
            tooltip=f"""
                <b>Stop ID:</b> {row['stop_id']}<br>
                <b>Time Taken:</b> {str(row['travel_duration'])}<br>
                <b>Trip:</b> {row['trip_id_used']}
            """
        ).add_to(interval_group)

    # Add feature groups to the map and add a layer control
    for interval in intervals:
        feature_groups[interval].add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)

    # Get a unique, sorted list of intervals and their corresponding colors
    legend_df = gdf[['interval', 'color']].drop_duplicates().sort_values(by='interval').set_index('interval')

    # Create the HTML for the legend
    legend_html = '''
     <div style="
         position: fixed;
         bottom: 50px;
         left: 50px;
         width: 180px;
         height: auto;
         border: 2px solid grey;
         z-index: 9999;
         font-size: 14px;
         background-color: white;
         opacity: .9;
         ">&nbsp;<b>Travel Time (minutes)</b><br>
     '''

    # Add a line for each interval and color
    for interval, row in legend_df.iterrows():
        color = row['color']
        # Note: Font Awesome is used for the circle icon (i)
        legend_html += f'&nbsp; <i class="fa fa-circle" style="color:{color}"></i>&nbsp; {interval}<br>'

    legend_html += '</div>'

    # Create a Branca element from the HTML and add it to the map
    legend = branca.element.Element(legend_html)
    m.get_root().html.add_child(legend)

    # Fit the map's view to the bounds of the reachable stops
    bounds = gdf.total_bounds
    m.fit_bounds([[bounds[1], bounds[0]], [bounds[3], bounds[2]]])

    # Save the map to an HTML file
    m.save(args.output)
    print(f"\nInteractive reachability map successfully saved to {args.output}")

if __name__ == "__main__":
    main()
