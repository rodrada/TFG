#!/usr/bin/env python3

import argparse
import sys
from database import pg_query_runner, QUERIES
from colors import ACCENT_PRIMARY

import random
import numpy as np

# Library imports with user-friendly error messages
try:
    import geopandas as gpd
    from shapely.geometry import Point
except ImportError as e:
    print(f"Error: A required library is not installed. Please install it using 'pip install geopandas'. Missing: {e.name}", file=sys.stderr)
    sys.exit(1)

try:
    import folium
except ImportError as e:
    print(f"Error: A required library is not installed. Please install it using 'pip install folium'. Missing: {e.name}", file=sys.stderr)
    sys.exit(1)

try:
    from haversine import haversine, Unit
except ImportError as e:
    print(f"Error: A required library is not installed. Please install it using 'pip install haversine'. Missing: {e.name}", file=sys.stderr)
    sys.exit(1)

def generate_dispersed_points(center_lat, center_lon, radius_km, num_points, min_separation_km):
    """
    Generates random points that are guaranteed to be at least a minimum distance apart.
    """
    points = []
    # Earth's radius in meters for coordinate calculation
    earth_radius = 6378137
    # Safety break to prevent infinite loops if parameters are impossible
    max_attempts = num_points * 100

    for i in range(max_attempts):
        # Stop once we have enough points
        if len(points) >= num_points:
            break

        # 1. Generate a candidate point randomly within the circle
        r = (radius_km * 1000) * np.sqrt(random.random())
        theta = random.random() * 2 * np.pi
        dx = r * np.cos(theta)
        dy = r * np.sin(theta)

        candidate_lat = center_lat + (dy / earth_radius) * (180 / np.pi)
        candidate_lon = center_lon + (dx / earth_radius) * (180 / np.pi) / np.cos(center_lat * np.pi / 180)

        candidate_point_coords = (candidate_lat, candidate_lon)

        # 2. Check if the candidate is far enough from all accepted points
        is_valid = True
        for p in points:
            existing_point_coords = (p['lat'], p['lon'])
            distance = haversine(candidate_point_coords, existing_point_coords, unit=Unit.KILOMETERS)

            if distance < min_separation_km:
                is_valid = False
                break # It's too close, no need to check others

        # 3. If it's valid, accept it
        if is_valid:
            points.append({'lat': candidate_lat, 'lon': candidate_lon})

    if len(points) < num_points:
        print(f"Warning: Could only generate {len(points)}/{num_points} points with the given separation distance.", file=sys.stderr)

    return points

def main():
    """
    Main function to parse arguments, run queries, and plot results on a map.
    """
    parser = argparse.ArgumentParser(
        description="Generates random points and plots nearby stops on an interactive map."
    )
    parser.add_argument("--latitude", type=float, required=True, help="Latitude of the central point.")
    parser.add_argument("--longitude", type=float, required=True, help="Longitude of the central point.")
    parser.add_argument("--min-search-radius", type=int, default=200, help="Minimum search radius in meters (inclusive).")
    parser.add_argument("--max-search-radius", type=int, default=1000, help="Maximum search radius in meters (inclusive).")
    parser.add_argument("--generation-radius", type=float, default=2.5, help="Radius in kilometers for generating random points.")
    parser.add_argument("--num-points", type=int, default=5, help="Number of random points to generate.")
    parser.add_argument("--min-separation", type=float, default=1.0, help="Minimum separation between points in kilometers.")
    parser.add_argument("--output", type=str, default="interactive_map.html", help="Path to save the output map HTML file.")
    args = parser.parse_args()

    all_origins = []
    all_stops = {}

    # 1. Generate random points to search from
    random_origins = generate_dispersed_points(args.latitude, args.longitude, args.generation_radius, args.num_points, args.min_separation)

    with pg_query_runner() as runner:
        # 2. Loop through each generated point and find nearby stops
        for origin in random_origins:
            origin['radius'] = random.randint(args.min_search_radius, args.max_search_radius)
            print(f"Searching at ({origin['lat']:.4f}, {origin['lon']:.4f}) with radius {origin['radius']:.0f}m...")
            all_origins.append(origin)

            results = runner(
                QUERIES['postgres']['stops_within_distance'],
                (origin['lat'], origin['lon'], origin['radius'])
            )
            for stop in results:
                # 3. Store by id to keep the dataset small and avoid overplotting
                all_stops[stop['id']] = stop

            print(f"\nTotal unique stops found: {len(all_stops)}")

    # Create an interactive map with Folium
    if not all_origins:
        print("No data was generated to plot.")
        return

    # Create a map centered on the initial point provided by the user
    m = folium.Map(location=[args.latitude, args.longitude], tiles='CartoDB Voyager', zoom_start=13)

    # Add the found stops to the map
    stops_list = list(all_stops.values())
    for stop in stops_list:
        folium.CircleMarker(
            location=[stop['lat'], stop['lon']],
            # A radius of 4-5px is small but large enough to look defined.
            radius=4,
            tooltip=stop.get('name', f"Stop ID: {stop['id']}"),
            popup=f"Stop ID: {stop['id']}<br>Name: {stop.get('name', 'N/A')}",
            color='red',
            weight=1,
            fill=True,
            fill_color='red',
            fill_opacity=0.6,
        ).add_to(m)

    # Add the randomly generated origin points and their search radii to the map
    for origin in all_origins:
        # Add a marker for the search origin
        folium.Marker(
            location=[origin['lat'], origin['lon']],
            popup="Search Origin",
            icon=folium.Icon(color='darkblue', icon_color='white', icon='search', prefix='fa')
        ).add_to(m)

        # Add a circle to show the search radius
        folium.Circle(
            location=[origin['lat'], origin['lon']],
            radius=origin['radius'] + 5,    # Make sure no stops seem to be "out" of the circle.
            color=ACCENT_PRIMARY,
            fill=True,
            fill_opacity=0.2
        ).add_to(m)

    # Save the map to an HTML file
    m.save(args.output)
    print(f"\nInteractive map successfully saved to {args.output}")

if __name__ == "__main__":
    main()
