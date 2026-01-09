#!/usr/bin/env python

# This script takes a directory containing a GTFS dataset missing
# optional fields and generates associated columns in the CSV files.

import sys;
import os;
import pandas as pd;

# Define columns for each GTFS file based on the specification
COLUMNS = {
    'agency.txt': ['agency_id', 'agency_name', 'agency_url', 'agency_timezone',
                   'agency_lang', 'agency_phone', 'agency_fare_url', 'agency_email'],
    'stops.txt': ['stop_id', 'stop_code', 'stop_name', 'tts_stop_name',
                  'stop_desc', 'stop_lat', 'stop_lon', 'zone_id',
                  'stop_url', 'location_type', 'parent_station', 'stop_timezone',
                  'wheelchair_boarding', 'level_id', 'platform_code'],
    'routes.txt': ['route_id', 'agency_id', 'route_short_name', 'route_long_name',
                   'route_desc', 'route_type', 'route_url', 'route_color',
                   'route_text_color', 'route_sort_order', 'continuous_pickup',
                   'continuous_drop_off', 'network_id'],
    'trips.txt': ['route_id', 'service_id', 'trip_id', 'trip_headsign',
                  'trip_short_name', 'direction_id', 'block_id', 'shape_id',
                  'wheelchair_accessible', 'bikes_allowed'],
    'stop_times.txt': ['trip_id', 'arrival_time', 'departure_time', 'stop_id',
                       'location_group_id', 'location_id', 'stop_sequence', 'stop_headsign',
                       'start_pickup_drop_off_window', 'end_pickup_drop_off_window', 'pickup_type', 'drop_off_type',
                       'continuous_pickup', 'continuous_drop_off', 'shape_dist_traveled', 'timepoint',
                       'pickup_booking_rule_id', 'drop_off_booking_rule_id'],
    'calendar.txt': ['service_id', 'monday', 'tuesday', 'wednesday',
                     'thursday', 'friday', 'saturday', 'sunday',
                     'start_date', 'end_date'],
    'calendar_dates.txt': ['service_id', 'date', 'exception_type'],
    'fare_attributes.txt': ['fare_id', 'price', 'currency_type', 'payment_method',
                            'transfers', 'agency_id', 'transfer_duration'],
    'fare_rules.txt': ['fare_id', 'route_id', 'origin_id', 'destination_id',
                       'contains_id'],
    'timeframes.txt': ['timeframe_group_id', 'start_time', 'end_time', 'service_id'],
    'fare_media.txt': ['fare_media_id', 'fare_media_name', 'fare_media_type'],
    'fare_products.txt': ['fare_product_id', 'fare_product_name', 'fare_media_id', 'amount',
                          'currency'],
    'fare_leg_rules.txt': ['leg_group_id', 'network_id', 'from_area_id', 'to_area_id',
                           'from_timeframe_group_id', 'to_timeframe_group_id', 'fare_product_id', 'rule_priority'],
    'fare_leg_join_rules.txt': ['from_network_id', 'to_network_id', 'from_stop_id', 'to_stop_id'],
    'fare_transfer_rules.txt': ['from_leg_group_id', 'to_leg_group_id', 'transfer_count', 'duration_limit',
                                'duration_limit_type', 'fare_transfer_type', 'fare_product_id'],
    'areas.txt': ['area_id', 'area_name'],
    'stop_areas.txt': ['area_id', 'stop_id'],
    'networks.txt': ['network_id', 'network_name'],
    'route_networks.txt': ['network_id', 'route_id'],
    'shapes.txt': ['shape_id', 'shape_pt_lat', 'shape_pt_lon', 'shape_pt_sequence',
                   'shape_dist_traveled'],
    'frequencies.txt': ['trip_id', 'start_time', 'end_time', 'headway_secs',
                        'exact_times'],
    'transfers.txt': ['from_stop_id', 'to_stop_id', 'from_route_id', 'to_route_id',
                      'from_trip_id', 'to_trip_id', 'transfer_type', 'min_transfer_time'],
    'pathways.txt': ['pathway_id', 'from_stop_id', 'to_stop_id', 'pathway_mode',
                     'is_bidirectional', 'length', 'traversal_time', 'stair_count',
                     'max_slope', 'min_width', 'signposted_as', 'reversed_signposted_as'],
    'levels.txt': ['level_id', 'level_index', 'level_name'],
    'location_groups.txt': ['location_group_id', 'location_group_name'],
    'location_group_stops.txt': ['location_group_id', 'stop_id'],
    'booking_rules.txt': ['booking_rule_id', 'booking_type', 'prior_notice_duration_min', 'prior_notice_duration_max',
                          'prior_notice_last_day', 'prior_notice_last_time', 'prior_notice_start_day', 'prior_notice_start_time',
                          'prior_notice_service_id', 'message', 'pickup_message', 'drop_off_message',
                          'phone_number', 'info_url', 'booking_url'],
    'translations.txt': ['table_name', 'field_name', 'language', 'translation',
                         'record_id', 'record_sub_id', 'field_value'],
    'feed_info.txt': ['feed_publisher_name', 'feed_publisher_url', 'feed_lang', 'default_lang',
                      'feed_start_date', 'feed_end_date', 'feed_version', 'feed_contact_email',
                      'feed_contact_url'],
    'attributions.txt': ['attribution_id', 'agency_id', 'route_id', 'trip_id',
                         'organization_name', 'is_producer', 'is_operator', 'is_authority',
                         'attribution_url', 'attribution_email', 'attribution_phone']
}

def update_dataset(gtfs_dir):
    for file_name, file_columns in COLUMNS.items():
        file_path = os.path.join(gtfs_dir, file_name)

        # Skip if the file does not exist
        if not os.path.isfile(file_path):
            continue

        # Read the CSV file, treating all fields as strings to prevent data type issues
        df = pd.read_csv(file_path, dtype=str)

        # Replace strings with only spaces with empty strings
        df = df.infer_objects(copy=False).replace(r'^\s*$', '', regex=True)

        # Add missing optional columns
        for col in file_columns:
            if col not in df.columns:
                df[col] = ''

        # Reorder the dataframe and write back to the original file
        column_order = COLUMNS[file_name]
        df = df[column_order]
        df.to_csv(file_path, index=False)
        print(f"Updated {file_name}.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: process_dataset.py <gtfs_directory>")
        sys.exit(1)
    directory = sys.argv[1]
    update_dataset(directory)
