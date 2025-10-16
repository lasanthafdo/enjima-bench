# created by t86kim

import os
import pandas as pd

def merge_csv_files(trip_data_dir, trip_fare_dir, output_file):
    """
    Merges corresponding trip_data and trip_fare CSV files into a single output CSV.

    Parameters:
    - trip_data_dir: Directory containing trip_data CSV files.
    - trip_fare_dir: Directory containing trip_fare CSV files.
    - output_file: Path to the output CSV file.
    """

    # Define the desired columns from trip_data
    trip_data_columns = [
        'medallion',
        'hack_license',
        'pickup_datetime',
        'dropoff_datetime',
        'trip_time_in_secs',
        'trip_distance',
        'pickup_longitude',
        'pickup_latitude',
        'dropoff_longitude',
        'dropoff_latitude'
    ]

    # Define the desired columns from trip_fare
    trip_fare_columns = [
        'payment_type',
        'fare_amount',
        'surcharge',
        'mta_tax',
        'tip_amount',
        'tolls_amount',
        'total_amount'
    ]

    # Define the output CSV headers
    output_headers = [
        'medallion',
        'hack_license',
        'pickup_datetime',
        'dropoff_datetime',
        'trip_time_in_secs',
        'trip_distance',
        'pickup_longitude',
        'pickup_latitude',
        'dropoff_longitude',
        'dropoff_latitude',
        'payment_type',
        'fare_amount',
        'surcharge',
        'mta_tax',
        'tip_amount',
        'tolls_amount',
        'total_amount'
    ]

    # Get a sorted list of trip_data CSV files
    trip_data_files = sorted([
        f for f in os.listdir(trip_data_dir)
        if f.startswith('trip_data_') and f.endswith('.csv')
    ])

    # Initialize a flag to write headers only once
    header_written = False

    # Open the output CSV file in write mode
    with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
        for data_file in trip_data_files:
            # Extract the file identifier (e.g., '1' from 'trip_data_1.csv')
            identifier = data_file.replace('trip_data_', '').replace('.csv', '')

            fare_file = f'trip_fare_{identifier}.csv'
            fare_file_path = os.path.join(trip_fare_dir, fare_file)

            data_file_path = os.path.join(trip_data_dir, data_file)

            # Check if the corresponding trip_fare file exists
            if not os.path.exists(fare_file_path):
                print(f"Warning: {fare_file} does not exist. Skipping this pair.")
                continue

            # Read trip_data CSV
            try:
                # trip_data_df = pd.read_csv(data_file_path, usecols=trip_data_columns)
                trip_data_df = pd.read_csv(data_file_path, usecols=[0,1,5,6,8,9,10,11,12,13])
            except Exception as e:
                print(f"Error reading {data_file}: {e}")
                continue

            # Read trip_fare CSV
            try:
                # trip_fare_df = pd.read_csv(fare_file_path, usecols=trip_fare_columns)
                trip_fare_df = pd.read_csv(fare_file_path, usecols=[4,5,6,7,8,9,10])
            except Exception as e:
                print(f"Error reading {fare_file}: {e}")
                continue

            # Ensure that both DataFrames have the same number of rows
            if len(trip_data_df) != len(trip_fare_df):
                print(f"Warning: Row count mismatch between {data_file} and {fare_file}. Skipping this pair.")
                continue

            # Combine the two DataFrames side-by-side
            combined_df = pd.concat([trip_data_df, trip_fare_df], axis=1)

            # Write to the output CSV
            if not header_written:
                combined_df.to_csv(outfile, header=output_headers, index=False)
                header_written = True
            else:
                combined_df.to_csv(outfile, header=False, index=False)

            print(f"Merged {data_file} and {fare_file} into {output_file}")

    print(f"All files have been merged into {output_file}")

if __name__ == "__main__":
    # Define the directories
    ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
    TRIP_DATA_DIR = os.path.join(ROOT_DIR, 'trip_data')
    TRIP_FARE_DIR = os.path.join(ROOT_DIR, 'trip_fare')
    OUTPUT_FILE = os.path.join(ROOT_DIR, 'output.csv')

    # Check if the trip_data and trip_fare directories exist
    if not os.path.isdir(TRIP_DATA_DIR):
        print(f"Error: Directory '{TRIP_DATA_DIR}' does not exist.")
        exit(1)

    if not os.path.isdir(TRIP_FARE_DIR):
        print(f"Error: Directory '{TRIP_FARE_DIR}' does not exist.")
        exit(1)

    # Call the merge function
    merge_csv_files(TRIP_DATA_DIR, TRIP_FARE_DIR, OUTPUT_FILE)