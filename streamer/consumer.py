import os
import json
import argparse
import pandas as pd

from kafka import KafkaConsumer
from datetime import datetime

DATA_LAKE_FOLDER='..\\data_lake'
BATCH_SIZE = 1000

# Ensure the data lake folder exists
os.makedirs(DATA_LAKE_FOLDER, exist_ok=True)

def get_batch_size_from_poll(consumer, timeout_ms=5000):
    """
    Poll messages from Kafka and return the batch size.
    """
    # Poll for a batch of messages
    msg_batch = consumer.poll(timeout_ms=timeout_ms)
    
    # Calculate the total number of messages
    total_messages = sum(len(messages) for messages in msg_batch.values())
    
    return total_messages, msg_batch

def process_data(messages, date_filter=None, location_filter=None):
    """
    Convert messages to DataFrame, filter data based on provided criteria, and return the DataFrame.
    """
    # Create DataFrame from messages
    records = []
    for msg in messages:
        try:
            data = msg.value
            records.append(data)
        except Exception as e:
            print(f"Error processing message: {e}")
            continue

    df = pd.DataFrame(records)

    # Convert 'pickup_datetime' to datetime and set timezone to UTC
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'], errors='coerce')

    # Apply filters
    if date_filter:
        start_date, end_date = date_filter
        # Convert start_date and end_date to timezone-aware datetimes
        start_date = pd.Timestamp(start_date).tz_localize('UTC')
        end_date = pd.Timestamp(end_date).tz_localize('UTC')
        df = df[(df['pickup_datetime'] >= start_date) & (df['pickup_datetime'] <= end_date)]
    
    if location_filter:
        longitude_min, longitude_max, latitude_min, latitude_max = location_filter
        df = df[(df['pickup_longitude'] >= longitude_min) & (df['pickup_longitude'] <= longitude_max) &
                (df['pickup_latitude'] >= latitude_min) & (df['pickup_latitude'] <= latitude_max)]

    return df

def save_to_parquet(df, filename):
    """
    Save the DataFrame to a Parquet file.
    """
    df.to_parquet(filename, engine='pyarrow', index=False)
    print(f"Data saved to {filename}")

def main():

    # Set up command-line argument parsing
    parser = argparse.ArgumentParser(description='Consume and filter Kafka data.')
    parser.add_argument('--start-date', type=str, required=False, help='Start date for filtering (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, required=False, help='End date for filtering (YYYY-MM-DD)')
    parser.add_argument('--longitude-min', type=float, required=False, help='Minimum longitude for filtering')
    parser.add_argument('--longitude-max', type=float, required=False, help='Maximum longitude for filtering')
    parser.add_argument('--latitude-min', type=float, required=False, help='Minimum latitude for filtering')
    parser.add_argument('--latitude-max', type=float, required=False, help='Maximum latitude for filtering')
    
    args = parser.parse_args()
    
    # Convert command-line arguments to filters
    # Example filter
    # date_filter = (datetime(2013, 6, 1), datetime(2015, 6, 1))  # Filter by date range
    # location_filter = (-74.0, -73.0, 40.5, 41.0)  # Filter by longitude and latitude range

    date_filter = None
    if args.start_date and args.end_date:
        date_filter = (args.start_date, args.end_date)
        
    location_filter = None
    if args.longitude_min is not None and args.longitude_max is not None and args.latitude_min is not None and args.latitude_max is not None:
        location_filter = (args.longitude_min, args.longitude_max, args.latitude_min, args.latitude_max)
    
    # Kafka Consumer configuration
    consumer = KafkaConsumer(
        'nyc_taxi_fares', # Topic
        bootstrap_servers=['localhost:9092'], # Host
        api_version=(2, 6, 0), # API Version
        max_poll_records=BATCH_SIZE, # Batch size
        auto_offset_reset='earliest',  # Start reading at the earliest offset
        enable_auto_commit=True,  # Enable auto commit of offsets
        group_id='nyc_taxi_fare_group', # Group ID
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Consuming messages from Kafka topic...")

    try:
        # Counter
        COUNTER = 0

        while True:
            # Consume a batch of messages
            batch_size, messages = get_batch_size_from_poll(consumer)
            print(f"Batch size: {batch_size}")

            if not messages:
                print("No new messages. Exiting.")
                break

            # Flatten the dictionary of messages
            message_list = [msg for topic_messages in messages.values() for msg in topic_messages]
            if not message_list:
                print("No messages to process.")
                continue

            filtered_data = process_data(message_list, date_filter=date_filter, location_filter=location_filter)

            if not filtered_data.empty:
                # Create a filename from the date strings
                filename = f"data_{COUNTER}_to_{COUNTER+batch_size+1}.parquet"

                print("Generated filename:", filename)

                # Define the path for the Parquet file
                parquet_file_path = os.path.join(DATA_LAKE_FOLDER, filename)

                # Save the filtered data to a Parquet file
                save_to_parquet(filtered_data, parquet_file_path)

                # Update counter
                COUNTER += batch_size

    except KeyboardInterrupt:
        print("Stopped consuming messages.")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()