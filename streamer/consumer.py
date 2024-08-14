from kafka import KafkaConsumer
import pandas as pd
import json
import os
from datetime import datetime

DATA_LAKE_FOLDER='.\\data_lake'
BATCH_SIZE = 1000

# Ensure the data lake folder exists
os.makedirs(DATA_LAKE_FOLDER, exist_ok=True)

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
    # Kafka Consumer configuration
    consumer = KafkaConsumer(
        'nyc_taxi_fares',
        bootstrap_servers=['localhost:9092'],
        api_version=(2, 6, 0),
        max_poll_records=BATCH_SIZE,
        auto_offset_reset='earliest',  # Start reading at the earliest offset
        enable_auto_commit=True,  # Enable auto commit of offsets
        group_id='nyc_taxi_fare_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Consuming messages from Kafka topic...")

    try:
        while True:
            # Consume a batch of messages
            messages = consumer.poll(timeout_ms=5000)
            if not messages:
                print("No new messages. Exiting.")
                break

            # Flatten the dictionary of messages
            message_list = [msg for topic_messages in messages.values() for msg in topic_messages]
            if not message_list:
                print("No messages to process.")
                continue

            # Example filters
            date_filter = (datetime(2013, 6, 1), datetime(2015, 6, 1))  # Filter by date range
            location_filter = (-74.0, -73.0, 40.5, 41.0)  # Filter by longitude and latitude range

            filtered_data = process_data(message_list, date_filter=date_filter, location_filter=location_filter)

            if not filtered_data.empty:
                # Define the path for the Parquet file
                parquet_file_path = os.path.join(DATA_LAKE_FOLDER, 'filtered_data.parquet')

                # Save the filtered data to a Parquet file
                save_to_parquet(filtered_data, parquet_file_path)

    except KeyboardInterrupt:
        print("Stopped consuming messages.")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()