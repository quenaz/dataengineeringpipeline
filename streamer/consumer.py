import os
import json
import argparse
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

from kafka import KafkaConsumer
from datetime import datetime
from dateutil import parser


def parse_args():
    """
    Parse command-line arguments for date and location filters.
    """
    parser = argparse.ArgumentParser(description="Filter messages based on date and location.")
    parser.add_argument("--start-date", type=str, help="Start date for filtering (format: YYYY-MM-DD).")
    parser.add_argument("--end-date", type=str, help="End date for filtering (format: YYYY-MM-DD).")
    parser.add_argument("--longitude-min", type=float, help="Minimum longitude for filtering.")
    parser.add_argument("--longitude-max", type=float, help="Maximum longitude for filtering.")
    parser.add_argument("--latitude-min", type=float, help="Minimum latitude for filtering.")
    parser.add_argument("--latitude-max", type=float, help="Maximum latitude for filtering.")
    return parser.parse_args()


def filter_message(message, date_filter=None, location_filter=None):
    """
    Filter messages based on either date or location.
    """
    # Parse the datetime and remove the timezone information (make it naive)
    pickup_datetime = parser.parse(message['pickup_datetime']).replace(tzinfo=None)
    pickup_longitude = message['pickup_longitude']
    pickup_latitude = message['pickup_latitude']
    
    # Initialize flags for date and location filtering
    if date_filter is not None:
        passed = (date_filter[0] <= pickup_datetime <= date_filter[1])
    elif location_filter is not None:
        passed = (
            location_filter[0] <= pickup_longitude <= location_filter[1] and
            location_filter[2] <= pickup_latitude <= location_filter[3]
        )
    else:
        passed = True
    
    # Return True if the message meets either the date or location filter
    return passed


def write_to_parquet(df, path):
    """
    Write filtered messages to Parquet files in the data lake structure.
    """
    # Convert the DataFrame to an Apache Arrow Table
    table = pa.Table.from_pandas(df)

    # Convert TIMESTAMP(NANOS,true) to TIMESTAMP(MILLIS,true)
    table = table.append_column(
        'pickup_datetime_millis',
        pc.cast(table['pickup_datetime'], pa.timestamp('ms'))
    ).drop(['pickup_datetime'])

    # Define the new column names
    new_column_names = [name if name != 'pickup_datetime_millis' else 'pickup_datetime' for name in table.column_names]

    # Rename the columns
    table = table.rename_columns(new_column_names)

    # Get the current time and format it
    current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Construct the filename with the current time
    filename = f"data_{current_time}.parquet"

    #pq.write_to_dataset(table, root_path=path, partition_cols=['year', 'month', 'day'])
    pq.write_table(table, os.path.join(path, filename))
    print(f"Data saved to {filename}")


def process_messages(messages):
    """
    Process a list of messages by filtering and then writing to Parquet files.
    """
    # Convert list of dicts to a DataFrame
    df = pd.DataFrame(messages)
    
    # Add partitioning information to the DataFrame
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    df['year'] = df['pickup_datetime'].dt.year
    df['month'] = df['pickup_datetime'].dt.month
    df['day'] = df['pickup_datetime'].dt.day

    # Define the data lake structure path
    parquet_path = os.path.join('data_lake', f'{df["year"].iloc[0]}', f'{df["month"].iloc[0]}', f'{df["day"].iloc[0]}')
    os.makedirs(parquet_path, exist_ok=True)

    # Write the DataFrame to Parquet files in the data lake
    write_to_parquet(df, parquet_path)


def main():
    # Set up command-line argument parsing
    args = parse_args()
    
    # Example filter
    # --start-date 2013-06-01 --end-date 2015-06-01
    # --longitude-min -74.0 --longitude.max -73.0 --latitude-min 40.5 --latitude-max 41.0

    # Set up date filter if both start_date and end_date are provided
    date_filter = None
    if args.start_date and args.end_date:
        date_filter = (datetime.strptime(args.start_date, '%Y-%m-%d'), 
                       datetime.strptime(args.end_date, '%Y-%m-%d'))
    
    # Set up location filter if all required location parameters are provided
    location_filter = None
    if args.longitude_min is not None and args.longitude_max is not None and args.latitude_min is not None and args.latitude_max is not None:
        location_filter = (args.longitude_min, args.longitude_max, args.latitude_min, args.latitude_max)
    
    # Initialize the Kafka consumer
    consumer = KafkaConsumer(
        'nyc_taxi_fares',  # Topic to subscribe to
        bootstrap_servers=['localhost:9092'], # Kafka host
        auto_offset_reset='earliest',  # Start reading at the earliest message in the topic
        enable_auto_commit=True,  # Commit offsets automatically
        group_id='nyc_taxi_fares_group',  # Consumer group id
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize JSON messages
    )

    print("Consumer started...")

    filtered_messages = []

    try:
        # Start consuming messages
        for message in consumer:
            # Message retrieval, deserialization, and processing
            message_value = message.value

            # Filter the message based on date and location
            if filter_message(message_value, date_filter, location_filter):
                filtered_messages.append(message_value)
                print(f"Message passed filters: {message_value}")

                # Process batch of messages after a certain number is collected
                if len(filtered_messages) >= 100:
                    process_messages(filtered_messages)
                    filtered_messages.clear()  # Clear the list after processing

    except KeyboardInterrupt:
        print("Consumer interrupted.")
    
    finally:
        # Process any remaining messages
        if filtered_messages:
            process_messages(filtered_messages)
        
        # Ensure graceful shutdown
        consumer.close()
        print("Consumer closed.")

if __name__ == "__main__":
    main()
