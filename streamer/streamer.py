from flask import Flask, Response
from kafka import KafkaConsumer
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import json
import os
from datetime import datetime

app = Flask(__name__)

# Kafka Consumer configuration
consumer = KafkaConsumer(
    'nyc_taxi_fares',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='nyc_taxi_fare_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

@app.route('/consume', methods=['GET'])
def consume():
    def stream():
        data = []
        
        for message in consumer:
            # Process each message
            record = message.value

            # Extract date and place (assuming the dataset has these fields)
            timestamp = datetime.strptime(record['pickup_datetime'], '%Y-%m-%d %H:%M:%S')
            year = timestamp.strftime('%Y')
            month = timestamp.strftime('%m')
            day = timestamp.strftime('%d')
            place = record['pickup_borough']  # Assuming a field for the borough

            # Define the directory path based on the data lake structure
            directory_path = os.path.join('data_lake', 'nyc_taxi_fares', f'year={year}', f'month={month}', f'day={day}', f'place={place}')
            os.makedirs(directory_path, exist_ok=True)

            # Add record to the batch
            data.append(record)

            # Write data in batches of 1000
            if len(data) >= 1000:
                # Convert to DataFrame
                df = pd.DataFrame(data)

                # Write to Parquet format in the designed directory structure
                table = pa.Table.from_pandas(df)
                pq.write_to_dataset(table, root_path=directory_path)

                # Clear the data buffer
                data.clear()

            yield f"data: {json.dumps(record)}\n\n"

        # Handle remaining records
        if data:
            df = pd.DataFrame(data)
            table = pa.Table.from_pandas(df)
            pq.write_to_dataset(table, root_path=directory_path)
            data.clear()

    return Response(stream(), content_type='text/event-stream')

if __name__ == '__main__':
    app.run(debug=True, threaded=True)
