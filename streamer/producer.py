import pandas as pd
from kafka import KafkaProducer
import json

# Load the dataset
dataset_path = 'data\\less_data.csv'
df = pd.read_csv(dataset_path)
df.fillna(0, inplace=True)  # Simple example of data cleansing

# Kafka Producer configuration
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    api_version=(2, 6, 0),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'nyc_taxi_fares'

# Produce events to Kafka
for index, row in df.iterrows():
    message = row.to_dict()
    producer.send(topic_name, value=message)
    print(f'Sent: {message}')

producer.flush()