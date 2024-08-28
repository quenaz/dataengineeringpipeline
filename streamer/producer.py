import pandas as pd
from kafka import KafkaProducer
import json

def load_and_clean_data(dataset_path):
    df = pd.read_csv(dataset_path)
    df.fillna(0, inplace=True)  # Simple example of data cleansing
    return df

def create_producer():
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        api_version=(2, 6, 0),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def produce_messages(df, producer, topic_name):
    for index, row in df.iterrows():
        message = row.to_dict()
        producer.send(topic_name, value=message)
        print(f'Sent: {message}')
    producer.flush()

if __name__ == "__main__":
    dataset_path = './streamer/data/less_data.csv'
    df = load_and_clean_data(dataset_path)
    producer = create_producer()
    topic_name = 'nyc_taxi_fares'
    produce_messages(df, producer, topic_name)
