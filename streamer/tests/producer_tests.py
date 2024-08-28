import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
from io import StringIO
from producer import load_and_clean_data, create_producer, produce_messages  # Update with your module name

class TestKafkaProducer(unittest.TestCase):

    @patch('producer.pd.read_csv')  # Mock the pandas read_csv function
    def test_load_and_clean_data(self, mock_read_csv):
        # Define mock data
        mock_csv_data = """
key,fare_amount,pickup_datetime,pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude,passenger_count
2009-06-15 17:26:21.0000001,4.5,2009-06-15 17:26:21 UTC,-73.844311,40.721319,-73.84161,40.712278,1
2010-01-05 16:52:16.0000002,16.9,2010-01-05 16:52:16 UTC,-74.016048,40.711303,-73.979268,40.782004,1
        """
        mock_read_csv.return_value = pd.read_csv(StringIO(mock_csv_data))
        
        # Call the function
        df = load_and_clean_data('./tests/fake.csv')
        
        expected_data = pd.DataFrame({'key': ['2009-06-15', '2010-01-05'], 'pickup_datetime': ['2009-06-15 17:26:21 UTC', '2010-01-05 16:52:16 UTC']})
        pd.testing.assert_frame_equal(df, expected_data)

    @patch('producer.KafkaProducer')  # Mock KafkaProducer
    def test_create_producer(self, mock_kafka_producer):
        # Call the function
        producer = create_producer()
        
        # Assert that KafkaProducer was called with the correct parameters
        mock_kafka_producer.assert_called_with(
            bootstrap_servers=['localhost:9092'],
            api_version=(2, 6, 0),
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.assertIsInstance(producer, MagicMock)  # Check that the producer is a MagicMock

    @patch('producer.KafkaProducer')  # Mock KafkaProducer
    @patch('producer.pd.DataFrame.iterrows')  # Mock DataFrame iterrows
    def test_produce_messages(self, mock_iterrows, mock_kafka_producer):
        # Mock KafkaProducer instance and its send method
        mock_producer = MagicMock()
        mock_kafka_producer.return_value = mock_producer
        
        # Define mock data
        mock_data = {'key': ['2009-06-15', '2010-01-05'], 'pickup_datetime': ['2009-06-15 17:26:21 UTC', '2010-01-05 16:52:16 UTC']}
        df = pd.DataFrame(mock_data)
        mock_iterrows.return_value = iter(df.iterrows())
        
        # Call the function
        produce_messages(df, mock_producer, 'test_topic')
        
        # Assert that producer.send was called with the correct parameters
        expected_calls = [(({'key': '2009-06-15', 'pickup_datetime': '2009-06-15 17:26:21 UTC'},),) for _ in range(len(df))]
        mock_producer.send.assert_has_calls(expected_calls)
        mock_producer.flush.assert_called_once()

if __name__ == '__main__':
    unittest.main()
