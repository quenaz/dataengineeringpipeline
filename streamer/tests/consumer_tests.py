import unittest
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
from datetime import datetime
from io import BytesIO
from unittest.mock import patch, MagicMock
from consumer import parse_args, filter_message, write_to_parquet, process_messages

class TestYourApplication(unittest.TestCase):

    def setUp(self):
        """Set up the test environment"""
        # Sample data for testing
        self.messages = [
            {
                'pickup_datetime': '2015-01-01T00:00:00Z',
                'pickup_longitude': -73.0,
                'pickup_latitude': 40.7
            },
            {
                'pickup_datetime': '2015-02-01T00:00:00Z',
                'pickup_longitude': -74.0,
                'pickup_latitude': 41.0
            }
        ]
        self.df = pd.DataFrame(self.messages)
        self.parquet_path = 'test_data_lake'
        if not os.path.exists(self.parquet_path):
            os.makedirs(self.parquet_path)

    def tearDown(self):
        """Clean up after tests"""
        if os.path.exists(self.parquet_path):
            for root, dirs, files in os.walk(self.parquet_path, topdown=False):
                for name in files:
                    os.remove(os.path.join(root, name))
                for name in dirs:
                    os.rmdir(os.path.join(root, name))
            os.rmdir(self.parquet_path)

    def test_parse_args(self):
        """Test argument parsing"""
        with patch('argparse.ArgumentParser.parse_args') as mock_parse_args:
            mock_parse_args.return_value = MagicMock(
                start_date='2013-06-01',
                end_date='2015-06-01',
                longitude_min=-74.0,
                longitude_max=-73.0,
                latitude_min=40.5,
                latitude_max=41.0
            )
            args = parse_args()
            self.assertEqual(args.start_date, '2013-06-01')
            self.assertEqual(args.end_date, '2015-06-01')
            self.assertEqual(args.longitude_min, -74.0)
            self.assertEqual(args.longitude_max, -73.0)
            self.assertEqual(args.latitude_min, 40.5)
            self.assertEqual(args.latitude_max, 41.0)

    def test_filter_message(self):
        """Test filtering of messages"""
        date_filter = (datetime(2015, 1, 1), datetime(2015, 12, 31))
        location_filter = (-74.0, -73.0, 40.5, 41.0)
        
        # Test date filtering
        self.assertTrue(filter_message(self.messages[0], date_filter=date_filter))
        self.assertTrue(filter_message(self.messages[1], date_filter=date_filter))
        
        # Test location filtering
        self.assertTrue(filter_message(self.messages[0], location_filter=location_filter))
        self.assertTrue(filter_message(self.messages[1], location_filter=location_filter))
        
        # Test no filter
        self.assertTrue(filter_message(self.messages[0]))
        self.assertTrue(filter_message(self.messages[1]))

    def test_write_to_parquet(self):
        """Test writing to Parquet file"""
        file_buffer = BytesIO()
        df = self.df.copy()
        df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
        write_to_parquet(df, self.parquet_path)
        
        # Check that the file was created
        parquet_files = [f for f in os.listdir(self.parquet_path) if f.endswith('.parquet')]
        self.assertTrue(len(parquet_files) > 0)

        # Read the file to verify content
        for parquet_file in parquet_files:
            file_path = os.path.join(self.parquet_path, parquet_file)
            table = pq.read_table(file_path)
            df_read = table.to_pandas()
            pd.testing.assert_frame_equal(df, df_read)

    def test_process_messages(self):
        """Test processing and partitioning of messages"""
        # Mock the write_to_parquet function to avoid actual file writing
        with patch('consumer.write_to_parquet') as mock_write_to_parquet:
            process_messages(self.messages)
            self.assertTrue(mock_write_to_parquet.called)

if __name__ == '__main__':
    unittest.main()
