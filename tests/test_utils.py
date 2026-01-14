import unittest
from unittest.mock import MagicMock, mock_open, patch
import json
import sys
import os

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from producer.utils import send_message, delivery_report, get_kafka_producer, KafkaConfig
# Verify utils has Producer imported for mocking
import producer.utils

class TestUtils(unittest.TestCase):
    def test_send_message(self):
        """Test the pure send_message function from utils"""
        mock_producer = MagicMock()
        callback = MagicMock()
        
        send_message(
            producer_instance=mock_producer,
            topic="test",
            key="key",
            value={"a": 1},
            callback=callback
        )
        
        # Verify produce was called on the mock instance
        self.assertTrue(mock_producer.produce.called)
        
    @patch('producer.utils.Producer')
    def test_get_kafka_producer(self, mock_producer_cls):
        """Test loading kafka producer from yaml"""
        yaml_content = "bootstrap_servers: 'host:9092'\nclient_id: 'test-client'"
        
        with patch('builtins.open', mock_open(read_data=yaml_content)):
            prod = get_kafka_producer('dummy.yml')
            
        # Verify Producer was instantiated with correct config
        mock_producer_cls.assert_called_once()
        call_args = mock_producer_cls.call_args[0][0]
        self.assertEqual(call_args['bootstrap.servers'], 'host:9092')
        self.assertEqual(call_args['client.id'], 'test-client')

if __name__ == '__main__':
    unittest.main()
