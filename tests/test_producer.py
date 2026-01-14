import unittest
from unittest.mock import MagicMock, patch
import json
import sys
import os
import argparse

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from producer.producer import produce_thread

class TestProducer(unittest.TestCase):
    @patch('producer.producer.send_message')
    @patch('time.sleep')
    def test_produce_thread(self, mock_sleep, mock_send_message):
        """Test the thread loop invokes send_message correctly"""
        mock_producer = MagicMock()
        topic = "test-topic"
        p_id = "test-1"

        # Break loop after one run
        mock_sleep.side_effect = KeyboardInterrupt
        
        try:
            produce_thread(mock_producer, topic, p_id)
        except KeyboardInterrupt:
            pass
            
        # Verify send_message called
        self.assertTrue(mock_send_message.called)
        args, kwargs = mock_send_message.call_args
        
        # Check arguments passed to send_message
        self.assertEqual(args[0], mock_producer)
        self.assertEqual(args[1], topic)
        self.assertEqual(kwargs['key'], p_id)
        self.assertEqual(kwargs['value']['producer_id'], p_id)

if __name__ == '__main__':
    unittest.main()
