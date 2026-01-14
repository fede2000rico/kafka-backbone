import unittest
from unittest.mock import MagicMock, patch
import json
import sys
import os

import argparse

# Add src to path so we can import consumer
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from consumer.consumer import main

class TestConsumer(unittest.TestCase):
    @patch('consumer.consumer.Consumer')
    @patch('builtins.print')
    def test_consumer_aggregation(self, mock_print, mock_consumer_cls):
        # Mock Consumer instance
        mock_consumer = MagicMock()
        mock_consumer_cls.return_value = mock_consumer

        # Message 1: Producer A (count=0)
        msg1 = MagicMock()
        msg1.error.return_value = None
        msg1.value.return_value = json.dumps({'producer_id': 'A', 'count': 0}).encode('utf-8')

        # Message 2: Producer B (count=0) -> Should trigger aggregation
        msg2 = MagicMock()
        msg2.error.return_value = None
        msg2.value.return_value = json.dumps({'producer_id': 'B', 'count': 0}).encode('utf-8')

        # Setup poll to return msg1, then msg2, then Interrupt
        mock_consumer.poll.side_effect = [msg1, msg2, KeyboardInterrupt]

        # Run main
        try:
            main()
        except KeyboardInterrupt:
            pass

        # Assertions
        mock_consumer.subscribe.assert_called_with(['data-stream'])
        
        # Check that we printed the AGGREGATED OUTPUT
        # The print calls will be many (Starting consumer etc), we need to check if one of them contains "AGGREGATED OUTPUT"
        core_print_found = False
        for call in mock_print.call_args_list:
            args, _ = call
            if args and "AGGREGATED OUTPUT" in str(args[0]):
                core_print_found = True
                # Validate content
                output_json = json.loads(str(args[0]).replace("AGGREGATED OUTPUT: ", ""))
                self.assertEqual(output_json['id'], 0)
                self.assertEqual(output_json['status'], "COMPLETED")
                break
        
        self.assertTrue(core_print_found, "Aggregated output not found in print calls")

if __name__ == '__main__':
    unittest.main()
