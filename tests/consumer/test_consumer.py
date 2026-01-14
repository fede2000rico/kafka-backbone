import unittest
from unittest.mock import MagicMock, patch
import json


from consumer.consumer import main

class TestConsumer(unittest.TestCase):
    
    @patch('consumer.consumer.get_kafka_consumer')
    @patch('builtins.print')
    def test_consumer_aggregation_success(self, mock_print, mock_get_kafka_consumer):
        """Test that consumer selects messages and prints ONLY when aggregation is complete"""
        mock_consumer = MagicMock()
        mock_get_kafka_consumer.return_value = (mock_consumer, "test-topic")
        
        # Sequence: A -> B -> KeyboardInterrupt
        msg_a = MagicMock()
        msg_a.error.return_value = None
        msg_a.key.return_value = b"machine_A"
        msg_a.value.return_value = json.dumps({"producer_id": "machine_A", "val": 1}).encode('utf-8')
        
        msg_b = MagicMock()
        msg_b.error.return_value = None
        msg_b.key.return_value = b"machine_B"
        msg_b.value.return_value = json.dumps({"producer_id": "machine_B", "val": 2}).encode('utf-8')
        
        mock_consumer.poll.side_effect = [msg_a, msg_b, KeyboardInterrupt]
        
        try:
            main()
        except KeyboardInterrupt:
            pass
            
        # Verify print called with aggregation
        expected_agg = "AGGREGATED: {'producer_id': 'machine_A', 'val': 1} + {'producer_id': 'machine_B', 'val': 2}"
        
        found = False
        for call in mock_print.call_args_list:
            if call.args and expected_agg in str(call.args[0]):
                found = True
        self.assertTrue(found, "Did not print aggregated message")

    @patch('consumer.consumer.get_kafka_consumer')
    @patch('builtins.print')
    def test_consumer_partial_aggregation(self, mock_print, mock_get_kafka_consumer):
        """Test that consumer DOES NOT print if only one message is received"""
        mock_consumer = MagicMock()
        mock_get_kafka_consumer.return_value = (mock_consumer, "test-topic")
        
        # Sequence: A -> A -> KeyboardInterrupt
        msg_a_1 = MagicMock()
        msg_a_1.error.return_value = None
        msg_a_1.key.return_value = b"machine_A"
        msg_a_1.value.return_value = json.dumps({"producer_id": "machine_A", "val": 1}).encode('utf-8')

        msg_a_2 = MagicMock()
        msg_a_2.error.return_value = None
        msg_a_2.key.return_value = b"machine_A"
        msg_a_2.value.return_value = json.dumps({"producer_id": "machine_A", "val": 2}).encode('utf-8')
        
        mock_consumer.poll.side_effect = [msg_a_1, msg_a_2, KeyboardInterrupt]
        
        try:
            main()
        except KeyboardInterrupt:
            pass
        
        # Verify NO aggregation printed
        for call in mock_print.call_args_list:
            if call.args:
                self.assertNotIn("AGGREGATED:", str(call.args[0]))

if __name__ == '__main__':
    unittest.main()
