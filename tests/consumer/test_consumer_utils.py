import unittest
from unittest.mock import patch, mock_open
from pydantic import ValidationError


from consumer.utils import get_kafka_consumer, KafkaConsumerConfig

class TestConsumerUtils(unittest.TestCase):
    
    def test_consumer_config_validation(self):
        """Test valid consumer config validation"""
        config = {
            "bootstrap_servers": "localhost:9094",
            "group_id": "test-group",
            "auto_offset_reset": "earliest",
            "topic": "test-topic"
        }
        model = KafkaConsumerConfig.model_validate(config)
        self.assertEqual(model.topic, "test-topic")
        dump = model.model_dump(by_alias=True, exclude={'topic'})
        self.assertEqual(dump['group.id'], "test-group")

    def test_consumer_config_missing_field(self):
        """Test missing topic raises validation error"""
        config = {
             "bootstrap_servers": "localhost:9094",
             "group_id": "test-group",
             "auto_offset_reset": "earliest"
        }
        with self.assertRaises(ValidationError):
             KafkaConsumerConfig.model_validate(config)

    @patch("builtins.open", new_callable=mock_open, read_data="bootstrap_servers: localhost:9092\ngroup_id: g1\nauto_offset_reset: earliest\ntopic: t1")
    @patch("consumer.utils.Consumer")
    def test_get_kafka_consumer_success(self, mock_consumer_cls, mock_file):
        """Test get_kafka_consumer returns consumer and topic"""
        consumer, topic = get_kafka_consumer("config.yml")
        
        self.assertEqual(topic, "t1")
        mock_consumer_cls.assert_called_once()
        # Check config passed to Consumer constructor
        call_args = mock_consumer_cls.call_args[0][0]
        self.assertEqual(call_args['group.id'], 'g1')
        self.assertNotIn('topic', call_args) # topic should be stripped

if __name__ == '__main__':
    unittest.main()
