import unittest
from unittest.mock import patch, mock_open
import yaml
from pydantic import ValidationError


from producer.utils import get_kafka_producer, KafkaConfig

class TestProducerUtils(unittest.TestCase):
    
    def test_kafka_config_validation_success(self):
        """Test valid config validation"""
        config = {
            "bootstrap_servers": "localhost:9092",
            "client_id": "test-client"
        }
        model = KafkaConfig.model_validate(config)
        dump = model.model_dump(by_alias=True)
        self.assertEqual(dump['bootstrap.servers'], "localhost:9092")
        self.assertEqual(dump['client.id'], "test-client")

    def test_kafka_config_validation_failure(self):
        """Test missing fields raise ValidationError"""
        config = {"client_id": "test-client"} # missing bootstrap_servers
        with self.assertRaises(ValidationError):
            KafkaConfig.model_validate(config)

    @patch("builtins.open", new_callable=mock_open, read_data="bootstrap_servers: localhost:9092\nclient_id: test")
    @patch("producer.utils.Producer")
    def test_get_kafka_producer_success(self, mock_producer_cls, mock_file):
        """Test get_kafka_producer loads config and returns producer"""
        producer = get_kafka_producer("dummy.yml")
        
        # Verify Producer initialized with correct config
        mock_producer_cls.assert_called_once()
        call_args = mock_producer_cls.call_args[0][0]
        self.assertEqual(call_args['bootstrap.servers'], 'localhost:9092')
        self.assertEqual(call_args['client.id'], 'test')

if __name__ == '__main__':
    unittest.main()
