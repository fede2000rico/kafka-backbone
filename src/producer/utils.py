from confluent_kafka import Producer
import json
import yaml
from typing import Any, Dict, Callable, Optional
from pydantic import BaseModel, Field, ConfigDict
import socket
import os

class KafkaConfig(BaseModel):
    # Using strict mode is implicitly enforced if we don't alias input fields
    # serialization_alias ensures that when we dump, we use the dot notation
    bootstrap_servers: str = Field(serialization_alias='bootstrap.servers')
    client_id: str = Field(serialization_alias='client.id')

def get_kafka_producer(path: str) -> Producer:
    """
    Loads configuration from a YAML file and returns an initialized Kafka Producer.
    
    Args:
        path (str): Path to the YAML configuration file.
        
    Returns:
        Producer: Configured confluent_kafka.Producer instance.
        
    Raises:
        FileNotFoundError: If the config file does not exist.
        yaml.YAMLError: If the config file is invalid YAML.
        ValidationError: If the config does not match the Pydantic model.
    """
    with open(path, 'r') as f:
        config_dict = yaml.safe_load(f)
        
    # Validate with Pydantic - strictly expects snake_case keys from YAML
    try:
        # Allow env var override
        if os.environ.get('KAFKA_BOOTSTRAP_SERVERS'):
            config_dict['bootstrap_servers'] = os.environ['KAFKA_BOOTSTRAP_SERVERS']
            
        kafka_conf = KafkaConfig.model_validate(config_dict)
    except Exception as e:
        print(f"Config validation error: {e}")
        raise e

    # Use 'by_alias=True' to export keys as 'bootstrap.servers', 'client.id'
    # which is what confluent_kafka expects due to serialization_alias
    conf = kafka_conf.model_dump(by_alias=True)
    
    # Ensure client.id is set if missing (though Pydantic requires it per model above, 
    # we can make it optional in model or default logic here if strictly needed, 
    # but based on previous code it seemed required or defaulted to hostname.
    # To keep exact behavior: if client_id was None in yaml/dict it would fail validation above.
    # If we want dynamic default, we can do it in Pydantic validator or here.
    # Let's assume validation passes and we rely on 'conf'.
    
    return Producer(conf)

def delivery_report(err: Optional[str], msg: Any) -> None:
    """
    Callback for delivery reports from Kafka.
    
    Args:
        err (Optional[str]): Error message if delivery failed, else None.
        msg (Any): The message object (confluent_kafka.Message) that was produced.
    """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        # print(f"Message delivered to {msg.topic()} [{msg.partition()}]")
        pass

def send_message(
    producer_instance: Producer, 
    topic: str, 
    key: str, 
    value: Dict[str, Any], 
    callback: Callable[[Optional[str], Any], None]
) -> None:
    """
    Sends a message to a Kafka topic using the provided producer instance.
    
    Args:
        producer_instance (Producer): The configured Kafka Producer instance.
        topic (str): The target Kafka topic.
        key (str): The key for the message (used for partitioning).
        value (Dict[str, Any]): The message payload, will be serialized to JSON.
        callback (Callable): The delivery report callback function.
    """
    try:
        producer_instance.produce(
            topic, 
            key=key, 
            value=json.dumps(value), 
            callback=callback
        )
        # Trigger any available delivery report callbacks from previous produce() calls
        producer_instance.poll(0)
    except BufferError:
        print(f"Local producer queue is full ({len(producer_instance)} messages awaiting delivery)")
    except Exception as e:
        print(f"Failed to send message: {e}")
