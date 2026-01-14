from confluent_kafka import Consumer
import json
import yaml
from typing import Any, Dict, Callable, Optional
from pydantic import BaseModel, Field, ConfigDict
import socket
import os

class KafkaConsumerConfig(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    
    bootstrap_servers: str = Field(serialization_alias='bootstrap.servers')
    group_id: str = Field(serialization_alias='group.id')
    auto_offset_reset: str = Field(serialization_alias='auto.offset.reset')
    enable_auto_commit: bool = Field(default=True, serialization_alias='enable.auto.commit')
    auto_commit_interval_ms: int = Field(default=5000, serialization_alias='auto.commit.interval.ms')
    topic: str
    
    model_config = ConfigDict(extra='allow') # Not a kafka config, but our app config

def get_kafka_consumer(path: str) -> tuple[Consumer, str]:
    """
    Loads configuration from a YAML file and returns an initialized Kafka Consumer and the topic to subscribe to.
    
    Args:
        path (str): Path to the YAML configuration file.
        
    Returns:
        tuple[Consumer, str]: Configured Consumer instance and topic name.
        
    Raises:
        FileNotFoundError: If the config file does not exist.
    """
    with open(path, 'r') as f:
        config_dict = yaml.safe_load(f)
        
    # Validate with Pydantic
    try:
        # Allow env var override
        if os.environ.get('KAFKA_BOOTSTRAP_SERVERS'):
            config_dict['bootstrap_servers'] = os.environ['KAFKA_BOOTSTRAP_SERVERS']
            
        kafka_conf = KafkaConsumerConfig.model_validate(config_dict)
    except Exception as e:
        print(f"Config validation error: {e}")
        raise e

    # Extract topic separately as it's not a consumer property
    topic = kafka_conf.topic
    
    # Dump for confluent_kafka (exclude topic)
    conf = kafka_conf.model_dump(by_alias=True, exclude={'topic'})
    
    return Consumer(conf), topic
