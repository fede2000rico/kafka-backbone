import sys
import time
import socket
import threading
from confluent_kafka import Producer

# Import utilities
try:
    from producer.utils import send_message, delivery_report, get_kafka_producer
except ImportError:
    # For running locally from src root without package install
    from utils import send_message, delivery_report, get_kafka_producer

def produce_thread(producer_instance: Producer, topic: str, producer_id: str):
    """
    Thread routine that generates data and sends it using send_message.
    """
    print(f"[{producer_id}] Starting producer thread...")
    count = 0
    try:
        while True:
            data = {
                "producer_id": producer_id,
                "count": count,
                "timestamp": time.time()
            }
            
            send_message(
                producer_instance,
                topic,
                key=producer_id,
                value=data,
                callback=delivery_report
            )
            
            count += 1
            time.sleep(1) # Send every 1 second
    except Exception as e:
        print(f"[{producer_id}] Error: {e}")

def main():
    # 1. Initialize Producer using Utils
    try:
        # Assuming run from root or src/producer, adjust path as needed
        try:
            config_path = 'src/producer/producer_config.yml' 
            producer = get_kafka_producer(config_path)
        except FileNotFoundError:
            # Fallback if running from src/producer directory
            config_path = 'producer_config.yml'
            producer = get_kafka_producer(config_path)
    except Exception as e:
        print(f"Failed to initialize producer: {e}")
        sys.exit(1)

    # 2. Define Configurations for multiple simulated producers
    configs = [
        {"id": "machine_A", "topic": "data-stream"},
        {"id": "machine_B", "topic": "data-stream"}
    ]

    # 5. Start Threads
    threads = []
    print(f"Starting {len(configs)} producer threads...")
    
    for cfg in configs:
        t = threading.Thread(
            target=produce_thread, 
            args=(producer, cfg["topic"], cfg["id"]), 
            daemon=True
        )
        t.start()
        threads.append(t)

    # 6. Keep Main Thread Alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping all producers...")
    finally:
        producer.flush()

if __name__ == "__main__":
    main()
