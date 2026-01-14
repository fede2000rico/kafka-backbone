import sys
import json
import time

from consumer.utils import get_kafka_consumer

def main():
    try:
        # Load Config
        try:
            config_path = 'src/consumer/consumer_config.yml'
            consumer, topic = get_kafka_consumer(config_path)
        except FileNotFoundError:
            config_path = 'consumer_config.yml'
            consumer, topic = get_kafka_consumer(config_path)

        consumer.subscribe([topic])
        print(f"Consumer subscribed to {topic}. Waiting for messages...")

        # Buffer for aggregation
        # We need to aggregate current cycle messages.
        # Assuming simple aggregation for demonstration: wait for latest from A and latest from B
        buffer = {
            "machine_A": None,
            "machine_B": None
        }

        try:
            while True:
                msg = consumer.poll(1.0)
                
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue

                try:
                    # Decode message
                    key = msg.key().decode('utf-8') if msg.key() else None
                    val_str = msg.value().decode('utf-8')
                    val = json.loads(val_str)
                    
                    producer_id = val.get('producer_id')
                    
                    # Update buffer
                    if producer_id in buffer:
                        buffer[producer_id] = val
                        # print(f"Received from {producer_id}")

                    # Check if we have both
                    if buffer["machine_A"] and buffer["machine_B"]:
                        # AGGREGATE
                        agg_msg = f"AGGREGATED: {buffer['machine_A']} + {buffer['machine_B']}"
                        print("------------------------------------------------")
                        print(agg_msg)
                        print("------------------------------------------------")
                        
                        # Clear buffer for next cycle
                        buffer["machine_A"] = None
                        buffer["machine_B"] = None

                except Exception as e:
                    print(f"Error processing message: {e}")

        except KeyboardInterrupt:
            pass
        finally:
            consumer.close()

    except Exception as e:
        print(f"Failed to start consumer: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
