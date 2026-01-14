import sys
import json
from confluent_kafka import Consumer, KafkaError

def main():
    """
    Main function for the Kafka Consumer with Aggregation Logic.
    
    It serves as a buffer:
    1. Reads a message.
    2. Checks the internal 'buffer' using the 'count' as a key.
    3. If 'count' is new: stores message and waits.
    4. If 'count' exists: combines the new message with the stored one and prints the result.
    """
    topic = "data-stream"
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'aggregator-group',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(conf)
    consumer.subscribe([topic])

    print(f"Starting Consumer listening on {topic}...")
    
    # Buffer dictionary: { count_id: [message1_data] }
    msg_buffer = {}

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            
            try:
                current_data = json.loads(msg.value().decode('utf-8'))
                count_id = current_data.get('count')
                
                if count_id is None:
                    print(f"Skipping message without count: {current_data}")
                    continue

                if count_id in msg_buffer:
                    # Partner found! Aggregate.
                    previous_data = msg_buffer.pop(count_id)
                    aggregated = {
                        "id": count_id,
                        "data_1": previous_data,
                        "data_2": current_data,
                        "status": "COMPLETED"
                    }
                    print(f"AGGREGATED OUTPUT: {json.dumps(aggregated)}")
                else:
                    # First arrival, store in buffer
                    msg_buffer[count_id] = current_data
                    # print(f"Buffered count {count_id}, waiting for partner...")

            except Exception as e:
                print(f"Error decoding message: {e}")

    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
