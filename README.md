# Kafka Backbone

This project demonstrates a Kafka-based data integration architecture using KRaft mode, with simple Python producers and consumers.

## Structure
- `src/producer`: Contains the producer implementation.
- `src/consumer`: Contains the consumer implementation.
- `docker-compose.yml`: Defines the Kafka infrastructure.

## Usage
1. Start the infrastructure:
   ```bash
   docker compose up -d
   ```

2. Run a producer:
   ```bash
   python src/producer/producer.py --id 1
   ```

3. Run the consumer:
   ```bash
   python src/consumer/consumer.py
   ```
