# AgriPulse India — Snowflake Kafka Connect Integration

This directory contains the Kafka Connect deployment assets and configuration for streaming clean Mandi wholesale prices from Kafka to Snowflake.

---

## 1. Architecture

```
[Mandi API / Producer]
        │
        ▼
[Kafka: mandi-prices-raw]
        │
        ▼
[Spark Structured Streaming (Validation & Standardization)]
        │
        ▼
[Kafka: mandi-prices-clean]
        │
        ├──► [Snowflake Kafka Sink Connector / Streaming Consumer]
        │            │
        │            ▼
        │    [Snowflake: CLEAN.CLEAN_MANDI_PRICES]
        │
        └──► [Kafka: mandi-prices-invalid] (Rejected records with errors)
```

---

## 2. Official Snowflake Kafka Sink Connector Setup

### Prerequisites
- Running Kafka cluster (`PLAINTEXT://kafka:9092` or `localhost:9092`)
- Confluent Platform 7.6.0+ Kafka Connect container
- Snowflake Private Key / RSA key-pair or user password

### Build & Run Kafka Connect Container
```bash
docker compose -f snowflake/kafka_connect/docker-compose.kafka-connect.yml up -d --build
```

### Submit Snowflake Sink Connector Configuration
```bash
curl -X POST -H "Content-Type: application/json" \
  --data @snowflake/kafka_connect/snowflake-sink-connector.json \
  http://localhost:8083/connectors
```

### Check Connector Status
```bash
curl http://localhost:8083/connectors/snowflake-sink-mandi-clean/status
```

---

## 3. Alternative: Direct Python Streaming Bridge

For local development or testing environments where Docker Connect daemon is not running:
```bash
python -m snowflake.kafka_connect.snowflake_stream_consumer --batch-size 50 --max-messages 100
```
This directly streams conformed records from Kafka into `CLEAN.CLEAN_MANDI_PRICES` using the official `snowflake-connector-python` with idempotent MERGE statements.
