# ClickHouse Kafka Streaming Ingestion

This directory provides two zero-cost, open-source ways to stream cleaned Mandi records into ClickHouse:

---

## Option 1: Native ClickHouse Kafka Engine (Recommended)

ClickHouse includes a built-in Kafka engine with zero external dependencies.
It reads directly from `mandi-prices-clean` and auto-populates `clean_mandi_prices` via a Materialized View:

```sql
-- See clickhouse/sql/06_kafka_engine.sql
CREATE TABLE agripulse_clean.kafka_mandi_prices_queue (...) ENGINE = Kafka ...;
CREATE MATERIALIZED VIEW agripulse_clean.mv_kafka_mandi_prices TO agripulse_clean.clean_mandi_prices ...;
```

---

## Option 2: Python Streaming Ingestor (Direct Micro-Batch)

For instant local execution without configuring external connector containers:

```bash
python -m clickhouse.kafka_connect.clickhouse_stream_consumer --batch-size 50 --max-messages 100
```

---

## Option 3: Official ClickHouse Kafka Connect Sink Connector

For production Kafka Connect clusters, use `clickhouse-sink-connector.json`:

```bash
curl -X POST -H "Content-Type: application/json" \
  --data @clickhouse/kafka_connect/clickhouse-sink-connector.json \
  http://localhost:8083/connectors
```
