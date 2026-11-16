"""
AgriPulse India — Kafka-to-ClickHouse Streaming Bridge
======================================================
Consumes standardized messages from Kafka topic 'mandi-prices-clean' and
streams them into ClickHouse agripulse_clean.clean_mandi_prices.
Provides zero-cost, immediate local testing and continuous ingestion.
"""

import os
import sys
import json
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from confluent_kafka import Consumer, KafkaError
from src.utils.logger import get_logger
from src.streaming.kafka_config import get_kafka_config
from clickhouse.connection import get_client

log = get_logger("ClickHouseStreamConsumer")


def create_clean_consumer(group_id: str = "clickhouse-mandi-ingestor") -> Consumer:
    """Initialize Kafka consumer for clean mandi prices."""
    kafka_cfg = get_kafka_config()
    conf = {
        "bootstrap.servers": kafka_cfg["bootstrap_servers"],
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    return Consumer(conf)


def flush_batch_to_clickhouse(client, records: List[Dict[str, Any]]) -> int:
    """Insert a micro-batch of clean mandi records into ClickHouse."""
    if not records:
        return 0

    rows = []
    for rec in records:
        arr_date = rec.get("arrival_date")
        arr_year = int(arr_date[:4]) if arr_date and len(arr_date) >= 4 else 2026
        arr_month = int(arr_date[5:7]) if arr_date and len(arr_date) >= 7 else 1
        rows.append((
            rec.get("state_name", "UNKNOWN"),
            rec.get("district_name", "UNKNOWN"),
            rec.get("market_name", "UNKNOWN"),
            rec.get("commodity_name", "UNKNOWN"),
            rec.get("variety_name", "Other"),
            rec.get("grade", "Standard"),
            arr_date,
            float(rec.get("min_price_inr_quintal") or 0.0),
            float(rec.get("max_price_inr_quintal") or 0.0),
            float(rec.get("modal_price_inr_quintal") or 0.0),
            arr_year,
            arr_month,
            1,
            "INR",
            "INR per quintal",
            rec.get("_record_key", ""),
            "KAFKA_STREAM",
            datetime.now(timezone.utc),
        ))

    columns = [
        "state_name", "district_name", "market_name", "commodity_name", "variety_name",
        "grade", "arrival_date", "min_price_inr_quintal", "max_price_inr_quintal",
        "modal_price_inr_quintal", "arrival_year", "arrival_month", "is_price_relation_valid",
        "currency", "price_unit", "record_hash", "source_channel", "loaded_at_utc"
    ]

    client.insert(
        table="clean_mandi_prices",
        data=rows,
        column_names=columns,
        database="agripulse_clean"
    )
    return len(rows)


def stream_kafka_to_clickhouse(
    topic: str = "mandi-prices-clean",
    batch_size: int = 100,
    poll_timeout_sec: float = 1.0,
    max_messages: Optional[int] = None,
) -> int:
    """Consume clean messages from Kafka and insert into ClickHouse."""
    consumer = create_clean_consumer()
    consumer.subscribe([topic])
    client = get_client()

    log.info("Starting Kafka-to-ClickHouse stream on topic '%s'...", topic)
    buffer = []
    total_processed = 0

    try:
        while True:
            if max_messages and total_processed >= max_messages:
                log.info("Reached limit of %d messages. Stopping stream test.", max_messages)
                break

            msg = consumer.poll(poll_timeout_sec)
            if msg is None:
                if buffer:
                    flushed = flush_batch_to_clickhouse(client, buffer)
                    consumer.commit()
                    total_processed += flushed
                    log.info("Flushed trailing batch of %d records (total: %d)", flushed, total_processed)
                    buffer.clear()
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    log.error("Kafka error: %s", msg.error())
                    break

            try:
                payload = json.loads(msg.value().decode("utf-8"))
                buffer.append(payload)

                if len(buffer) >= batch_size:
                    flushed = flush_batch_to_clickhouse(client, buffer)
                    consumer.commit()
                    total_processed += flushed
                    log.info("Streamed %d records into ClickHouse (total: %d)", flushed, total_processed)
                    buffer.clear()

            except Exception as parse_err:
                log.warning("Skipping invalid JSON message: %s", parse_err)

    finally:
        if buffer:
            try:
                flushed = flush_batch_to_clickhouse(client, buffer)
                consumer.commit()
                total_processed += flushed
            except Exception as flush_err:
                log.error("Error during trailing flush: %s", flush_err)
        consumer.close()
        client.close()

    log.info("ClickHouse streaming ingestor finished. Total records: %d", total_processed)
    return total_processed


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Stream Mandi prices from Kafka into ClickHouse")
    parser.add_argument("--max-messages", type=int, default=None, help="Stop after N messages")
    parser.add_argument("--batch-size", type=int, default=50, help="Batch size")
    args = parser.parse_args()

    stream_kafka_to_clickhouse(max_messages=args.max_messages, batch_size=args.batch_size)
