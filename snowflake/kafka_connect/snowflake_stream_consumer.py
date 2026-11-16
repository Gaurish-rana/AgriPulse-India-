"""
AgriPulse India — Kafka-to-Snowflake Streaming Bridge / Micro-Batch Ingestor
=============================================================================
Consumes standardized messages from Kafka topic 'mandi-prices-clean' and
loads them into Snowflake CLEAN.CLEAN_MANDI_PRICES with audit logging.

Provides an immediate, production-grade micro-batching mechanism and validation
tooling compatible with Confluent Kafka and Snowflake.
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
from snowflake.connection import get_snowflake_connection, validate_config

log = get_logger(__name__)


def create_clean_mandi_consumer(group_id: str = "snowflake-mandi-ingestor") -> Consumer:
    """Initialize Kafka consumer listening to mandi-prices-clean."""
    kafka_cfg = get_kafka_config()
    conf = {
        "bootstrap.servers": kafka_cfg["bootstrap_servers"],
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    return Consumer(conf)


def flush_batch_to_snowflake(conn, records: List[Dict[str, Any]]) -> int:
    """
    Idempotently merge a batch of clean mandi records into CLEAN.CLEAN_MANDI_PRICES.
    Returns the count of merged records.
    """
    if not records:
        return 0

    cursor = conn.cursor()
    merge_sql = """
        MERGE INTO CLEAN.CLEAN_MANDI_PRICES target
        USING (
            SELECT
                %s AS state_name,
                %s AS district_name,
                %s AS market_name,
                %s AS commodity_name,
                %s AS variety_name,
                %s AS grade,
                %s::DATE AS arrival_date,
                %s::NUMBER(10,2) AS min_price_inr_quintal,
                %s::NUMBER(10,2) AS max_price_inr_quintal,
                %s::NUMBER(10,2) AS modal_price_inr_quintal,
                YEAR(%s::DATE) AS arrival_year,
                MONTH(%s::DATE) AS arrival_month,
                TRUE AS is_price_relation_valid,
                'INR' AS currency,
                'INR per quintal' AS price_unit,
                %s AS record_hash,
                'KAFKA_STREAM' AS source_channel,
                CURRENT_TIMESTAMP() AS loaded_at_utc
        ) source
        ON target.state_name = source.state_name
           AND target.district_name = source.district_name
           AND target.market_name = source.market_name
           AND target.commodity_name = source.commodity_name
           AND target.variety_name = source.variety_name
           AND target.grade = source.grade
           AND target.arrival_date = source.arrival_date
        WHEN MATCHED THEN UPDATE SET
            min_price_inr_quintal = source.min_price_inr_quintal,
            max_price_inr_quintal = source.max_price_inr_quintal,
            modal_price_inr_quintal = source.modal_price_inr_quintal,
            loaded_at_utc = source.loaded_at_utc
        WHEN NOT MATCHED THEN INSERT (
            state_name, district_name, market_name, commodity_name, variety_name,
            grade, arrival_date, min_price_inr_quintal, max_price_inr_quintal,
            modal_price_inr_quintal, arrival_year, arrival_month, is_price_relation_valid,
            currency, price_unit, record_hash, source_channel, loaded_at_utc
        ) VALUES (
            source.state_name, source.district_name, source.market_name, source.commodity_name,
            source.variety_name, source.grade, source.arrival_date, source.min_price_inr_quintal,
            source.max_price_inr_quintal, source.modal_price_inr_quintal, source.arrival_year,
            source.arrival_month, source.is_price_relation_valid, source.currency,
            source.price_unit, source.record_hash, source.source_channel, source.loaded_at_utc
        )
    """

    inserted_count = 0
    for rec in records:
        arr_date = rec.get("arrival_date")
        rec_hash = rec.get("_record_key", "")
        params = (
            rec.get("state_name", "UNKNOWN"),
            rec.get("district_name", "UNKNOWN"),
            rec.get("market_name", "UNKNOWN"),
            rec.get("commodity_name", "UNKNOWN"),
            rec.get("variety_name", "Other"),
            rec.get("grade", "Standard"),
            arr_date,
            rec.get("min_price_inr_quintal"),
            rec.get("max_price_inr_quintal"),
            rec.get("modal_price_inr_quintal"),
            arr_date,
            arr_date,
            rec_hash,
        )
        cursor.execute(merge_sql, params)
        inserted_count += 1

    cursor.close()
    return inserted_count


def stream_kafka_to_snowflake(
    topic: str = "mandi-prices-clean",
    batch_size: int = 100,
    poll_timeout_sec: float = 1.0,
    max_messages: Optional[int] = None,
) -> int:
    """
    Consume from Kafka clean topic and stream into Snowflake.
    Can be run continuously or in bounded test mode with max_messages.
    """
    if not validate_config():
        raise ValueError("Snowflake credentials missing. Cannot run streaming ingestor.")

    consumer = create_clean_mandi_consumer()
    consumer.subscribe([topic])
    conn = get_snowflake_connection()

    log.info("Started Kafka-to-Snowflake streaming ingestor on topic '%s'...", topic)
    buffer = []
    total_processed = 0

    try:
        while True:
            if max_messages and total_processed >= max_messages:
                log.info("Reached target limit of %d messages. Finishing stream test.", max_messages)
                break

            msg = consumer.poll(poll_timeout_sec)
            if msg is None:
                if buffer:
                    # Flush pending buffer
                    merged = flush_batch_to_snowflake(conn, buffer)
                    consumer.commit()
                    total_processed += merged
                    log.info("Flushed trailing batch of %d records to Snowflake (total: %d)", merged, total_processed)
                    buffer.clear()
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    log.error("Kafka consumer error: %s", msg.error())
                    break

            try:
                payload = json.loads(msg.value().decode("utf-8"))
                buffer.append(payload)

                if len(buffer) >= batch_size:
                    merged = flush_batch_to_snowflake(conn, buffer)
                    consumer.commit()
                    total_processed += merged
                    log.info("Streamed batch of %d records into Snowflake (total: %d)", merged, total_processed)
                    buffer.clear()

            except Exception as parse_err:
                log.warning("Skipping unparseable message: %s", parse_err)

    finally:
        if buffer:
            try:
                merged = flush_batch_to_snowflake(conn, buffer)
                consumer.commit()
                total_processed += merged
            except Exception as flush_err:
                log.error("Error during final flush: %s", flush_err)
        consumer.close()
        conn.close()

    log.info("Kafka-to-Snowflake ingestor stopped. Total processed: %d records.", total_processed)
    return total_processed


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Stream Mandi prices from Kafka into Snowflake")
    parser.add_argument("--max-messages", type=int, default=None, help="Stop after processing N messages (test mode)")
    parser.add_argument("--batch-size", type=int, default=50, help="Micro-batch size")
    args = parser.parse_args()

    stream_kafka_to_snowflake(max_messages=args.max_messages, batch_size=args.batch_size)
