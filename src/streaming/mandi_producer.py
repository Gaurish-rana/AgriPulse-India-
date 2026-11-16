"""
AgriPulse India — Mandi Price Kafka Producer
==============================================
Periodically polls the Government of India data.gov.in Mandi Price API
and streams each wholesale price record to Kafka topic: mandi-prices-raw.

Architectural Clarification:
    The Government data.gov.in API provides daily wholesale price snapshots
    rather than genuine millisecond-level tick data. This producer acts as a
    PERIODIC API POLLING PRODUCER that fetches latest market records, wraps each
    record with ingestion metadata and a stable business key, deduplicates against
    previously published record signatures, and publishes events into Kafka.

Features:
    - Reuses existing Mandi API integration and headers from src.ingestion.mandi_api
    - Ingestion metadata attached: _message_id, _ingested_at_utc, _source, _record_key, _is_synthetic
    - Persistent state tracking (data/interim/producer_seen_records.json) to prevent duplicate publishing
    - Resilient error handling for API timeouts, pagination, rate limits, and Kafka broker outages
    - Supports live API extraction as well as synthetic test event generation for local validation

Usage:
    # Run a single extraction & publish cycle:
    python -m src.streaming.mandi_producer --once

    # Run continuous periodic polling:
    python -m src.streaming.mandi_producer --interval 60

    # Publish synthetic test records for pipeline verification:
    python -m src.streaming.mandi_producer --synthetic 10
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Set, Tuple

# Ensure project root in sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from confluent_kafka import Producer, KafkaError
from src.ingestion.mandi_api import fetch_page
from src.streaming.kafka_config import KafkaConfig, StreamingConfig
from src.streaming.schemas import build_raw_message
from src.utils.config import MandiConfig
from src.utils.logger import get_logger

log = get_logger("mandi_producer")


class MandiKafkaProducer:
    """Kafka producer client for streaming Mandi wholesale price events."""

    def __init__(self, bootstrap_servers: str = None, topic: str = None, state_file: str = None):
        self.bootstrap_servers = bootstrap_servers or KafkaConfig.BOOTSTRAP_SERVERS
        self.topic = topic or KafkaConfig.TOPIC_RAW
        self.state_file = state_file or StreamingConfig.PRODUCER_STATE_FILE
        self.seen_record_keys: Set[str] = self._load_state()

        # Initialize Confluent Kafka Producer
        conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "acks": "all",
            "retries": 3,
            "enable.idempotence": True,
            "compression.type": "gzip",
            "linger.ms": 20,
        }
        try:
            self.producer = Producer(conf)
            log.info("Kafka Producer initialized for broker: %s, topic: %s", self.bootstrap_servers, self.topic)
        except Exception as e:
            log.error("Failed to initialize Kafka Producer: %s", e)
            raise

        self._delivery_success = 0
        self._delivery_errors = 0

    def _delivery_callback(self, err, msg):
        """Callback invoked upon message delivery or permanent failure."""
        if err is not None:
            log.error("Message delivery failed for key %s: %s", msg.key(), err)
            self._delivery_errors += 1
        else:
            self._delivery_success += 1

    def _load_state(self) -> Set[str]:
        """Load previously published record keys from disk to prevent duplicate publishing."""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    keys = set(data.get("seen_keys", []))
                    log.info("Loaded %d previously seen record keys from %s", len(keys), self.state_file)
                    return keys
            except Exception as e:
                log.warning("Could not read producer state file (%s). Starting fresh. Error: %s", self.state_file, e)
        return set()

    def _save_state(self):
        """Persist seen record keys to disk."""
        os.makedirs(os.path.dirname(os.path.abspath(self.state_file)), exist_ok=True)
        try:
            with open(self.state_file, "w", encoding="utf-8") as f:
                # Store latest 50,000 keys to keep state file bounded
                keys_list = list(self.seen_record_keys)[-50000:]
                json.dump({"updated_at": datetime.now(timezone.utc).isoformat(), "seen_keys": keys_list}, f)
        except Exception as e:
            log.error("Failed to persist producer state file: %s", e)

    def publish_records(self, records: List[Dict[str, Any]], is_synthetic: bool = False) -> Tuple[int, int]:
        """
        Wrap raw records with metadata and stream to Kafka.
        Returns: (published_count, skipped_duplicate_count)
        """
        published = 0
        skipped = 0

        for r in records:
            wrapped = build_raw_message(r, is_synthetic=is_synthetic)
            record_key = wrapped["_record_key"]

            # Deduplication against seen records
            if record_key in self.seen_record_keys and not is_synthetic:
                skipped += 1
                continue

            payload_bytes = json.dumps(wrapped, ensure_ascii=False).encode("utf-8")
            key_bytes = record_key.encode("utf-8")

            try:
                self.producer.produce(
                    topic=self.topic,
                    key=key_bytes,
                    value=payload_bytes,
                    callback=self._delivery_callback,
                )
                self.producer.poll(0)  # Serve delivery report callbacks
                published += 1
                self.seen_record_keys.add(record_key)
            except BufferError:
                log.warning("Producer queue full; flushing messages...")
                self.producer.flush(timeout=5)
                self.producer.produce(
                    topic=self.topic,
                    key=key_bytes,
                    value=payload_bytes,
                    callback=self._delivery_callback,
                )
                published += 1
                self.seen_record_keys.add(record_key)
            except Exception as exc:
                log.error("Failed to produce record to Kafka: %s", exc)

        # Flush remaining delivery callbacks
        self.producer.flush(timeout=10)
        self._save_state()
        log.info(
            "Batch published: %d messages sent, %d existing duplicates skipped (Success: %d, Errors: %d)",
            published, skipped, self._delivery_success, self._delivery_errors,
        )
        return published, skipped

    def poll_and_publish_api(self, max_records: int = 1000, page_size: int = 500) -> int:
        """
        Poll data.gov.in Mandi API and stream fetched records into Kafka.
        """
        log.info("Polling Mandi Prices API (max_records=%d, page_size=%d)...", max_records, page_size)
        total_published = 0
        offset = 0

        while offset < max_records:
            try:
                page_data = fetch_page(offset=offset, limit=page_size)
            except Exception as e:
                log.error("API fetch error at offset %d: %s. Aborting current poll cycle.", offset, e)
                break

            records = page_data.get("records", [])
            total_available = int(page_data.get("total", 0))

            if not records:
                log.info("No records returned from API at offset %d.", offset)
                break

            pub, skip = self.publish_records(records, is_synthetic=False)
            total_published += pub
            offset += page_size

            if offset >= total_available:
                log.info("Reached end of available API records (%d).", total_available)
                break

            time.sleep(0.5)  # Polite delay between API calls

        return total_published

    def publish_synthetic_events(self, count: int = 10, include_invalid: bool = True) -> int:
        """
        Publish clearly labeled synthetic events for local testing and pipeline verification.
        Never mixed with real data without explicit _is_synthetic flag.
        """
        import random
        log.info("Generating %d synthetic Mandi events for local testing...", count)

        states = ["Punjab", "Haryana", "Maharashtra", "Keralam", "Chattisgarh"]
        commodities = ["Wheat", "Paddy(Common)", "Onion", "Tomato", "Cotton"]
        grades = ["FAQ", "Medium", "Large"]

        test_records = []
        today_str = datetime.now().strftime("%d/%m/%Y")

        for i in range(count):
            state = random.choice(states)
            comm = random.choice(commodities)
            base_price = random.randint(1500, 5000)

            rec = {
                "state": state,
                "district": f"TestDistrict_{i % 5}",
                "market": f"APMC_Market_{i % 3}",
                "commodity": comm,
                "variety": "Common",
                "grade": random.choice(grades),
                "arrival_date": today_str,
                "min_price": float(base_price),
                "max_price": float(base_price + random.randint(100, 500)),
                "modal_price": float(base_price + 50),
            }

            # If include_invalid is true, deliberately inject invalid cases for test validation:
            if include_invalid and i == 0:
                # Inverted price violation: min > max
                rec["min_price"] = 9999.0
                rec["max_price"] = 1000.0
                rec["modal_price"] = 5000.0
            elif include_invalid and i == 1:
                # Invalid price: negative price
                rec["min_price"] = -500.0
            elif include_invalid and i == 2:
                # Unparseable date
                rec["arrival_date"] = "INVALID_DATE_99/99/9999"

            test_records.append(rec)

        pub, skip = self.publish_records(test_records, is_synthetic=True)
        return pub


def main():
    parser = argparse.ArgumentParser(description="AgriPulse India Mandi Price Kafka Producer")
    parser.add_argument("--once", action="store_true", help="Run a single poll cycle from API and exit")
    parser.add_argument("--interval", type=int, default=StreamingConfig.POLL_INTERVAL_SECONDS, help="Periodic polling interval in seconds")
    parser.add_argument("--max-records", type=int, default=StreamingConfig.MAX_RECORDS_PER_POLL, help="Max records per poll cycle")
    parser.add_argument("--synthetic", type=int, default=0, help="Publish N synthetic test records instead of polling live API")
    parser.add_argument("--bootstrap-servers", type=str, default=KafkaConfig.BOOTSTRAP_SERVERS, help="Kafka bootstrap broker list")
    args = parser.parse_args()

    producer = MandiKafkaProducer(bootstrap_servers=args.bootstrap_servers)

    if args.synthetic > 0:
        print(f"\nPublishing {args.synthetic} synthetic test records to {KafkaConfig.TOPIC_RAW}...")
        n = producer.publish_synthetic_events(count=args.synthetic, include_invalid=True)
        print(f"✓ Successfully published {n} test records to Kafka.")
        return

    if args.once:
        print(f"\nRunning single Mandi API extraction cycle...")
        pub = producer.poll_and_publish_api(max_records=args.max_records)
        print(f"✓ Cycle finished: {pub} records published to {KafkaConfig.TOPIC_RAW}.")
        return

    print(f"\nStarting continuous Mandi API polling daemon (interval: {args.interval}s)...")
    try:
        while True:
            producer.poll_and_publish_api(max_records=args.max_records)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nProducer stopped by user.")


if __name__ == "__main__":
    main()
