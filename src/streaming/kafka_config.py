"""
AgriPulse India — Kafka and Streaming Configuration
=====================================================
Central configuration module for Kafka producers, topics, Spark streaming,
and checkpoint directories. Loads settings from .env with fallback defaults.
"""

import os
from dotenv import load_dotenv

load_dotenv()


class KafkaConfig:
    """Kafka broker and topic configurations."""
    BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    # Topic names as per Phase 2 specifications
    TOPIC_RAW: str = os.getenv("KAFKA_TOPIC_RAW", "mandi-prices-raw")
    TOPIC_CLEAN: str = os.getenv("KAFKA_TOPIC_CLEAN", "mandi-prices-clean")
    TOPIC_INVALID: str = os.getenv("KAFKA_TOPIC_INVALID", "mandi-prices-invalid")

    # Default topic partitions and replication factor for local dev
    DEFAULT_PARTITIONS: int = int(os.getenv("KAFKA_DEFAULT_PARTITIONS", "3"))
    DEFAULT_REPLICATION_FACTOR: int = int(os.getenv("KAFKA_REPLICATION_FACTOR", "1"))
    DEFAULT_RETENTION_MS: int = int(os.getenv("KAFKA_RETENTION_MS", str(7 * 24 * 3600 * 1000)))  # 7 days

    # Producer tuning
    PRODUCER_CONFIG = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": "all",
        "retries": 3,
        "enable.idempotence": True,
        "compression.type": "gzip",
        "batch.size": 16384,
        "linger.ms": 20,
    }


class StreamingConfig:
    """Spark Structured Streaming and Polling Configurations."""
    # Polling interval for periodic Mandi API extraction
    POLL_INTERVAL_SECONDS: int = int(os.getenv("STREAMING_POLL_INTERVAL_SEC", "60"))
    MAX_RECORDS_PER_POLL: int = int(os.getenv("STREAMING_MAX_RECORDS_PER_POLL", "1000"))

    # State file to track published record signatures across restarts
    PRODUCER_STATE_FILE: str = os.path.join("data", "interim", "producer_seen_records.json")

    # Spark Structured Streaming engine settings
    SPARK_APP_NAME: str = "AgriPulse-MandiPrices-StructuredStreaming"
    SPARK_MASTER: str = os.getenv("SPARK_MASTER", "local[*]")
    CHECKPOINT_DIR_CLEAN: str = os.path.join("data", "checkpoints", "spark_mandi_clean")
    CHECKPOINT_DIR_INVALID: str = os.path.join("data", "checkpoints", "spark_mandi_invalid")

    # Processing trigger interval
    TRIGGER_INTERVAL: str = os.getenv("SPARK_TRIGGER_INTERVAL", "5 seconds")
    WATERMARK_DELAY: str = os.getenv("SPARK_WATERMARK_DELAY", "10 minutes")

    # Maven package for Spark Kafka integration
    # For Spark 3.5.x with Scala 2.12:
    KAFKA_PACKAGE: str = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
