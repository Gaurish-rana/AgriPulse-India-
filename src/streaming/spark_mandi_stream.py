"""
AgriPulse India — Spark Structured Streaming Pipeline for Mandi Prices
========================================================================
Connects to Kafka raw topic (mandi-prices-raw), parses incoming JSON records,
validates market pricing constraints, standardizes schema and state names,
and forks records into:
  1. Clean Stream   -> Published to Kafka topic: mandi-prices-clean
  2. Invalid Stream -> Published to Kafka topic: mandi-prices-invalid (with rejection reason)

Features:
  - Checkpointing for fault-tolerance and crash recovery
  - Zero silent record drops (invalid events captured with diagnostic failure code)
  - Reuses state name standardization rules from Phase 1.5
  - Configurable trigger processing interval (default 5 seconds)
  - Dual sink modes: Kafka sink (production) and Console sink (local testing/inspection)

Usage:
    # Run streaming pipeline with Kafka sink:
    python -m src.streaming.spark_mandi_stream

    # Run with console sink for local verification:
    python -m src.streaming.spark_mandi_stream --console

    # Run single-batch test verification:
    python -m src.streaming.spark_mandi_stream --once --console
"""

import argparse
import os
import sys

# Ensure Python executable matches current environment on Windows to avoid Microsoft Store stub
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Ensure project root in sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.streaming.kafka_config import KafkaConfig, StreamingConfig
from src.streaming.schemas import get_spark_raw_schema
from src.utils.logger import get_logger

log = get_logger("spark_mandi_stream")


def create_spark_session(app_name: str = None, master: str = None, include_kafka_package: bool = True):
    """Initialize and configure PySpark Session."""
    from pyspark.sql import SparkSession

    app_name = app_name or StreamingConfig.SPARK_APP_NAME
    master = master or StreamingConfig.SPARK_MASTER

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.driver.host", "localhost")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    )

    if include_kafka_package:
        # Download/include official Spark-Kafka connector
        builder = builder.config("spark.jars.packages", StreamingConfig.KAFKA_PACKAGE)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark Session created: %s (version: %s)", app_name, spark.version)
    return spark


def build_streaming_transformation(df_raw_stream):
    """
    Transforms and validates incoming raw Mandi messages.
    Returns: (df_clean_stream, df_invalid_stream)
    """
    from pyspark.sql.functions import (
        col,
        current_timestamp,
        date_format,
        from_json,
        initcap,
        lower,
        struct,
        to_date,
        to_json,
        trim,
        when,
    )

    raw_schema = get_spark_raw_schema()

    # 1. Parse JSON payload from Kafka 'value' column
    parsed_df = (
        df_raw_stream.selectExpr("CAST(key AS STRING) as raw_key", "CAST(value AS STRING) as raw_json")
        .select(col("raw_key"), col("raw_json"), from_json(col("raw_json"), raw_schema).alias("data"))
        .select("raw_key", "raw_json", "data.*")
    )

    # 2. Standardize State Names (Phase 1.5 logic)
    cleaned_state = (
        when(lower(trim(col("state"))) == "keralam", "Kerala")
        .when(lower(trim(col("state"))) == "chattisgarh", "Chhattisgarh")
        .when(lower(trim(col("state"))) == "pondicherry", "Puducherry")
        .when(lower(trim(col("state"))) == "orissa", "Odisha")
        .when(lower(trim(col("state"))) == "uttaranchal", "Uttarakhand")
        .otherwise(initcap(trim(col("state"))))
    )

    # 3. Standardize Text & Parse Dates
    standardized_df = (
        parsed_df.withColumn("state_name", cleaned_state)
        .withColumn("district_name", initcap(trim(col("district"))))
        .withColumn("market_name", trim(col("market")))
        .withColumn("commodity_name", trim(col("commodity")))
        .withColumn("variety_name", trim(col("variety")))
        .withColumn("grade_name", trim(col("grade")))
        .withColumn("arrival_date_parsed", to_date(trim(col("arrival_date")), "d/M/yyyy"))
        .withColumn("min_price_inr_quintal", col("min_price").cast("double"))
        .withColumn("max_price_inr_quintal", col("max_price").cast("double"))
        .withColumn("modal_price_inr_quintal", col("modal_price").cast("double"))
    )

    # 4. Formulate Validation Rules
    has_valid_date = col("arrival_date_parsed").isNotNull()
    has_mandatory_fields = (
        col("state_name").isNotNull()
        & col("commodity_name").isNotNull()
        & col("market_name").isNotNull()
    )
    has_positive_prices = (
        col("min_price_inr_quintal").isNotNull()
        & col("max_price_inr_quintal").isNotNull()
        & col("modal_price_inr_quintal").isNotNull()
        & (col("min_price_inr_quintal") > 0)
        & (col("max_price_inr_quintal") > 0)
        & (col("modal_price_inr_quintal") > 0)
    )
    has_valid_price_relation = (
        (col("min_price_inr_quintal") <= col("modal_price_inr_quintal"))
        & (col("modal_price_inr_quintal") <= col("max_price_inr_quintal"))
    )

    is_valid_record = (
        has_valid_date
        & has_mandatory_fields
        & has_positive_prices
        & has_valid_price_relation
    )

    # Diagnostic Rejection Reason
    rejection_reason = (
        when(~has_valid_date, "INVALID_OR_UNPARSEABLE_DATE")
        .when(~has_mandatory_fields, "MISSING_MANDATORY_FIELD")
        .when(~has_positive_prices, "NON_POSITIVE_PRICE")
        .when(~has_valid_price_relation, "INVALID_PRICE_RELATION_MIN_GT_MODAL_OR_MAX")
        .otherwise("UNKNOWN_VALIDATION_FAILURE")
    )

    df_evaluated = standardized_df.withColumn("is_valid", is_valid_record).withColumn(
        "rejection_reason", when(~col("is_valid"), rejection_reason).otherwise(None)
    )

    # ---------------------------------------------------------------------
    # Clean Stream Preparation
    # ---------------------------------------------------------------------
    df_clean_payload = (
        df_evaluated.filter(col("is_valid"))
        .select(
            col("_record_key").alias("key"),
            struct(
                col("state_name"),
                col("district_name"),
                col("market_name"),
                col("commodity_name"),
                col("variety_name"),
                col("grade_name").alias("grade"),
                date_format(col("arrival_date_parsed"), "yyyy-MM-dd").alias("arrival_date"),
                col("min_price_inr_quintal"),
                col("max_price_inr_quintal"),
                col("modal_price_inr_quintal"),
                current_timestamp().alias("processed_at_utc"),
                col("_message_id"),
                col("_ingested_at_utc"),
                col("_is_synthetic"),
            ).alias("clean_data"),
        )
        .select(col("key"), to_json(col("clean_data")).alias("value"))
    )

    # ---------------------------------------------------------------------
    # Invalid Stream Preparation
    # ---------------------------------------------------------------------
    df_invalid_payload = (
        df_evaluated.filter(~col("is_valid"))
        .select(
            col("raw_key").alias("key"),
            struct(
                col("raw_json").alias("raw_payload"),
                col("rejection_reason"),
                current_timestamp().alias("rejected_at_utc"),
                col("_message_id"),
                col("_is_synthetic"),
            ).alias("invalid_data"),
        )
        .select(col("key"), to_json(col("invalid_data")).alias("value"))
    )

    return df_clean_payload, df_invalid_payload


def run_streaming_pipeline(
    bootstrap_servers: str = None,
    raw_topic: str = None,
    clean_topic: str = None,
    invalid_topic: str = None,
    console_sink: bool = False,
    once: bool = False,
):
    """Run Spark Structured Streaming engine."""
    servers = bootstrap_servers or KafkaConfig.BOOTSTRAP_SERVERS
    raw_topic = raw_topic or KafkaConfig.TOPIC_RAW
    clean_topic = clean_topic or KafkaConfig.TOPIC_CLEAN
    invalid_topic = invalid_topic or KafkaConfig.TOPIC_INVALID

    log.info("Starting Spark Mandi Streaming Pipeline...")
    log.info("Kafka Broker: %s | Source Topic: %s", servers, raw_topic)

    spark = create_spark_session(include_kafka_package=True)

    # Read from Kafka raw topic
    df_raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", servers)
        .option("subscribe", raw_topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    df_clean, df_invalid = build_streaming_transformation(df_raw_stream)

    os.makedirs(StreamingConfig.CHECKPOINT_DIR_CLEAN, exist_ok=True)
    os.makedirs(StreamingConfig.CHECKPOINT_DIR_INVALID, exist_ok=True)

    if console_sink:
        log.info("Configured for CONSOLE output (local inspection mode)")
        query_clean = (
            df_clean.writeStream.outputMode("append")
            .format("console")
            .option("truncate", "false")
            .queryName("mandi_clean_console")
        )
        query_invalid = (
            df_invalid.writeStream.outputMode("append")
            .format("console")
            .option("truncate", "false")
            .queryName("mandi_invalid_console")
        )
    else:
        log.info("Configured for KAFKA output: clean -> %s, invalid -> %s", clean_topic, invalid_topic)
        query_clean = (
            df_clean.writeStream.outputMode("append")
            .format("kafka")
            .option("kafka.bootstrap.servers", servers)
            .option("topic", clean_topic)
            .option("checkpointLocation", StreamingConfig.CHECKPOINT_DIR_CLEAN)
            .queryName("mandi_clean_kafka")
        )
        query_invalid = (
            df_invalid.writeStream.outputMode("append")
            .format("kafka")
            .option("kafka.bootstrap.servers", servers)
            .option("topic", invalid_topic)
            .option("checkpointLocation", StreamingConfig.CHECKPOINT_DIR_INVALID)
            .queryName("mandi_invalid_kafka")
        )

    if once:
        q1 = query_clean.trigger(availableNow=True).start()
        q2 = query_invalid.trigger(availableNow=True).start()
        q1.awaitTermination()
        q2.awaitTermination()
        log.info("Single-batch availableNow execution completed.")
    else:
        q1 = query_clean.trigger(processingTime=StreamingConfig.TRIGGER_INTERVAL).start()
        q2 = query_invalid.trigger(processingTime=StreamingConfig.TRIGGER_INTERVAL).start()
        log.info("Streams active. Waiting for termination...")
        spark.streams.awaitAnyTermination()


def main():
    parser = argparse.ArgumentParser(description="AgriPulse India Spark Structured Streaming for Mandi Prices")
    parser.add_argument("--console", action="store_true", help="Print streaming outputs to console instead of Kafka")
    parser.add_argument("--once", action="store_true", help="Process currently available data in one microbatch and stop")
    parser.add_argument("--bootstrap-servers", type=str, default=KafkaConfig.BOOTSTRAP_SERVERS, help="Kafka broker address")
    parser.add_argument("--raw-topic", type=str, default=KafkaConfig.TOPIC_RAW, help="Source Kafka topic")
    parser.add_argument("--clean-topic", type=str, default=KafkaConfig.TOPIC_CLEAN, help="Clean Kafka topic")
    parser.add_argument("--invalid-topic", type=str, default=KafkaConfig.TOPIC_INVALID, help="Invalid Kafka topic")
    args = parser.parse_args()

    run_streaming_pipeline(
        bootstrap_servers=args.bootstrap_servers,
        raw_topic=args.raw_topic,
        clean_topic=args.clean_topic,
        invalid_topic=args.invalid_topic,
        console_sink=args.console,
        once=args.once,
    )


if __name__ == "__main__":
    main()
