"""
AgriPulse India — Streaming Message Schemas
============================================
Defines Python dictionary specifications and PySpark StructType schemas
for messages flowing through the Kafka streaming pipeline:
  1. Raw Topic (mandi-prices-raw): Original API payload + ingestion metadata
  2. Clean Topic (mandi-prices-clean): Standardized, validated market records
  3. Invalid Topic (mandi-prices-invalid): Rejected records with explicit failure reason
"""

from typing import Dict, Any

# -------------------------------------------------------------------------
# Raw Mandi Message Structure (Published by Python Producer)
# -------------------------------------------------------------------------
RAW_MANDI_FIELDS = [
    "state",
    "district",
    "market",
    "commodity",
    "variety",
    "grade",
    "arrival_date",
    "min_price",
    "max_price",
    "modal_price",
]

RAW_METADATA_FIELDS = [
    "_message_id",
    "_ingested_at_utc",
    "_source",
    "_record_key",
    "_is_synthetic",
]


def get_spark_raw_schema():
    """
    Returns PySpark StructType schema for parsing raw JSON messages
    from mandi-prices-raw. Lazy import to allow module import when PySpark is loading.
    """
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        StringType,
        StructField,
        StructType,
    )

    return StructType([
        StructField("state", StringType(), True),
        StructField("district", StringType(), True),
        StructField("market", StringType(), True),
        StructField("commodity", StringType(), True),
        StructField("variety", StringType(), True),
        StructField("grade", StringType(), True),
        StructField("arrival_date", StringType(), True),
        StructField("min_price", DoubleType(), True),
        StructField("max_price", DoubleType(), True),
        StructField("modal_price", DoubleType(), True),
        # Ingestion metadata
        StructField("_message_id", StringType(), True),
        StructField("_ingested_at_utc", StringType(), True),
        StructField("_source", StringType(), True),
        StructField("_record_key", StringType(), True),
        StructField("_is_synthetic", BooleanType(), True),
    ])


def build_raw_message(record: Dict[str, Any], is_synthetic: bool = False, source: str = "data.gov.in") -> Dict[str, Any]:
    """Helper to wrap raw API record with required ingestion metadata and stable key."""
    import uuid
    from datetime import datetime, timezone

    # Build stable business key: state_district_market_commodity_variety_grade_arrival_date
    key_parts = [
        str(record.get("state", "")).strip().lower(),
        str(record.get("district", "")).strip().lower(),
        str(record.get("market", "")).strip().lower(),
        str(record.get("commodity", "")).strip().lower(),
        str(record.get("variety", "")).strip().lower(),
        str(record.get("grade", "")).strip().lower(),
        str(record.get("arrival_date", "")).strip(),
    ]
    record_key = "#".join(key_parts)

    msg = dict(record)
    msg["_message_id"] = str(uuid.uuid4())
    msg["_ingested_at_utc"] = datetime.now(timezone.utc).isoformat()
    msg["_source"] = source
    msg["_record_key"] = record_key
    msg["_is_synthetic"] = is_synthetic
    return msg
