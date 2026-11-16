# AgriPulse India – Agricultural Big Data Analytics Platform

A scalable Big Data engineering and analytics platform designed to analyze Indian agricultural dynamics using three foundational data streams: **Government Mandi Wholesale Prices**, **Historical Crop Production**, and **NASA POWER High-Resolution Weather Observations**.

---

## 1. Overall Technology Architecture

```
[Mandi API] ---> [Python Producer] ---> [Kafka: mandi-prices-raw] ---> [Spark Structured Streaming]
                                                                                |
                                               +--------------------------------+-------------------------------+
                                               |                                                                |
                                    [Validation & State Normalization]                               [Diagnostic Rejection]
                                               |                                                                |
                                               v                                                                v
                                   [Kafka: mandi-prices-clean]                                    [Kafka: mandi-prices-invalid]
                                               |
                                               v
                                        [Kafka Connect] ---> [Snowflake DWH] ---> [Power BI Dashboards]
                                                                    ^
[Historical Batch Ingestion] ---> [Batch Cleaning (Phase 1.5)] -----+
  • Crop Production (246K rows)      • Parquet Partitioning
  • NASA POWER (16.18M rows)         • Unit Segregation & Boundary Dedup
```

- **Mandi Wholesale Prices**: Handled via real-time / periodic streaming through Apache Kafka and Spark Structured Streaming.
- **Crop Production & NASA Weather**: Handled via batch cleaning and Parquet partitioning.
- **Downstream Warehouse & BI**: Snowflake and Power BI (subsequent phases).

---

## 2. Directory Structure

```
AgriPulse-India/
├── data/
│   ├── raw/
│   │   ├── mandi/                      ← Raw JSON/CSV fetches from data.gov.in (12,726 rows)
│   │   ├── crop_production/            ← Raw historical crop production CSV (246,091 rows)
│   │   └── nasa_power/                 ← 784 grid-tiled NASA POWER CSVs (16.18M rows, 406.6 MB)
│   ├── interim/                        ← Staging files, state maps & producer state file
│   │   └── producer_seen_records.json  ← Producer deduplication state tracking
│   ├── checkpoints/                    ← Spark Structured Streaming checkpoints
│   │   ├── spark_mandi_clean/          ← Checkpoint dir for clean stream
│   │   └── spark_mandi_invalid/        ← Checkpoint dir for invalid stream
│   └── processed/
│       ├── mandi_prices_clean/         ← Processed Mandi data (Parquet)
│       ├── crop_production_clean/      ← Processed Crop Production (Parquet)
│       └── nasa_weather_clean/         ← Processed NASA Weather (Year-partitioned Parquet)
├── src/
│   ├── ingestion/
│   │   ├── mandi_api.py                ← Paginated Mandi API fetcher
│   │   ├── crop_production.py          ← Raw crop production loader & inspector
│   │   └── nasa_power.py               ← NASA POWER raw scanner & loader
│   ├── batch/
│   │   ├── clean_mandi.py              ← Mandi Price cleaning & validation
│   │   ├── clean_crop_production.py    ← Crop Production cleaning & unit segregation
│   │   ├── clean_nasa_weather.py       ← Memory-efficient NASA tile processor
│   │   ├── standardization.py          ← Text stripping, snake_casing & date utils
│   │   └── data_quality.py             ← Comprehensive before/after quality profiler
│   ├── streaming/
│   │   ├── kafka_config.py             ← Kafka & Spark streaming settings
│   │   ├── schemas.py                  ← Raw, clean, and invalid message schemas
│   │   ├── mandi_producer.py           ← Periodic API polling & Kafka producer
│   │   └── spark_mandi_stream.py       ← Spark Structured Streaming engine
│   └── utils/
│       ├── config.py                   ← Environment & directory settings
│       ├── logger.py                   ← Standardized console & file logging
│       ├── validators.py               ← Core assertion & range check utilities
│       └── state_name_mapping.csv      ← Official GoI state name standardization table
├── kafka/
│   ├── docker-compose.yml              ← KRaft-mode single-broker Kafka setup
│   └── create_topics.py                ← Programmatic Kafka topic provisioning script
├── tests/
│   ├── test_mandi_producer.py          ← Producer metadata & key stability unit tests
│   ├── test_spark_stream.py            ← Spark streaming transformation unit tests
│   ├── test_streaming_pipeline.py      ← 10-step full streaming integration test suite
│   └── test_streaming_results.json     ← Test execution results audit log
├── run_batch_pipeline.py               ← Unified runner for the batch cleaning pipeline
├── state_name_mapping.csv              ← Root reference mapping table
├── data_quality_batch_report.json      ← Consolidated Phase 1.5 pipeline QA report
├── requirements.txt                    ← Python dependencies (including PySpark & Confluent-Kafka)
├── .env.example                        ← Environment configuration template
└── README.md
```

---

## 3. Phase 2: Kafka Streaming & Spark Structured Streaming

### Kafka Topics Setup

| Topic Name | Partitions | Replication | Retention | Purpose |
| :--- | :---: | :---: | :---: | :--- |
| `mandi-prices-raw` | 3 | 1 | 7 days | Ingested raw market events from Producer + metadata |
| `mandi-prices-clean` | 3 | 1 | 14 days | Standardized, validated market records from Spark |
| `mandi-prices-invalid` | 1 | 1 | 30 days | Rejected records with explicit diagnostic failure reason |

#### Starting Kafka via Docker Compose (KRaft Mode)
```bash
# Start Kafka broker in the background
cd kafka
docker compose up -d

# Verify broker is running
docker compose ps

# Create required topics
python create_topics.py

# Stop Kafka when finished
docker compose down
```

### Python Kafka Producer (`src/streaming/mandi_producer.py`)
- **Periodic API Polling**: Fetches from Government `data.gov.in` Mandi API. (Clarification: The government API provides daily wholesale snapshots; polling is configured to periodically query new data).
- **Ingestion Metadata**: Attaches `_message_id` (UUID), `_ingested_at_utc` (ISO 8601), `_source`, `_record_key`, and `_is_synthetic`.
- **Stable Record Key**: Constructs deterministic keys:
  `{state}#{district}#{market}#{commodity}#{variety}#{grade}#{arrival_date}`
- **Duplicate Suppression**: Tracks seen keys in `data/interim/producer_seen_records.json` across restarts.
- **Commands**:
  ```bash
  # Run a single extraction & publish cycle:
  python -m src.streaming.mandi_producer --once

  # Run continuous periodic polling (every 60s):
  python -m src.streaming.mandi_producer --interval 60

  # Inject synthetic test records for pipeline verification:
  python -m src.streaming.mandi_producer --synthetic 10
  ```

### Spark Structured Streaming (`src/streaming/spark_mandi_stream.py`)
- **Engine**: PySpark 3.5.1 Structured Streaming engine connecting to `mandi-prices-raw`.
- **Parsing**: Deserializes JSON payloads with explicit `StructType` schema.
- **Transformations**:
  - State name standardization using Phase 1.5 mapping (`Chattisgarh` → `Chhattisgarh`, `Keralam` → `Kerala`, `Pondicherry` → `Puducherry`).
  - Text trimming across market, district, commodity, variety, and grade.
  - Date parsing from `DD/MM/YYYY` to standard ISO `YYYY-MM-DD`.
  - Double numeric casting for `min_price`, `max_price`, and `modal_price`.
- **Routing**:
  - **Clean Stream**: Valid records (`min_price <= modal_price <= max_price`, positive prices, valid dates) published to `mandi-prices-clean`.
  - **Invalid Stream**: Failed records published to `mandi-prices-invalid` with rejection reason:
    - `INVALID_PRICE_RELATION_MIN_GT_MODAL_OR_MAX`
    - `NON_POSITIVE_PRICE`
    - `INVALID_OR_UNPARSEABLE_DATE`
    - `MISSING_MANDATORY_FIELD`
- **Checkpointing**: Checkpoints saved at `data/checkpoints/spark_mandi_clean` and `data/checkpoints/spark_mandi_invalid` for fault tolerance.
- **Commands**:
  ```bash
  # Run Spark streaming pipeline to Kafka sinks:
  python -m src.streaming.spark_mandi_stream

  # Run with console sink for local verification:
  python -m src.streaming.spark_mandi_stream --console

  # Run single-batch availableNow verification:
  python -m src.streaming.spark_mandi_stream --once --console
  ```

---

## 4. Testing & Verification

A comprehensive 10-step test workflow is provided:

```bash
# Run unit tests for Mandi Producer
python -m unittest tests/test_mandi_producer.py

# Run unit tests for Spark Streaming Transformations
python -m unittest tests/test_spark_stream.py

# Run end-to-end 10-step integration verification suite
python tests/test_streaming_pipeline.py
```

### Verification Checklist:
1. Broker connectivity detection and graceful offline fallback.
2. Topic creation and partition specification verification.
3. Producer ingestion metadata wrapping (`_message_id`, `_ingested_at_utc`).
4. Stable deterministic business key computation.
5. JSON serialization and schema conformity.
6. Spark Structured Streaming deserialization.
7. Clean stream routing with state name normalization.
8. Invalid stream routing with diagnostic error codes.
9. Persistent duplicate record suppression.
10. Checkpoint directory persistence for crash recovery.

---

## 5. Phase 1.5 Batch Pipeline (Reference)

To re-run the batch cleaning pipeline for historical Crop Production and NASA Weather:

```bash
python run_batch_pipeline.py
```

Processed batch data is saved in Snappy Parquet:
- `data/processed/crop_production_clean/crop_production_clean.parquet` (246K rows)
- `data/processed/nasa_weather_clean/year=YYYY/part-0.parquet` (7.12M rows, 2020–2026)
- `data/processed/mandi_prices_clean/mandi_prices_clean.parquet` (12.7K rows)

---

## 6. Phase 3: ClickHouse Open-Source Data Warehouse Integration

The platform integrates all cleaned batch and streaming data into a 100% free, self-hosted **ClickHouse** analytical warehouse via Docker Compose:

### Database Architecture
- **`agripulse_raw` Database**: Landing zone for raw ingestions and pipeline execution tracking (`load_audit`, `raw_mandi_prices`, `raw_crop_production`, `raw_weather`).
- **`agripulse_clean` Database**: Conformed tables utilizing `ReplacingMergeTree` for automatic deduplication (`clean_mandi_prices`, `clean_crop_production`, `clean_weather`, `kafka_mandi_prices_queue`).
- **`agripulse_mart` Database (Star Schema)**:
  - Dimensions: `dim_date` (1995–2035), `dim_location`, `dim_commodity`.
  - Facts: `fact_mandi_prices` (₹/quintal), `fact_crop_production` (Coconut Nuts vs Field Crop Tonnes strictly segregated), `fact_weather` (sub-zero temperatures down to -17°C preserved).

### Quick Execution Commands
```bash
# 1. Start ClickHouse locally (FOSS version)
docker compose -f clickhouse/docker-compose.clickhouse.yml up -d

# 2. Run Master Orchestrator (Auto-deploys DDLs, batch loads, streams, and validates)
python run_phase3_clickhouse.py

# 3. Direct Batch Load
python -m clickhouse.batch_loader --dataset all

# 4. Run Streaming Consumer from Kafka topic mandi-prices-clean
python -m clickhouse.kafka_connect.clickhouse_stream_consumer --batch-size 50

# 5. Run Full Validation Suite
python -m clickhouse.validation
```

---

## 7. Next Steps (Phase 4 Roadmap)

- **Apache Airflow**: Orchestrate daily batch jobs, sensor checks, and pipeline monitoring.
- **Power BI / Analytics**: Connect DirectQuery/Import to Snowflake MART models for dashboard reporting.
