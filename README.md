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
                                [ClickHouse Stream Consumer]
                                               |
                                               v
[Historical Batch Ingestion] ---> [ClickHouse Data Warehouse (Local Docker)] ---> [Power BI Dashboards (Phase 5)]
  • Crop Production (246K rows)      • agripulse_raw (audit & staging)
  • NASA POWER (7.12M rows)          • agripulse_clean (ReplacingMergeTree conformed)
                                     • agripulse_mart (Star Schema Facts & Dims)
                                               ^
                                               |
                                 [Apache Airflow Orchestration]
                                   • 5 Automated Pipelines & Monitoring
```

- **Mandi Wholesale Prices**: Handled via real-time / periodic streaming through Apache Kafka, Spark Structured Streaming, and ClickHouse Stream Ingestion.
- **Crop Production & NASA Weather**: Handled via batch cleaning, Parquet partitioning, and Apache Arrow streaming into ClickHouse.
- **Data Warehouse**: Free, open-source, self-hosted ClickHouse (databases: `agripulse_raw`, `agripulse_clean`, `agripulse_mart`).
- **Orchestration & Monitoring**: Apache Airflow running on Docker with 5 dedicated DAGs and a centralized Web UI.
- **Downstream BI**: Power BI connected via native ClickHouse ODBC/REST (Phase 5).

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

## 7. Phase 4: Apache Airflow Orchestration & Workflow Monitoring

AgriPulse India uses **Apache Airflow** (LocalExecutor with PostgreSQL backend running via Docker Compose) to coordinate periodic ingestion, manage incremental batch loads, execute data quality assertions, and monitor distributed infrastructure components.

### Airflow Pipeline Architecture

| DAG ID | Schedule | Purpose & Core Tasks |
|---|---|---|
| `mandi_price_pipeline` | `0 */4 * * *` (Every 4 hrs) | 1. Mandi API endpoint reachability check.<br>2. Mandi Kafka Producer polling (respects `data/interim/producer_seen_records.json`).<br>3. Verifies clean streaming records in ClickHouse `agripulse_clean.clean_mandi_prices`.<br>4. Outputs `reports/airflow/mandi_pipeline_report.json`. |
| `crop_production_pipeline` | `0 2 * * *` (Daily at 02:00 UTC) | 1. Scans `data/raw/crop_production/` using fingerprint hash (`data/interim/processed_crop_files.json`).<br>2. **Strict Rule**: Skips execution when no files changed (never reloads existing 246,091 records).<br>3. Cleans, standardizes state names, validates units (Nuts vs Tonnes), and updates warehouse.<br>4. Outputs `reports/airflow/crop_pipeline_report.json`. |
| `weather_pipeline` | `0 3 * * *` (Daily at 03:00 UTC) | 1. Scans `data/raw/nasa_power/` against `data/interim/processed_weather_files.json`.<br>2. **Strict Rule**: Skips execution when all 784 tiles are processed (prevents re-reading 7.12M records).<br>3. Cleans new tiles, preserves negative temperatures (-34.6°C), partitions Parquet, and loads via Arrow streaming.<br>4. Outputs `reports/airflow/weather_pipeline_report.json`. |
| `clickhouse_quality_pipeline` | `0 4 * * *` (Daily at 04:00 UTC) | 1. Validates all 3 ClickHouse databases (`agripulse_raw`, `agripulse_clean`, `agripulse_mart`).<br>2. Checks row counts against strict historical benchmarks (`>= 12,726` Mandi, `>= 246,091` Crop, `>= 7,121,520` Weather).<br>3. Validates dimensional key integrity and ensures 0 unmapped foreign keys.<br>4. Validates unit segregation (Coconuts in Nuts, other crops in Tonnes) and negative temperatures.<br>5. Refreshes analytical marts and outputs `reports/airflow/clickhouse_quality_report.json`. |
| `infrastructure_monitoring` | `*/15 * * * *` (Every 15 mins) | 1. Apache Kafka Broker health, connectivity, and topic/partition checks.<br>2. Spark Structured Streaming checkpoint directory and runtime health.<br>3. ClickHouse HTTP (8123) / Native (9000) health and database readiness.<br>4. Outputs `reports/airflow/infrastructure_health_report.json`. |

### Airflow Web Interface & Access
- **Web UI URL**: [http://localhost:8080](http://localhost:8080)
- **Default Credentials**: `admin` / `admin`
- **Container Architecture**:
  - `agripulse-airflow-webserver`: Airflow UI service exposed on host port 8080.
  - `agripulse-airflow-scheduler`: Airflow job scheduler and task monitor.
  - `agripulse-airflow-postgres`: PostgreSQL 15 metadata database.
  - `agripulse-airflow-init`: Auto-initialization database migration and admin user creation.
  - Network: Connected to shared bridge `agripulse-net` alongside `agripulse-clickhouse` and `agripulse-kafka`.

### Quick Commands for Airflow

```bash
# 1. Start Airflow services
docker compose -f airflow/docker-compose.yml up -d

# 2. List registered DAGs (0 import errors)
docker exec agripulse-airflow-scheduler airflow dags list

# 3. Test run any DAG manually from CLI
docker exec agripulse-airflow-scheduler airflow dags test infrastructure_monitoring
docker exec agripulse-airflow-scheduler airflow dags test clickhouse_quality_pipeline
docker exec agripulse-airflow-scheduler airflow dags test crop_production_pipeline
docker exec agripulse-airflow-scheduler airflow dags test weather_pipeline
docker exec agripulse-airflow-scheduler airflow dags test mandi_price_pipeline

# 4. View Airflow container logs
docker compose -f airflow/docker-compose.yml logs -f
```

---

## 8. Phase 5 Roadmap: Power BI Analytics & Reporting

- **Power BI Direct Connection**: Connect Power BI to ClickHouse Star Schema marts (`agripulse_mart.fact_mandi_prices`, `fact_crop_production`, `fact_weather`).
- **Interactive Dashboards**:
  1. Mandi Wholesale Price Volatility & Arbitrage Tracker.
  2. Crop Production vs Climate & Rainfall Correlation.
  3. Regional Weather Extremes & Drought / Flood Vulnerability Index.
