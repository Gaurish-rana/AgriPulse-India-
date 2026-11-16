# AgriPulse India — Phase 3: Snowflake Cloud Data Warehouse Integration

This directory houses the Snowflake Data Warehouse architecture, SQL migration scripts, batch Parquet loaders, Kafka Connect streaming integrations, and data validation suites.

---

## 1. Database & Schema Architecture

All datasets are loaded and modeled within the central **`AGRIPULSE_DB`** database under three conformed schemas:

```
AGRIPULSE_DB
├── RAW
│   ├── RAW_MANDI_PRICES        (Raw landing payloads with metadata)
│   ├── RAW_CROP_PRODUCTION     (Raw historical crop production rows)
│   ├── RAW_WEATHER             (Raw NASA daily grid cell observations)
│   ├── LOAD_AUDIT              (Complete pipeline execution audit trail)
│   └── AGRIPULSE_STAGE         (Internal stage for Parquet staging)
│
├── CLEAN
│   ├── CLEAN_MANDI_PRICES      (12,726 cleansed wholesale market prices, ₹/quintal)
│   ├── CLEAN_CROP_PRODUCTION   (246,091 conformed crop production rows, Tonnes vs Nuts)
│   └── CLEAN_WEATHER           (7,121,520 gridded meteorology rows, negative temps preserved)
│
└── MART (Star Schema)
    ├── DIM_DATE                (1995–2035 calendar & agricultural year)
    ├── DIM_LOCATION            (Conformed states, districts, & APMC markets)
    ├── DIM_COMMODITY           (Conformed crops & market commodities)
    ├── FACT_MANDI_PRICES       (Grain: market-commodity-grade-date)
    ├── FACT_CROP_PRODUCTION    (Grain: district-crop-season-year)
    └── FACT_WEATHER            (Grain: 0.5x0.5 deg grid cell-date)
```

### Granularity & Modeling Rules
> [!IMPORTANT]
> - **Distinct Analytical Grains**: Mandi prices (daily APMC market), Crop Production (annual/seasonal district), and NASA Weather (daily $0.5^\circ \times 0.5^\circ$ grid cells) operate at fundamentally different granularities and must never be blindly joined.
> - **Measurement Unit Segregation**: Coconut production is exclusively tracked in `production_coconut_nuts` (Number of Nuts). All other crops are in `production_tonnes` (Metric Tonnes).
> - **Sub-zero Temperatures Preserved**: Himalayan sub-zero readings down to $-17^\circ\text{C}$ are legitimate readings and preserved with `is_negative_temperature` quality flags.
> - **Missing Values**: Missing crop production is flagged with `is_production_missing` and never converted to zero.

---

## 2. SQL Scripts Index

Located in `snowflake/sql/`:
1. `01_database_setup.sql`: Sets up `AGRIPULSE_WH`, `AGRIPULSE_DB`, schemas, file formats, and stages.
2. `02_raw_tables.sql`: DDL for RAW landing tables and `LOAD_AUDIT`.
3. `03_clean_tables.sql`: DDL for conformed CLEAN tables with primary business keys.
4. `04_mart_tables.sql`: DDL for dimensional models (Star Schema).
5. `05_warehouse_transforms.sql`: ELT procedures populating Dimensions and Facts via idempotent MERGE statements.

---

## 3. Batch Loading (Parquet -> Snowflake)

Cleansed Parquet datasets are staged via Snowflake internal stages (`PUT`) and ingested using high-performance `COPY INTO`:

```bash
# Test sample load (100 rows per dataset)
python -m snowflake.batch_loader --sample

# Full production batch load
python -m snowflake.batch_loader --dataset all
```

---

## 4. Kafka Connect Streaming Ingestion

Confluent Kafka streaming records from `mandi-prices-clean` are continuously ingested into `CLEAN.CLEAN_MANDI_PRICES`:

```bash
# Option A: Deploy Official Snowflake Kafka Sink Connector via Docker
docker compose -f snowflake/kafka_connect/docker-compose.kafka-connect.yml up -d
curl -X POST -H "Content-Type: application/json" \
  --data @snowflake/kafka_connect/snowflake-sink-connector.json \
  http://localhost:8083/connectors

# Option B: Direct Python Streaming Bridge (local/test harness)
python -m snowflake.kafka_connect.snowflake_stream_consumer --batch-size 50 --max-messages 100
```

---

## 5. Warehouse Validation & Reconciliation

Validate row counts, source parity, nulls, duplicates, and unit constraints:

```bash
python -m snowflake.validation
```
This generates `snowflake_validation_report.json` with comprehensive metrics.
