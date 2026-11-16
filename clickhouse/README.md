# AgriPulse India — Phase 3: ClickHouse Open-Source Data Warehouse

A 100% free, open-source, self-hosted analytical data warehouse using **ClickHouse** running on Docker Compose.

---

## 1. Why ClickHouse?

- **100% Free & Open-Source**: No paid tiers, no cloud credits, no license fees.
- **Extreme Performance**: Columnar storage with vector execution, achieving query speeds up to 100x faster than traditional row stores.
- **High-Efficiency Compression**: LZ4/ZSTD compression reduces the 7.12M NASA weather observations to minimal disk footprint.
- **Native Kafka Engine**: Ingests directly from Kafka topics via SQL Materialized Views without requiring external connector runtimes.

---

## 2. Database Architecture

Three isolated databases:

```
┌────────────────────────────────────────────────────────────────────────┐
│                        ClickHouse Local Warehouse                      │
├────────────────────┬────────────────────┬──────────────────────────────┤
│   agripulse_raw    │  agripulse_clean   │        agripulse_mart        │
├────────────────────┼────────────────────┼──────────────────────────────┤
│ • raw_mandi_prices │ • clean_mandi      │ • dim_date (1995–2035)       │
│ • raw_crop_prod    │ • clean_crop       │ • dim_location (APMC/Dist)   │
│ • raw_weather      │ • clean_weather    │ • dim_commodity (Crops/APMC) │
│ • load_audit       │ • kafka_engine     │ • fact_mandi_prices          │
│                    │                    │ • fact_crop_production       │
│                    │                    │ • fact_weather               │
└────────────────────┴────────────────────┴──────────────────────────────┘
```

---

## 3. Quick Start (Running Locally via Docker Compose)

### Step 1: Start ClickHouse Container
```bash
docker compose -f clickhouse/docker-compose.clickhouse.yml up -d
```
Verify the server is healthy:
```bash
curl http://localhost:8123/ping
# Output: Ok.
```

### Step 2: Run DDL Scripts (Automatic on container startup or via Python)
```bash
python -m clickhouse.connection
```

### Step 3: Run Batch Parquet Ingestion
```bash
# Sample test (100 rows per dataset)
python -m clickhouse.batch_loader --sample

# Complete batch ingestion (12.7K Mandi, 246K Crop, 7.12M Weather)
python -m clickhouse.batch_loader --dataset all
```

### Step 4: Run Analytical MART Transformations
```bash
python -c "from clickhouse.connection import execute_script; execute_script('clickhouse/sql/05_warehouse_transforms.sql')"
```

### Step 5: Run Full Validation & Reconciliation
```bash
python -m clickhouse.validation
```
*Generates `clickhouse_validation_report.json` with source-to-ClickHouse reconciliation metrics.*
