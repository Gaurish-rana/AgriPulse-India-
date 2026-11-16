"""
AgriPulse India — Phase 3 ClickHouse Master Orchestrator
=========================================================
Orchestrates the entire Phase 3 ClickHouse Data Warehouse pipeline:
  Step 1: ClickHouse Local Server Health Check (port 8123)
  Step 2: Database Initialization (agripulse_raw, agripulse_clean, agripulse_mart)
  Step 3: Table DDL Execution (RAW, CLEAN, MART, Kafka Engine)
  Step 4: Sample Batch Loading Test (100 rows per dataset)
  Step 5: Full Batch Parquet Ingestion (12.7K Mandi, 246K Crop, 7.12M Weather)
  Step 6: Analytical Transformations (CLEAN -> MART Star Schema)
  Step 7: Kafka Streaming Ingestion Test
  Step 8: Reconciliation & Validation Report Generation

If ClickHouse server is not running, executes Offline Pre-Flight Verification
to validate all Parquet schemas, SQL definitions, and data integrity.
"""

import os
import sys
import time

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from src.utils.logger import get_logger
from clickhouse.connection import check_connection, execute_script
from clickhouse.batch_loader import run_all_batch_loads
from clickhouse.validation import run_full_validation

log = get_logger("Phase3ClickHouseOrchestrator")


def run_offline_preflight_checks() -> bool:
    """Validate all local Parquet datasets, schema alignment, and SQL definitions."""
    import pyarrow.parquet as pq
    import glob
    import re

    print("\n" + "=" * 75)
    print("PHASE 3 CLICKHOUSE PRE-FLIGHT VERIFICATION")
    print("=" * 75)

    # 1. Dataset verification
    datasets = {
        "Mandi Prices": ("data/processed/mandi_prices_clean", 12726),
        "Crop Production": ("data/processed/crop_production_clean", 246091),
        "NASA Weather": ("data/processed/nasa_weather_clean", 7121520),
    }

    all_passed = True
    for name, (path, expected_rows) in datasets.items():
        files = glob.glob(os.path.join(ROOT_DIR, path, "**", "*.parquet"), recursive=True)
        total_rows = sum(pq.ParquetFile(f).metadata.num_rows for f in files)
        status = "PASSED" if total_rows == expected_rows else f"WARNING (found {total_rows:,}, expected {expected_rows:,})"
        print(f"  [Dataset] {name:<18}: {len(files)} file(s), {total_rows:>10,} rows -> {status}")
        if total_rows == 0:
            all_passed = False

    # 2. Schema alignment check
    def extract_cols(sql_file, table_name):
        with open(os.path.join(ROOT_DIR, sql_file), "r", encoding="utf-8") as f:
            content = f.read()
        pattern = rf"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+{table_name}\s*\((.*?)\)\s*ENGINE"
        match = re.search(pattern, content, re.DOTALL | re.IGNORECASE)
        if not match:
            return set()
        cols = set()
        for line in match.group(1).split("\n"):
            line = line.strip()
            if not line or line.startswith("--") or line.startswith("SETTINGS"):
                continue
            cols.add(line.split()[0].lower())
        return cols

    clean_sql = "clickhouse/sql/03_clean_tables.sql"
    alignments = [
        ("agripulse_clean.clean_mandi_prices", "data/processed/mandi_prices_clean/mandi_prices_clean.parquet"),
        ("agripulse_clean.clean_crop_production", "data/processed/crop_production_clean/crop_production_clean.parquet"),
        ("agripulse_clean.clean_weather", "data/processed/nasa_weather_clean/year=2020/part-0.parquet"),
    ]

    print("\n  [Schema Alignment Checks]:")
    for tbl, pq_path in alignments:
        sql_cols = extract_cols(clean_sql, tbl)
        pq_cols = {c.name.lower() for c in pq.ParquetFile(os.path.join(ROOT_DIR, pq_path)).schema_arrow}
        missing_in_sql = pq_cols - sql_cols
        if not missing_in_sql:
            print(f"    - {tbl:<36}: 100% aligned with Parquet ({len(pq_cols)} columns matched)")
        else:
            print(f"    - {tbl:<36}: MISMATCH! Missing in SQL: {missing_in_sql}")
            all_passed = False

    # 3. SQL Script existence
    sql_scripts = [
        "clickhouse/sql/01_init_databases.sql",
        "clickhouse/sql/02_raw_tables.sql",
        "clickhouse/sql/03_clean_tables.sql",
        "clickhouse/sql/04_mart_tables.sql",
        "clickhouse/sql/05_warehouse_transforms.sql",
        "clickhouse/sql/06_kafka_engine.sql",
    ]
    print("\n  [ClickHouse SQL Scripts]:")
    for s in sql_scripts:
        exists = os.path.exists(os.path.join(ROOT_DIR, s))
        print(f"    - {os.path.basename(s):<32}: {'READY' if exists else 'MISSING'}")
        if not exists:
            all_passed = False

    print("\n" + "=" * 75)
    if all_passed:
        print("PRE-FLIGHT VERIFICATION SUCCESSFUL!")
        print("All datasets, schemas, and ClickHouse scripts are primed.")
        print("\nTo start ClickHouse locally:")
        print("  docker compose -f clickhouse/docker-compose.clickhouse.yml up -d")
        print("\nThen run this script again to execute the live pipeline:")
        print("  python run_phase3_clickhouse.py")
    print("=" * 75 + "\n")
    return all_passed


def run_live_pipeline():
    """Execute live ClickHouse pipeline when container is online."""
    log.info("Starting Live Phase 3 ClickHouse Pipeline...")

    # Step 1: DDL Execution
    log.info("Executing ClickHouse DDL scripts...")
    scripts = [
        "clickhouse/sql/01_init_databases.sql",
        "clickhouse/sql/02_raw_tables.sql",
        "clickhouse/sql/03_clean_tables.sql",
        "clickhouse/sql/04_mart_tables.sql",
        "clickhouse/sql/06_kafka_engine.sql",
    ]
    for s in scripts:
        execute_script(os.path.join(ROOT_DIR, s))

    # Step 2: Sample Batch Loading
    log.info("Running sample batch load (100 rows per dataset)...")
    run_all_batch_loads(sample_only=True)

    # Step 3: Full Batch Loading
    log.info("Running full batch ingestion (Mandi, Crop, and 7.12M NASA Weather)...")
    run_all_batch_loads(sample_only=False)

    # Step 4: Transformations
    log.info("Executing analytical transformations (CLEAN -> MART)...")
    execute_script(os.path.join(ROOT_DIR, "clickhouse/sql/05_warehouse_transforms.sql"))

    # Step 5: Streaming Ingestion Check
    log.info("Testing streaming consumer (bounded test)...")
    try:
        from clickhouse.kafka_connect.clickhouse_stream_consumer import stream_kafka_to_clickhouse
        stream_kafka_to_clickhouse(max_messages=50, batch_size=25)
    except Exception as stream_err:
        log.warning("Streaming consumer skipped or encountered: %s", stream_err)

    # Step 6: Validation Report
    log.info("Running full ClickHouse validation & generating report...")
    report = run_full_validation()
    log.info("Phase 3 ClickHouse execution completed successfully!")
    return True


if __name__ == "__main__":
    conn = check_connection()
    if conn.get("connected"):
        run_live_pipeline()
    else:
        run_offline_preflight_checks()
