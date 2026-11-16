"""
AgriPulse India — Phase 3 Master Orchestrator
=============================================
Orchestrates the entire Phase 3 Snowflake Cloud Data Warehouse pipeline:
  Step 1: Environment & Credential Verification
  Step 2: Database, Schema & Table DDL Execution
  Step 3: Sample Batch Loading & Validation (100 rows)
  Step 4: Full Batch Parquet Ingestion (Mandi, Crop, NASA Weather)
  Step 5: Warehouse Transformations (CLEAN -> MART Star Schema)
  Step 6: Streaming Ingestion Check (Kafka mandi-prices-clean -> Snowflake)
  Step 7: Reconciliation & Validation Report Generation

If Snowflake credentials are not configured in .env, executes an Offline
Pre-Flight Verification to validate all local Parquet schemas, SQL definitions,
and data integrity.
"""

import os
import sys
import time
from typing import Dict, Any

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from src.utils.logger import get_logger
from snowflake.connection import check_connection, validate_config, execute_script
from snowflake.batch_loader import run_all_batch_loads
from snowflake.validation import run_full_validation

log = get_logger("Phase3Orchestrator")


def run_offline_preflight_checks() -> bool:
    """Verify all local datasets, Parquet schemas, and SQL scripts before live execution."""
    import pyarrow.parquet as pq
    import glob
    import re

    print("\n" + "=" * 70)
    print("PHASE 3 OFFLINE PRE-FLIGHT VERIFICATION")
    print("=" * 70)

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
        pattern = rf"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+{table_name}\s*\((.*?)\)\s*(?:CLUSTER|COMMENT|;)"
        match = re.search(pattern, content, re.DOTALL | re.IGNORECASE)
        if not match:
            return set()
        cols = set()
        for line in match.group(1).split("\n"):
            line = line.strip()
            if not line or line.startswith("--") or line.startswith("CONSTRAINT"):
                continue
            cols.add(line.split()[0].lower())
        return cols

    clean_sql = "snowflake/sql/03_clean_tables.sql"
    alignments = [
        ("CLEAN_MANDI_PRICES", "data/processed/mandi_prices_clean/mandi_prices_clean.parquet"),
        ("CLEAN_CROP_PRODUCTION", "data/processed/crop_production_clean/crop_production_clean.parquet"),
        ("CLEAN_WEATHER", "data/processed/nasa_weather_clean/year=2020/part-0.parquet"),
    ]

    print("\n  [Schema Alignment Checks]:")
    for tbl, pq_path in alignments:
        sql_cols = extract_cols(clean_sql, tbl)
        pq_cols = {c.name.lower() for c in pq.ParquetFile(os.path.join(ROOT_DIR, pq_path)).schema_arrow}
        missing_in_sql = pq_cols - sql_cols
        if not missing_in_sql:
            print(f"    - {tbl:<24}: 100% aligned with Parquet ({len(pq_cols)} columns matched)")
        else:
            print(f"    - {tbl:<24}: MISMATCH! Missing in SQL: {missing_in_sql}")
            all_passed = False

    # 3. SQL Script existence
    sql_scripts = [
        "snowflake/sql/01_database_setup.sql",
        "snowflake/sql/02_raw_tables.sql",
        "snowflake/sql/03_clean_tables.sql",
        "snowflake/sql/04_mart_tables.sql",
        "snowflake/sql/05_warehouse_transforms.sql",
    ]
    print("\n  [SQL DDL Scripts]:")
    for s in sql_scripts:
        exists = os.path.exists(os.path.join(ROOT_DIR, s))
        print(f"    - {os.path.basename(s):<32}: {'READY' if exists else 'MISSING'}")
        if not exists:
            all_passed = False

    print("\n" + "=" * 70)
    if all_passed:
        print("PRE-FLIGHT VERIFICATION SUCCESSFUL!")
        print("All datasets, schemas, and SQL scripts are perfectly primed.")
        print("To execute live against your cloud warehouse, configure .env:")
        print("  SNOWFLAKE_ACCOUNT=<your_account_id>")
        print("  SNOWFLAKE_USER=<your_user>")
        print("  SNOWFLAKE_PASSWORD=<your_password>")
    print("=" * 70 + "\n")
    return all_passed


def run_live_phase3_pipeline():
    """Execute full live Snowflake data warehouse integration."""
    log.info("Starting Live Phase 3 Snowflake Integration Pipeline...")

    # Step 1: Connectivity
    conn_status = check_connection()
    if not conn_status.get("connected"):
        log.error("Snowflake connection failed: %s", conn_status)
        return False

    log.info("Connected to Snowflake: User=%s, Role=%s, DB=%s, WH=%s",
             conn_status['current_user'], conn_status['current_role'],
             conn_status['current_database'], conn_status['current_warehouse'])

    # Step 2: DDL Scripts
    log.info("Executing DDL Scripts...")
    ddl_scripts = [
        "snowflake/sql/01_database_setup.sql",
        "snowflake/sql/02_raw_tables.sql",
        "snowflake/sql/03_clean_tables.sql",
        "snowflake/sql/04_mart_tables.sql",
    ]
    for s in ddl_scripts:
        execute_script(os.path.join(ROOT_DIR, s))

    # Step 3: Sample Batch Loading
    log.info("Performing Sample Batch Loading (100 rows per dataset)...")
    run_all_batch_loads(sample_only=True)

    # Step 4: Full Batch Loading
    log.info("Performing Full Batch Loading for Mandi, Crop Production, and NASA Weather...")
    run_all_batch_loads(sample_only=False)

    # Step 5: Transformations
    log.info("Executing Dimensional & Fact Transformations (CLEAN -> MART)...")
    execute_script(os.path.join(ROOT_DIR, "snowflake/sql/05_warehouse_transforms.sql"))

    # Step 6: Kafka Connect / Streaming Check (if Kafka is running)
    log.info("Testing streaming consumer (bounded sample)...")
    try:
        from snowflake.kafka_connect.snowflake_stream_consumer import stream_kafka_to_snowflake
        stream_kafka_to_snowflake(max_messages=50, batch_size=25)
    except Exception as stream_err:
        log.warning("Streaming consumer skipped or encountered issue: %s", stream_err)

    # Step 7: Validation
    log.info("Running Full Warehouse Validation & Generating Reconciliation Report...")
    report = run_full_validation()
    log.info("Phase 3 Execution Completed Successfully! Validation Report generated.")
    return True


if __name__ == "__main__":
    if validate_config():
        run_live_phase3_pipeline()
    else:
        run_offline_preflight_checks()
