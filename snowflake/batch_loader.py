"""
AgriPulse India — Snowflake Batch Data Loader
=============================================
Loads cleansed Parquet datasets into Snowflake Cloud Data Warehouse using
staged internal file loading (PUT + COPY INTO) with full load audit tracking,
deduplication/idempotency, and chunked memory-efficient streaming for NASA Weather.

Sources:
  1. Mandi Prices: data/processed/mandi_prices_clean/
  2. Crop Production: data/processed/crop_production_clean/
  3. NASA Weather: data/processed/nasa_weather_clean/ (7 partition files, 7.12M rows)

Target Tables:
  - CLEAN.CLEAN_MANDI_PRICES
  - CLEAN.CLEAN_CROP_PRODUCTION
  - CLEAN.CLEAN_WEATHER
  - RAW.LOAD_AUDIT
"""

import os
import sys
import glob
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import pyarrow.parquet as pq

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from src.utils.logger import get_logger
from snowflake.connection import get_snowflake_connection, validate_config

log = get_logger(__name__)

DATA_DIRS = {
    "mandi": os.path.join(ROOT_DIR, "data", "processed", "mandi_prices_clean"),
    "crop": os.path.join(ROOT_DIR, "data", "processed", "crop_production_clean"),
    "weather": os.path.join(ROOT_DIR, "data", "processed", "nasa_weather_clean"),
}

TABLE_MAPPING = {
    "mandi": "CLEAN.CLEAN_MANDI_PRICES",
    "crop": "CLEAN.CLEAN_CROP_PRODUCTION",
    "weather": "CLEAN.CLEAN_WEATHER",
}


def record_audit_start(conn, target_table: str, file_name: str, file_row_count: int) -> int:
    """Insert initial in-progress record in RAW.LOAD_AUDIT and return load_id."""
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO RAW.LOAD_AUDIT (
            pipeline_stage, target_table, file_name, file_row_count, load_status, load_start_time
        ) VALUES (
            'BATCH_PARQUET', %s, %s, %s, 'IN_PROGRESS', CURRENT_TIMESTAMP()
        )
    """, (target_table, os.path.basename(file_name), file_row_count))
    
    cursor.execute("SELECT MAX(load_id) FROM RAW.LOAD_AUDIT WHERE target_table = %s AND file_name = %s",
                   (target_table, os.path.basename(file_name)))
    row = cursor.fetchone()
    load_id = row[0] if row else 0
    cursor.close()
    return load_id


def record_audit_complete(conn, load_id: int, rows_inserted: int, status: str = "SUCCESS", error_msg: Optional[str] = None):
    """Update audit record with completion status and row counts."""
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE RAW.LOAD_AUDIT
        SET rows_inserted = %s,
            load_status = %s,
            load_end_time = CURRENT_TIMESTAMP(),
            error_message = %s
        WHERE load_id = %s
    """, (rows_inserted, status, error_msg, load_id))
    cursor.close()


def is_file_already_loaded(conn, target_table: str, file_name: str) -> bool:
    """Check if the exact file has already been successfully loaded into the table."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*)
        FROM RAW.LOAD_AUDIT
        WHERE target_table = %s
          AND file_name = %s
          AND load_status = 'SUCCESS'
    """, (target_table, os.path.basename(file_name)))
    count = cursor.fetchone()[0]
    cursor.close()
    return count > 0


def stage_and_copy_parquet(conn, file_path: str, target_table: str, stage_prefix: str) -> int:
    """
    Stage a local Parquet file to @AGRIPULSE_STAGE and execute COPY INTO target_table.
    Uses MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE.
    Returns the number of rows loaded.
    """
    cursor = conn.cursor()
    base_name = os.path.basename(file_path)
    stage_path = f"@RAW.AGRIPULSE_STAGE/{stage_prefix}/"

    # Normalize Windows backslashes for Snowflake PUT
    normalized_file_path = file_path.replace("\\", "/")

    log.info("Staging file to Snowflake stage: PUT file://%s -> %s", normalized_file_path, stage_path)
    put_sql = f"PUT 'file://{normalized_file_path}' {stage_path} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
    cursor.execute(put_sql)

    log.info("Executing COPY INTO %s from %s%s...", target_table, stage_path, base_name)
    copy_sql = f"""
        COPY INTO {target_table}
        FROM '{stage_path}{base_name}'
        FILE_FORMAT = (TYPE = 'PARQUET')
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = 'CONTINUE'
    """
    cursor.execute(copy_sql)
    rows_loaded = 0
    copy_results = cursor.fetchall()
    for row in copy_results:
        # Snowflake COPY INTO returns: [file, status, rows_parsed, rows_loaded, ...]
        if len(row) >= 4 and str(row[3]).isdigit():
            rows_loaded += int(row[3])
        elif len(row) >= 2:
            log.info("Copy result: %s - status: %s", row[0], row[1])

    cursor.close()
    return rows_loaded


def load_dataset(dataset_key: str, sample_only: bool = False, sample_rows: int = 100, force: bool = False) -> Dict[str, Any]:
    """
    Load a single dataset (mandi, crop, or weather) into Snowflake.
    """
    data_dir = DATA_DIRS[dataset_key]
    target_table = TABLE_MAPPING[dataset_key]

    # Find parquet files
    parquet_files = sorted(glob.glob(os.path.join(data_dir, "**", "*.parquet"), recursive=True))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {data_dir}")

    log.info("Found %d parquet file(s) for '%s' in %s", len(parquet_files), dataset_key, data_dir)

    conn = get_snowflake_connection()
    summary = {
        "dataset": dataset_key,
        "target_table": target_table,
        "files_processed": 0,
        "total_source_rows": 0,
        "total_rows_loaded": 0,
        "status": "SUCCESS",
        "errors": [],
    }

    try:
        files_to_load = [parquet_files[0]] if sample_only else parquet_files

        for file_path in files_to_load:
            file_name = os.path.basename(file_path)
            
            # Read metadata without reading entire file into memory
            pf = pq.ParquetFile(file_path)
            file_rows = pf.metadata.num_rows
            summary["total_source_rows"] += file_rows

            if not force and is_file_already_loaded(conn, target_table, file_name):
                log.info("File %s already loaded into %s. Skipping (use --force to reload).", file_name, target_table)
                continue

            load_id = record_audit_start(conn, target_table, file_name, file_rows)

            try:
                if sample_only:
                    log.info("Sample mode: Loading first %d rows of %s into %s", sample_rows, file_name, target_table)
                    # Read sample rows and write temporary sample parquet
                    table_sample = pf.read_row_group(0)
                    if table_sample.num_rows > sample_rows:
                        table_sample = table_sample.slice(0, sample_rows)
                    
                    sample_file_path = os.path.join(ROOT_DIR, "data", "interim", f"sample_{file_name}")
                    pq.write_table(table_sample, sample_file_path)
                    
                    rows_loaded = stage_and_copy_parquet(conn, sample_file_path, target_table, f"samples_{dataset_key}")
                    if os.path.exists(sample_file_path):
                        os.remove(sample_file_path)
                else:
                    rows_loaded = stage_and_copy_parquet(conn, file_path, target_table, f"batch_{dataset_key}")

                record_audit_complete(conn, load_id, rows_loaded, status="SUCCESS")
                summary["files_processed"] += 1
                summary["total_rows_loaded"] += rows_loaded
                log.info("Loaded %d rows from %s into %s", rows_loaded, file_name, target_table)

            except Exception as file_exc:
                log.error("Failed loading file %s: %s", file_name, file_exc)
                record_audit_complete(conn, load_id, 0, status="FAILED", error_msg=str(file_exc))
                summary["errors"].append({"file": file_name, "error": str(file_exc)})
                summary["status"] = "PARTIAL_ERROR"

    finally:
        conn.close()

    return summary


def run_all_batch_loads(sample_only: bool = False, force: bool = False) -> List[Dict[str, Any]]:
    """Execute batch loading across all 3 datasets."""
    results = []
    for ds in ["mandi", "crop", "weather"]:
        log.info("==================================================")
        log.info("Initiating batch load for: %s (sample_only=%s)", ds.upper(), sample_only)
        log.info("==================================================")
        res = load_dataset(ds, sample_only=sample_only, force=force)
        results.append(res)
    return results


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Snowflake Batch Parquet Loader for AgriPulse India")
    parser.add_argument("--dataset", choices=["mandi", "crop", "weather", "all"], default="all")
    parser.add_argument("--sample", action="store_true", help="Load only a small sample (100 rows)")
    parser.add_argument("--force", action="store_true", help="Force reload already loaded files")
    args = parser.parse_args()

    if not validate_config():
        print("Cannot run batch load: Snowflake credentials missing in .env")
        sys.exit(1)

    if args.dataset == "all":
        out = run_all_batch_loads(sample_only=args.sample, force=args.force)
    else:
        out = [load_dataset(args.dataset, sample_only=args.sample, force=args.force)]

    print("\nBatch Load Summary:")
    for item in out:
        print(f"  [{item['dataset']}] Loaded: {item['total_rows_loaded']:,} / {item['total_source_rows']:,} rows (Status: {item['status']})")
