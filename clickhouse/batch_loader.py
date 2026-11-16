"""
AgriPulse India — ClickHouse Batch Data Loader
==============================================
Loads conformed Parquet datasets into the self-hosted ClickHouse Data Warehouse
using high-performance Apache Arrow binary streaming (`client.insert_arrow`)
with full load audit tracking and memory-efficient chunking for the 7.12M NASA records.

Sources:
  1. Mandi Prices: data/processed/mandi_prices_clean/ (12,726 rows)
  2. Crop Production: data/processed/crop_production_clean/ (246,091 rows)
  3. NASA Weather: data/processed/nasa_weather_clean/ (7,121,520 rows, 7 partition files)

Target Tables:
  - agripulse_clean.clean_mandi_prices
  - agripulse_clean.clean_crop_production
  - agripulse_clean.clean_weather
  - agripulse_raw.load_audit
"""

import os
import sys
import glob
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import pyarrow.parquet as pq
import pyarrow as pa

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from src.utils.logger import get_logger
from clickhouse.connection import get_client

log = get_logger("ClickHouseBatchLoader")

DATA_DIRS = {
    "mandi": os.path.join(ROOT_DIR, "data", "processed", "mandi_prices_clean"),
    "crop": os.path.join(ROOT_DIR, "data", "processed", "crop_production_clean"),
    "weather": os.path.join(ROOT_DIR, "data", "processed", "nasa_weather_clean"),
}

TABLE_MAPPING = {
    "mandi": "agripulse_clean.clean_mandi_prices",
    "crop": "agripulse_clean.clean_crop_production",
    "weather": "agripulse_clean.clean_weather",
}


def record_audit_start(client, target_table: str, file_name: str, file_row_count: int) -> str:
    """Insert initial in-progress record into agripulse_raw.load_audit."""
    load_id = client.command("SELECT generateUUIDv4()").strip()
    client.command(f"""
        INSERT INTO agripulse_raw.load_audit (
            load_id, pipeline_stage, target_table, file_name, file_row_count, load_status, load_start_time
        ) VALUES (
            toUUID('{load_id}'), 'BATCH_PARQUET', '{target_table}', '{os.path.basename(file_name)}',
            {file_row_count}, 'IN_PROGRESS', now()
        )
    """)
    return load_id


def record_audit_complete(client, load_id: str, rows_inserted: int, status: str = "SUCCESS", error_msg: Optional[str] = None):
    """Update audit record with completion status."""
    if error_msg:
        escaped_err = error_msg.replace("'", "''")
        err_str = f"'{escaped_err}'"
    else:
        err_str = "NULL"
    client.command(f"""
        ALTER TABLE agripulse_raw.load_audit
        UPDATE rows_inserted = {rows_inserted},
               load_status = '{status}',
               load_end_time = now(),
               error_message = {err_str}
        WHERE load_id = toUUID('{load_id}')
    """)


def is_file_already_loaded(client, target_table: str, file_name: str) -> bool:
    """Check if file was already successfully loaded."""
    res = client.query(f"""
        SELECT count() FROM agripulse_raw.load_audit
        WHERE target_table = '{target_table}'
          AND file_name = '{os.path.basename(file_name)}'
          AND load_status = 'SUCCESS'
    """)
    return res.result_rows[0][0] > 0


def load_parquet_to_clickhouse(
    client, file_path: str, target_table: str, chunk_size: int = 250000, sample_rows: Optional[int] = None
) -> int:
    """
    Stream a Parquet file into ClickHouse using PyArrow batches.
    Memory-efficient: never loads more than chunk_size rows into memory at once.
    """
    pf = pq.ParquetFile(file_path)
    total_file_rows = pf.metadata.num_rows
    rows_to_read = min(total_file_rows, sample_rows) if sample_rows else total_file_rows

    table_parts = target_table.split(".")
    db_name, tbl_name = table_parts[0], table_parts[1]

    total_inserted = 0
    # Stream in Arrow record batches
    for batch in pf.iter_batches(batch_size=chunk_size):
        if sample_rows and total_inserted >= sample_rows:
            break
        
        arrow_table = pa.Table.from_batches([batch])
        if sample_rows and (total_inserted + arrow_table.num_rows > sample_rows):
            arrow_table = arrow_table.slice(0, sample_rows - total_inserted)

        client.insert_arrow(
            table=tbl_name,
            arrow_table=arrow_table,
            database=db_name,
        )
        total_inserted += arrow_table.num_rows

    return total_inserted


def load_dataset(dataset_key: str, sample_only: bool = False, sample_rows: int = 100, force: bool = False) -> Dict[str, Any]:
    """Load a specific dataset (mandi, crop, or weather) into ClickHouse."""
    data_dir = DATA_DIRS[dataset_key]
    target_table = TABLE_MAPPING[dataset_key]

    parquet_files = sorted(glob.glob(os.path.join(data_dir, "**", "*.parquet"), recursive=True))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {data_dir}")

    log.info("Found %d parquet file(s) for '%s' in %s", len(parquet_files), dataset_key, data_dir)

    client = get_client()
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
            pf = pq.ParquetFile(file_path)
            file_rows = pf.metadata.num_rows
            summary["total_source_rows"] += file_rows

            if not force and is_file_already_loaded(client, target_table, file_name):
                log.info("File %s already loaded into %s. Skipping (use --force to reload).", file_name, target_table)
                continue

            load_id = record_audit_start(client, target_table, file_name, file_rows)
            try:
                limit_rows = sample_rows if sample_only else None
                loaded = load_parquet_to_clickhouse(client, file_path, target_table, sample_rows=limit_rows)

                record_audit_complete(client, load_id, loaded, status="SUCCESS")
                summary["files_processed"] += 1
                summary["total_rows_loaded"] += loaded
                log.info("Loaded %d rows from %s into %s", loaded, file_name, target_table)

            except Exception as file_exc:
                log.error("Failed loading %s into %s: %s", file_name, target_table, file_exc)
                record_audit_complete(client, load_id, 0, status="FAILED", error_msg=str(file_exc))
                summary["errors"].append({"file": file_name, "error": str(file_exc)})
                summary["status"] = "PARTIAL_ERROR"

    finally:
        client.close()

    return summary


def run_all_batch_loads(sample_only: bool = False, force: bool = False) -> List[Dict[str, Any]]:
    """Execute batch loading across Mandi, Crop Production, and NASA Weather."""
    results = []
    for ds in ["mandi", "crop", "weather"]:
        log.info("==================================================")
        log.info("Starting ClickHouse batch load for: %s (sample_only=%s)", ds.upper(), sample_only)
        log.info("==================================================")
        res = load_dataset(ds, sample_only=sample_only, force=force)
        results.append(res)
    return results


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="ClickHouse Batch Parquet Loader for AgriPulse India")
    parser.add_argument("--dataset", choices=["mandi", "crop", "weather", "all"], default="all")
    parser.add_argument("--sample", action="store_true", help="Load only a small sample (100 rows)")
    parser.add_argument("--force", action="store_true", help="Force reload files")
    args = parser.parse_args()

    if args.dataset == "all":
        out = run_all_batch_loads(sample_only=args.sample, force=args.force)
    else:
        out = [load_dataset(args.dataset, sample_only=args.sample, force=args.force)]

    print("\nClickHouse Batch Load Summary:")
    for item in out:
        print(f"  [{item['dataset']}] Loaded: {item['total_rows_loaded']:,} / {item['total_source_rows']:,} rows (Status: {item['status']})")
