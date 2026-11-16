"""
AgriPulse India — Airflow DAG 2: Crop Production Pipeline
==========================================================
Manages historical and newly available crop production data:
  1. Inspects data/raw/crop_production/ for new or modified CSV files.
  2. Uses incremental file checksum/mtime tracking (data/interim/processed_crop_files.json).
  3. Skips re-processing if no files changed (Strict Rule: Do not reload the 246,091 records).
  4. If new data arrives: cleans, standardizes state names, validates units (Nuts vs Tonnes),
     writes Parquet, and loads into ClickHouse.
  5. Outputs reports/airflow/crop_pipeline_report.json.

Schedule: Daily at 02:00 UTC.
"""

import os
import sys
import json
import glob
import hashlib
from datetime import datetime, timezone, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = "/opt/airflow" if os.path.exists("/opt/airflow/dags") else os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

DEFAULT_ARGS = {
    "owner": "agripulse",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

TRACKING_FILE = os.path.join(PROJECT_ROOT, "data", "interim", "processed_crop_files.json")


def compute_file_hash(filepath: str) -> str:
    """Compute MD5 hash of first and last 64KB for efficient file fingerprinting."""
    hasher = hashlib.md5()
    size = os.path.getsize(filepath)
    with open(filepath, "rb") as f:
        hasher.update(f.read(65536))
        if size > 131072:
            f.seek(size - 65536)
            hasher.update(f.read(65536))
    return f"{hasher.hexdigest()}_{size}"


def check_for_new_or_modified_files(**context):
    """
    Checks data/raw/crop_production/ against processed_crop_files.json.
    Returns list of files requiring processing.
    """
    raw_dir = os.path.join(PROJECT_ROOT, "data", "raw", "crop_production")
    csv_files = glob.glob(os.path.join(raw_dir, "*.csv"))

    processed_state = {}
    if os.path.exists(TRACKING_FILE):
        try:
            with open(TRACKING_FILE, "r", encoding="utf-8") as f:
                processed_state = json.load(f)
        except Exception:
            processed_state = {}

    files_to_process = []
    current_hashes = {}
    for f in csv_files:
        fname = os.path.basename(f)
        fhash = compute_file_hash(f)
        current_hashes[fname] = fhash
        if fname not in processed_state or processed_state[fname] != fhash:
            files_to_process.append(f)

    is_incremental_run = len(files_to_process) > 0
    print(f"Crop File Check: Total files={len(csv_files)}, New/Modified={len(files_to_process)}")
    return {
        "files_to_process": files_to_process,
        "is_incremental_run": is_incremental_run,
        "current_hashes": current_hashes,
    }


def clean_and_standardize_crop_data(**context):
    """
    Executes cleaning logic only if new or modified files are detected.
    Preserves existing 246,091 records when no changes exist.
    """
    ti = context["ti"]
    file_check = ti.xcom_pull(task_ids="check_new_crop_files") or {}
    files_to_process = file_check.get("files_to_process", [])

    if not files_to_process:
        print("Incremental Check: No new or modified crop files found. Skipping batch clean.")
        return {"action": "SKIPPED", "processed_records": 0}

    from src.batch.clean_crop_production import clean_crop_production_data as clean_crop_production
    print(f"Executing crop cleaning for {len(files_to_process)} file(s)...")
    res = clean_crop_production()
    return {"action": "PROCESSED", "metrics": res}


def load_crop_to_clickhouse(**context):
    """Loads new records into ClickHouse if cleaning was triggered."""
    ti = context["ti"]
    clean_res = ti.xcom_pull(task_ids="clean_crop_data") or {}
    file_check = ti.xcom_pull(task_ids="check_new_crop_files") or {}

    if clean_res.get("action") == "SKIPPED":
        print("Skipping ClickHouse reload: No new crop data to ingest.")
    else:
        from clickhouse.batch_loader import load_dataset
        print("Loading newly processed crop records into ClickHouse...")
        load_dataset("crop", sample_only=False, force=False)

    # Update processed file state tracking
    current_hashes = file_check.get("current_hashes", {})
    if current_hashes:
        with open(TRACKING_FILE, "w", encoding="utf-8") as f:
            json.dump(current_hashes, f, indent=2)

    from clickhouse.connection import get_client
    client = get_client()
    try:
        cnt = client.query("SELECT count() FROM agripulse_clean.clean_crop_production FINAL").result_rows[0][0]
        print(f"Current ClickHouse Crop Production Row Count: {cnt:,}")
        return {"total_warehouse_rows": cnt}
    finally:
        client.close()


def generate_crop_report(**context):
    """Generates execution report in reports/airflow/crop_pipeline_report.json."""
    ti = context["ti"]
    file_check = ti.xcom_pull(task_ids="check_new_crop_files") or {}
    clean_res = ti.xcom_pull(task_ids="clean_crop_data") or {}
    ch_res = ti.xcom_pull(task_ids="load_crop_to_warehouse") or {}

    report = {
        "pipeline": "crop_production_pipeline",
        "executed_at_utc": datetime.now(timezone.utc).isoformat(),
        "incremental_action": clean_res.get("action", "SKIPPED"),
        "files_checked": len(file_check.get("current_hashes", {})),
        "files_reprocessed": len(file_check.get("files_to_process", [])),
        "warehouse_status": ch_res,
        "status": "SUCCESS",
    }

    report_dir = os.path.join(PROJECT_ROOT, "reports", "airflow")
    os.makedirs(report_dir, exist_ok=True)
    report_file = os.path.join(report_dir, "crop_pipeline_report.json")
    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"Crop Pipeline Report written: {report_file}")
    return report


with DAG(
    dag_id="crop_production_pipeline",
    default_args=DEFAULT_ARGS,
    description="Incremental processing and ClickHouse sync for Crop Production data",
    schedule_interval="0 2 * * *",  # Daily at 02:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["agripulse", "batch", "crop"],
) as dag:

    t1_check_files = PythonOperator(
        task_id="check_new_crop_files",
        python_callable=check_for_new_or_modified_files,
    )

    t2_clean_data = PythonOperator(
        task_id="clean_crop_data",
        python_callable=clean_and_standardize_crop_data,
    )

    t3_load_clickhouse = PythonOperator(
        task_id="load_crop_to_warehouse",
        python_callable=load_crop_to_clickhouse,
    )

    t4_report = PythonOperator(
        task_id="generate_crop_report",
        python_callable=generate_crop_report,
    )

    t1_check_files >> t2_clean_data >> t3_load_clickhouse >> t4_report
