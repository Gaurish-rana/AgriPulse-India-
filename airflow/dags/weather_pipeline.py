"""
AgriPulse India — Airflow DAG 3: NASA Weather Pipeline
======================================================
Automates the processing of newly available NASA POWER meteorology data:
  1. Inspects data/raw/nasa_power/ for unprocessed tile CSV files.
  2. Uses incremental file tracking (data/interim/processed_weather_files.json).
  3. Strict Rule: Skips re-processing when all 784 historical tiles are already processed
     (Prevents unnecessary reloads of the 7.12M records).
  4. If new tiles arrive: executes chunked cleaning, preserves negative temperatures,
     generates partitioned Parquet, and loads into ClickHouse agripulse_clean.clean_weather.
  5. Outputs reports/airflow/weather_pipeline_report.json.

Schedule: Daily at 03:00 UTC.
"""

import os
import sys
import json
import glob
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
    "retry_delay": timedelta(minutes=10),
}

TRACKING_FILE = os.path.join(PROJECT_ROOT, "data", "interim", "processed_weather_files.json")


def check_for_unprocessed_weather_files(**context):
    """
    Checks data/raw/nasa_power/ against processed_weather_files.json.
    Returns list of newly added tiles.
    """
    raw_dir = os.path.join(PROJECT_ROOT, "data", "raw", "nasa_power")
    csv_files = glob.glob(os.path.join(raw_dir, "*.csv"))

    processed_files = set()
    if os.path.exists(TRACKING_FILE):
        try:
            with open(TRACKING_FILE, "r", encoding="utf-8") as f:
                processed_files = set(json.load(f))
        except Exception:
            processed_files = set()

    unprocessed = [f for f in csv_files if os.path.basename(f) not in processed_files]
    print(f"Weather File Check: Total tiles={len(csv_files)}, Already processed={len(processed_files)}, New={len(unprocessed)}")
    return {
        "total_files": len(csv_files),
        "unprocessed_files": unprocessed,
        "is_incremental_run": len(unprocessed) > 0,
    }


def clean_weather_data(**context):
    """
    Executes cleaning logic only if new tiles are detected.
    Preserves existing 7,121,520 records when no new files exist.
    """
    ti = context["ti"]
    file_check = ti.xcom_pull(task_ids="check_new_weather_files") or {}
    unprocessed = file_check.get("unprocessed_files", [])

    if not unprocessed:
        print("Incremental Check: All NASA tiles are already processed. Skipping 7.12M row reload.")
        return {"action": "SKIPPED", "tiles_processed": 0}

    from src.batch.clean_nasa_weather import clean_nasa_weather
    print(f"Processing {len(unprocessed)} new NASA tile(s)...")
    res = clean_nasa_weather()
    return {"action": "PROCESSED", "metrics": res}


def load_weather_to_clickhouse(**context):
    """Loads new weather records into ClickHouse if cleaning was triggered."""
    ti = context["ti"]
    clean_res = ti.xcom_pull(task_ids="clean_weather_data") or {}

    if clean_res.get("action") == "SKIPPED":
        print("Skipping ClickHouse reload: No new weather tiles to ingest.")
    else:
        from clickhouse.batch_loader import load_dataset
        print("Loading new weather records into ClickHouse via Apache Arrow streaming...")
        load_dataset("weather", sample_only=False, force=False)

    # Record all raw files as processed
    raw_dir = os.path.join(PROJECT_ROOT, "data", "raw", "nasa_power")
    all_basenames = [os.path.basename(f) for f in glob.glob(os.path.join(raw_dir, "*.csv"))]
    with open(TRACKING_FILE, "w", encoding="utf-8") as f:
        json.dump(all_basenames, f, indent=2)

    from clickhouse.connection import get_client
    client = get_client()
    try:
        cnt = client.query("SELECT count() FROM agripulse_clean.clean_weather FINAL").result_rows[0][0]
        neg_cnt = client.query("SELECT count() FROM agripulse_clean.clean_weather FINAL WHERE is_negative_temperature = 1").result_rows[0][0]
        print(f"Current ClickHouse Weather Row Count: {cnt:,} (Negative temps preserved: {neg_cnt:,})")
        return {"total_warehouse_rows": cnt, "negative_temperatures_preserved": neg_cnt}
    finally:
        client.close()


def generate_weather_report(**context):
    """Generates execution report in reports/airflow/weather_pipeline_report.json."""
    ti = context["ti"]
    file_check = ti.xcom_pull(task_ids="check_new_weather_files") or {}
    clean_res = ti.xcom_pull(task_ids="clean_weather_data") or {}
    ch_res = ti.xcom_pull(task_ids="load_weather_to_warehouse") or {}

    report = {
        "pipeline": "weather_pipeline",
        "executed_at_utc": datetime.now(timezone.utc).isoformat(),
        "incremental_action": clean_res.get("action", "SKIPPED"),
        "tiles_inspected": file_check.get("total_files", 0),
        "new_tiles_processed": len(file_check.get("unprocessed_files", [])),
        "warehouse_status": ch_res,
        "status": "SUCCESS",
    }

    report_dir = os.path.join(PROJECT_ROOT, "reports", "airflow")
    os.makedirs(report_dir, exist_ok=True)
    report_file = os.path.join(report_dir, "weather_pipeline_report.json")
    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"Weather Pipeline Report written: {report_file}")
    return report


with DAG(
    dag_id="weather_pipeline",
    default_args=DEFAULT_ARGS,
    description="Incremental processing and ClickHouse sync for NASA POWER meteorology",
    schedule_interval="0 3 * * *",  # Daily at 03:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["agripulse", "batch", "weather"],
) as dag:

    t1_check_files = PythonOperator(
        task_id="check_new_weather_files",
        python_callable=check_for_unprocessed_weather_files,
    )

    t2_clean_data = PythonOperator(
        task_id="clean_weather_data",
        python_callable=clean_weather_data,
    )

    t3_load_clickhouse = PythonOperator(
        task_id="load_weather_to_warehouse",
        python_callable=load_weather_to_clickhouse,
    )

    t4_report = PythonOperator(
        task_id="generate_weather_report",
        python_callable=generate_weather_report,
    )

    t1_check_files >> t2_clean_data >> t3_load_clickhouse >> t4_report
