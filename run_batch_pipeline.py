"""
AgriPulse India — Main Batch Cleaning & Standardization Pipeline
==================================================================
Runs the complete batch-processing pipeline across all three datasets:
  1. Mandi Wholesale Prices
  2. Historical Crop Production
  3. NASA POWER Agricultural Weather

Usage:
    python run_batch_pipeline.py
"""

import os
import sys
import time
from datetime import datetime
import glob
import json

# Ensure project root is in sys.path
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.batch.clean_mandi import clean_mandi_data
from src.batch.clean_crop_production import clean_crop_production_data
from src.batch.clean_nasa_weather import clean_nasa_weather_data
from src.batch.data_quality import safe_convert, save_quality_report
from src.utils.logger import get_logger

log = get_logger("run_batch_pipeline")


def run_pipeline():
    start_total = time.time()
    print("=" * 75)
    print("   AGRIPULSE INDIA — PHASE 1.5 BATCH DATA CLEANING PIPELINE")
    print(f"   Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 75)

    pipeline_report = {
        "pipeline_name": "AgriPulse India Phase 1.5 Batch Processing",
        "started_at": datetime.now().isoformat(),
        "stages": {},
    }

    # ---------------------------------------------------------
    # STAGE 1: MANDI PRICES CLEANING
    # ---------------------------------------------------------
    print("\n>>> [1/3] Running Mandi Price Cleaning & Validation...")
    t0 = time.time()
    mandi_raw_files = glob.glob(os.path.join("data", "raw", "mandi", "*.csv"))
    if not mandi_raw_files:
        raise FileNotFoundError("No raw Mandi CSV files found in data/raw/mandi/")
    latest_mandi_csv = sorted(mandi_raw_files)[-1]
    log.info("Processing Mandi CSV: %s", latest_mandi_csv)

    df_mandi, mandi_report = clean_mandi_data(
        raw_csv_path=latest_mandi_csv,
        output_dir=os.path.join("data", "processed", "mandi_prices_clean"),
        report_output_path="data_quality_report_mandi.json",
    )
    t_mandi = time.time() - t0
    pipeline_report["stages"]["mandi_prices"] = {
        "status": "SUCCESS",
        "duration_seconds": round(t_mandi, 2),
        "raw_rows": mandi_report["summary"]["rows_raw_input"],
        "clean_rows": mandi_report["summary"]["rows_processed_output"],
        "unique_states": mandi_report["after_quality_profile"]["state_summary"]["unique_state_count"],
        "unique_commodities": mandi_report["after_quality_profile"]["item_summary"]["unique_item_count"],
        "output_file": os.path.join("data", "processed", "mandi_prices_clean", "mandi_prices_clean.parquet"),
    }
    print(f"    ✓ Mandi Prices Cleaned: {len(df_mandi):,} records in {t_mandi:.2f}s")

    # ---------------------------------------------------------
    # STAGE 2: CROP PRODUCTION CLEANING
    # ---------------------------------------------------------
    print("\n>>> [2/3] Running Crop Production Cleaning & Validation...")
    t0 = time.time()
    crop_raw_csv = os.path.join("data", "raw", "crop_production", "crop_production.csv")
    if not os.path.exists(crop_raw_csv):
        raise FileNotFoundError(f"Raw Crop Production CSV not found at {crop_raw_csv}")

    df_crop, crop_report = clean_crop_production_data(
        raw_csv_path=crop_raw_csv,
        output_dir=os.path.join("data", "processed", "crop_production_clean"),
        report_output_path="data_quality_report_crop.json",
    )
    t_crop = time.time() - t0
    pipeline_report["stages"]["crop_production"] = {
        "status": "SUCCESS",
        "duration_seconds": round(t_crop, 2),
        "raw_rows": crop_report["summary"]["rows_raw_input"],
        "clean_rows": crop_report["summary"]["rows_processed_output"],
        "unique_states": crop_report["after_quality_profile"]["state_summary"]["unique_state_count"],
        "unique_crops": crop_report["after_quality_profile"]["item_summary"]["unique_item_count"],
        "missing_production_flagged": crop_report["after_quality_profile"]["missing_values_count"]["production"],
        "output_file": os.path.join("data", "processed", "crop_production_clean", "crop_production_clean.parquet"),
    }
    print(f"    ✓ Crop Production Cleaned: {len(df_crop):,} records in {t_crop:.2f}s")

    # ---------------------------------------------------------
    # STAGE 3: NASA POWER WEATHER CLEANING
    # ---------------------------------------------------------
    print("\n>>> [3/3] Running NASA POWER Weather Cleaning & Parquet Partitioning...")
    t0 = time.time()
    nasa_raw_dir = os.path.join("data", "raw", "nasa_power")

    nasa_report = clean_nasa_weather_data(
        raw_dir=nasa_raw_dir,
        output_dir=os.path.join("data", "processed", "nasa_weather_clean"),
        report_output_path="data_quality_report_nasa.json",
    )
    t_nasa = time.time() - t0
    pipeline_report["stages"]["nasa_weather"] = {
        "status": "SUCCESS",
        "duration_seconds": round(t_nasa, 2),
        "raw_files": nasa_report["summary"]["total_raw_csv_files"],
        "raw_rows_input": nasa_report["summary"]["total_raw_rows_input"],
        "consolidated_output_rows": nasa_report["summary"]["total_consolidated_output_rows"],
        "boundary_duplicates_dropped": nasa_report["summary"]["boundary_duplicates_dropped"],
        "missing_markers_nullified": nasa_report["summary"]["missing_markers_converted_to_null"],
        "subzero_temps_preserved": nasa_report["summary"]["negative_temperatures_preserved"],
        "partitions_dir": os.path.join("data", "processed", "nasa_weather_clean"),
    }
    print(f"    ✓ NASA Weather Cleaned: {nasa_report['summary']['total_consolidated_output_rows']:,} rows in {t_nasa:.2f}s")

    # ---------------------------------------------------------
    # SUMMARY & CONSOLIDATED REPORT
    # ---------------------------------------------------------
    total_duration = time.time() - start_total
    pipeline_report["completed_at"] = datetime.now().isoformat()
    pipeline_report["total_pipeline_duration_seconds"] = round(total_duration, 2)

    consolidated_report_path = "data_quality_batch_report.json"
    save_quality_report(safe_convert(pipeline_report), consolidated_report_path)
    log.info("Consolidated pipeline report saved to: %s", consolidated_report_path)

    print("\n" + "=" * 75)
    print("   BATCH CLEANING PIPELINE COMPLETED SUCCESSFULLY")
    print(f"   Total Elapsed Time: {total_duration:.2f} seconds ({total_duration/60:.2f} minutes)")
    print("=" * 75)
    print(f"  [1] Mandi Prices Cleaned     : {pipeline_report['stages']['mandi_prices']['clean_rows']:,} records")
    print(f"  [2] Crop Production Cleaned  : {pipeline_report['stages']['crop_production']['clean_rows']:,} records")
    print(f"  [3] NASA Weather Cleaned     : {pipeline_report['stages']['nasa_weather']['consolidated_output_rows']:,} records (Year 2020-2026 partitions)")
    print("-" * 75)
    print("  Artifacts & Output Locations:")
    print("    • Mandi Clean Parquet    : data/processed/mandi_prices_clean/mandi_prices_clean.parquet")
    print("    • Crop Clean Parquet     : data/processed/crop_production_clean/crop_production_clean.parquet")
    print("    • NASA Clean Partitions  : data/processed/nasa_weather_clean/year=YYYY/part-0.parquet")
    print("    • State Name Mapping     : state_name_mapping.csv & src/utils/state_name_mapping.csv")
    print("    • Mandi Quality Report   : data_quality_report_mandi.json")
    print("    • Crop Quality Report    : data_quality_report_crop.json")
    print("    • NASA Quality Report    : data_quality_report_nasa.json")
    print("    • Consolidated QA Report : data_quality_batch_report.json")
    print("=" * 75)


if __name__ == "__main__":
    run_pipeline()
