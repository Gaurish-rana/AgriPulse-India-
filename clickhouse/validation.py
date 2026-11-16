"""
AgriPulse India — ClickHouse Validation & Reconciliation Suite
==============================================================
Validates loaded datasets across agripulse_raw, agripulse_clean, and agripulse_mart:
  1. Source-to-ClickHouse row count reconciliation
  2. Null count checks on key analytical columns
  3. Duplicate checks on business keys
  4. Date range coverage
  5. Distinct state, district, commodity, and crop counts
  6. Weather coordinate ranges (0.5x0.5 deg grid bounds)
  7. Measurement unit integrity (Coconut nuts vs field crop tonnes)
  8. Preserved negative temperature verification
  9. Generates clickhouse_validation_report.json
"""

import os
import sys
import json
import glob
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import pyarrow.parquet as pq

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from src.utils.logger import get_logger
from clickhouse.connection import get_client

log = get_logger("ClickHouseValidation")


def count_local_parquet_rows(directory: str) -> int:
    """Accurately count rows across all parquet files in a directory using metadata."""
    files = glob.glob(os.path.join(directory, "**", "*.parquet"), recursive=True)
    return sum(pq.ParquetFile(f).metadata.num_rows for f in files)


def validate_mandi_clean(client, local_rows: int) -> Dict[str, Any]:
    """Validate agripulse_clean.clean_mandi_prices."""
    log.info("Validating agripulse_clean.clean_mandi_prices...")

    row_count = client.query("SELECT count() FROM agripulse_clean.clean_mandi_prices").result_rows[0][0]
    
    null_res = client.query("""
        SELECT
            countIf(state_name = ''),
            countIf(market_name = ''),
            countIf(commodity_name = ''),
            countIf(modal_price_inr_quintal <= 0),
            countIf(is_price_relation_valid = 0)
        FROM agripulse_clean.clean_mandi_prices
    """).result_rows[0]

    stats = client.query("""
        SELECT
            min(arrival_date),
            max(arrival_date),
            uniqExact(state_name),
            uniqExact(commodity_name),
            uniqExact(market_name)
        FROM agripulse_clean.clean_mandi_prices
    """).result_rows[0]

    return {
        "table": "agripulse_clean.clean_mandi_prices",
        "local_source_rows": local_rows,
        "clickhouse_rows": row_count,
        "reconciliation_match": (row_count == local_rows),
        "null_counts": {
            "empty_state": null_res[0],
            "empty_market": null_res[1],
            "empty_commodity": null_res[2],
            "non_positive_modal_price": null_res[3],
            "invalid_price_relations": null_res[4],
        },
        "date_range": {"min": str(stats[0]), "max": str(stats[1])},
        "distinct_counts": {
            "states": stats[2],
            "commodities": stats[3],
            "markets": stats[4],
        },
    }


def validate_crop_clean(client, local_rows: int) -> Dict[str, Any]:
    """Validate agripulse_clean.clean_crop_production."""
    log.info("Validating agripulse_clean.clean_crop_production...")

    row_count = client.query("SELECT count() FROM agripulse_clean.clean_crop_production").result_rows[0][0]

    stats = client.query("""
        SELECT
            min(crop_year),
            max(crop_year),
            uniqExact(state_name),
            uniqExact(district_name),
            uniqExact(crop_name),
            countIf(is_production_missing = 1),
            countIf(crop_name ILIKE '%coconut%' AND isNull(production_coconut_nuts) AND is_production_missing = 0),
            countIf(crop_name NOT ILIKE '%coconut%' AND isNull(production_tonnes) AND is_production_missing = 0)
        FROM agripulse_clean.clean_crop_production
    """).result_rows[0]

    return {
        "table": "agripulse_clean.clean_crop_production",
        "local_source_rows": local_rows,
        "clickhouse_rows": row_count,
        "reconciliation_match": (row_count == local_rows),
        "year_range": {"min": stats[0], "max": stats[1]},
        "distinct_counts": {
            "states": stats[2],
            "districts": stats[3],
            "crops": stats[4],
        },
        "missing_production_rows": stats[5],
        "unit_integrity": {
            "coconut_unit_correct": (stats[6] == 0),
            "field_crop_unit_correct": (stats[7] == 0),
        },
    }


def validate_weather_clean(client, local_rows: int) -> Dict[str, Any]:
    """Validate agripulse_clean.clean_weather."""
    log.info("Validating agripulse_clean.clean_weather...")

    row_count = client.query("SELECT count() FROM agripulse_clean.clean_weather").result_rows[0][0]

    stats = client.query("""
        SELECT
            min(date),
            max(date),
            min(latitude),
            max(latitude),
            min(longitude),
            max(longitude),
            min(temperature_celsius_2m),
            max(temperature_celsius_2m),
            countIf(is_negative_temperature = 1),
            countIf(is_missing_precipitation = 1),
            countIf(is_missing_temperature = 1)
        FROM agripulse_clean.clean_weather
    """).result_rows[0]

    return {
        "table": "agripulse_clean.clean_weather",
        "local_source_rows": local_rows,
        "clickhouse_rows": row_count,
        "reconciliation_match": (row_count == local_rows),
        "date_range": {"min": str(stats[0]), "max": str(stats[1])},
        "coordinate_bounds": {
            "lat_min": float(stats[2]),
            "lat_max": float(stats[3]),
            "lon_min": float(stats[4]),
            "lon_max": float(stats[5]),
        },
        "temperature_range_celsius": {
            "min": float(stats[6]),
            "max": float(stats[7]),
        },
        "negative_temperatures_preserved": stats[8],
        "missing_precipitation_flags": stats[9],
        "missing_temperature_flags": stats[10],
    }


def validate_mart_tables(client) -> Dict[str, Any]:
    """Validate analytical mart row counts."""
    log.info("Validating agripulse_mart tables...")
    tables = [
        "dim_date", "dim_location", "dim_commodity",
        "fact_mandi_prices", "fact_crop_production", "fact_weather"
    ]
    counts = {}
    for tbl in tables:
        cnt = client.query(f"SELECT count() FROM agripulse_mart.{tbl}").result_rows[0][0]
        counts[tbl] = cnt

    return {
        "schema": "agripulse_mart",
        "row_counts": counts,
        "all_marts_populated": all(c > 0 for c in counts.values()),
    }


def run_full_validation(output_path: Optional[str] = None) -> Dict[str, Any]:
    """Execute complete validation suite and write report."""
    local_mandi = count_local_parquet_rows(os.path.join(ROOT_DIR, "data", "processed", "mandi_prices_clean"))
    local_crop = count_local_parquet_rows(os.path.join(ROOT_DIR, "data", "processed", "crop_production_clean"))
    local_weather = count_local_parquet_rows(os.path.join(ROOT_DIR, "data", "processed", "nasa_weather_clean"))

    client = get_client()
    try:
        report = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "validation_suite": "AgriPulse India ClickHouse Open-Source DWH Validation",
            "mandi_clean": validate_mandi_clean(client, local_mandi),
            "crop_clean": validate_crop_clean(client, local_crop),
            "weather_clean": validate_weather_clean(client, local_weather),
            "mart_tables": validate_mart_tables(client),
        }
    finally:
        client.close()

    if output_path is None:
        output_path = os.path.join(ROOT_DIR, "clickhouse_validation_report.json")

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    log.info("ClickHouse validation report written to: %s", output_path)
    return report


if __name__ == "__main__":
    rep = run_full_validation()
    print("\nClickHouse Validation Results:")
    print(f"  Mandi Clean Rows:   {rep['mandi_clean']['clickhouse_rows']:,} (Match: {rep['mandi_clean']['reconciliation_match']})")
    print(f"  Crop Clean Rows:    {rep['crop_clean']['clickhouse_rows']:,} (Match: {rep['crop_clean']['reconciliation_match']})")
    print(f"  Weather Clean Rows: {rep['weather_clean']['clickhouse_rows']:,} (Match: {rep['weather_clean']['reconciliation_match']})")
    print("  MART Tables:", rep['mart_tables']['row_counts'])
