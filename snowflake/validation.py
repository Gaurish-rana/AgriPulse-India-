"""
AgriPulse India — Snowflake Validation & Reconciliation Module
==============================================================
Validates loaded datasets across RAW, CLEAN, and MART schemas:
  1. Source-to-Snowflake row count reconciliation
  2. Null count checks on key analytical columns
  3. Duplicate checks on business keys
  4. Date range coverage
  5. Distinct state, district, commodity, and crop counts
  6. Weather coordinate ranges (0.5x0.5 deg grid bounds)
  7. Measurement unit integrity (Coconut nuts vs field crop tonnes)
  8. Generates a structured JSON validation report
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
from snowflake.connection import get_snowflake_connection, validate_config

log = get_logger(__name__)


def count_local_parquet_rows(directory: str) -> int:
    """Accurately count rows across all parquet files in a directory using metadata."""
    files = glob.glob(os.path.join(directory, "**", "*.parquet"), recursive=True)
    total = 0
    for f in files:
        pf = pq.ParquetFile(f)
        total += pf.metadata.num_rows
    return total


def run_query(conn, query: str) -> List[tuple]:
    """Execute a read-only query and return fetched rows."""
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    return rows


def validate_mandi_clean(conn, local_rows: int) -> Dict[str, Any]:
    """Validate CLEAN.CLEAN_MANDI_PRICES."""
    log.info("Validating CLEAN.CLEAN_MANDI_PRICES...")
    
    # Row count
    row_count = run_query(conn, "SELECT COUNT(*) FROM CLEAN.CLEAN_MANDI_PRICES")[0][0]
    
    # Nulls on critical columns
    nulls = run_query(conn, """
        SELECT
            COUNT(CASE WHEN state_name IS NULL THEN 1 END),
            COUNT(CASE WHEN market_name IS NULL THEN 1 END),
            COUNT(CASE WHEN commodity_name IS NULL THEN 1 END),
            COUNT(CASE WHEN arrival_date IS NULL THEN 1 END),
            COUNT(CASE WHEN modal_price_inr_quintal IS NULL THEN 1 END)
        FROM CLEAN.CLEAN_MANDI_PRICES
    """)[0]
    
    # Date and distinct stats
    stats = run_query(conn, """
        SELECT
            MIN(arrival_date),
            MAX(arrival_date),
            COUNT(DISTINCT state_name),
            COUNT(DISTINCT commodity_name),
            COUNT(DISTINCT market_name),
            COUNT(CASE WHEN modal_price_inr_quintal <= 0 THEN 1 END),
            COUNT(CASE WHEN NOT is_price_relation_valid THEN 1 END)
        FROM CLEAN.CLEAN_MANDI_PRICES
    """)[0]
    
    # Duplicates check on business key
    dup_rows = run_query(conn, """
        SELECT COUNT(*) FROM (
            SELECT state_name, district_name, market_name, commodity_name, variety_name, grade, arrival_date
            FROM CLEAN.CLEAN_MANDI_PRICES
            GROUP BY 1, 2, 3, 4, 5, 6, 7
            HAVING COUNT(*) > 1
        )
    """)[0][0]

    return {
        "table": "CLEAN.CLEAN_MANDI_PRICES",
        "local_source_rows": local_rows,
        "snowflake_rows": row_count,
        "reconciliation_match": (row_count == local_rows),
        "duplicate_business_keys": dup_rows,
        "null_counts": {
            "state_name": nulls[0],
            "market_name": nulls[1],
            "commodity_name": nulls[2],
            "arrival_date": nulls[3],
            "modal_price_inr_quintal": nulls[4],
        },
        "date_range": {"min": str(stats[0]), "max": str(stats[1])},
        "distinct_counts": {
            "states": stats[2],
            "commodities": stats[3],
            "markets": stats[4],
        },
        "invalid_prices": stats[5],
        "inverted_prices_count": stats[6],
    }


def validate_crop_clean(conn, local_rows: int) -> Dict[str, Any]:
    """Validate CLEAN.CLEAN_CROP_PRODUCTION."""
    log.info("Validating CLEAN.CLEAN_CROP_PRODUCTION...")
    
    row_count = run_query(conn, "SELECT COUNT(*) FROM CLEAN.CLEAN_CROP_PRODUCTION")[0][0]
    
    stats = run_query(conn, """
        SELECT
            MIN(crop_year),
            MAX(crop_year),
            COUNT(DISTINCT state_name),
            COUNT(DISTINCT district_name),
            COUNT(DISTINCT crop_name),
            COUNT(CASE WHEN is_production_missing THEN 1 END),
            COUNT(CASE WHEN crop_name ILIKE '%coconut%' AND production_coconut_nuts IS NULL AND NOT is_production_missing THEN 1 END),
            COUNT(CASE WHEN crop_name NOT ILIKE '%coconut%' AND production_tonnes IS NULL AND NOT is_production_missing THEN 1 END)
        FROM CLEAN.CLEAN_CROP_PRODUCTION
    """)[0]

    dup_rows = run_query(conn, """
        SELECT COUNT(*) FROM (
            SELECT state_name, district_name, crop_year, season, crop_name
            FROM CLEAN.CLEAN_CROP_PRODUCTION
            GROUP BY 1, 2, 3, 4, 5
            HAVING COUNT(*) > 1
        )
    """)[0][0]

    return {
        "table": "CLEAN.CLEAN_CROP_PRODUCTION",
        "local_source_rows": local_rows,
        "snowflake_rows": row_count,
        "reconciliation_match": (row_count == local_rows),
        "duplicate_business_keys": dup_rows,
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


def validate_weather_clean(conn, local_rows: int) -> Dict[str, Any]:
    """Validate CLEAN.CLEAN_WEATHER."""
    log.info("Validating CLEAN.CLEAN_WEATHER...")
    
    row_count = run_query(conn, "SELECT COUNT(*) FROM CLEAN.CLEAN_WEATHER")[0][0]
    
    stats = run_query(conn, """
        SELECT
            MIN(weather_date),
            MAX(weather_date),
            MIN(latitude),
            MAX(latitude),
            MIN(longitude),
            MAX(longitude),
            MIN(temperature_celsius_2m),
            MAX(temperature_celsius_2m),
            COUNT(CASE WHEN is_negative_temperature THEN 1 END),
            COUNT(CASE WHEN is_missing_precipitation THEN 1 END),
            COUNT(CASE WHEN is_missing_temperature THEN 1 END)
        FROM CLEAN.CLEAN_WEATHER
    """)[0]

    return {
        "table": "CLEAN.CLEAN_WEATHER",
        "local_source_rows": local_rows,
        "snowflake_rows": row_count,
        "reconciliation_match": (row_count == local_rows),
        "date_range": {"min": str(stats[0]), "max": str(stats[1])},
        "coordinate_bounds": {
            "lat_min": float(stats[2]) if stats[2] is not None else None,
            "lat_max": float(stats[3]) if stats[3] is not None else None,
            "lon_min": float(stats[4]) if stats[4] is not None else None,
            "lon_max": float(stats[5]) if stats[5] is not None else None,
        },
        "temperature_range_celsius": {
            "min": float(stats[6]) if stats[6] is not None else None,
            "max": float(stats[7]) if stats[7] is not None else None,
        },
        "negative_temperatures_preserved": stats[8],
        "missing_precipitation_flags": stats[9],
        "missing_temperature_flags": stats[10],
    }


def validate_mart_tables(conn) -> Dict[str, Any]:
    """Validate analytical MART dimensions and fact tables."""
    log.info("Validating MART Schema tables...")
    
    mart_counts = {}
    for tbl in [
        "DIM_DATE", "DIM_LOCATION", "DIM_COMMODITY",
        "FACT_MANDI_PRICES", "FACT_CROP_PRODUCTION", "FACT_WEATHER"
    ]:
        cnt = run_query(conn, f"SELECT COUNT(*) FROM MART.{tbl}")[0][0]
        mart_counts[tbl] = cnt

    return {
        "schema": "MART",
        "row_counts": mart_counts,
        "all_marts_populated": all(c > 0 for c in mart_counts.values()),
    }


def run_full_validation(output_path: Optional[str] = None) -> Dict[str, Any]:
    """Run all validation suites and produce a comprehensive report."""
    if not validate_config():
        raise ValueError("Snowflake credentials missing. Cannot execute validation.")

    local_mandi = count_local_parquet_rows(os.path.join(ROOT_DIR, "data", "processed", "mandi_prices_clean"))
    local_crop = count_local_parquet_rows(os.path.join(ROOT_DIR, "data", "processed", "crop_production_clean"))
    local_weather = count_local_parquet_rows(os.path.join(ROOT_DIR, "data", "processed", "nasa_weather_clean"))

    conn = get_snowflake_connection()
    try:
        report = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "validation_suite": "AgriPulse India Snowflake Warehouse Validation",
            "mandi_clean": validate_mandi_clean(conn, local_mandi),
            "crop_clean": validate_crop_clean(conn, local_crop),
            "weather_clean": validate_weather_clean(conn, local_weather),
            "mart_tables": validate_mart_tables(conn),
        }
    finally:
        conn.close()

    if output_path is None:
        output_path = os.path.join(ROOT_DIR, "snowflake_validation_report.json")

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    log.info("Validation report written to: %s", output_path)
    return report


if __name__ == "__main__":
    if not validate_config():
        print("Cannot run validation: Snowflake credentials missing in .env")
        sys.exit(1)

    rep = run_full_validation()
    print("\nSnowflake Validation Results:")
    print(f"  Mandi Clean Rows:   {rep['mandi_clean']['snowflake_rows']:,} (Match: {rep['mandi_clean']['reconciliation_match']})")
    print(f"  Crop Clean Rows:    {rep['crop_clean']['snowflake_rows']:,} (Match: {rep['crop_clean']['reconciliation_match']})")
    print(f"  Weather Clean Rows: {rep['weather_clean']['snowflake_rows']:,} (Match: {rep['weather_clean']['reconciliation_match']})")
    print("  MART Tables:", rep['mart_tables']['row_counts'])
