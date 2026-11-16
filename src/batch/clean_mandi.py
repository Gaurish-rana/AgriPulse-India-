"""
AgriPulse India — Mandi Price Cleaning & Batch Processing
===========================================================
Task 1 Implementation:
  - Standardizes column names to clean snake_case with explicit unit suffixes.
  - Converts arrival_date (DD/MM/YYYY) to standard ISO format (YYYY-MM-DD).
  - Casts price columns into float64 numeric types.
  - Trims leading/trailing whitespace across all text columns.
  - Standardizes state names using state_name_mapping.csv.
  - Validates price logic (min_price <= modal_price <= max_price).
  - Checks for duplicate records using the multi-attribute business key:
      (state, district, market, commodity, variety, grade, arrival_date).
  - Preserves raw input data unchanged.
  - Exports processed data to Parquet at data/processed/mandi_prices_clean/.
  - Generates comprehensive before-and-after Data Quality JSON report.

Usage:
    python -m src.batch.clean_mandi
"""

import glob
import os
from datetime import datetime
from typing import Dict, Any, Tuple
import pandas as pd

from src.batch.data_quality import (
    build_comparative_report,
    profile_dataset,
    save_quality_report,
)
from src.batch.standardization import (
    load_state_mapping,
    parse_date_series,
    standardize_column_names,
    standardize_state_name,
    standardize_text,
)
from src.utils.config import MandiConfig, PathConfig
from src.utils.logger import get_logger

log = get_logger(__name__)


MANDI_COLUMN_MAPPING = {
    "state": "state_name",
    "district": "district_name",
    "market": "market_name",
    "commodity": "commodity_name",
    "variety": "variety_name",
    "grade": "grade",
    "arrival_date": "arrival_date",
    "min_price": "min_price_inr_quintal",
    "max_price": "max_price_inr_quintal",
    "modal_price": "modal_price_inr_quintal",
}

BUSINESS_KEY = [
    "state_name",
    "district_name",
    "market_name",
    "commodity_name",
    "variety_name",
    "grade",
    "arrival_date",
]


def clean_mandi_data(
    raw_csv_path: str,
    output_dir: str = os.path.join("data", "processed", "mandi_prices_clean"),
    report_output_path: str = "data_quality_report_mandi.json",
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Clean, standardize, validate, and persist Mandi price dataset.
    Returns: (cleaned_df, quality_report_dict)
    """
    log.info("Starting Mandi Price batch cleaning on: %s", raw_csv_path)
    df_raw = pd.read_csv(raw_csv_path, encoding="utf-8")
    log.info("Raw Mandi dataset loaded: %d rows, %d columns", len(df_raw), len(df_raw.columns))

    # 1. Profile Before Cleaning
    before_profile = profile_dataset(
        df=df_raw,
        dataset_name="mandi_prices",
        stage="raw",
        date_col="arrival_date",
        state_col="state",
        item_col="commodity",
        numeric_cols=["min_price", "max_price", "modal_price"],
        business_key_cols=["state", "district", "market", "commodity", "variety", "grade", "arrival_date"],
    )

    decisions = []
    df = df_raw.copy()

    # 2. Standardize Column Names
    df = standardize_column_names(df, custom_map=MANDI_COLUMN_MAPPING)
    decisions.append({
        "step": "column_standardization",
        "action": "Renamed raw columns to snake_case with explicit unit suffixes (INR/quintal).",
        "affected_columns": list(MANDI_COLUMN_MAPPING.values()),
        "records_affected": len(df),
    })

    # 3. Clean Text Columns (Strip whitespace, normalize spacing)
    text_cols = ["state_name", "district_name", "market_name", "commodity_name", "variety_name", "grade"]
    for col in text_cols:
        if col in df.columns:
            df[col] = standardize_text(df[col], casing="preserve")
    decisions.append({
        "step": "text_whitespace_stripping",
        "action": "Trimmed leading/trailing whitespace and normalized internal repeated spaces.",
        "affected_columns": text_cols,
        "records_affected": len(df),
    })

    # 4. Standardize State Names using Mapping Table
    state_map = load_state_mapping()
    states_before = set(df["state_name"].unique())
    df["state_name"] = standardize_state_name(df["state_name"], mapping=state_map)
    states_after = set(df["state_name"].unique())
    changed_states = states_before - states_after
    decisions.append({
        "step": "state_name_standardization",
        "action": "Normalized state names against official Government of India standards using state_name_mapping.csv.",
        "states_mapped": list(changed_states),
        "records_affected": int((df["state_name"] != df_raw["state"]).sum()),
    })

    # 5. Convert and Validate Arrival Date (DD/MM/YYYY -> YYYY-MM-DD)
    raw_dates = df["arrival_date"].copy()
    df["arrival_date"] = parse_date_series(df["arrival_date"], format_pattern="%d/%m/%Y")
    unparseable_dates = int(df["arrival_date"].isnull().sum() - raw_dates.isnull().sum())
    # Add year and month helper partition columns
    df["arrival_year"] = pd.to_datetime(df["arrival_date"]).dt.year
    df["arrival_month"] = pd.to_datetime(df["arrival_date"]).dt.month
    decisions.append({
        "step": "date_standardization",
        "action": "Converted arrival_date from DD/MM/YYYY to ISO 8601 YYYY-MM-DD format.",
        "unparseable_dates_found": unparseable_dates,
        "records_affected": len(df),
    })

    # 6. Numeric Casting and Price Validation
    price_cols = ["min_price_inr_quintal", "max_price_inr_quintal", "modal_price_inr_quintal"]
    for p_col in price_cols:
        df[p_col] = pd.to_numeric(df[p_col], errors="coerce")

    # Business rule: min_price <= modal_price <= max_price
    invalid_price_relation = (
        (df["min_price_inr_quintal"] > df["modal_price_inr_quintal"])
        | (df["modal_price_inr_quintal"] > df["max_price_inr_quintal"])
        | (df["min_price_inr_quintal"] > df["max_price_inr_quintal"])
    )
    invalid_price_count = int(invalid_price_relation.sum())

    # Check for zero or negative prices
    non_positive_prices = int(
        ((df["min_price_inr_quintal"] <= 0) | (df["max_price_inr_quintal"] <= 0) | (df["modal_price_inr_quintal"] <= 0)).sum()
    )

    df["is_price_relation_valid"] = ~invalid_price_relation
    df["currency"] = "INR"
    df["price_unit"] = "INR/quintal"

    decisions.append({
        "step": "price_validation",
        "action": "Verified numeric types and validated min <= modal <= max constraint. Flagged in is_price_relation_valid.",
        "invalid_price_logic_records": invalid_price_count,
        "non_positive_price_records": non_positive_prices,
        "records_retained": len(df),
    })

    # 7. Duplicate Record Detection & Handling
    exact_dups = int(df.duplicated().sum())
    b_key_dups = int(df.duplicated(subset=BUSINESS_KEY, keep=False).sum())
    if b_key_dups > 0:
        log.warning("Detected %d duplicate records on business key. Preserving first occurrence.", b_key_dups)
        df = df.drop_duplicates(subset=BUSINESS_KEY, keep="first")
        decisions.append({
            "step": "deduplication",
            "action": "Removed duplicate records on business key, keeping first occurrence.",
            "duplicates_removed": b_key_dups,
        })
    else:
        decisions.append({
            "step": "deduplication",
            "action": "Verified zero exact and zero business key duplicates.",
            "duplicates_removed": 0,
        })

    # 8. Profile After Cleaning
    after_profile = profile_dataset(
        df=df,
        dataset_name="mandi_prices",
        stage="processed_clean",
        date_col="arrival_date",
        state_col="state_name",
        item_col="commodity_name",
        numeric_cols=price_cols,
        business_key_cols=BUSINESS_KEY,
    )

    # 9. Build Comparative Report
    quality_report = build_comparative_report(
        dataset_name="mandi_prices",
        before_profile=before_profile,
        after_profile=after_profile,
        cleaning_decisions=decisions,
    )
    save_quality_report(quality_report, report_output_path)
    log.info("Mandi data quality report saved to: %s", report_output_path)

    # 10. Persist Processed Data in Parquet Format
    os.makedirs(output_dir, exist_ok=True)
    parquet_path = os.path.join(output_dir, "mandi_prices_clean.parquet")
    df.to_parquet(parquet_path, index=False, engine="pyarrow", compression="snappy")
    log.info("Cleaned Mandi dataset saved to Parquet: %s (%d rows)", parquet_path, len(df))

    return df, quality_report


if __name__ == "__main__":
    raw_files = glob.glob(os.path.join("data", "raw", "mandi", "*.csv"))
    if not raw_files:
        raise FileNotFoundError("No raw Mandi CSV files found in data/raw/mandi/")
    raw_input = raw_files[0]
    df_clean, report = clean_mandi_data(raw_input)
    print("\n" + "=" * 60)
    print("  MANDI PRICES CLEANING COMPLETE")
    print("=" * 60)
    print(f"  Input Rows  : {report['summary']['rows_raw_input']:,}")
    print(f"  Output Rows : {report['summary']['rows_processed_output']:,}")
    print(f"  States      : {report['after_quality_profile']['state_summary']['unique_state_count']}")
    print(f"  Commodities : {report['after_quality_profile']['item_summary']['unique_item_count']}")
    print(f"  Date Range  : {report['after_quality_profile']['date_range']['min_date']} to {report['after_quality_profile']['date_range']['max_date']}")
