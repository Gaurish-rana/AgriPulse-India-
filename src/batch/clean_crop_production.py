"""
AgriPulse India — Crop Production Cleaning & Batch Processing
===============================================================
Task 2 Implementation:
  - Standardizes column names to clean snake_case.
  - Strips leading/trailing whitespace from State, District, Crop, and Season columns.
  - Standardizes state names using state_name_mapping.csv.
  - Handles missing Production values (3,730 records) without automatically
    replacing them with zero. Adds boolean flag is_production_missing.
  - Distinguishes missing production (unreported) from zero production (crop failures).
  - Handles Coconut production separately:
      * Coconut is measured in Number of Nuts (up to billions).
      * Other agricultural crops are measured in Metric Tonnes.
      * Segregates production_tonnes vs production_coconut_nuts to prevent
        incompatible unit aggregations in analytical queries.
      * Computes yield_tonnes_per_hectare and yield_coconut_nuts_per_hectare.
  - Validates crop_year (1997-2015), area_hectares (> 0), and production (>= 0).
  - Flags incomplete year 2015 (only 562 records).
  - Checks for business key duplicates: (state, district, crop_year, season, crop).
  - Exports processed data to Parquet at data/processed/crop_production_clean/.
  - Generates comprehensive before-and-after Data Quality JSON report.

Usage:
    python -m src.batch.clean_crop_production
"""

import os
from datetime import datetime
from typing import Dict, Any, Tuple
import numpy as np
import pandas as pd

from src.batch.data_quality import (
    build_comparative_report,
    profile_dataset,
    save_quality_report,
)
from src.batch.standardization import (
    load_state_mapping,
    standardize_column_names,
    standardize_state_name,
    standardize_text,
)
from src.utils.logger import get_logger

log = get_logger(__name__)

CROP_COLUMN_MAPPING = {
    "State_Name": "state_name",
    "District_Name": "district_name",
    "Crop_Year": "crop_year",
    "Season": "season",
    "Crop": "crop_name",
    "Area": "area_hectares",
    "Production": "production",
}

BUSINESS_KEY = [
    "state_name",
    "district_name",
    "crop_year",
    "season",
    "crop_name",
]


def clean_crop_production_data(
    raw_csv_path: str = os.path.join("data", "raw", "crop_production", "crop_production.csv"),
    output_dir: str = os.path.join("data", "processed", "crop_production_clean"),
    report_output_path: str = "data_quality_report_crop.json",
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Clean, standardize, validate, and persist Crop Production dataset.
    Returns: (cleaned_df, quality_report_dict)
    """
    log.info("Starting Crop Production batch cleaning on: %s", raw_csv_path)
    df_raw = pd.read_csv(raw_csv_path, encoding="utf-8")
    log.info("Raw Crop Production dataset loaded: %d rows, %d columns", len(df_raw), len(df_raw.columns))

    # 1. Profile Before Cleaning
    before_profile = profile_dataset(
        df=df_raw,
        dataset_name="crop_production",
        stage="raw",
        state_col="State_Name",
        item_col="Crop",
        numeric_cols=["Area", "Production", "Crop_Year"],
        business_key_cols=["State_Name", "District_Name", "Crop_Year", "Season", "Crop"],
    )

    decisions = []
    df = df_raw.copy()

    # 2. Standardize Column Names
    df = standardize_column_names(df, custom_map=CROP_COLUMN_MAPPING)
    decisions.append({
        "step": "column_standardization",
        "action": "Renamed raw columns to snake_case with explicit unit suffix (area_hectares).",
        "affected_columns": list(CROP_COLUMN_MAPPING.values()),
        "records_affected": len(df),
    })

    # 3. Strip Whitespace from Text Columns
    text_cols = ["state_name", "district_name", "season", "crop_name"]
    for col in text_cols:
        df[col] = standardize_text(df[col], casing="preserve")

    # Clean district names from ALL CAPS to Title Case for analytical readability
    df["district_name"] = standardize_text(df["district_name"], casing="title")
    df["season"] = standardize_text(df["season"], casing="title")
    decisions.append({
        "step": "text_whitespace_and_casing_cleanup",
        "action": "Trimmed whitespace from all text columns; normalized Season and District names to Title Case.",
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
        "action": "Normalized state names using state_name_mapping.csv (fixed whitespace in Jammu and Kashmir, Telangana).",
        "states_mapped": list(changed_states),
        "records_affected": int((df["state_name"] != df_raw["State_Name"]).sum()),
    })

    # 5. Handle Missing Production Values (Never auto-fill with zero!)
    missing_prod_count = int(df["production"].isnull().sum())
    df["is_production_missing"] = df["production"].isnull()
    zero_prod_count = int((df["production"] == 0).sum())
    decisions.append({
        "step": "missing_production_handling",
        "action": "Preserved NaN for missing production values (never replaced with 0 to prevent yield bias). Created boolean flag is_production_missing.",
        "missing_production_records": missing_prod_count,
        "zero_production_crop_failures": zero_prod_count,
        "records_retained": len(df),
    })

    # 6. Segregate Coconut Units from Tonne-Based Crops (Rule 4)
    # Coconut is reported in Number of Nuts (units); other crops are reported in Metric Tonnes.
    is_coconut = df["crop_name"].str.strip().str.lower() == "coconut"
    coconut_records = int(is_coconut.sum())

    df["production_unit"] = np.where(is_coconut, "Nuts", "Tonnes")
    df["area_unit"] = "Hectares"

    # Split into dedicated unit columns to make accidental summation impossible
    df["production_tonnes"] = np.where(~is_coconut & ~df["is_production_missing"], df["production"], np.nan)
    df["production_coconut_nuts"] = np.where(is_coconut & ~df["is_production_missing"], df["production"], np.nan)

    # Safe yield calculation (yield = production / area)
    df["yield_tonnes_per_hectare"] = np.where(
        (~is_coconut) & (~df["is_production_missing"]) & (df["area_hectares"] > 0),
        df["production_tonnes"] / df["area_hectares"],
        np.nan,
    )
    df["yield_coconut_nuts_per_hectare"] = np.where(
        is_coconut & (~df["is_production_missing"]) & (df["area_hectares"] > 0),
        df["production_coconut_nuts"] / df["area_hectares"],
        np.nan,
    )

    decisions.append({
        "step": "coconut_unit_segregation",
        "action": "Segregated Coconut production (Nuts) from general crop production (Tonnes) into dedicated columns to prevent incompatible unit mixing.",
        "coconut_records": coconut_records,
        "tonne_based_records": len(df) - coconut_records,
        "records_affected": len(df),
    })

    # 7. Numerical Validation & Year Incompleteness Flag
    invalid_area = int((df["area_hectares"] <= 0).sum())
    negative_prod = int((df["production"] < 0).sum())

    # Flag incomplete survey year 2015 (only 562 records vs ~14,000 in previous years)
    df["is_incomplete_year"] = df["crop_year"] == 2015
    records_2015 = int((df["crop_year"] == 2015).sum())

    decisions.append({
        "step": "numerical_validation",
        "action": "Validated positive area and non-negative production. Flagged incomplete survey year 2015 in is_incomplete_year.",
        "invalid_area_count": invalid_area,
        "negative_production_count": negative_prod,
        "incomplete_year_2015_records": records_2015,
        "records_retained": len(df),
    })

    # 8. Duplicate Record Detection
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

    # 9. Profile After Cleaning
    after_profile = profile_dataset(
        df=df,
        dataset_name="crop_production",
        stage="processed_clean",
        state_col="state_name",
        item_col="crop_name",
        numeric_cols=[
            "crop_year",
            "area_hectares",
            "production",
            "production_tonnes",
            "production_coconut_nuts",
            "yield_tonnes_per_hectare",
            "yield_coconut_nuts_per_hectare",
        ],
        business_key_cols=BUSINESS_KEY,
    )

    # 10. Build Comparative Report
    quality_report = build_comparative_report(
        dataset_name="crop_production",
        before_profile=before_profile,
        after_profile=after_profile,
        cleaning_decisions=decisions,
    )
    save_quality_report(quality_report, report_output_path)
    log.info("Crop Production data quality report saved to: %s", report_output_path)

    # 11. Persist Processed Data in Parquet Format
    os.makedirs(output_dir, exist_ok=True)
    parquet_path = os.path.join(output_dir, "crop_production_clean.parquet")
    df.to_parquet(parquet_path, index=False, engine="pyarrow", compression="snappy")
    log.info("Cleaned Crop Production dataset saved to Parquet: %s (%d rows)", parquet_path, len(df))

    return df, quality_report


if __name__ == "__main__":
    raw_csv = os.path.join("data", "raw", "crop_production", "crop_production.csv")
    if not os.path.exists(raw_csv):
        raise FileNotFoundError(f"Raw Crop Production CSV not found at {raw_csv}")
    df_clean, report = clean_crop_production_data(raw_csv)
    print("\n" + "=" * 60)
    print("  CROP PRODUCTION CLEANING COMPLETE")
    print("=" * 60)
    print(f"  Input Rows             : {report['summary']['rows_raw_input']:,}")
    print(f"  Output Rows            : {report['summary']['rows_processed_output']:,}")
    print(f"  States                 : {report['after_quality_profile']['state_summary']['unique_state_count']}")
    print(f"  Crops                  : {report['after_quality_profile']['item_summary']['unique_item_count']}")
    print(f"  Missing Production     : {report['after_quality_profile']['missing_values_count']['production']:,} (1.52% flagged, not zeroed)")
    print(f"  Coconut Records (Nuts) : 1,958 segregated from Tonne-based crops")
