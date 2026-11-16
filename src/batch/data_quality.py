"""
AgriPulse India — Batch Data Quality Profiler and Validator
============================================================
Computes comprehensive before-and-after data quality metrics for each dataset:
  - Row and column counts
  - Missing values count and percentage per column
  - Duplicate record detection (exact duplicates and business key duplicates)
  - Data types per column
  - Date ranges
  - Categorical cardinailities (states, commodities, crops)
  - Coordinate bounding boxes (NASA latitude / longitude extents)
  - Parameter statistical distributions
  - Explicit tracking of cleaned, retained, flagged, or deduplicated records with reasons
"""

import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional
import numpy as np
import pandas as pd


def safe_convert(obj: Any) -> Any:
    """Recursively convert NumPy/Pandas scalar types into standard Python types for JSON serialization."""
    if isinstance(obj, (np.integer, int)):
        return int(obj)
    elif isinstance(obj, (np.floating, float)):
        return float(obj) if not np.isnan(obj) else None
    elif isinstance(obj, (np.bool_, bool)):
        return bool(obj)
    elif isinstance(obj, (pd.Timestamp, datetime)):
        return obj.isoformat()
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {k: safe_convert(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple, set)):
        return [safe_convert(v) for v in obj]
    return obj


def profile_dataset(
    df: pd.DataFrame,
    dataset_name: str,
    stage: str = "raw",
    date_col: Optional[str] = None,
    state_col: Optional[str] = None,
    item_col: Optional[str] = None,
    lat_col: Optional[str] = None,
    lon_col: Optional[str] = None,
    numeric_cols: Optional[List[str]] = None,
    business_key_cols: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Profile a DataFrame and return a structured dictionary of quality metrics.
    """
    if df is None or len(df) == 0:
        return {
            "dataset_name": dataset_name,
            "stage": stage,
            "total_rows": 0,
            "total_columns": 0,
            "error": "DataFrame is empty or None",
        }

    total_rows = len(df)
    total_cols = len(df.columns)

    # 1. Column missing values & data types
    col_missing = {}
    col_missing_pct = {}
    col_dtypes = {}
    for col in df.columns:
        null_count = int(df[col].isnull().sum())
        col_missing[col] = null_count
        col_missing_pct[col] = round((null_count / total_rows) * 100, 3)
        col_dtypes[col] = str(df[col].dtype)

    # 2. Duplicate detection
    exact_duplicates = int(df.duplicated().sum())
    business_key_duplicates = 0
    if business_key_cols:
        valid_b_cols = [c for c in business_key_cols if c in df.columns]
        if valid_b_cols:
            business_key_duplicates = int(df.duplicated(subset=valid_b_cols, keep=False).sum())

    # 3. Date range
    date_range = None
    if date_col and date_col in df.columns:
        parsed_dates = pd.to_datetime(df[date_col], errors="coerce")
        valid_dates = parsed_dates.dropna()
        if len(valid_dates) > 0:
            date_range = {
                "min_date": valid_dates.min().strftime("%Y-%m-%d"),
                "max_date": valid_dates.max().strftime("%Y-%m-%d"),
                "unparseable_date_count": int(parsed_dates.isnull().sum() - df[date_col].isnull().sum()),
            }

    # 4. State metrics
    state_summary = None
    if state_col and state_col in df.columns:
        unique_states = sorted([str(s) for s in df[state_col].dropna().unique()])
        state_summary = {
            "unique_state_count": len(unique_states),
            "states": unique_states,
        }

    # 5. Item / Commodity / Crop metrics
    item_summary = None
    if item_col and item_col in df.columns:
        unique_items = df[item_col].dropna().unique()
        top_items = df[item_col].value_counts().head(10).to_dict()
        item_summary = {
            "unique_item_count": int(len(unique_items)),
            "top_10_frequencies": {str(k): int(v) for k, v in top_items.items()},
        }

    # 6. Geographical bounding box
    geo_summary = None
    if lat_col and lon_col and lat_col in df.columns and lon_col in df.columns:
        geo_summary = {
            "min_latitude": float(df[lat_col].min()),
            "max_latitude": float(df[lat_col].max()),
            "min_longitude": float(df[lon_col].min()),
            "max_longitude": float(df[lon_col].max()),
            "unique_grid_points": int(df[[lat_col, lon_col]].drop_duplicates().shape[0]),
        }

    # 7. Numeric distributions
    numeric_summary = {}
    if numeric_cols is None:
        numeric_cols = [c for c in df.select_dtypes(include=[np.number]).columns]

    for col in numeric_cols:
        if col in df.columns:
            s = df[col].dropna()
            if len(s) > 0:
                numeric_summary[col] = {
                    "count": int(len(s)),
                    "min": float(s.min()),
                    "p25": float(s.quantile(0.25)),
                    "median": float(s.median()),
                    "p75": float(s.quantile(0.75)),
                    "max": float(s.max()),
                    "mean": float(s.mean()),
                    "std": float(s.std()) if len(s) > 1 else 0.0,
                    "zero_count": int((s == 0).sum()),
                    "negative_count": int((s < 0).sum()),
                }

    profile = {
        "dataset_name": dataset_name,
        "stage": stage,
        "timestamp": datetime.now().isoformat(),
        "total_rows": total_rows,
        "total_columns": total_cols,
        "column_names": list(df.columns),
        "data_types": col_dtypes,
        "missing_values_count": col_missing,
        "missing_values_pct": col_missing_pct,
        "exact_duplicates": exact_duplicates,
        "business_key_duplicates": business_key_duplicates,
        "date_range": date_range,
        "state_summary": state_summary,
        "item_summary": item_summary,
        "geo_summary": geo_summary,
        "numeric_summary": numeric_summary,
    }
    return safe_convert(profile)


def build_comparative_report(
    dataset_name: str,
    before_profile: Dict[str, Any],
    after_profile: Dict[str, Any],
    cleaning_decisions: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Construct a traceable comparative report tracking every transformation decision.
    """
    rows_before = before_profile.get("total_rows", 0)
    rows_after = after_profile.get("total_rows", 0)
    row_delta = rows_after - rows_before

    report = {
        "dataset_name": dataset_name,
        "report_generated_at": datetime.now().isoformat(),
        "summary": {
            "rows_raw_input": rows_before,
            "rows_processed_output": rows_after,
            "net_row_change": row_delta,
            "columns_before": before_profile.get("total_columns", 0),
            "columns_after": after_profile.get("total_columns", 0),
        },
        "cleaning_decisions_and_traceability": cleaning_decisions,
        "before_quality_profile": before_profile,
        "after_quality_profile": after_profile,
    }
    return safe_convert(report)


def save_quality_report(report_dict: Dict[str, Any], output_path: str) -> None:
    """Save quality report dictionary to a formatted JSON file."""
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report_dict, f, indent=2, ensure_ascii=False)
