"""
AgriPulse India — NASA POWER Weather Cleaning & Batch Processing
==================================================================
Task 3 Implementation:
  - Memory-efficient processing: Processes the 784 CSV files year-by-year (2020-2026).
  - Skips 9-line metadata headers on each raw file.
  - Investigates and resolves tile boundary overlap:
      * Adjacent tiles share boundary latitudes (e.g. 10.5, 14.5, 18.5, etc.)
        with identical values.
      * Deduplicates on (LAT, LON, YEAR, DOY) within each parameter and year.
  - Converts -999 missing markers into proper null values (NaN).
  - Preserves legitimate negative temperatures (e.g. Himalayan winter observations).
  - Creates explicit data quality flags:
      * is_missing_precipitation: -999 missing rainfall marker converted to null
      * is_missing_temperature: -999 missing temperature marker converted to null
      * is_negative_temperature: True for temperatures below 0 °C
      * is_subzero_high_altitude: True for negative temp at lat >= 30.5° (Himalayan grid)
      * is_extreme_precipitation: True for rainfall > 200 mm/day
  - Consolidates PRECTOTCORR and T2M onto a single daily grid-cell record:
      (date, year, month, day, day_of_year, latitude, longitude,
       precipitation_mm_per_day, temperature_celsius_2m, ...)
  - Partitions output by year in Snappy-compressed Parquet:
      data/processed/nasa_weather_clean/year=YYYY/part-0.parquet
  - Generates comprehensive before-and-after Data Quality JSON report.

Usage:
    python -m src.batch.clean_nasa_weather
"""

import gc
import glob
import io
import os
from datetime import datetime
from typing import Dict, Any, List, Tuple
import numpy as np
import pandas as pd

from src.batch.data_quality import (
    build_comparative_report,
    safe_convert,
    save_quality_report,
)
from src.utils.logger import get_logger

log = get_logger(__name__)


def read_nasa_csv(filepath: str) -> pd.DataFrame:
    """Read a raw NASA POWER CSV file, skipping the metadata header."""
    with open(filepath, "r", encoding="utf-8") as f:
        lines = f.readlines()

    header_end = 0
    for idx, line in enumerate(lines):
        if "-END HEADER-" in line:
            header_end = idx
            break

    data_str = "".join(lines[header_end + 1:])
    df = pd.read_csv(io.StringIO(data_str))
    return df


def clean_nasa_weather_data(
    raw_dir: str = os.path.join("data", "raw", "nasa_power"),
    output_dir: str = os.path.join("data", "processed", "nasa_weather_clean"),
    report_output_path: str = "data_quality_report_nasa.json",
    target_years: List[str] = None,
) -> Dict[str, Any]:
    """
    Clean, standardize, validate, and partition NASA POWER weather dataset year-by-year.
    Never loads all 16M rows into memory at once.
    """
    if target_years is None:
        target_years = ["2020", "2021", "2022", "2023", "2024", "2025", "2026"]

    log.info("Starting memory-efficient NASA POWER weather batch cleaning across years: %s", target_years)
    os.makedirs(output_dir, exist_ok=True)

    # Accumulators for overall metrics
    total_raw_files = 0
    total_raw_rows = 0
    total_processed_rows = 0
    total_boundary_duplicates_dropped = 0
    total_missing_precip_markers = 0
    total_missing_temp_markers = 0
    total_negative_temps_preserved = 0
    total_himalayan_subzero_records = 0
    total_extreme_rain_records = 0

    year_summaries = {}
    overall_min_lat = 999.0
    overall_max_lat = -999.0
    overall_min_lon = 999.0
    overall_max_lon = -999.0

    decisions = []

    for year_str in target_years:
        log.info("--- Processing NASA POWER Year: %s ---", year_str)
        p_pattern = os.path.join(raw_dir, f"PRECTOTCORR_{year_str}_*.csv")
        t_pattern = os.path.join(raw_dir, f"T2M_{year_str}_*.csv")

        p_files = glob.glob(p_pattern)
        t_files = glob.glob(t_pattern)
        total_raw_files += len(p_files) + len(t_files)

        if not p_files or not t_files:
            log.warning("Missing files for year %s (precip files=%d, temp files=%d)", year_str, len(p_files), len(t_files))
            continue

        # 1. Process PRECTOTCORR tiles for this year
        dfs_p = []
        year_raw_p_rows = 0
        for f in p_files:
            df_tile = read_nasa_csv(f)
            year_raw_p_rows += len(df_tile)
            dfs_p.append(df_tile)

        df_p_combined = pd.concat(dfs_p, ignore_index=True)
        del dfs_p
        gc.collect()

        # Deduplicate boundary overlap
        p_dups_count = int(df_p_combined.duplicated(subset=["LAT", "LON", "YEAR", "DOY"]).sum())
        df_p_dedup = df_p_combined.drop_duplicates(subset=["LAT", "LON", "YEAR", "DOY"], keep="first").copy()
        del df_p_combined
        gc.collect()

        # Convert -999 missing marker to null
        missing_p_count = int((df_p_dedup["PRECTOTCORR"] == -999).sum())
        total_missing_precip_markers += missing_p_count
        df_p_dedup["is_missing_precipitation"] = df_p_dedup["PRECTOTCORR"] == -999
        df_p_dedup["precipitation_mm_per_day"] = np.where(
            df_p_dedup["is_missing_precipitation"], np.nan, df_p_dedup["PRECTOTCORR"]
        )
        df_p_dedup = df_p_dedup.drop(columns=["PRECTOTCORR"])

        # 2. Process T2M tiles for this year
        dfs_t = []
        year_raw_t_rows = 0
        for f in t_files:
            df_tile = read_nasa_csv(f)
            year_raw_t_rows += len(df_tile)
            dfs_t.append(df_tile)

        df_t_combined = pd.concat(dfs_t, ignore_index=True)
        del dfs_t
        gc.collect()

        t_dups_count = int(df_t_combined.duplicated(subset=["LAT", "LON", "YEAR", "DOY"]).sum())
        df_t_dedup = df_t_combined.drop_duplicates(subset=["LAT", "LON", "YEAR", "DOY"], keep="first").copy()
        del df_t_combined
        gc.collect()

        missing_t_count = int((df_t_dedup["T2M"] == -999).sum())
        total_missing_temp_markers += missing_t_count
        df_t_dedup["is_missing_temperature"] = df_t_dedup["T2M"] == -999
        df_t_dedup["temperature_celsius_2m"] = np.where(
            df_t_dedup["is_missing_temperature"], np.nan, df_t_dedup["T2M"]
        )
        df_t_dedup = df_t_dedup.drop(columns=["T2M"])

        year_boundary_dups = p_dups_count + t_dups_count
        total_boundary_duplicates_dropped += year_boundary_dups
        total_raw_rows += year_raw_p_rows + year_raw_t_rows

        # 3. Merge Precipitation and Temperature onto single daily grid-cell record
        df_merged = pd.merge(
            df_p_dedup,
            df_t_dedup,
            on=["LAT", "LON", "YEAR", "DOY"],
            how="inner",
        )
        del df_p_dedup, df_t_dedup
        gc.collect()

        # 4. Standardize Column Names and Types
        df_merged = df_merged.rename(columns={
            "LAT": "latitude",
            "LON": "longitude",
            "YEAR": "year",
            "DOY": "day_of_year",
        })

        # Parse Date: YYYY-DOY -> YYYY-MM-DD
        date_series = pd.to_datetime(
            df_merged["year"].astype(str) + "-" + df_merged["day_of_year"].astype(str),
            format="%Y-%j",
        )
        df_merged["date"] = date_series.dt.strftime("%Y-%m-%d")
        df_merged["month"] = date_series.dt.month
        df_merged["day"] = date_series.dt.day

        # 5. Quality Flags and Physical Range Checks
        # Legitimate negative temperatures (Rule 3)
        valid_temp = df_merged["temperature_celsius_2m"].dropna()
        neg_temp_count = int((valid_temp < 0).sum())
        himalayan_subzero_count = int(((valid_temp < 0) & (df_merged["latitude"] >= 30.5)).sum())
        extreme_cold_count = int((valid_temp < -20.0).sum())

        valid_precip = df_merged["precipitation_mm_per_day"].dropna()
        extreme_rain_count = int((valid_precip > 200.0).sum())

        total_negative_temps_preserved += neg_temp_count
        total_himalayan_subzero_records += himalayan_subzero_count
        total_extreme_rain_records += extreme_rain_count

        df_merged["is_negative_temperature"] = df_merged["temperature_celsius_2m"] < 0
        df_merged["is_subzero_high_altitude"] = (df_merged["temperature_celsius_2m"] < 0) & (df_merged["latitude"] >= 30.5)
        df_merged["is_extreme_precipitation"] = df_merged["precipitation_mm_per_day"] > 200.0

        # Units and metadata
        df_merged["precipitation_unit"] = "mm/day"
        df_merged["temperature_unit"] = "celsius"

        # Track geo bounds
        overall_min_lat = min(overall_min_lat, float(df_merged["latitude"].min()))
        overall_max_lat = max(overall_max_lat, float(df_merged["latitude"].max()))
        overall_min_lon = min(overall_min_lon, float(df_merged["longitude"].min()))
        overall_max_lon = max(overall_max_lon, float(df_merged["longitude"].max()))

        # Reorder columns logically
        col_order = [
            "date",
            "year",
            "month",
            "day",
            "day_of_year",
            "latitude",
            "longitude",
            "precipitation_mm_per_day",
            "temperature_celsius_2m",
            "precipitation_unit",
            "temperature_unit",
            "is_missing_precipitation",
            "is_missing_temperature",
            "is_negative_temperature",
            "is_subzero_high_altitude",
            "is_extreme_precipitation",
        ]
        df_merged = df_merged[col_order]

        # 6. Save Year Partition to Parquet
        year_partition_dir = os.path.join(output_dir, f"year={year_str}")
        os.makedirs(year_partition_dir, exist_ok=True)
        parquet_file = os.path.join(year_partition_dir, "part-0.parquet")
        df_merged.to_parquet(parquet_file, index=False, engine="pyarrow", compression="snappy")

        n_rows = len(df_merged)
        total_processed_rows += n_rows
        year_summaries[year_str] = {
            "processed_rows": n_rows,
            "raw_input_rows": year_raw_p_rows + year_raw_t_rows,
            "boundary_duplicates_dropped": year_boundary_dups,
            "missing_precipitation_converted": missing_p_count,
            "missing_temperature_converted": missing_t_count,
            "negative_temperatures_preserved": neg_temp_count,
            "himalayan_subzero_records": himalayan_subzero_count,
            "parquet_file": parquet_file,
        }

        log.info(
            "Year %s complete: %d grid-day rows written to %s (freed memory)",
            year_str, n_rows, parquet_file,
        )

        del df_merged
        gc.collect()

    # Log summary decisions
    decisions.append({
        "step": "tile_boundary_overlap_resolution",
        "action": (
            "Investigated tile overlap: adjacent North/South tiles share boundary latitude values "
            "(e.g. 10.5, 14.5, 18.5, 22.5, 26.5, 30.5, 34.5) with identical readings. Deduplicated on (LAT, LON, YEAR, DOY)."
        ),
        "total_boundary_duplicates_dropped": total_boundary_duplicates_dropped,
    })
    decisions.append({
        "step": "missing_marker_conversion",
        "action": "Converted -999 missing source data markers to null (NaN). Added is_missing_precipitation and is_missing_temperature flags.",
        "missing_precip_markers_converted": total_missing_precip_markers,
        "missing_temp_markers_converted": total_missing_temp_markers,
    })
    decisions.append({
        "step": "temperature_validation_and_preservation",
        "action": "Preserved legitimate negative temperatures in Himalayan/Ladakh grid cells (lat >= 30.5°). Created is_negative_temperature and is_subzero_high_altitude flags.",
        "negative_temperatures_preserved": total_negative_temps_preserved,
        "himalayan_subzero_records": total_himalayan_subzero_records,
    })
    decisions.append({
        "step": "consolidation_and_parquet_partitioning",
        "action": (
            "Merged precipitation and temperature onto unified daily grid observations. "
            "Partitioned by year using Snappy-compressed Parquet. Avoided full memory load."
        ),
        "partitions_created": list(year_summaries.keys()),
        "total_consolidated_rows": total_processed_rows,
    })

    # Build overall report
    quality_report = {
        "dataset_name": "nasa_power_weather",
        "report_generated_at": datetime.now().isoformat(),
        "summary": {
            "total_raw_csv_files": total_raw_files,
            "total_raw_rows_input": total_raw_rows,
            "total_consolidated_output_rows": total_processed_rows,
            "boundary_duplicates_dropped": total_boundary_duplicates_dropped,
            "missing_markers_converted_to_null": total_missing_precip_markers + total_missing_temp_markers,
            "negative_temperatures_preserved": total_negative_temps_preserved,
            "himalayan_subzero_records": total_himalayan_subzero_records,
            "extreme_precipitation_records": total_extreme_rain_records,
        },
        "geographical_extent": {
            "min_latitude": overall_min_lat,
            "max_latitude": overall_max_lat,
            "min_longitude": overall_min_lon,
            "max_longitude": overall_max_lon,
            "grid_resolution": "0.5 deg lat x 0.625 deg lon (MERRA-2)",
            "coverage_region": "All India subcontinent (6.5°N - 37.5°N, 68.125°E - 95.625°E)",
        },
        "year_by_year_breakdown": year_summaries,
        "cleaning_decisions_and_traceability": decisions,
    }

    save_quality_report(safe_convert(quality_report), report_output_path)
    log.info("NASA POWER data quality report saved to: %s", report_output_path)
    return quality_report


if __name__ == "__main__":
    report = clean_nasa_weather_data()
    print("\n" + "=" * 60)
    print("  NASA POWER WEATHER CLEANING COMPLETE")
    print("=" * 60)
    print(f"  Raw CSV Files Processed : {report['summary']['total_raw_csv_files']:,}")
    print(f"  Raw Input Rows          : {report['summary']['total_raw_rows_input']:,}")
    print(f"  Consolidated Rows Out   : {report['summary']['total_consolidated_output_rows']:,}")
    print(f"  Boundary Duplicates Cut : {report['summary']['boundary_duplicates_dropped']:,}")
    print(f"  -999 Markers Nullified  : {report['summary']['missing_markers_converted_to_null']:,}")
    print(f"  Subzero Temps Preserved : {report['summary']['negative_temperatures_preserved']:,} (Himalayan readings)")
    print(f"  Lat Extent              : {report['geographical_extent']['min_latitude']}° to {report['geographical_extent']['max_latitude']}°")
    print(f"  Lon Extent              : {report['geographical_extent']['min_longitude']}° to {report['geographical_extent']['max_longitude']}°")
