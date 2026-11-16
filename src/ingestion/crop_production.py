"""
Crop Production CSV Loader
============================
Loads the crop_production.csv from data/raw/crop_production/
and runs data-quality checks on the actual schema.

Confirmed schema (from government dataset):
    State_Name    – State name (object, some have trailing whitespace)
    District_Name – District name (object, ALL CAPS)
    Crop_Year     – Harvest year (int64, range 1997–2015)
    Season        – Cropping season (object, has trailing whitespace)
    Crop          – Crop name (object, 124 unique crops)
    Area          – Cultivation area in hectares (float64)
    Production    – Production in tonnes (float64, 1.52% null)

Note: Coconut production is measured in number of nuts, not tonnes.
Note: 2015 data is incomplete (only 562 rows).

Usage:
    python -m src.ingestion.crop_production
"""
import os
import glob
import pandas as pd
from src.utils.config import CropConfig
from src.utils.validators import full_quality_report
from src.utils.logger import get_logger

log = get_logger(__name__)
cfg = CropConfig()

# Expected columns from the confirmed dataset schema
EXPECTED_COLUMNS = [
    "State_Name", "District_Name", "Crop_Year",
    "Season", "Crop", "Area", "Production",
]


def load_all_csvs() -> pd.DataFrame:
    """
    Load all CSVs from the crop_production raw directory.
    Returns a single concatenated DataFrame.
    """
    pattern = os.path.join(cfg.RAW_DIR, "*.csv")
    csv_files = [f for f in glob.glob(pattern) if not f.endswith(".gitkeep")]

    if not csv_files:
        log.warning(
            "No CSV files found in %s. "
            "Place your crop production CSVs there and re-run.",
            cfg.RAW_DIR,
        )
        return pd.DataFrame()

    frames = []
    for f in csv_files:
        log.info("Loading: %s", f)
        try:
            df = pd.read_csv(f, encoding="utf-8", low_memory=False)
            df["_source_file"] = os.path.basename(f)
            frames.append(df)
            log.info("  -> %d rows, columns: %s", len(df), list(df.columns))
        except Exception as exc:
            log.error("Failed to load %s: %s", f, exc)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    log.info("Total crop production records loaded: %d", len(combined))

    # Validate columns
    actual_cols = [c for c in combined.columns if not c.startswith("_")]
    missing_cols = [c for c in EXPECTED_COLUMNS if c not in actual_cols]
    extra_cols = [c for c in actual_cols if c not in EXPECTED_COLUMNS]

    if missing_cols:
        log.warning("Missing expected columns: %s", missing_cols)
    if extra_cols:
        log.info("Extra columns found: %s", extra_cols)

    return combined


def inspect_and_report(df: pd.DataFrame) -> dict:
    """Run quality checks on the loaded DataFrame and return a report."""
    if df.empty:
        return {"error": "No data loaded."}

    report = full_quality_report(df, "crop_production")

    # Additional crop-specific checks
    if "Season" in df.columns:
        whitespace_seasons = [s for s in df["Season"].unique() if s != s.strip()]
        if whitespace_seasons:
            report["season_whitespace"] = True
            log.warning("Season values have trailing whitespace: %s", whitespace_seasons)

    if "State_Name" in df.columns:
        ws_states = [s for s in df["State_Name"].unique() if s != s.strip()]
        if ws_states:
            report["state_whitespace"] = ws_states
            log.warning("State names with whitespace: %s", ws_states)

    if "Production" in df.columns:
        null_prod = int(df["Production"].isnull().sum())
        zero_prod = int((df["Production"] == 0).sum())
        report["null_production"] = null_prod
        report["zero_production"] = zero_prod

    if "Crop_Year" in df.columns:
        year_counts = df["Crop_Year"].value_counts().sort_index()
        small_years = {int(yr): int(cnt) for yr, cnt in year_counts.items() if cnt < 1000}
        if small_years:
            report["incomplete_years"] = small_years
            log.warning("Years with < 1000 rows (possibly incomplete): %s", small_years)

    return report


if __name__ == "__main__":
    df = load_all_csvs()
    if not df.empty:
        report = inspect_and_report(df)
        print("\n=== Crop Production Quality Report ===")
        for k, v in report.items():
            print("  {}: {}".format(k, v))
        print("\nSample rows:")
        print(df.head(3).to_string())
        print("\nYear range: {} to {}".format(df["Crop_Year"].min(), df["Crop_Year"].max()))
        print("States: {}".format(df["State_Name"].nunique()))
        print("Crops: {}".format(df["Crop"].nunique()))
