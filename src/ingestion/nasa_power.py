"""
NASA POWER Weather Data Loader
================================
Loads pre-downloaded NASA POWER CSV files from data/raw/nasa_power/.
These files use a tile-based grid structure with metadata headers.

File naming convention:
    {PARAMETER}_{YEAR}_lat{lat1}p{d1}-{lat2}p{d2}_lon{lon1}p{d1}-{lon2}p{d2}.csv
    e.g., PRECTOTCORR_2020_lat10p5-14p5_lon68p0-72p0.csv

File format:
    - 9-line metadata header (-BEGIN HEADER- to -END HEADER-)
    - CSV data with columns: LAT, LON, YEAR, DOY, <PARAMETER>
    - Missing value marker: -999

Parameters:
    - PRECTOTCORR: Precipitation Corrected (mm/day)
    - T2M:         Temperature at 2 Meters (degrees Celsius)

Coverage:
    - Latitude:  6.5 to 37.5 (India)
    - Longitude: 68.0 to 96.0 (India)
    - Years:     2020 to 2026
    - Grid:      0.5 x 0.625 degree (MERRA-2 native resolution)
    - Tiles:     8 lat bands x 7 lon bands = 56 tiles

Usage:
    python -m src.ingestion.nasa_power
"""
import os
import io
import glob
import pandas as pd
from collections import Counter
from src.utils.config import NASAConfig
from src.utils.logger import get_logger

log = get_logger(__name__)
cfg = NASAConfig()

# Missing value marker used by NASA POWER
MISSING_VALUE = -999


def parse_filename(filename: str) -> dict:
    """
    Extract parameter, year, and tile info from a NASA POWER filename.
    
    Example: PRECTOTCORR_2020_lat10p5-14p5_lon68p0-72p0.csv
    Returns: {parameter: 'PRECTOTCORR', year: '2020', 
              lat_range: '10.5-14.5', lon_range: '68.0-72.0'}
    """
    name = filename.replace(".csv", "")
    parts = name.split("_")
    param = parts[0]
    year = parts[1]
    lat_part = parts[2].replace("lat", "").replace("p", ".")
    lon_part = parts[3].replace("lon", "").replace("p", ".")
    return {
        "parameter": param,
        "year": year,
        "lat_range": lat_part,
        "lon_range": lon_part,
    }


def load_single_file(filepath: str) -> pd.DataFrame:
    """
    Load a single NASA POWER CSV, skipping the metadata header.
    Rows with -999 values are flagged but NOT removed.
    
    Returns a DataFrame with columns: LAT, LON, YEAR, DOY, <PARAM>
    """
    with open(filepath, "r", encoding="utf-8") as fh:
        lines = fh.readlines()

    # Find end of metadata header
    header_end = 0
    for i, line in enumerate(lines):
        if line.strip() == "-END HEADER-":
            header_end = i
            break

    data_text = "".join(lines[header_end + 1:])
    df = pd.read_csv(io.StringIO(data_text))

    # Flag missing values (-999) without removing them
    value_col = df.columns[-1]  # PRECTOTCORR or T2M
    n_missing = int((df[value_col] == MISSING_VALUE).sum())
    if n_missing > 0:
        log.warning(
            "  %s: %d rows with missing marker (-999) in %s",
            os.path.basename(filepath), n_missing, value_col,
        )

    # Add source metadata
    info = parse_filename(os.path.basename(filepath))
    df["_source_file"] = os.path.basename(filepath)
    df["_parameter"] = info["parameter"]

    return df


def scan_all_files() -> dict:
    """
    Scan the raw NASA POWER directory and return a summary
    without loading all data into memory.
    """
    pattern = os.path.join(cfg.RAW_DIR, "*.csv")
    files = [f for f in glob.glob(pattern) if not f.endswith(".gitkeep")]

    if not files:
        log.warning("No NASA POWER CSV files found in %s.", cfg.RAW_DIR)
        return {"files": 0}

    params = Counter()
    years = Counter()
    tiles = set()
    total_size = 0

    for f in files:
        info = parse_filename(os.path.basename(f))
        params[info["parameter"]] += 1
        years[info["year"]] += 1
        tiles.add(info["lat_range"] + "_" + info["lon_range"])
        total_size += os.path.getsize(f)

    summary = {
        "files": len(files),
        "total_size_mb": round(total_size / 1e6, 1),
        "parameters": dict(params),
        "years": sorted(years.keys()),
        "tiles": len(tiles),
    }

    log.info("NASA POWER scan: %d files, %.1f MB, params=%s, years=%s, tiles=%d",
             summary["files"], summary["total_size_mb"],
             list(params.keys()), summary["years"], summary["tiles"])

    return summary


def load_parameter_year(parameter: str, year: str) -> pd.DataFrame:
    """
    Load all tiles for a given parameter and year into one DataFrame.
    
    Args:
        parameter: 'PRECTOTCORR' or 'T2M'
        year:      e.g. '2020'
    
    Returns:
        Combined DataFrame for all tiles of that parameter/year.
    """
    pattern = os.path.join(cfg.RAW_DIR, f"{parameter}_{year}_*.csv")
    files = glob.glob(pattern)

    if not files:
        log.warning("No files found for %s_%s", parameter, year)
        return pd.DataFrame()

    frames = []
    for f in sorted(files):
        df = load_single_file(f)
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)
    log.info("Loaded %s_%s: %d rows from %d tiles",
             parameter, year, len(combined), len(files))
    return combined


if __name__ == "__main__":
    print("=" * 60)
    print("  AgriPulse India – NASA POWER Data Scanner")
    print("=" * 60)

    summary = scan_all_files()

    print("\nSummary:")
    for k, v in summary.items():
        print(f"  {k}: {v}")

    # Load one sample to show structure
    if summary.get("files", 0) > 0:
        print("\nLoading sample: PRECTOTCORR_2020...")
        df_sample = load_parameter_year("PRECTOTCORR", "2020")
        if not df_sample.empty:
            print(f"  Rows: {len(df_sample):,}")
            print(f"  Columns: {list(df_sample.columns)}")
            print(f"  Missing markers (-999): {(df_sample['PRECTOTCORR'] == -999).sum()}")
            print(f"\n  Sample:")
            print(df_sample.head(3).to_string(index=False))
