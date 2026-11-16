"""
AgriPulse India — Batch Standardization Module
================================================
Provides reusable functions for column naming, text stripping,
state-name mapping, date standardization, and measurement unit definitions
across all AgriPulse India datasets.
"""

import os
import re
import pandas as pd
from typing import Dict, Optional, Union

# Documented measurement units across datasets
MEASUREMENT_UNITS = {
    "mandi_prices": {
        "min_price_inr_quintal": {
            "unit": "INR per quintal",
            "symbol": "₹/q",
            "description": "Minimum wholesale price observed in the market on arrival date. 1 quintal = 100 kg.",
        },
        "max_price_inr_quintal": {
            "unit": "INR per quintal",
            "symbol": "₹/q",
            "description": "Maximum wholesale price observed in the market on arrival date. 1 quintal = 100 kg.",
        },
        "modal_price_inr_quintal": {
            "unit": "INR per quintal",
            "symbol": "₹/q",
            "description": "Modal (most frequent/representative) wholesale price in the market on arrival date. 1 quintal = 100 kg.",
        },
    },
    "crop_production": {
        "area_hectares": {
            "unit": "Hectares",
            "symbol": "ha",
            "description": "Total land area cultivated for the specific crop, season, and district (1 ha = 10,000 sq. meters = 2.471 acres).",
        },
        "production": {
            "unit": "Varies by crop (Tonnes for most crops; Number of Nuts for Coconut)",
            "description": "Total production harvested. Coconut is reported in Number of Nuts; all other agricultural crops are reported in Metric Tonnes.",
        },
        "production_tonnes": {
            "unit": "Metric Tonnes",
            "symbol": "t",
            "description": "Reported production for tonne-based agricultural crops (excludes Coconut). 1 tonne = 1,000 kg.",
        },
        "production_coconut_nuts": {
            "unit": "Number of Nuts",
            "symbol": "nuts",
            "description": "Reported production for Coconut only. Must NEVER be summed directly with tonne-based production.",
        },
        "yield_tonnes_per_hectare": {
            "unit": "Tonnes per Hectare",
            "symbol": "t/ha",
            "description": "Calculated as production / area_hectares for tonne-based crops where production is not missing.",
        },
        "yield_coconut_nuts_per_hectare": {
            "unit": "Nuts per Hectare",
            "symbol": "nuts/ha",
            "description": "Calculated as production / area_hectares for Coconut where production is not missing.",
        },
    },
    "nasa_weather": {
        "precipitation_mm_per_day": {
            "parameter": "PRECTOTCORR",
            "unit": "Millimeters per day",
            "symbol": "mm/day",
            "description": "MERRA-2 Precipitation Corrected (daily total rainfall / moisture flux at surface).",
        },
        "temperature_celsius_2m": {
            "parameter": "T2M",
            "unit": "Degrees Celsius",
            "symbol": "°C",
            "description": "MERRA-2 Air Temperature at 2 meters above ground level (daily average).",
        },
        "latitude": {
            "unit": "Degrees North",
            "symbol": "°N",
            "description": "Grid cell center latitude in WGS84 coordinate system (0.5 degree grid spacing).",
        },
        "longitude": {
            "unit": "Degrees East",
            "symbol": "°E",
            "description": "Grid cell center longitude in WGS84 coordinate system (0.625 degree grid spacing).",
        },
    },
}


def find_mapping_file(filename: str = "state_name_mapping.csv") -> str:
    """Find the path to state_name_mapping.csv across common project locations."""
    candidates = [
        os.path.join(os.path.dirname(__file__), "..", "utils", filename),
        os.path.join(os.path.dirname(__file__), "..", "..", filename),
        os.path.join(os.getcwd(), filename),
        os.path.join(os.getcwd(), "src", "utils", filename),
    ]
    for c in candidates:
        abs_c = os.path.abspath(c)
        if os.path.exists(abs_c):
            return abs_c
    raise FileNotFoundError(f"State mapping file '{filename}' could not be located in candidate paths.")


def load_state_mapping(mapping_path: Optional[str] = None) -> Dict[str, str]:
    """
    Load state mapping CSV into a dictionary: raw_state_name -> standard_state_name.
    Also creates lowercase and stripped keys for robust fallback matching.
    """
    if mapping_path is None:
        mapping_path = find_mapping_file()

    df_map = pd.read_csv(mapping_path)
    mapping = {}
    for _, row in df_map.iterrows():
        raw = str(row["raw_state_name"])
        std = str(row["standard_state_name"])
        mapping[raw] = std
        mapping[raw.strip()] = std
        mapping[raw.strip().lower()] = std
    return mapping


def standardize_state_name(
    series: pd.Series,
    mapping: Optional[Dict[str, str]] = None,
) -> pd.Series:
    """
    Standardize a pandas Series of state names using official GoI names.
    Handles trailing whitespace and regional spellings (e.g. Keralam -> Kerala).
    """
    if mapping is None:
        mapping = load_state_mapping()

    def _map_single(val):
        if pd.isna(val):
            return val
        s = str(val).strip()
        # Direct lookup (exact, then stripped, then lower)
        if val in mapping:
            return mapping[val]
        if s in mapping:
            return mapping[s]
        if s.lower() in mapping:
            return mapping[s.lower()]
        # Fallback to stripped title case
        return s.title()

    return series.apply(_map_single)


def standardize_text(
    series: pd.Series,
    casing: str = "preserve",
) -> pd.Series:
    """
    Clean text columns by:
      - Stripping leading and trailing whitespace
      - Normalizing internal repeated whitespaces into single spaces
      - Applying optional casing: 'title', 'upper', 'lower', or 'preserve'
    """
    def _clean_str(val):
        if pd.isna(val):
            return val
        cleaned = re.sub(r"\s+", " ", str(val)).strip()
        if casing == "title":
            return cleaned.title()
        elif casing == "upper":
            return cleaned.upper()
        elif casing == "lower":
            return cleaned.lower()
        return cleaned

    return series.apply(_clean_str)


def to_snake_case(name: str) -> str:
    """Convert any column name to clean lowercase snake_case."""
    # Handle XML entities like _x0020_
    s = re.sub(r"_x[0-9a-fA-F]{4}_", " ", str(name))
    # Replace non-alphanumeric chars with space
    s = re.sub(r"[^0-9a-zA-Z]+", " ", s)
    # Insert underscore between camelCase transitions
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    # Lowercase and replace spaces with underscore
    s = re.sub(r"\s+", "_", s).strip("_").lower()
    return s


def standardize_column_names(
    df: pd.DataFrame,
    custom_map: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
    """
    Standardize DataFrame column names:
      - Applies custom rename dictionary if provided
      - Automatically converts any remaining columns to snake_case
    """
    df = df.copy()
    if custom_map:
        df = df.rename(columns=custom_map)
    # Auto-convert remaining columns to snake_case
    df.columns = [to_snake_case(c) for c in df.columns]
    return df


def parse_date_series(
    series: pd.Series,
    format_pattern: Optional[str] = None,
) -> pd.Series:
    """
    Parse a series of dates into standardized ISO YYYY-MM-DD date strings.
    Invalid dates are converted to pd.NaT without throwing an exception.
    """
    if format_pattern:
        parsed = pd.to_datetime(series, format=format_pattern, errors="coerce")
    else:
        parsed = pd.to_datetime(series, errors="coerce")
    return parsed.dt.strftime("%Y-%m-%d")
