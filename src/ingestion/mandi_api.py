"""
Mandi Price Ingestion  –  data.gov.in API
==========================================
Fetches daily wholesale mandi price data from the Government of India
Open Data portal and saves all records as a CSV in data/raw/mandi/.

Confirmed API schema (from data.gov.in metadata):
    state        – State name (keyword)
    district     – District name (keyword)
    market       – Market / APMC name (keyword)
    commodity    – Commodity name (keyword)
    variety      – Commodity variety (keyword)
    grade        – Grade (keyword)
    arrival_date – Price date (DD/MM/YYYY format)
    min_price    – Minimum price in Rs/quintal (double)
    max_price    – Maximum price in Rs/quintal (double)
    modal_price  – Modal (most common) price in Rs/quintal (double)

Total available records: ~12,726 (as of Nov 2026)

Environment variables required (set in .env):
    MANDI_API_KEY       – your data.gov.in API key
    MANDI_API_BASE_URL  – resource endpoint URL

Usage:
    python -m src.ingestion.mandi_api
"""
import os
import time
import requests
import pandas as pd
from datetime import datetime
from src.utils.config import MandiConfig
from src.utils.logger import get_logger

log = get_logger(__name__)
cfg = MandiConfig()

# Browser-like headers to avoid server-side blocking
# data.gov.in rejects requests without a proper User-Agent
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

# Confirmed column names from the API metadata
EXPECTED_COLUMNS = [
    "state", "district", "market", "commodity",
    "variety", "grade", "arrival_date",
    "min_price", "max_price", "modal_price",
]

# Confirmed date format in arrival_date field
ARRIVAL_DATE_FORMAT = "%d/%m/%Y"


def fetch_page(offset: int, limit: int = None) -> dict:
    """Fetch a single page of mandi records from the API."""
    if not cfg.API_KEY:
        raise EnvironmentError(
            "MANDI_API_KEY is not set. Add it to your .env file."
        )

    limit = limit or cfg.RECORDS_PER_PAGE
    params = {
        "api-key": cfg.API_KEY,
        "format": "json",
        "limit": limit,
        "offset": offset,
    }

    log.info("Fetching mandi records: offset=%d  limit=%d", offset, limit)
    try:
        resp = requests.get(
            cfg.BASE_URL,
            params=params,
            headers=HEADERS,
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    except requests.exceptions.Timeout:
        log.error("Request timed out at offset=%d. Retrying once...", offset)
        time.sleep(3)
        resp = requests.get(cfg.BASE_URL, params=params, headers=HEADERS, timeout=60)
        resp.raise_for_status()
        return resp.json()

    except requests.RequestException as exc:
        log.error("API request failed at offset=%d: %s", offset, exc)
        raise


def ingest_all(max_records: int = 15000, delay_sec: float = 1.0) -> str:
    """
    Paginate through the full Mandi API and save all records to CSV.

    Args:
        max_records: Safety cap (set above total so we get everything).
        delay_sec:   Polite delay between API calls.

    Returns:
        Path to the saved CSV file, or empty string if nothing was fetched.
    """
    os.makedirs(cfg.RAW_DIR, exist_ok=True)
    all_records = []
    offset = 0
    total = None

    while offset < max_records:
        data = fetch_page(offset)
        records = data.get("records", [])

        if total is None:
            total = int(data.get("total", 0))
            log.info("Total records available on server: %d", total)

        if not records:
            log.info("No more records returned. Stopping at offset %d.", offset)
            break

        all_records.extend(records)
        log.info(
            "Progress: %d / %d records fetched (%.1f%%)",
            len(all_records), total,
            len(all_records) / total * 100 if total else 0,
        )

        offset += cfg.RECORDS_PER_PAGE
        if offset >= total:
            log.info("All %d records fetched.", total)
            break

        time.sleep(delay_sec)

    if not all_records:
        log.warning("No records were fetched from Mandi API.")
        return ""

    df = pd.DataFrame(all_records)

    # --- Basic validation after fetch ---
    log.info("Columns in fetched data: %s", list(df.columns))

    missing_cols = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    if missing_cols:
        log.warning("Unexpected: these expected columns are missing: %s", missing_cols)

    extra_cols = [c for c in df.columns if c not in EXPECTED_COLUMNS]
    if extra_cols:
        log.info("Extra columns found (not in schema): %s", extra_cols)

    # Parse arrival_date to validate format
    parsed_dates = pd.to_datetime(
        df["arrival_date"], format=ARRIVAL_DATE_FORMAT, errors="coerce"
    )
    bad_dates = parsed_dates.isnull().sum()
    if bad_dates:
        log.warning("%d rows have unparseable arrival_date values.", bad_dates)
    else:
        log.info("All arrival_date values parsed successfully (DD/MM/YYYY).")

    # Save to CSV
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(cfg.RAW_DIR, f"mandi_prices_{timestamp}.csv")
    df.to_csv(out_path, index=False, encoding="utf-8")

    log.info("Saved %d records to %s", len(df), out_path)
    log.info("Price range – min_price: %.0f–%.0f | max_price: %.0f–%.0f | modal_price: %.0f–%.0f",
             df["min_price"].min(), df["min_price"].max(),
             df["max_price"].min(), df["max_price"].max(),
             df["modal_price"].min(), df["modal_price"].max())

    return out_path


if __name__ == "__main__":
    path = ingest_all()
    if path:
        print(f"\nMandi data saved to: {path}")
        df = pd.read_csv(path)
        print(f"Total rows     : {len(df)}")
        print(f"Columns        : {list(df.columns)}")
        print(f"\nSample (first 3 rows):")
        print(df.head(3).to_string(index=False))
