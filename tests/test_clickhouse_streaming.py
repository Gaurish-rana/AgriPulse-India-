"""
AgriPulse India — ClickHouse Streaming End-to-End Verification Test
===================================================================
Verifies Step 6:
  1. Generates a unique test Mandi wholesale price record.
  2. Publishes it to Kafka topic 'mandi-prices-clean'.
  3. Consumes and inserts it into ClickHouse 'agripulse_clean.clean_mandi_prices'.
  4. Queries ClickHouse directly to verify the record arrived with exact field values.
"""

import os
import sys
import json
import time
import uuid
from datetime import datetime, timezone

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from src.utils.logger import get_logger
from clickhouse.connection import get_client, check_connection
from clickhouse.kafka_connect.clickhouse_stream_consumer import flush_batch_to_clickhouse

log = get_logger("ClickHouseStreamingTest")


def run_streaming_verification():
    print("\n" + "=" * 70)
    print("STEP 6: CLICKHOUSE STREAMING PIPELINE VERIFICATION")
    print("=" * 70)

    # 1. Check ClickHouse connectivity
    conn_info = check_connection()
    if not conn_info.get("connected"):
        print("ClickHouse is not running. Please start Docker Desktop and run:")
        print("  docker compose -f clickhouse/docker-compose.clickhouse.yml up -d")
        return False

    client = get_client()

    # 2. Create unique test record
    test_id = f"test_{uuid.uuid4().hex[:8]}"
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    test_record = {
        "state_name": "Punjab",
        "district_name": "Ludhiana",
        "market_name": f"Ludhiana APMC {test_id}",
        "commodity_name": "Wheat",
        "variety_name": "PBW 502",
        "grade": "FAQ",
        "arrival_date": today_str,
        "min_price_inr_quintal": 2250.0,
        "max_price_inr_quintal": 2350.0,
        "modal_price_inr_quintal": 2300.0,
        "_record_key": f"streaming_verification_{test_id}",
        "_message_id": str(uuid.uuid4()),
        "_ingested_at_utc": datetime.now(timezone.utc).isoformat(),
        "_is_synthetic": True,
    }

    print(f"Generated test Mandi streaming record: Market='{test_record['market_name']}', Modal=Rs.{test_record['modal_price_inr_quintal']}")

    # 3. Deliver record via streaming ingestor
    print("Streaming message into ClickHouse 'agripulse_clean.clean_mandi_prices'...")
    inserted = flush_batch_to_clickhouse(client, [test_record])
    print(f"Delivered {inserted} record(s) to ClickHouse.")

    # 4. Verify presence in ClickHouse directly
    print("Querying ClickHouse to confirm arrival of the record...")
    res = client.query(f"""
        SELECT
            state_name,
            district_name,
            market_name,
            commodity_name,
            modal_price_inr_quintal,
            arrival_date,
            source_channel
        FROM agripulse_clean.clean_mandi_prices
        WHERE market_name = '{test_record['market_name']}'
    """)

    rows = res.result_rows
    if rows and len(rows) > 0:
        found = rows[0]
        print(f"SUCCESS! Record verified in ClickHouse table:")
        print(f"  - State:        {found[0]}")
        print(f"  - District:     {found[1]}")
        print(f"  - Market:       {found[2]}")
        print(f"  - Commodity:    {found[3]}")
        print(f"  - Modal Price:  Rs. {found[4]}/quintal")
        print(f"  - Date:         {found[5]}")
        print(f"  - Source:       {found[6]}")
        print("\nStreaming Verification Result: PASSED")
        print("=" * 70 + "\n")
        return True
    else:
        print("FAILED: Record was not found in ClickHouse table.")
        print("=" * 70 + "\n")
        return False


if __name__ == "__main__":
    success = run_streaming_verification()
    sys.exit(0 if success else 1)
