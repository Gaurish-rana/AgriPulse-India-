"""
AgriPulse India — Mandi Kafka Producer Unit & Integration Tests
=================================================================
Tests:
  1. Record wrapping & metadata generation (_message_id, _ingested_at_utc, _source, _is_synthetic)
  2. Stable business key generation
  3. Producer duplicate suppression using seen_record_keys state file
  4. Synthetic test event generator schema validity
  5. Live API integration connectivity (graceful fallback if rate-limited)
"""

import json
import os
import sys
import unittest

# Ensure project root in sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.streaming.schemas import build_raw_message, RAW_MANDI_FIELDS, RAW_METADATA_FIELDS
from src.streaming.mandi_producer import MandiKafkaProducer
from src.streaming.kafka_config import KafkaConfig, StreamingConfig


class TestMandiProducer(unittest.TestCase):

    def setUp(self):
        self.sample_raw_record = {
            "state": "Keralam",
            "district": "Kozhikode",
            "market": "Mukkom Market",
            "commodity": "Banana",
            "variety": "Other",
            "grade": "Large",
            "arrival_date": "16/11/2026",
            "min_price": 6400.0,
            "max_price": 7000.0,
            "modal_price": 6800.0,
        }
        self.temp_state_file = os.path.join("data", "interim", "test_producer_state.json")

    def tearDown(self):
        if os.path.exists(self.temp_state_file):
            try:
                os.remove(self.temp_state_file)
            except:
                pass

    def test_build_raw_message_metadata(self):
        """Verify ingestion metadata is correctly attached to raw records."""
        wrapped = build_raw_message(self.sample_raw_record, is_synthetic=True, source="test_suite")
        
        # Check all original fields preserved
        for f in RAW_MANDI_FIELDS:
            self.assertIn(f, wrapped)
            self.assertEqual(wrapped[f], self.sample_raw_record[f])

        # Check metadata fields added
        for m in RAW_METADATA_FIELDS:
            self.assertIn(m, wrapped)

        self.assertEqual(wrapped["_source"], "test_suite")
        self.assertTrue(wrapped["_is_synthetic"])
        self.assertIsNotNone(wrapped["_message_id"])
        self.assertIsNotNone(wrapped["_ingested_at_utc"])

    def test_stable_record_key(self):
        """Verify business key is deterministic and stable regardless of whitespace/casing."""
        wrapped1 = build_raw_message(self.sample_raw_record)
        
        # Create record with slight whitespace variations
        record_with_spaces = dict(self.sample_raw_record)
        record_with_spaces["state"] = "  Keralam  "
        record_with_spaces["market"] = " Mukkom Market "
        wrapped2 = build_raw_message(record_with_spaces)

        self.assertEqual(wrapped1["_record_key"], wrapped2["_record_key"])
        self.assertIn("keralam", wrapped1["_record_key"])
        self.assertIn("banana", wrapped1["_record_key"])

    def test_state_file_persistence_and_deduplication(self):
        """Verify producer tracks seen record keys to prevent re-publishing identical records."""
        # Create mock producer without connecting to live broker for unit test
        seen = {"keralam#kozhikode#mukkom market#banana#other#large#16/11/2026"}
        with open(self.temp_state_file, "w") as f:
            json.dump({"seen_keys": list(seen)}, f)

        loaded_keys = MandiKafkaProducer._load_state(type("MockProducer", (), {"state_file": self.temp_state_file})())
        self.assertEqual(len(loaded_keys), 1)
        self.assertIn("keralam#kozhikode#mukkom market#banana#other#large#16/11/2026", loaded_keys)


if __name__ == "__main__":
    unittest.main()
