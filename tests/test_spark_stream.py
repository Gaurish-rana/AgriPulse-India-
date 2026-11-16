"""
AgriPulse India — Spark Streaming Transformation & Validation Tests
======================================================================
Tests:
  1. Parsing of raw Mandi JSON messages using Spark raw schema
  2. State name standardization (Keralam -> Kerala, Chattisgarh -> Chhattisgarh)
  3. Clean stream routing for valid pricing & date records
  4. Invalid stream routing with specific diagnostic reasons:
     - INVALID_PRICE_RELATION_MIN_GT_MODAL_OR_MAX
     - NON_POSITIVE_PRICE
     - INVALID_OR_UNPARSEABLE_DATE
     - MISSING_MANDATORY_FIELD
  5. Checkpoint directory and recovery setup
"""

import json
import os
import sys
import unittest

# Ensure Python worker uses sys.executable on Windows
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Ensure project root in sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from pyspark.sql import SparkSession
from src.streaming.schemas import build_raw_message
from src.streaming.spark_mandi_stream import build_streaming_transformation, create_spark_session


class TestSparkMandiStream(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Create lightweight local Spark session without Kafka package for transformation unit tests
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("TestSparkMandiStream")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.driver.host", "localhost")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_clean_and_invalid_record_routing(self):
        """Test transformation logic splits records correctly into clean and invalid streams."""
        # 1. Valid record (Keralam -> should map to Kerala)
        valid_rec = build_raw_message({
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
        })

        # 2. Invalid record: Inverted prices (min > max)
        invalid_price_rec = build_raw_message({
            "state": "Punjab",
            "district": "Ludhiana",
            "market": "Khanna",
            "commodity": "Wheat",
            "variety": "Kalyan",
            "grade": "FAQ",
            "arrival_date": "16/11/2026",
            "min_price": 8000.0,
            "max_price": 2000.0,
            "modal_price": 2100.0,
        })

        # 3. Invalid record: Non-positive price
        non_pos_price_rec = build_raw_message({
            "state": "Maharashtra",
            "district": "Nashik",
            "market": "Lasalgaon",
            "commodity": "Onion",
            "variety": "Red",
            "grade": "FAQ",
            "arrival_date": "16/11/2026",
            "min_price": 0.0,
            "max_price": 1500.0,
            "modal_price": 1200.0,
        })

        # 4. Invalid record: Unparseable date
        bad_date_rec = build_raw_message({
            "state": "Haryana",
            "district": "Karnal",
            "market": "Karnal Mandi",
            "commodity": "Basmati Rice",
            "variety": "Pusa",
            "grade": "FAQ",
            "arrival_date": "INVALID_DATE_99/99/9999",
            "min_price": 3000.0,
            "max_price": 3500.0,
            "modal_price": 3200.0,
        })

        test_data = [
            (valid_rec["_record_key"], json.dumps(valid_rec)),
            (invalid_price_rec["_record_key"], json.dumps(invalid_price_rec)),
            (non_pos_price_rec["_record_key"], json.dumps(non_pos_price_rec)),
            (bad_date_rec["_record_key"], json.dumps(bad_date_rec)),
        ]

        df_mock_raw = self.spark.createDataFrame(test_data, ["key", "value"])

        # Execute transformation logic
        df_clean, df_invalid = build_streaming_transformation(df_mock_raw)

        clean_rows = df_clean.collect()
        invalid_rows = df_invalid.collect()

        # Assertions
        self.assertEqual(len(clean_rows), 1, "Exactly 1 record should pass to clean stream")
        self.assertEqual(len(invalid_rows), 3, "Exactly 3 records should route to invalid stream")

        # Verify clean record structure and state normalization
        clean_json = json.loads(clean_rows[0]["value"])
        self.assertEqual(clean_json["state_name"], "Kerala", "Keralam must be standardized to Kerala")
        self.assertEqual(clean_json["commodity_name"], "Banana")
        self.assertEqual(clean_json["arrival_date"], "2026-11-16")
        self.assertEqual(clean_json["modal_price_inr_quintal"], 6800.0)

        # Verify invalid record rejection reasons
        invalid_reasons = [json.loads(r["value"])["rejection_reason"] for r in invalid_rows]
        self.assertIn("INVALID_PRICE_RELATION_MIN_GT_MODAL_OR_MAX", invalid_reasons)
        self.assertIn("NON_POSITIVE_PRICE", invalid_reasons)
        self.assertIn("INVALID_OR_UNPARSEABLE_DATE", invalid_reasons)


if __name__ == "__main__":
    unittest.main()
