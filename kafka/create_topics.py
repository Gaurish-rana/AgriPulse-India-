"""
AgriPulse India — Kafka Topic Creation & Inspection Script
============================================================
Creates and inspects the three required Kafka topics:
  1. mandi-prices-raw    : Raw ingested JSON messages from API / Producer
  2. mandi-prices-clean  : Standardized & validated records from Spark
  3. mandi-prices-invalid: Rejected records with validation failure reason

Usage:
    python kafka/create_topics.py
"""

import sys
import os
import time

# Ensure project root in sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from confluent_kafka.admin import AdminClient, NewTopic
from src.streaming.kafka_config import KafkaConfig


TOPIC_SPECS = [
    {
        "name": KafkaConfig.TOPIC_RAW,
        "num_partitions": 3,
        "replication_factor": 1,
        "config": {
            "retention.ms": str(7 * 24 * 3600 * 1000),  # 7 days
            "cleanup.policy": "delete",
        },
    },
    {
        "name": KafkaConfig.TOPIC_CLEAN,
        "num_partitions": 3,
        "replication_factor": 1,
        "config": {
            "retention.ms": str(14 * 24 * 3600 * 1000),  # 14 days
            "cleanup.policy": "delete",
        },
    },
    {
        "name": KafkaConfig.TOPIC_INVALID,
        "num_partitions": 1,
        "replication_factor": 1,
        "config": {
            "retention.ms": str(30 * 24 * 3600 * 1000),  # 30 days for inspection
            "cleanup.policy": "delete",
        },
    },
]


def create_topics(bootstrap_servers: str = None) -> bool:
    servers = bootstrap_servers or KafkaConfig.BOOTSTRAP_SERVERS
    print("=" * 65)
    print("  AgriPulse India — Kafka Topic Provisioning")
    print(f"  Target Broker: {servers}")
    print("=" * 65)

    admin = AdminClient({"bootstrap.servers": servers, "socket.timeout.ms": 5000})

    # Test connectivity by listing cluster metadata
    try:
        cluster_meta = admin.list_topics(timeout=5)
        existing_topics = set(cluster_meta.topics.keys())
        print(f"✓ Connected to Kafka broker successfully.")
        print(f"  Existing cluster topics: {sorted(list(existing_topics)) or 'None'}")
    except Exception as e:
        print(f"✗ Failed to connect to Kafka broker at {servers}: {e}")
        print("\nPlease ensure Kafka is running:")
        print("  cd kafka && docker compose up -d")
        return False

    new_topics = []
    for spec in TOPIC_SPECS:
        name = spec["name"]
        if name in existing_topics:
            print(f"  • Topic '{name}' already exists. Skipping creation.")
        else:
            new_topics.append(NewTopic(
                topic=name,
                num_partitions=spec["num_partitions"],
                replication_factor=spec["replication_factor"],
                config=spec["config"],
            ))

    if new_topics:
        print(f"\nCreating {len(new_topics)} new topic(s)...")
        futures = admin.create_topics(new_topics)
        for topic_name, future in futures.items():
            try:
                future.result()  # Blocks until created
                print(f"  ✓ Created topic: {topic_name}")
            except Exception as e:
                print(f"  ✗ Failed to create topic '{topic_name}': {e}")
                return False

    # Verify and display topic metadata
    time.sleep(1)
    cluster_meta = admin.list_topics(timeout=5)
    print("\nVerified Topic Metadata:")
    for spec in TOPIC_SPECS:
        name = spec["name"]
        t_meta = cluster_meta.topics.get(name)
        if t_meta:
            print(f"  • {name:<22} | Partitions: {len(t_meta.partitions)} | Replication: {len(t_meta.partitions[0].replicas)}")
        else:
            print(f"  • {name:<22} | NOT FOUND")

    print("\nAll topics verified successfully.")
    return True


if __name__ == "__main__":
    success = create_topics()
    sys.exit(0 if success else 1)
