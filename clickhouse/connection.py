"""
AgriPulse India — ClickHouse Connection & Session Manager
==========================================================
Manages connections to the local/self-hosted ClickHouse open-source warehouse.
Uses official clickhouse-connect client over HTTP (port 8123).
Reads settings from environment variables with safe local defaults.
"""

import os
import sys
from typing import Dict, Any, Optional
from dotenv import load_dotenv

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from src.utils.logger import get_logger

log = get_logger(__name__)
load_dotenv()


def get_clickhouse_config() -> Dict[str, Any]:
    """Retrieve ClickHouse configuration with secure local defaults."""
    return {
        "host": os.getenv("CLICKHOUSE_HOST", "localhost").strip(),
        "port": int(os.getenv("CLICKHOUSE_PORT", "8123")),
        "username": os.getenv("CLICKHOUSE_USER", "default").strip(),
        "password": os.getenv("CLICKHOUSE_PASSWORD", "").strip(),
        "database": os.getenv("CLICKHOUSE_DB_CLEAN", "agripulse_clean").strip(),
    }


def get_client(database: Optional[str] = None):
    """
    Establish and return a live clickhouse-connect Client.
    """
    try:
        import clickhouse_connect
    except ImportError:
        log.error("clickhouse-connect is not installed. Run: pip install clickhouse-connect")
        raise

    cfg = get_clickhouse_config()
    db = database or cfg["database"]

    client = clickhouse_connect.get_client(
        host=cfg["host"],
        port=cfg["port"],
        username=cfg["username"],
        password=cfg["password"],
        database=db,
    )
    return client


def check_connection() -> Dict[str, Any]:
    """
    Test ClickHouse connectivity and return active server metadata.
    """
    try:
        client = get_client(database="default")
        version = client.server_version
        dbs = [row[0] for row in client.query("SHOW DATABASES").result_rows]
        client.close()

        result = {
            "status": "CONNECTED",
            "server_version": version,
            "databases": dbs,
            "connected": True,
        }
        log.info("ClickHouse connection successful: version=%s, databases=%s", version, dbs)
        return result
    except Exception as exc:
        log.warning("ClickHouse connection test failed: %s", exc)
        return {
            "status": "UNAVAILABLE",
            "error": str(exc),
            "connected": False,
        }


def execute_script(file_path: str, database: Optional[str] = None) -> bool:
    """
    Execute a multi-statement SQL script file against ClickHouse.
    """
    if not os.path.exists(file_path):
        log.error("SQL file not found: %s", file_path)
        return False

    client = get_client(database=database)
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            sql_content = f.read()

        # Split commands cleanly by semicolon ignoring comments
        statements = []
        current = []
        for line in sql_content.splitlines():
            stripped = line.strip()
            if stripped.startswith("--"):
                continue
            current.append(line)
            if stripped.endswith(";"):
                stmt = "\n".join(current).strip().rstrip(";")
                if stmt:
                    statements.append(stmt)
                current = []

        log.info("Executing %d statements from %s...", len(statements), os.path.basename(file_path))
        for stmt in statements:
            client.command(stmt)
        log.info("Successfully executed: %s", os.path.basename(file_path))
        return True
    finally:
        client.close()


if __name__ == "__main__":
    res = check_connection()
    print("ClickHouse Connection Status:")
    for k, v in res.items():
        print(f"  {k}: {v}")
