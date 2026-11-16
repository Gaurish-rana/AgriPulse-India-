"""
AgriPulse India — Snowflake Connection & Session Manager
=========================================================
Manages secure connection to Snowflake Cloud Data Warehouse.
Reads all credentials from environment variables.
Never hardcodes credentials or sensitive account info.
"""

import os
import sys
from typing import Optional, Dict, Any
from dotenv import load_dotenv

# Ensure root dir is in path
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from src.utils.logger import get_logger

log = get_logger(__name__)
load_dotenv()


def get_snowflake_config() -> Dict[str, str]:
    """Retrieve Snowflake configuration from environment variables."""
    return {
        "account": os.getenv("SNOWFLAKE_ACCOUNT", "").strip(),
        "user": os.getenv("SNOWFLAKE_USER", "").strip(),
        "password": os.getenv("SNOWFLAKE_PASSWORD", "").strip(),
        "database": os.getenv("SNOWFLAKE_DATABASE", "AGRIPULSE_DB").strip(),
        "schema": os.getenv("SNOWFLAKE_SCHEMA", "CLEAN").strip(),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "AGRIPULSE_WH").strip(),
        "role": os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN").strip(),
    }


def validate_config() -> bool:
    """Check whether necessary Snowflake configuration variables are provided."""
    cfg = get_snowflake_config()
    missing = [k for k in ["account", "user", "password"] if not cfg[k]]
    if missing:
        log.warning(
            "Missing required Snowflake environment variables: %s. "
            "Please configure them in your .env file.",
            ", ".join([f"SNOWFLAKE_{m.upper()}" for m in missing]),
        )
        return False
    return True


def get_snowflake_connection():
    """
    Establish and return a live Snowflake connection.
    Raises ValueError if configuration is missing, or snowflake.connector.Error if auth fails.
    """
    try:
        import snowflake.connector
    except ImportError:
        log.error("snowflake-connector-python is not installed. Run: pip install snowflake-connector-python")
        raise

    cfg = get_snowflake_config()
    if not cfg["account"] or not cfg["user"] or not cfg["password"]:
        raise ValueError(
            "Snowflake credentials missing! Please configure SNOWFLAKE_ACCOUNT, "
            "SNOWFLAKE_USER, and SNOWFLAKE_PASSWORD in your .env file."
        )

    log.info(
        "Connecting to Snowflake account '%s' as user '%s' (DB: %s, WH: %s, Role: %s)...",
        cfg["account"],
        cfg["user"],
        cfg["database"],
        cfg["warehouse"],
        cfg["role"],
    )

    conn = snowflake.connector.connect(
        account=cfg["account"],
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["database"],
        schema=cfg["schema"],
        warehouse=cfg["warehouse"],
        role=cfg["role"],
        autocommit=True,
    )
    return conn


def check_connection() -> Dict[str, Any]:
    """
    Test Snowflake connectivity and return active session metadata.
    """
    if not validate_config():
        return {
            "status": "CONFIG_MISSING",
            "message": "Snowflake environment variables (SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD) are not set in .env",
            "connected": False,
        }

    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE(), CURRENT_VERSION()")
        row = cursor.fetchone()
        cursor.close()
        conn.close()

        result = {
            "status": "CONNECTED",
            "current_user": row[0],
            "current_role": row[1],
            "current_database": row[2],
            "current_schema": row[3],
            "current_warehouse": row[4],
            "snowflake_version": row[5],
            "connected": True,
        }
        log.info("Snowflake connection successful: %s", result)
        return result
    except Exception as exc:
        log.error("Snowflake connection failed: %s", exc)
        return {
            "status": "FAILED",
            "error": str(exc),
            "connected": False,
        }


def execute_script(file_path: str, conn=None) -> bool:
    """
    Execute a multi-statement SQL script file against Snowflake.
    """
    if not os.path.exists(file_path):
        log.error("SQL file not found: %s", file_path)
        return False

    should_close = False
    if conn is None:
        conn = get_snowflake_connection()
        should_close = True

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            sql_content = f.read()

        log.info("Executing SQL script: %s", os.path.basename(file_path))
        cursor = conn.cursor()
        for cmd in conn.execute_stream(sql_content):
            for row in cmd:
                pass  # consume cursor
        cursor.close()
        log.info("Successfully executed: %s", os.path.basename(file_path))
        return True
    except Exception as exc:
        log.error("Error executing script %s: %s", file_path, exc)
        raise
    finally:
        if should_close and conn:
            conn.close()


if __name__ == "__main__":
    status = check_connection()
    print("Snowflake Connection Status:")
    for k, v in status.items():
        print(f"  {k}: {v}")
