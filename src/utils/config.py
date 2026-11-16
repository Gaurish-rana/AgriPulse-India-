"""
Central configuration loader.
All settings are read from environment variables (via .env).
No secrets are hard-coded here.
"""
import os
from dotenv import load_dotenv

load_dotenv()


class MandiConfig:
    API_KEY: str = os.getenv("MANDI_API_KEY", "")
    BASE_URL: str = os.getenv(
        "MANDI_API_BASE_URL",
        "https://api.data.gov.in/resource/9ef84268-d588-465a-a308-a864a43d0070",
    )
    RECORDS_PER_PAGE: int = 500
    RAW_DIR: str = os.path.join("data", "raw", "mandi")


class NASAConfig:
    BASE_URL: str = os.getenv(
        "NASA_POWER_BASE_URL",
        "https://power.larc.nasa.gov/api/temporal/daily/point",
    )
    PARAMETERS: list = ["PRECTOTCORR", "T2M"]   # rainfall, temperature
    COMMUNITY: str = "AG"
    RAW_DIR: str = os.path.join("data", "raw", "nasa_power")


class CropConfig:
    RAW_DIR: str = os.path.join("data", "raw", "crop_production")


class PathConfig:
    INTERIM_DIR: str = os.path.join("data", "interim")
    PROCESSED_DIR: str = os.path.join("data", "processed")
    LOG_DIR: str = os.getenv("LOG_DIR", "logs")


class SnowflakeConfig:
    ACCOUNT: str = os.getenv("SNOWFLAKE_ACCOUNT", "")
    USER: str = os.getenv("SNOWFLAKE_USER", "")
    PASSWORD: str = os.getenv("SNOWFLAKE_PASSWORD", "")
    DATABASE: str = os.getenv("SNOWFLAKE_DATABASE", "AGRIPULSE_DB")
    SCHEMA: str = os.getenv("SNOWFLAKE_SCHEMA", "CLEAN")
    WAREHOUSE: str = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    ROLE: str = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
