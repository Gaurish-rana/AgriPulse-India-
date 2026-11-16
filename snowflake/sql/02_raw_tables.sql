-- ============================================================================
-- AgriPulse India — Phase 3: Raw Schema Tables & Audit
-- File: snowflake/sql/02_raw_tables.sql
-- Description: Raw landing tables and pipeline audit tracking
-- ============================================================================

USE DATABASE AGRIPULSE_DB;
USE SCHEMA RAW;

-- 1. Batch & Streaming Load Audit Table
CREATE TABLE IF NOT EXISTS LOAD_AUDIT (
    load_id INT AUTOINCREMENT PRIMARY KEY,
    pipeline_stage VARCHAR(50) NOT NULL,          -- e.g., 'BATCH_PARQUET', 'KAFKA_STREAM'
    target_table VARCHAR(100) NOT NULL,
    file_name VARCHAR(500),
    file_row_count NUMBER(12, 0),
    rows_inserted NUMBER(12, 0),
    load_status VARCHAR(20) NOT NULL,             -- 'SUCCESS', 'FAILED', 'PARTIAL'
    load_start_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    load_end_time TIMESTAMP_NTZ,
    error_message VARCHAR(2000),
    loaded_by VARCHAR(100) DEFAULT CURRENT_USER()
)
COMMENT = 'Audit trail tracking every batch and streaming data ingestion event';

-- 2. RAW Mandi Prices Landing Table
-- Holds raw API/Kafka JSON payloads or staged raw tabular records
CREATE TABLE IF NOT EXISTS RAW_MANDI_PRICES (
    raw_id INT AUTOINCREMENT PRIMARY KEY,
    state VARCHAR(200),
    district VARCHAR(200),
    market VARCHAR(200),
    commodity VARCHAR(200),
    variety VARCHAR(200),
    grade VARCHAR(100),
    arrival_date VARCHAR(50),
    min_price VARCHAR(50),
    max_price VARCHAR(50),
    modal_price VARCHAR(50),
    raw_payload VARIANT,
    source_channel VARCHAR(50) DEFAULT 'BATCH',  -- 'BATCH', 'KAFKA_STREAM'
    source_file VARCHAR(500),
    ingested_at_utc TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Landing table for Mandi wholesale prices before standardization';

-- 3. RAW Crop Production Landing Table
CREATE TABLE IF NOT EXISTS RAW_CROP_PRODUCTION (
    raw_id INT AUTOINCREMENT PRIMARY KEY,
    state_name VARCHAR(200),
    district_name VARCHAR(200),
    crop_year VARCHAR(50),
    season VARCHAR(100),
    crop VARCHAR(200),
    area VARCHAR(50),
    production VARCHAR(50),
    source_file VARCHAR(500),
    ingested_at_utc TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Landing table for historical crop production before unit segregation and cleaning';

-- 4. RAW NASA Weather Landing Table
CREATE TABLE IF NOT EXISTS RAW_WEATHER (
    raw_id INT AUTOINCREMENT PRIMARY KEY,
    observation_date VARCHAR(50),
    year VARCHAR(10),
    month VARCHAR(10),
    day VARCHAR(10),
    latitude VARCHAR(50),
    longitude VARCHAR(50),
    prectotcorr VARCHAR(50),                      -- Raw PRECTOTCORR parameter
    t2m VARCHAR(50),                              -- Raw T2M parameter
    source_tile_file VARCHAR(500),
    ingested_at_utc TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Landing table for NASA POWER daily tile meteorology before missing marker cleaning';
