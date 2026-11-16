-- ============================================================================
-- AgriPulse India — Phase 3: ClickHouse Raw Schema Tables & Audit
-- File: clickhouse/sql/02_raw_tables.sql
-- Description: Raw landing tables and pipeline audit tracking in agripulse_raw
-- ============================================================================

-- 1. Pipeline Load Audit Table
CREATE TABLE IF NOT EXISTS agripulse_raw.load_audit (
    load_id UUID DEFAULT generateUUIDv4(),
    pipeline_stage LowCardinality(String),        -- 'BATCH_PARQUET', 'KAFKA_STREAM'
    target_table String,
    file_name String,
    file_row_count UInt64,
    rows_inserted UInt64,
    load_status LowCardinality(String),          -- 'SUCCESS', 'FAILED', 'PARTIAL'
    load_start_time DateTime64(3, 'UTC') DEFAULT now(),
    load_end_time Nullable(DateTime64(3, 'UTC')),
    error_message Nullable(String),
    loaded_by String DEFAULT currentUser()
)
ENGINE = MergeTree()
ORDER BY (load_start_time, target_table);

-- 2. Raw Mandi Prices Landing Table
CREATE TABLE IF NOT EXISTS agripulse_raw.raw_mandi_prices (
    raw_id UUID DEFAULT generateUUIDv4(),
    state Nullable(String),
    district Nullable(String),
    market Nullable(String),
    commodity Nullable(String),
    variety Nullable(String),
    grade Nullable(String),
    arrival_date Nullable(String),
    min_price Nullable(String),
    max_price Nullable(String),
    modal_price Nullable(String),
    raw_payload Nullable(String),
    source_channel LowCardinality(String) DEFAULT 'BATCH',
    source_file Nullable(String),
    ingested_at_utc DateTime64(3, 'UTC') DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (ingested_at_utc, raw_id);

-- 3. Raw Crop Production Landing Table
CREATE TABLE IF NOT EXISTS agripulse_raw.raw_crop_production (
    raw_id UUID DEFAULT generateUUIDv4(),
    state_name Nullable(String),
    district_name Nullable(String),
    crop_year Nullable(String),
    season Nullable(String),
    crop Nullable(String),
    area Nullable(String),
    production Nullable(String),
    source_file Nullable(String),
    ingested_at_utc DateTime64(3, 'UTC') DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (ingested_at_utc, raw_id);

-- 4. Raw NASA Weather Landing Table
CREATE TABLE IF NOT EXISTS agripulse_raw.raw_weather (
    raw_id UUID DEFAULT generateUUIDv4(),
    observation_date Nullable(String),
    year Nullable(String),
    month Nullable(String),
    day Nullable(String),
    latitude Nullable(String),
    longitude Nullable(String),
    prectotcorr Nullable(String),
    t2m Nullable(String),
    source_tile_file Nullable(String),
    ingested_at_utc DateTime64(3, 'UTC') DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (ingested_at_utc, raw_id);
