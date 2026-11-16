-- ============================================================================
-- AgriPulse India — Phase 3: ClickHouse Clean Schema Tables
-- File: clickhouse/sql/03_clean_tables.sql
-- Description: Standardized, validated, and conformed analytical tables in agripulse_clean
-- ============================================================================

-- 1. CLEAN Mandi Wholesale Prices Table
-- Grain: 1 record per market, commodity, variety, grade, arrival_date
CREATE TABLE IF NOT EXISTS agripulse_clean.clean_mandi_prices (
    state_name LowCardinality(String),
    district_name LowCardinality(String),
    market_name String,
    commodity_name LowCardinality(String),
    variety_name String,
    grade LowCardinality(String),
    arrival_date Date32,
    min_price_inr_quintal Float64,
    max_price_inr_quintal Float64,
    modal_price_inr_quintal Float64,
    arrival_year UInt16,
    arrival_month UInt8,
    is_price_relation_valid UInt8 DEFAULT 1,
    currency LowCardinality(String) DEFAULT 'INR',
    price_unit LowCardinality(String) DEFAULT 'INR per quintal',
    record_hash String,
    source_channel LowCardinality(String) DEFAULT 'BATCH',
    loaded_at_utc DateTime64(3, 'UTC') DEFAULT now()
)
ENGINE = ReplacingMergeTree(loaded_at_utc)
PARTITION BY toYYYYMM(arrival_date)
ORDER BY (state_name, district_name, market_name, commodity_name, variety_name, grade, arrival_date)
SETTINGS index_granularity = 8192;

-- 2. CLEAN Crop Production Table
-- Grain: 1 record per state, district, crop_year, season, crop_name
-- Units strictly segregated: Coconut in Nuts, Field crops in Tonnes
CREATE TABLE IF NOT EXISTS agripulse_clean.clean_crop_production (
    state_name LowCardinality(String),
    district_name LowCardinality(String),
    crop_year UInt16,
    season LowCardinality(String),
    crop_name LowCardinality(String),
    area_hectares Nullable(Float64),
    production Nullable(Float64),
    is_production_missing UInt8 DEFAULT 0,
    production_unit LowCardinality(String),
    area_unit LowCardinality(String) DEFAULT 'Hectares',
    production_tonnes Nullable(Float64),
    production_coconut_nuts Nullable(Float64),
    yield_tonnes_per_hectare Nullable(Float64),
    yield_coconut_nuts_per_hectare Nullable(Float64),
    is_incomplete_year UInt8 DEFAULT 0,
    loaded_at_utc DateTime64(3, 'UTC') DEFAULT now()
)
ENGINE = ReplacingMergeTree(loaded_at_utc)
ORDER BY (state_name, district_name, crop_year, season, crop_name)
SETTINGS index_granularity = 8192;

-- 3. CLEAN NASA Weather Table
-- Grain: 1 record per latitude, longitude, and date
-- Partitioned by year; sub-zero negative temperatures down to -17C preserved
CREATE TABLE IF NOT EXISTS agripulse_clean.clean_weather (
    date Date32,
    year UInt16,
    month UInt8,
    day UInt8,
    day_of_year UInt16,
    latitude Float64,
    longitude Float64,
    precipitation_mm_per_day Nullable(Float64),
    temperature_celsius_2m Nullable(Float64),
    precipitation_unit LowCardinality(String) DEFAULT 'mm per day',
    temperature_unit LowCardinality(String) DEFAULT 'Celsius',
    is_missing_precipitation UInt8 DEFAULT 0,
    is_missing_temperature UInt8 DEFAULT 0,
    is_negative_temperature UInt8 DEFAULT 0,
    is_subzero_high_altitude UInt8 DEFAULT 0,
    is_extreme_precipitation UInt8 DEFAULT 0,
    loaded_at_utc DateTime64(3, 'UTC') DEFAULT now()
)
ENGINE = ReplacingMergeTree(loaded_at_utc)
PARTITION BY year
ORDER BY (latitude, longitude, date)
SETTINGS index_granularity = 8192;
