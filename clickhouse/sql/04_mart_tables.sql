-- ============================================================================
-- AgriPulse India — Phase 3: ClickHouse Analytical MART Schema (Star Schema)
-- File: clickhouse/sql/04_mart_tables.sql
-- Description: Conformed dimensions and distinct-grain fact tables in agripulse_mart
-- ============================================================================

-- ============================================================================
-- 1. CONFORMED DIMENSIONS
-- ============================================================================

-- Conformed Date Dimension
CREATE TABLE IF NOT EXISTS agripulse_mart.dim_date (
    date_key UInt32,                              -- YYYYMMDD (e.g. 20261117)
    full_date Date32,
    year UInt16,
    month UInt8,
    month_name LowCardinality(String),
    day UInt8,
    day_of_year UInt16,
    quarter UInt8,
    day_of_week UInt8,
    day_name LowCardinality(String),
    is_weekend UInt8,
    agricultural_year LowCardinality(String)      -- e.g. '2025-2026'
)
ENGINE = MergeTree()
ORDER BY (date_key);

-- Conformed Geographical Location Dimension
CREATE TABLE IF NOT EXISTS agripulse_mart.dim_location (
    location_key UUID DEFAULT generateUUIDv4(),
    state_name LowCardinality(String),
    district_name LowCardinality(String),
    market_name Nullable(String),                 -- NULL for district-only crop records
    location_type LowCardinality(String),         -- 'MARKET', 'DISTRICT', 'GRID_TILE'
    latitude Nullable(Float64),
    longitude Nullable(Float64),
    region LowCardinality(String),
    created_at_utc DateTime64(3, 'UTC') DEFAULT now()
)
ENGINE = ReplacingMergeTree(created_at_utc)
ORDER BY (state_name, district_name, ifNull(market_name, ''));

-- Conformed Commodity & Crop Dimension
CREATE TABLE IF NOT EXISTS agripulse_mart.dim_commodity (
    commodity_key UUID DEFAULT generateUUIDv4(),
    commodity_name LowCardinality(String),
    commodity_group LowCardinality(String),       -- Cereals, Pulses, Oilseeds, Vegetables, Fruits, etc.
    variety_name String DEFAULT 'Other',
    grade LowCardinality(String) DEFAULT 'Standard',
    standard_unit LowCardinality(String),         -- 'Quintal', 'Metric Tonnes', 'Number of Nuts'
    created_at_utc DateTime64(3, 'UTC') DEFAULT now()
)
ENGINE = ReplacingMergeTree(created_at_utc)
ORDER BY (commodity_name, variety_name, grade);

-- ============================================================================
-- 2. DISTINCT-GRAIN FACT TABLES
-- ============================================================================

-- Fact 1: Mandi Wholesale Prices
-- Grain: 1 record per market, commodity, variety, grade, arrival_date
CREATE TABLE IF NOT EXISTS agripulse_mart.fact_mandi_prices (
    mandi_fact_key String,                        -- cityHash64/MD5 hex string
    date_key UInt32,
    arrival_date Date32,
    state_name LowCardinality(String),
    district_name LowCardinality(String),
    market_name String,
    commodity_name LowCardinality(String),
    variety_name String,
    grade LowCardinality(String),
    min_price_inr_quintal Float64,
    max_price_inr_quintal Float64,
    modal_price_inr_quintal Float64,
    price_spread_inr_quintal Float64,
    arrival_quantity_quintals Nullable(Float64),
    currency LowCardinality(String) DEFAULT 'INR',
    price_unit LowCardinality(String) DEFAULT 'INR per quintal',
    is_price_relation_valid UInt8 DEFAULT 1,
    source_channel LowCardinality(String) DEFAULT 'BATCH',
    loaded_at_utc DateTime64(3, 'UTC') DEFAULT now()
)
ENGINE = ReplacingMergeTree(loaded_at_utc)
PARTITION BY toYYYYMM(arrival_date)
ORDER BY (mandi_fact_key);

-- Fact 2: Crop Production
-- Grain: 1 record per state, district, crop_year, season, crop_name
CREATE TABLE IF NOT EXISTS agripulse_mart.fact_crop_production (
    crop_fact_key String,
    crop_year UInt16,
    state_name LowCardinality(String),
    district_name LowCardinality(String),
    crop_name LowCardinality(String),
    season LowCardinality(String),
    area_hectares Nullable(Float64),
    production Nullable(Float64),
    production_unit LowCardinality(String),
    production_tonnes Nullable(Float64),
    production_coconut_nuts Nullable(Float64),
    yield_tonnes_per_hectare Nullable(Float64),
    yield_coconut_nuts_per_hectare Nullable(Float64),
    is_production_missing UInt8 DEFAULT 0,
    is_incomplete_year UInt8 DEFAULT 0,
    loaded_at_utc DateTime64(3, 'UTC') DEFAULT now()
)
ENGINE = ReplacingMergeTree(loaded_at_utc)
ORDER BY (crop_fact_key);

-- Fact 3: NASA POWER Weather
-- Grain: 1 record per latitude, longitude, and date
CREATE TABLE IF NOT EXISTS agripulse_mart.fact_weather (
    weather_fact_key String,
    date_key UInt32,
    weather_date Date32,
    latitude Float64,
    longitude Float64,
    rainfall_mm_per_day Nullable(Float64),
    temperature_celsius Nullable(Float64),
    is_negative_temperature UInt8 DEFAULT 0,
    is_extreme_rainfall UInt8 DEFAULT 0,
    is_subzero_high_altitude UInt8 DEFAULT 0,
    rainfall_unit LowCardinality(String) DEFAULT 'mm per day',
    temperature_unit LowCardinality(String) DEFAULT 'Celsius',
    loaded_at_utc DateTime64(3, 'UTC') DEFAULT now()
)
ENGINE = ReplacingMergeTree(loaded_at_utc)
PARTITION BY toYear(weather_date)
ORDER BY (weather_fact_key);
