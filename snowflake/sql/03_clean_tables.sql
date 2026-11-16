-- ============================================================================
-- AgriPulse India — Phase 3: Clean Schema Tables
-- File: snowflake/sql/03_clean_tables.sql
-- Description: Standardized, validated, and conformed analytical tables
-- ============================================================================

USE DATABASE AGRIPULSE_DB;
USE SCHEMA CLEAN;

-- 1. CLEAN Mandi Prices Table
-- Units: min_price, max_price, modal_price in INR per quintal (₹/q). 1 quintal = 100 kg.
CREATE TABLE IF NOT EXISTS CLEAN_MANDI_PRICES (
    state_name VARCHAR(100) NOT NULL,
    district_name VARCHAR(100) NOT NULL,
    market_name VARCHAR(150) NOT NULL,
    commodity_name VARCHAR(100) NOT NULL,
    variety_name VARCHAR(100) NOT NULL,
    grade VARCHAR(50) NOT NULL,
    arrival_date DATE NOT NULL,
    min_price_inr_quintal NUMBER(10, 2),
    max_price_inr_quintal NUMBER(10, 2),
    modal_price_inr_quintal NUMBER(10, 2),
    arrival_year INT,
    arrival_month INT,
    is_price_relation_valid BOOLEAN DEFAULT TRUE,
    currency VARCHAR(10) DEFAULT 'INR',
    price_unit VARCHAR(50) DEFAULT 'INR per quintal',
    record_hash VARCHAR(64),                      -- SHA-256 / MD5 business key hash
    source_channel VARCHAR(50) DEFAULT 'BATCH',  -- 'BATCH' or 'KAFKA_STREAM'
    loaded_at_utc TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_clean_mandi PRIMARY KEY (state_name, district_name, market_name, commodity_name, variety_name, grade, arrival_date)
)
COMMENT = 'Cleansed wholesale Mandi prices with standardized state names, parsed dates, and validated prices (₹/quintal)';

-- 2. CLEAN Crop Production Table
-- Units:
--   area_hectares: Hectares (ha)
--   production_tonnes: Metric Tonnes (t) for non-coconut crops
--   production_coconut_nuts: Number of Nuts for Coconut
--   yield: tonnes/ha or nuts/ha
CREATE TABLE IF NOT EXISTS CLEAN_CROP_PRODUCTION (
    state_name VARCHAR(100) NOT NULL,
    district_name VARCHAR(100) NOT NULL,
    crop_year INT NOT NULL,
    season VARCHAR(50) NOT NULL,
    crop_name VARCHAR(100) NOT NULL,
    area_hectares NUMBER(14, 4),
    production NUMBER(16, 4),                     -- Original reported numeric value
    is_production_missing BOOLEAN DEFAULT FALSE,
    production_unit VARCHAR(50),                  -- 'Metric Tonnes' or 'Number of Nuts'
    area_unit VARCHAR(50) DEFAULT 'Hectares',
    production_tonnes NUMBER(16, 4),              -- Tonnes only (NULL for Coconut)
    production_coconut_nuts NUMBER(18, 2),        -- Nuts only (NULL for other crops)
    yield_tonnes_per_hectare NUMBER(14, 4),
    yield_coconut_nuts_per_hectare NUMBER(16, 2),
    is_incomplete_year BOOLEAN DEFAULT FALSE,
    loaded_at_utc TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_clean_crop PRIMARY KEY (state_name, district_name, crop_year, season, crop_name)
)
COMMENT = 'Historical crop production with standardized states, segregated units (Tonnes vs Nuts), and preserved missing markers';

-- 3. CLEAN NASA Weather Table
-- Units:
--   precipitation_mm_per_day: mm/day
--   temperature_celsius_2m: degrees Celsius (°C)
-- Preserves negative temperatures in Himalayan regions, missing markers converted to NULL.
CREATE TABLE IF NOT EXISTS CLEAN_WEATHER (
    date DATE NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    day_of_year INT,
    latitude NUMBER(7, 4) NOT NULL,
    longitude NUMBER(7, 4) NOT NULL,
    precipitation_mm_per_day NUMBER(8, 3),        -- NULL if missing (-999 removed)
    temperature_celsius_2m NUMBER(6, 2),          -- Legitimate negative temperatures preserved
    precipitation_unit VARCHAR(50) DEFAULT 'mm per day',
    temperature_unit VARCHAR(50) DEFAULT 'Celsius',
    is_missing_precipitation BOOLEAN DEFAULT FALSE,
    is_missing_temperature BOOLEAN DEFAULT FALSE,
    is_negative_temperature BOOLEAN DEFAULT FALSE,
    is_subzero_high_altitude BOOLEAN DEFAULT FALSE,
    is_extreme_precipitation BOOLEAN DEFAULT FALSE,
    loaded_at_utc TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_clean_weather PRIMARY KEY (date, latitude, longitude)
)
CLUSTER BY (date, latitude, longitude)
COMMENT = 'Cleansed daily gridded NASA POWER meteorology (0.5x0.5 deg) with quality flags and preserved sub-zero temperatures';
