-- ============================================================================
-- AgriPulse India — Phase 3: Analytical MART Schema (Star Schema)
-- File: snowflake/sql/04_mart_tables.sql
-- Description: Conformed dimensions and distinct-grain fact tables
-- ============================================================================

USE DATABASE AGRIPULSE_DB;
USE SCHEMA MART;

-- ============================================================================
-- 1. CONFORMED DIMENSIONS
-- ============================================================================

-- Date Dimension
CREATE TABLE IF NOT EXISTS DIM_DATE (
    date_key INT PRIMARY KEY,                     -- YYYYMMDD format (e.g. 20261117)
    full_date DATE NOT NULL UNIQUE,
    year INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day INT NOT NULL,
    day_of_year INT NOT NULL,
    quarter INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    agricultural_year VARCHAR(20)                 -- July-to-June crop year designation
)
COMMENT = 'Conformed calendar and agricultural date dimension covering 1995 to 2035';

-- Location Dimension
-- Supports states, districts, and APMC markets
CREATE TABLE IF NOT EXISTS DIM_LOCATION (
    location_key INT AUTOINCREMENT PRIMARY KEY,
    state_name VARCHAR(100) NOT NULL,
    district_name VARCHAR(100) NOT NULL,
    market_name VARCHAR(150),                     -- APMC Market name (NULL for pure district-level crop data)
    location_type VARCHAR(50) NOT NULL,           -- 'MARKET', 'DISTRICT', 'GRID_TILE'
    latitude NUMBER(7, 4),
    longitude NUMBER(7, 4),
    region VARCHAR(50),                           -- Northern, Southern, Eastern, Western, Central, North-Eastern
    created_at_utc TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT uq_location UNIQUE (state_name, district_name, market_name)
)
COMMENT = 'Conformed geographical location dimension linking state, district, and APMC markets';

-- Commodity Dimension
CREATE TABLE IF NOT EXISTS DIM_COMMODITY (
    commodity_key INT AUTOINCREMENT PRIMARY KEY,
    commodity_name VARCHAR(100) NOT NULL,
    commodity_group VARCHAR(100),                 -- Cereals, Pulses, Oilseeds, Vegetables, Fruits, Cash Crops, etc.
    variety_name VARCHAR(100) DEFAULT 'Other',
    grade VARCHAR(50) DEFAULT 'Standard',
    standard_unit VARCHAR(50),                    -- 'Quintal', 'Tonnes', 'Number of Nuts'
    created_at_utc TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT uq_commodity UNIQUE (commodity_name, variety_name, grade)
)
COMMENT = 'Conformed agricultural crop and market commodity dimension';

-- ============================================================================
-- 2. FACT TABLES (DISTINCT GRAINS)
-- ============================================================================

-- Fact 1: Mandi Wholesale Prices
-- Grain: 1 record per market, commodity, variety, grade, and arrival date.
CREATE TABLE IF NOT EXISTS FACT_MANDI_PRICES (
    mandi_fact_key VARCHAR(64) PRIMARY KEY,       -- MD5(state|district|market|commodity|variety|grade|date)
    date_key INT NOT NULL,                        -- FK to DIM_DATE
    arrival_date DATE NOT NULL,
    state_name VARCHAR(100) NOT NULL,
    district_name VARCHAR(100) NOT NULL,
    market_name VARCHAR(150) NOT NULL,
    commodity_name VARCHAR(100) NOT NULL,
    variety_name VARCHAR(100) NOT NULL,
    grade VARCHAR(50) NOT NULL,
    min_price_inr_quintal NUMBER(10, 2),
    max_price_inr_quintal NUMBER(10, 2),
    modal_price_inr_quintal NUMBER(10, 2),
    price_spread_inr_quintal NUMBER(10, 2),       -- max_price - min_price
    arrival_quantity_quintals NUMBER(12, 2),      -- Estimated / reported daily arrivals
    currency VARCHAR(10) DEFAULT 'INR',
    price_unit VARCHAR(50) DEFAULT 'INR per quintal',
    is_price_relation_valid BOOLEAN DEFAULT TRUE,
    source_channel VARCHAR(50) DEFAULT 'BATCH',   -- 'BATCH' or 'KAFKA_STREAM'
    loaded_at_utc TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT fk_mandi_date FOREIGN KEY (date_key) REFERENCES DIM_DATE(date_key)
)
CLUSTER BY (arrival_date, state_name, commodity_name)
COMMENT = 'Fact table for APMC Mandi wholesale prices at daily market-commodity grain (₹/quintal)';

-- Fact 2: Crop Production
-- Grain: 1 record per state, district, crop, season, and crop year.
-- Note: Unit segregation is strictly maintained. Coconut production is in Nuts; others in Tonnes.
CREATE TABLE IF NOT EXISTS FACT_CROP_PRODUCTION (
    crop_fact_key VARCHAR(64) PRIMARY KEY,        -- MD5(state|district|crop_year|season|crop)
    crop_year INT NOT NULL,
    state_name VARCHAR(100) NOT NULL,
    district_name VARCHAR(100) NOT NULL,
    crop_name VARCHAR(100) NOT NULL,
    season VARCHAR(50) NOT NULL,
    area_hectares NUMBER(14, 4),
    production NUMBER(16, 4),                     -- Reported numerical value
    production_unit VARCHAR(50) NOT NULL,         -- 'Metric Tonnes' or 'Number of Nuts'
    production_tonnes NUMBER(16, 4),              -- Tonnes for field crops (NULL for Coconut)
    production_coconut_nuts NUMBER(18, 2),        -- Nuts for Coconut (NULL for field crops)
    yield_tonnes_per_hectare NUMBER(14, 4),       -- Calculated metric yield (Tonnes / Ha)
    yield_coconut_nuts_per_hectare NUMBER(16, 2), -- Calculated coconut yield (Nuts / Ha)
    is_production_missing BOOLEAN DEFAULT FALSE,  -- True if government report had NULL production
    is_incomplete_year BOOLEAN DEFAULT FALSE,
    loaded_at_utc TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (crop_year, state_name, crop_name)
COMMENT = 'Fact table for historical crop production at annual/seasonal district-crop grain with strict unit segregation';

-- Fact 3: NASA POWER Weather
-- Grain: 1 record per 0.5x0.5 degree latitude/longitude grid cell and calendar date.
CREATE TABLE IF NOT EXISTS FACT_WEATHER (
    weather_fact_key VARCHAR(64) PRIMARY KEY,     -- MD5(latitude|longitude|date)
    date_key INT NOT NULL,                        -- FK to DIM_DATE
    weather_date DATE NOT NULL,
    latitude NUMBER(7, 4) NOT NULL,
    longitude NUMBER(7, 4) NOT NULL,
    rainfall_mm_per_day NUMBER(8, 3),             -- Daily precipitation in mm
    temperature_celsius NUMBER(6, 2),             -- 2m daily temperature (°C, negative preserved)
    is_negative_temperature BOOLEAN DEFAULT FALSE,
    is_extreme_rainfall BOOLEAN DEFAULT FALSE,
    is_subzero_high_altitude BOOLEAN DEFAULT FALSE,
    rainfall_unit VARCHAR(50) DEFAULT 'mm per day',
    temperature_unit VARCHAR(50) DEFAULT 'Celsius',
    loaded_at_utc TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT fk_weather_date FOREIGN KEY (date_key) REFERENCES DIM_DATE(date_key)
)
CLUSTER BY (weather_date, latitude, longitude)
COMMENT = 'Fact table for daily NASA POWER meteorology at 0.5x0.5 deg grid-day grain';
