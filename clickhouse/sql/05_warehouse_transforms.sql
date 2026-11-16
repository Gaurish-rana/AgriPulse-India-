-- ============================================================================
-- AgriPulse India — Phase 3: ClickHouse Warehouse Transformations
-- File: clickhouse/sql/05_warehouse_transforms.sql
-- Description: Population of analytical dimensions and fact tables (CLEAN -> MART)
-- ============================================================================

-- ============================================================================
-- 1. POPULATE DIM_DATE (Calendar & Agricultural Year 1995 to 2035)
-- ============================================================================
INSERT INTO agripulse_mart.dim_date (
    date_key, full_date, year, month, month_name, day, day_of_year,
    quarter, day_of_week, day_name, is_weekend, agricultural_year
)
SELECT
    toYYYYMMDD(d) AS date_key,
    d AS full_date,
    toYear(d) AS year,
    toMonth(d) AS month,
    case toMonth(d)
        when 1 then 'January' when 2 then 'February' when 3 then 'March'
        when 4 then 'April' when 5 then 'May' when 6 then 'June'
        when 7 then 'July' when 8 then 'August' when 9 then 'September'
        when 10 then 'October' when 11 then 'November' when 12 then 'December'
    end AS month_name,
    toDayOfMonth(d) AS day,
    toDayOfYear(d) AS day_of_year,
    toQuarter(d) AS quarter,
    toDayOfWeek(d) AS day_of_week,
    case toDayOfWeek(d)
        when 1 then 'Monday' when 2 then 'Tuesday' when 3 then 'Wednesday'
        when 4 then 'Thursday' when 5 then 'Friday' when 6 then 'Saturday' when 7 then 'Sunday'
    end AS day_name,
    if(toDayOfWeek(d) IN (6, 7), 1, 0) AS is_weekend,
    if(toMonth(d) >= 7,
       concat(toString(toYear(d)), '-', toString(toYear(d) + 1)),
       concat(toString(toYear(d) - 1), '-', toString(toYear(d)))) AS agricultural_year
FROM (
    SELECT toDate('1995-01-01') + number AS d
    FROM numbers(15000)
    WHERE d <= toDate('2035-12-31')
)
WHERE date_key NOT IN (SELECT date_key FROM agripulse_mart.dim_date);

-- ============================================================================
-- 2. POPULATE CONFORMED DIM_LOCATION
-- ============================================================================
-- APMC Markets from Mandi Prices
INSERT INTO agripulse_mart.dim_location (
    state_name, district_name, market_name, location_type, latitude, longitude, region
)
SELECT DISTINCT
    state_name,
    district_name,
    market_name,
    'MARKET' AS location_type,
    CAST(NULL, 'Nullable(Float64)') AS latitude,
    CAST(NULL, 'Nullable(Float64)') AS longitude,
    multiIf(
        state_name IN ('Jammu and Kashmir', 'Himachal Pradesh', 'Punjab', 'Haryana', 'Uttarakhand', 'Uttar Pradesh', 'Chandigarh', 'Delhi'), 'Northern',
        state_name IN ('Tamil Nadu', 'Kerala', 'Karnataka', 'Andhra Pradesh', 'Telangana', 'Puducherry'), 'Southern',
        state_name IN ('Maharashtra', 'Gujarat', 'Goa', 'Rajasthan'), 'Western',
        state_name IN ('West Bengal', 'Bihar', 'Odisha', 'Jharkhand'), 'Eastern',
        state_name IN ('Madhya Pradesh', 'Chhattisgarh'), 'Central',
        'North-Eastern'
    ) AS region
FROM agripulse_clean.clean_mandi_prices;

-- District-level entries from Crop Production
INSERT INTO agripulse_mart.dim_location (
    state_name, district_name, market_name, location_type, latitude, longitude, region
)
SELECT DISTINCT
    state_name,
    district_name,
    CAST(NULL, 'Nullable(String)') AS market_name,
    'DISTRICT' AS location_type,
    CAST(NULL, 'Nullable(Float64)') AS latitude,
    CAST(NULL, 'Nullable(Float64)') AS longitude,
    multiIf(
        state_name IN ('Jammu and Kashmir', 'Himachal Pradesh', 'Punjab', 'Haryana', 'Uttarakhand', 'Uttar Pradesh', 'Chandigarh', 'Delhi'), 'Northern',
        state_name IN ('Tamil Nadu', 'Kerala', 'Karnataka', 'Andhra Pradesh', 'Telangana', 'Puducherry'), 'Southern',
        state_name IN ('Maharashtra', 'Gujarat', 'Goa', 'Rajasthan'), 'Western',
        state_name IN ('West Bengal', 'Bihar', 'Odisha', 'Jharkhand'), 'Eastern',
        state_name IN ('Madhya Pradesh', 'Chhattisgarh'), 'Central',
        'North-Eastern'
    ) AS region
FROM agripulse_clean.clean_crop_production;

-- ============================================================================
-- 3. POPULATE CONFORMED DIM_COMMODITY
-- ============================================================================
INSERT INTO agripulse_mart.dim_commodity (
    commodity_name, commodity_group, variety_name, grade, standard_unit
)
SELECT DISTINCT
    commodity_name,
    'Mandi Wholesale' AS commodity_group,
    variety_name,
    grade,
    'Quintal' AS standard_unit
FROM agripulse_clean.clean_mandi_prices;

INSERT INTO agripulse_mart.dim_commodity (
    commodity_name, commodity_group, variety_name, grade, standard_unit
)
SELECT DISTINCT
    crop_name AS commodity_name,
    'Field Crop' AS commodity_group,
    'All' AS variety_name,
    'Standard' AS grade,
    production_unit AS standard_unit
FROM agripulse_clean.clean_crop_production;

-- ============================================================================
-- 4. POPULATE FACT_MANDI_PRICES
-- ============================================================================
INSERT INTO agripulse_mart.fact_mandi_prices (
    mandi_fact_key, date_key, arrival_date, state_name, district_name, market_name,
    commodity_name, variety_name, grade, min_price_inr_quintal, max_price_inr_quintal,
    modal_price_inr_quintal, price_spread_inr_quintal, arrival_quantity_quintals,
    currency, price_unit, is_price_relation_valid, source_channel, loaded_at_utc
)
SELECT
    hex(MD5(concat(
        lower(trim(state_name)), '#',
        lower(trim(district_name)), '#',
        lower(trim(market_name)), '#',
        lower(trim(commodity_name)), '#',
        lower(trim(variety_name)), '#',
        lower(trim(grade)), '#',
        toString(arrival_date)
    ))) AS mandi_fact_key,
    toYYYYMMDD(arrival_date) AS date_key,
    arrival_date,
    state_name,
    district_name,
    market_name,
    commodity_name,
    variety_name,
    grade,
    min_price_inr_quintal,
    max_price_inr_quintal,
    modal_price_inr_quintal,
    round(max_price_inr_quintal - min_price_inr_quintal, 2) AS price_spread_inr_quintal,
    CAST(NULL, 'Nullable(Float64)') AS arrival_quantity_quintals,
    currency,
    price_unit,
    is_price_relation_valid,
    source_channel,
    now() AS loaded_at_utc
FROM agripulse_clean.clean_mandi_prices;

-- ============================================================================
-- 5. POPULATE FACT_CROP_PRODUCTION
-- ============================================================================
INSERT INTO agripulse_mart.fact_crop_production (
    crop_fact_key, crop_year, state_name, district_name, crop_name, season,
    area_hectares, production, production_unit, production_tonnes, production_coconut_nuts,
    yield_tonnes_per_hectare, yield_coconut_nuts_per_hectare, is_production_missing,
    is_incomplete_year, loaded_at_utc
)
SELECT
    hex(MD5(concat(
        lower(trim(state_name)), '#',
        lower(trim(district_name)), '#',
        toString(crop_year), '#',
        lower(trim(season)), '#',
        lower(trim(crop_name))
    ))) AS crop_fact_key,
    crop_year,
    state_name,
    district_name,
    crop_name,
    season,
    area_hectares,
    production,
    production_unit,
    production_tonnes,
    production_coconut_nuts,
    yield_tonnes_per_hectare,
    yield_coconut_nuts_per_hectare,
    is_production_missing,
    is_incomplete_year,
    now() AS loaded_at_utc
FROM agripulse_clean.clean_crop_production;

-- ============================================================================
-- 6. POPULATE FACT_WEATHER
-- ============================================================================
INSERT INTO agripulse_mart.fact_weather (
    weather_fact_key, date_key, weather_date, latitude, longitude,
    rainfall_mm_per_day, temperature_celsius, is_negative_temperature,
    is_extreme_rainfall, is_subzero_high_altitude, rainfall_unit, temperature_unit, loaded_at_utc
)
SELECT
    hex(MD5(concat(
        toString(latitude), '#',
        toString(longitude), '#',
        toString(date)
    ))) AS weather_fact_key,
    toYYYYMMDD(date) AS date_key,
    date AS weather_date,
    latitude,
    longitude,
    precipitation_mm_per_day AS rainfall_mm_per_day,
    temperature_celsius_2m AS temperature_celsius,
    is_negative_temperature,
    is_extreme_precipitation AS is_extreme_rainfall,
    is_subzero_high_altitude,
    precipitation_unit AS rainfall_unit,
    temperature_unit,
    now() AS loaded_at_utc
FROM agripulse_clean.clean_weather;
