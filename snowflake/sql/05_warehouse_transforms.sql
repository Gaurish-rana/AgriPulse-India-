-- ============================================================================
-- AgriPulse India — Phase 3: Warehouse Transformations (CLEAN -> MART)
-- File: snowflake/sql/05_warehouse_transforms.sql
-- Description: Population and synchronization of Conformed Dimensions & Facts
-- ============================================================================

USE DATABASE AGRIPULSE_DB;

-- ============================================================================
-- 1. POPULATE DIM_DATE (Calendar Year 1995 to 2035)
-- ============================================================================
MERGE INTO MART.DIM_DATE target
USING (
    WITH date_range AS (
        SELECT DATEADD(day, SEQ4(), '1995-01-01'::DATE) AS full_date
        FROM TABLE(GENERATOR(ROWCOUNT => 15000))
        WHERE DATEADD(day, SEQ4(), '1995-01-01'::DATE) <= '2035-12-31'::DATE
    )
    SELECT
        TO_NUMBER(TO_VARCHAR(full_date, 'YYYYMMDD')) AS date_key,
        full_date,
        YEAR(full_date) AS year,
        MONTH(full_date) AS month,
        MONTHNAME(full_date) AS month_name,
        DAY(full_date) AS day,
        DAYOFYEAR(full_date) AS day_of_year,
        QUARTER(full_date) AS quarter,
        DAYOFWEEK(full_date) AS day_of_week,
        DAYNAME(full_date) AS day_name,
        IFF(DAYOFWEEK(full_date) IN (0, 6), TRUE, FALSE) AS is_weekend,
        CASE
            WHEN MONTH(full_date) >= 7 THEN CONCAT(YEAR(full_date), '-', YEAR(full_date) + 1)
            ELSE CONCAT(YEAR(full_date) - 1, '-', YEAR(full_date))
        END AS agricultural_year
    FROM date_range
) source
ON target.date_key = source.date_key
WHEN NOT MATCHED THEN INSERT (
    date_key, full_date, year, month, month_name, day, day_of_year,
    quarter, day_of_week, day_name, is_weekend, agricultural_year
) VALUES (
    source.date_key, source.full_date, source.year, source.month, source.month_name,
    source.day, source.day_of_year, source.quarter, source.day_of_week, source.day_name,
    source.is_weekend, source.agricultural_year
);

-- ============================================================================
-- 2. POPULATE CONFORMED DIM_LOCATION
-- ============================================================================
-- Insert distinct APMC markets from Mandi Prices
MERGE INTO MART.DIM_LOCATION target
USING (
    SELECT DISTINCT
        state_name,
        district_name,
        market_name,
        'MARKET' AS location_type,
        NULL::NUMBER(7,4) AS latitude,
        NULL::NUMBER(7,4) AS longitude,
        CASE
            WHEN state_name IN ('Jammu and Kashmir', 'Himachal Pradesh', 'Punjab', 'Haryana', 'Uttarakhand', 'Uttar Pradesh', 'Chandigarh', 'Delhi') THEN 'Northern'
            WHEN state_name IN ('Tamil Nadu', 'Kerala', 'Karnataka', 'Andhra Pradesh', 'Telangana', 'Puducherry') THEN 'Southern'
            WHEN state_name IN ('Maharashtra', 'Gujarat', 'Goa', 'Rajasthan') THEN 'Western'
            WHEN state_name IN ('West Bengal', 'Bihar', 'Odisha', 'Jharkhand') THEN 'Eastern'
            WHEN state_name IN ('Madhya Pradesh', 'Chhattisgarh') THEN 'Central'
            ELSE 'North-Eastern'
        END AS region
    FROM CLEAN.CLEAN_MANDI_PRICES
) source
ON target.state_name = source.state_name 
   AND target.district_name = source.district_name 
   AND target.market_name = source.market_name
WHEN NOT MATCHED THEN INSERT (
    state_name, district_name, market_name, location_type, latitude, longitude, region
) VALUES (
    source.state_name, source.district_name, source.market_name, source.location_type,
    source.latitude, source.longitude, source.region
);

-- Insert distinct District-level entries from Crop Production (where market is NULL)
MERGE INTO MART.DIM_LOCATION target
USING (
    SELECT DISTINCT
        state_name,
        district_name,
        CAST(NULL AS VARCHAR(150)) AS market_name,
        'DISTRICT' AS location_type,
        NULL::NUMBER(7,4) AS latitude,
        NULL::NUMBER(7,4) AS longitude,
        CASE
            WHEN state_name IN ('Jammu and Kashmir', 'Himachal Pradesh', 'Punjab', 'Haryana', 'Uttarakhand', 'Uttar Pradesh', 'Chandigarh', 'Delhi') THEN 'Northern'
            WHEN state_name IN ('Tamil Nadu', 'Kerala', 'Karnataka', 'Andhra Pradesh', 'Telangana', 'Puducherry') THEN 'Southern'
            WHEN state_name IN ('Maharashtra', 'Gujarat', 'Goa', 'Rajasthan') THEN 'Western'
            WHEN state_name IN ('West Bengal', 'Bihar', 'Odisha', 'Jharkhand') THEN 'Eastern'
            WHEN state_name IN ('Madhya Pradesh', 'Chhattisgarh') THEN 'Central'
            ELSE 'North-Eastern'
        END AS region
    FROM CLEAN.CLEAN_CROP_PRODUCTION
) source
ON target.state_name = source.state_name 
   AND target.district_name = source.district_name 
   AND target.market_name IS NULL
WHEN NOT MATCHED THEN INSERT (
    state_name, district_name, market_name, location_type, latitude, longitude, region
) VALUES (
    source.state_name, source.district_name, source.market_name, source.location_type,
    source.latitude, source.longitude, source.region
);

-- ============================================================================
-- 3. POPULATE CONFORMED DIM_COMMODITY
-- ============================================================================
MERGE INTO MART.DIM_COMMODITY target
USING (
    SELECT DISTINCT
        commodity_name,
        variety_name,
        grade,
        'Mandi Commodity' AS commodity_group,
        'Quintal' AS standard_unit
    FROM CLEAN.CLEAN_MANDI_PRICES
) source
ON target.commodity_name = source.commodity_name
   AND target.variety_name = source.variety_name
   AND target.grade = source.grade
WHEN NOT MATCHED THEN INSERT (
    commodity_name, commodity_group, variety_name, grade, standard_unit
) VALUES (
    source.commodity_name, source.commodity_group, source.variety_name, source.grade, source.standard_unit
);

-- Insert Crop Production crops into DIM_COMMODITY
MERGE INTO MART.DIM_COMMODITY target
USING (
    SELECT DISTINCT
        crop_name AS commodity_name,
        'Field Crop' AS commodity_group,
        'All' AS variety_name,
        'Standard' AS grade,
        production_unit AS standard_unit
    FROM CLEAN.CLEAN_CROP_PRODUCTION
) source
ON target.commodity_name = source.commodity_name
   AND target.variety_name = source.variety_name
   AND target.grade = source.grade
WHEN NOT MATCHED THEN INSERT (
    commodity_name, commodity_group, variety_name, grade, standard_unit
) VALUES (
    source.commodity_name, source.commodity_group, source.variety_name, source.grade, source.standard_unit
);

-- ============================================================================
-- 4. TRANSFORM & MERGE FACT_MANDI_PRICES
-- ============================================================================
MERGE INTO MART.FACT_MANDI_PRICES target
USING (
    SELECT
        MD5(CONCAT_WS('#',
            LOWER(TRIM(state_name)),
            LOWER(TRIM(district_name)),
            LOWER(TRIM(market_name)),
            LOWER(TRIM(commodity_name)),
            LOWER(TRIM(variety_name)),
            LOWER(TRIM(grade)),
            TO_VARCHAR(arrival_date, 'YYYY-MM-DD')
        )) AS mandi_fact_key,
        TO_NUMBER(TO_VARCHAR(arrival_date, 'YYYYMMDD')) AS date_key,
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
        ROUND(max_price_inr_quintal - min_price_inr_quintal, 2) AS price_spread_inr_quintal,
        NULL::NUMBER(12,2) AS arrival_quantity_quintals,
        currency,
        price_unit,
        is_price_relation_valid,
        source_channel
    FROM CLEAN.CLEAN_MANDI_PRICES
) source
ON target.mandi_fact_key = source.mandi_fact_key
WHEN MATCHED THEN UPDATE SET
    min_price_inr_quintal = source.min_price_inr_quintal,
    max_price_inr_quintal = source.max_price_inr_quintal,
    modal_price_inr_quintal = source.modal_price_inr_quintal,
    price_spread_inr_quintal = source.price_spread_inr_quintal,
    loaded_at_utc = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    mandi_fact_key, date_key, arrival_date, state_name, district_name, market_name,
    commodity_name, variety_name, grade, min_price_inr_quintal, max_price_inr_quintal,
    modal_price_inr_quintal, price_spread_inr_quintal, arrival_quantity_quintals,
    currency, price_unit, is_price_relation_valid, source_channel, loaded_at_utc
) VALUES (
    source.mandi_fact_key, source.date_key, source.arrival_date, source.state_name,
    source.district_name, source.market_name, source.commodity_name, source.variety_name,
    source.grade, source.min_price_inr_quintal, source.max_price_inr_quintal,
    source.modal_price_inr_quintal, source.price_spread_inr_quintal, source.arrival_quantity_quintals,
    source.currency, source.price_unit, source.is_price_relation_valid, source.source_channel, CURRENT_TIMESTAMP()
);

-- ============================================================================
-- 5. TRANSFORM & MERGE FACT_CROP_PRODUCTION
-- ============================================================================
MERGE INTO MART.FACT_CROP_PRODUCTION target
USING (
    SELECT
        MD5(CONCAT_WS('#',
            LOWER(TRIM(state_name)),
            LOWER(TRIM(district_name)),
            TO_VARCHAR(crop_year),
            LOWER(TRIM(season)),
            LOWER(TRIM(crop_name))
        )) AS crop_fact_key,
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
        is_incomplete_year
    FROM CLEAN.CLEAN_CROP_PRODUCTION
) source
ON target.crop_fact_key = source.crop_fact_key
WHEN MATCHED THEN UPDATE SET
    area_hectares = source.area_hectares,
    production = source.production,
    production_tonnes = source.production_tonnes,
    production_coconut_nuts = source.production_coconut_nuts,
    yield_tonnes_per_hectare = source.yield_tonnes_per_hectare,
    yield_coconut_nuts_per_hectare = source.yield_coconut_nuts_per_hectare,
    is_production_missing = source.is_production_missing,
    loaded_at_utc = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    crop_fact_key, crop_year, state_name, district_name, crop_name, season,
    area_hectares, production, production_unit, production_tonnes, production_coconut_nuts,
    yield_tonnes_per_hectare, yield_coconut_nuts_per_hectare, is_production_missing,
    is_incomplete_year, loaded_at_utc
) VALUES (
    source.crop_fact_key, source.crop_year, source.state_name, source.district_name,
    source.crop_name, source.season, source.area_hectares, source.production,
    source.production_unit, source.production_tonnes, source.production_coconut_nuts,
    source.yield_tonnes_per_hectare, source.yield_coconut_nuts_per_hectare,
    source.is_production_missing, source.is_incomplete_year, CURRENT_TIMESTAMP()
);

-- ============================================================================
-- 6. TRANSFORM & MERGE FACT_WEATHER
-- ============================================================================
MERGE INTO MART.FACT_WEATHER target
USING (
    SELECT
        MD5(CONCAT_WS('#',
            TO_VARCHAR(latitude, '00.0000'),
            TO_VARCHAR(longitude, '000.0000'),
            TO_VARCHAR(date, 'YYYY-MM-DD')
        )) AS weather_fact_key,
        TO_NUMBER(TO_VARCHAR(date, 'YYYYMMDD')) AS date_key,
        date AS weather_date,
        latitude,
        longitude,
        precipitation_mm_per_day AS rainfall_mm_per_day,
        temperature_celsius_2m AS temperature_celsius,
        is_negative_temperature,
        is_extreme_precipitation AS is_extreme_rainfall,
        is_subzero_high_altitude,
        precipitation_unit AS rainfall_unit,
        temperature_unit
    FROM CLEAN.CLEAN_WEATHER
) source
ON target.weather_fact_key = source.weather_fact_key
WHEN MATCHED THEN UPDATE SET
    rainfall_mm_per_day = source.rainfall_mm_per_day,
    temperature_celsius = source.temperature_celsius,
    is_negative_temperature = source.is_negative_temperature,
    is_extreme_rainfall = source.is_extreme_rainfall,
    loaded_at_utc = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    weather_fact_key, date_key, weather_date, latitude, longitude,
    rainfall_mm_per_day, temperature_celsius, is_negative_temperature,
    is_extreme_rainfall, is_subzero_high_altitude, rainfall_unit, temperature_unit, loaded_at_utc
) VALUES (
    source.weather_fact_key, source.date_key, source.weather_date, source.latitude, source.longitude,
    source.rainfall_mm_per_day, source.temperature_celsius, source.is_negative_temperature,
    source.is_extreme_rainfall, source.is_subzero_high_altitude, source.rainfall_unit, source.temperature_unit, CURRENT_TIMESTAMP()
);
