-- ==============================================================================
-- AgriPulse India — Power BI Analytical SQL: NASA POWER Weather Analytics
-- Database: agripulse_mart
-- ==============================================================================

-- 1. Optimized Monthly Grid Tile Summary for Power BI Dashboards
-- Aggregates 7.12M daily records into ~195K monthly tile snapshots
SELECT
    year_month,
    year,
    month,
    latitude,
    longitude,
    observation_days,
    avg_temperature_celsius,
    min_temperature_celsius,
    max_temperature_celsius,
    total_rainfall_mm,
    avg_daily_rainfall_mm,
    subzero_days_count,
    extreme_rainfall_days_count
FROM agripulse_mart.view_weather_monthly_summary;

-- 2. National Monthly Climate & Extreme Weather Trends (2020–2026)
-- Sub-second aggregation across all 7.12M records
SELECT
    year,
    month,
    concat(toString(year), '-', lpad(toString(month), 2, '0')) AS period_label,
    total_observations,
    national_avg_temperature,
    national_min_temperature,
    national_max_temperature,
    national_avg_daily_rainfall,
    national_total_rainfall,
    subzero_observations,
    extreme_rainfall_events
FROM agripulse_mart.view_weather_regional_trends
ORDER BY year ASC, month ASC;

-- 3. Regional Temperature Extremes & Sub-Zero Preservation Benchmark
-- Validates authentic Himalayan frost preservation down to -34.62°C
SELECT
    latitude,
    longitude,
    count() AS total_readings,
    countIf(temperature_celsius < 0) AS subzero_reading_count,
    round(min(temperature_celsius), 2) AS record_cold_celsius,
    round(avg(temperature_celsius), 2) AS mean_temperature_celsius,
    round(max(temperature_celsius), 2) AS record_heat_celsius,
    round(sum(rainfall_mm_per_day), 2) AS cumulative_rainfall_mm
FROM agripulse_mart.fact_weather FINAL
GROUP BY latitude, longitude
HAVING subzero_reading_count > 0
ORDER BY record_cold_celsius ASC
LIMIT 30;

-- 4. Monsoon & Extreme Precipitation Analysis (>100mm/day events)
SELECT
    toYear(weather_date) AS observation_year,
    toMonth(weather_date) AS observation_month,
    count() AS extreme_rainfall_incidents,
    round(max(rainfall_mm_per_day), 2) AS peak_24hr_rainfall_mm,
    round(avg(rainfall_mm_per_day), 2) AS avg_extreme_rainfall_mm
FROM agripulse_mart.fact_weather FINAL
WHERE is_extreme_rainfall = 1
GROUP BY observation_year, observation_month
ORDER BY extreme_rainfall_incidents DESC
LIMIT 24;
