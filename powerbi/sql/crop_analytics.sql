-- ==============================================================================
-- AgriPulse India — Power BI Analytical SQL: Crop Production Analytics
-- Database: agripulse_mart
-- ==============================================================================

-- 1. Full Star Schema Model Query for Crop Production
-- Preserves unit segregation (Tonnes vs Coconut Nuts) and flags missing data
SELECT
    f.crop_fact_key,
    f.crop_year,
    f.state_name,
    f.district_name,
    loc.region,
    f.crop_name,
    f.season,
    f.area_hectares,
    f.production,
    f.production_tonnes,
    f.production_coconut_nuts,
    f.yield_tonnes_per_hectare,
    f.yield_coconut_nuts_per_hectare,
    f.is_production_missing,
    f.is_incomplete_year
FROM agripulse_mart.fact_crop_production FINAL AS f
LEFT JOIN (
    SELECT DISTINCT state_name, region 
    FROM agripulse_mart.dim_location FINAL
) AS loc 
    ON f.state_name = loc.state_name;

-- 2. Annual Historical Crop Production Trends (1997–2015)
-- Explicitly segregates Metric Tonnes from Coconut Nuts
SELECT
    crop_year,
    count() AS total_crop_records,
    uniqExact(crop_name) AS distinct_crops,
    uniqExact(state_name) AS reporting_states,
    round(sum(area_hectares), 2) AS total_cultivated_area_ha,
    round(sum(production_tonnes), 2) AS field_crops_production_tonnes,
    round(sum(production_coconut_nuts), 2) AS coconut_production_nuts,
    round(avg(yield_tonnes_per_hectare), 3) AS avg_field_crop_yield_tonnes_ha,
    countIf(is_production_missing = 1) AS missing_production_records,
    is_incomplete_year
FROM agripulse_mart.fact_crop_production FINAL
GROUP BY crop_year, is_incomplete_year
ORDER BY crop_year ASC;

-- 3. Top 15 Field Crops by Cumulative Production (Metric Tonnes)
SELECT
    crop_name,
    count() AS record_count,
    uniqExact(state_name) AS cultivating_states,
    round(sum(area_hectares), 2) AS total_area_ha,
    round(sum(production_tonnes), 2) AS total_production_tonnes,
    round(avg(yield_tonnes_per_hectare), 2) AS avg_yield_tonnes_ha
FROM agripulse_mart.fact_crop_production FINAL
WHERE production_tonnes IS NOT NULL AND production_tonnes > 0
GROUP BY crop_name
ORDER BY total_production_tonnes DESC
LIMIT 15;

-- 4. State-Wise Agricultural Production & Yield Comparison
SELECT
    state_name,
    count() AS crop_records,
    uniqExact(crop_name) AS crop_diversity,
    round(sum(area_hectares), 2) AS total_area_ha,
    round(sum(production_tonnes), 2) AS total_production_tonnes,
    round(avg(yield_tonnes_per_hectare), 2) AS avg_yield_tonnes_ha,
    round(sum(production_coconut_nuts), 2) AS total_coconut_nuts
FROM agripulse_mart.fact_crop_production FINAL
GROUP BY state_name
ORDER BY total_production_tonnes DESC;

-- 5. Season-Wise Crop Production Distribution
SELECT
    season,
    count() AS record_count,
    round(sum(area_hectares), 2) AS area_ha,
    round(sum(production_tonnes), 2) AS production_tonnes,
    round(avg(yield_tonnes_per_hectare), 2) AS avg_yield_tonnes_ha
FROM agripulse_mart.fact_crop_production FINAL
WHERE production_tonnes IS NOT NULL
GROUP BY season
ORDER BY production_tonnes DESC;
