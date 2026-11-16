-- ==============================================================================
-- AgriPulse India — Power BI Analytical SQL: Mandi Price Analytics
-- Database: agripulse_mart
-- ==============================================================================

-- 1. Full Star Schema Model Query for Mandi Prices
-- Connects fact_mandi_prices with conformed dimensions
SELECT
    f.mandi_fact_key,
    f.date_key,
    f.arrival_date,
    d.year,
    d.month,
    d.month_name,
    d.quarter_name,
    d.day_name,
    f.state_name,
    f.district_name,
    f.market_name,
    loc.region,
    loc.latitude AS market_latitude,
    loc.longitude AS market_longitude,
    f.commodity_name,
    c.commodity_group,
    f.variety_name,
    f.grade,
    f.min_price_inr_quintal,
    f.max_price_inr_quintal,
    f.modal_price_inr_quintal,
    f.price_spread_inr_quintal,
    f.arrival_quantity_quintals,
    f.price_unit,
    f.is_price_relation_valid
FROM agripulse_mart.fact_mandi_prices FINAL AS f
LEFT JOIN agripulse_mart.dim_date AS d 
    ON f.date_key = d.date_key
LEFT JOIN (
    SELECT DISTINCT state_name, district_name, market_name, region, latitude, longitude 
    FROM agripulse_mart.dim_location FINAL
) AS loc 
    ON f.state_name = loc.state_name 
   AND f.district_name = loc.district_name 
   AND f.market_name = loc.market_name
LEFT JOIN (
    SELECT DISTINCT commodity_name, variety_name, grade, commodity_group 
    FROM agripulse_mart.dim_commodity FINAL
) AS c 
    ON f.commodity_name = c.commodity_name 
   AND f.variety_name = c.variety_name 
   AND f.grade = c.grade;

-- 2. State-Wise Price & Volume Summary
SELECT
    state_name,
    count() AS total_records,
    uniqExact(market_name) AS active_markets,
    uniqExact(commodity_name) AS distinct_commodities,
    round(avg(modal_price_inr_quintal), 2) AS avg_modal_price,
    round(min(min_price_inr_quintal), 2) AS lowest_min_price,
    round(max(max_price_inr_quintal), 2) AS highest_max_price,
    round(avg(price_spread_inr_quintal), 2) AS avg_price_spread
FROM agripulse_mart.fact_mandi_prices FINAL
GROUP BY state_name
ORDER BY avg_modal_price DESC;

-- 3. Top 20 Commodities by Modal Price Benchmark
SELECT
    commodity_name,
    count() AS record_count,
    uniqExact(state_name) AS states_reporting,
    round(avg(modal_price_inr_quintal), 2) AS avg_modal_price,
    round(min(min_price_inr_quintal), 2) AS min_price,
    round(max(max_price_inr_quintal), 2) AS max_price,
    round(avg(price_spread_inr_quintal), 2) AS avg_spread
FROM agripulse_mart.fact_mandi_prices FINAL
GROUP BY commodity_name
ORDER BY avg_modal_price DESC
LIMIT 20;

-- 4. Market Arbitrage & Spread Analysis
SELECT
    market_name,
    state_name,
    district_name,
    count() AS commodities_traded,
    round(avg(modal_price_inr_quintal), 2) AS market_avg_price,
    round(avg(price_spread_inr_quintal), 2) AS market_avg_spread
FROM agripulse_mart.fact_mandi_prices FINAL
GROUP BY market_name, state_name, district_name
ORDER BY market_avg_spread DESC
LIMIT 30;
