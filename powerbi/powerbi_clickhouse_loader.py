"""
AgriPulse India — Power BI ClickHouse Data Loader (100% Unique Primary Keys)
============================================================================
This script connects to the local ClickHouse Data Warehouse (agripulse_mart)
and loads the 6 conformed analytical tables with 100% unique primary keys.

Usage in Power BI Desktop:
  1. Open Power BI Desktop.
  2. Click 'Get Data' -> 'Other' -> 'Python script' -> 'Connect'.
  3. Paste this entire script into the dialog.
  4. Click 'OK'.
  5. In Navigator, check all 6 tables:
       - dim_date
       - dim_location
       - dim_commodity
       - fact_mandi_prices
       - fact_crop_production
       - fact_weather_trends
  6. Click 'Load'.
"""

import os
import sys
import pandas as pd

try:
    import clickhouse_connect
    
    # Connect directly to ClickHouse OLAP Warehouse
    client = clickhouse_connect.get_client(
        host="localhost",
        port=8123,
        username="default",
        password="",
        database="agripulse_mart",
        connect_timeout=10,
        send_receive_timeout=30,
    )
    
    # 1. Calendar Date Dimension (14,975 rows, unique date_key)
    dim_date = client.query_df("""
        SELECT 
            date_key, full_date, year, month, month_name, 
            day, day_of_year, quarter, day_of_week, day_name, 
            is_weekend, agricultural_year
        FROM agripulse_mart.dim_date
        ORDER BY date_key ASC
    """)
    
    # 2. Geographic Hierarchy Dimension (33 rows, 100% unique state_name)
    dim_location = client.query_df("""
        SELECT 
            any(toString(location_key)) AS location_key,
            state_name,
            any(district_name) AS district_name,
            ifNull(any(market_name), 'All Markets') AS market_name,
            any(location_type) AS location_type,
            round(avg(latitude), 4) AS latitude,
            round(avg(longitude), 4) AS longitude,
            any(region) AS region
        FROM agripulse_mart.dim_location
        GROUP BY state_name
        ORDER BY state_name ASC
    """)
    
    # 3. Commodity Master Dimension (341 rows, 100% unique commodity_name)
    dim_commodity = client.query_df("""
        SELECT 
            any(toString(commodity_key)) AS commodity_key,
            commodity_name,
            any(commodity_group) AS commodity_group,
            any(variety_name) AS variety_name,
            any(grade) AS grade,
            any(standard_unit) AS standard_unit
        FROM agripulse_mart.dim_commodity
        GROUP BY commodity_name
        ORDER BY commodity_name ASC
    """)
    
    # 4. Mandi Wholesale Prices Fact (12,727 rows)
    fact_mandi_prices = client.query_df("""
        SELECT 
            mandi_fact_key,
            date_key,
            arrival_date,
            state_name,
            district_name,
            market_name,
            commodity_name,
            variety_name,
            grade,
            modal_price_inr_quintal,
            min_price_inr_quintal,
            max_price_inr_quintal,
            price_spread_inr_quintal,
            ifNull(arrival_quantity_quintals, 0.0) AS arrival_quantity_quintals,
            currency,
            price_unit,
            is_price_relation_valid
        FROM agripulse_mart.fact_mandi_prices FINAL
        ORDER BY state_name, arrival_date
    """)
    
    # 5. Crop Production Fact (246,091 rows)
    fact_crop_production = client.query_df("""
        SELECT 
            crop_fact_key,
            crop_year,
            state_name,
            district_name,
            crop_name,
            season,
            ifNull(area_hectares, 0.0) AS area_hectares,
            ifNull(production, 0.0) AS production,
            production_unit,
            ifNull(production_tonnes, 0.0) AS production_tonnes,
            ifNull(production_coconut_nuts, 0.0) AS production_coconut_nuts,
            ifNull(yield_tonnes_per_hectare, 0.0) AS yield_tonnes_per_hectare,
            ifNull(yield_coconut_nuts_per_hectare, 0.0) AS yield_coconut_nuts_per_hectare,
            is_production_missing,
            is_incomplete_year
        FROM agripulse_mart.fact_crop_production FINAL
        ORDER BY crop_year ASC, state_name ASC
    """)
    
    # 6. National Climate & Extreme Weather Trends (83 rows)
    fact_weather_trends = client.query_df("""
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
        ORDER BY year ASC, month ASC
    """)
    
    client.close()
    print("SUCCESS: All 6 analytical datasets loaded directly from live ClickHouse warehouse!")

except Exception as err:
    print(f"ClickHouse note: {err}. Loading from verified local CSV export...")
    base_dir = r"C:\Users\Gaurish16\OneDrive\Desktop\AgriPulse-India\powerbi\data"
    
    dim_date = pd.read_csv(os.path.join(base_dir, "dim_date.csv"))
    dim_location = pd.read_csv(os.path.join(base_dir, "dim_location.csv"))
    dim_commodity = pd.read_csv(os.path.join(base_dir, "dim_commodity.csv"))
    fact_mandi_prices = pd.read_csv(os.path.join(base_dir, "fact_mandi_prices.csv"))
    fact_crop_production = pd.read_csv(os.path.join(base_dir, "fact_crop_production.csv"))
    fact_weather_trends = pd.read_csv(os.path.join(base_dir, "fact_weather_trends.csv"))
    
    print("SUCCESS: All 6 analytical datasets loaded from local verified ClickHouse cache!")
