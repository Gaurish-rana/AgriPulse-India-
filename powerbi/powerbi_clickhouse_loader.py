"""
AgriPulse India — Power BI ClickHouse Data Loader
=================================================
This script connects to the local ClickHouse Data Warehouse (agripulse_mart)
and loads conformed analytical datasets into pandas DataFrames.
If ClickHouse is not running, it automatically falls back to the staged CSV data.

Usage in Power BI Desktop:
  1. Open Power BI Desktop.
  2. Click 'Get Data' -> 'Other' -> 'Python script' -> 'Connect'.
  3. Paste the contents of this script.
  4. Select tables:
       - dim_date
       - dim_location
       - dim_commodity
       - fact_mandi_prices
       - fact_crop_production
       - fact_weather_trends
       - fact_weather_monthly
  5. Click 'Load'.
"""

import os
import sys
import pandas as pd

# Flag to track data source
loaded_from_live = False

try:
    import clickhouse_connect
    
    print("Attempting connection to ClickHouse at localhost:8123...")
    client = clickhouse_connect.get_client(
        host="localhost",
        port=8123,
        username="default",
        password="",
        database="agripulse_mart",
        connect_timeout=5,
    )
    
    # 1. Calendar Date Dimension
    dim_date = client.query_df("""
        SELECT 
            date_key,
            full_date,
            year,
            month,
            month_name,
            day,
            day_of_year,
            quarter,
            concat('Q', toString(quarter)) AS quarter_name,
            day_of_week,
            day_name,
            is_weekend,
            agricultural_year
        FROM agripulse_mart.dim_date
        ORDER BY date_key ASC
    """)
    
    # 2. Geographic Hierarchy Dimension
    dim_location = client.query_df("""
        SELECT 
            toString(location_key) AS location_key,
            state_name,
            district_name,
            ifNull(market_name, 'All Markets') AS market_name,
            location_type,
            ifNull(latitude, 0.0) AS latitude,
            ifNull(longitude, 0.0) AS longitude,
            region
        FROM agripulse_mart.dim_location FINAL
        ORDER BY state_name, district_name, market_name
    """)
    
    # 3. Commodity Master Dimension
    dim_commodity = client.query_df("""
        SELECT 
            toString(commodity_key) AS commodity_key,
            commodity_name,
            commodity_group,
            variety_name,
            grade,
            standard_unit
        FROM agripulse_mart.dim_commodity FINAL
        ORDER BY commodity_group, commodity_name
    """)
    
    # 4. Mandi Wholesale Prices Fact
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
    
    # 5. Crop Production Fact
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
    
    # 6. NASA POWER Weather Monthly Summary Mart View
    fact_weather_monthly = client.query_df("""
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
        FROM agripulse_mart.view_weather_monthly_summary
        ORDER BY year ASC, month ASC
    """)
    
    # 7. National Climate & Extreme Weather Trends Mart View
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
    loaded_from_live = True
    print("SUCCESS: All 7 analytical datasets loaded directly from live ClickHouse warehouse!")

except Exception as err:
    print(f"ClickHouse live query fallback: {err}")
    # Fallback to local staged CSVs in powerbi/data/
    base_dir = r"C:\Users\Gaurish16\OneDrive\Desktop\AgriPulse-India\powerbi\data"
    
    dim_date = pd.read_csv(os.path.join(base_dir, "dim_date.csv"))
    dim_location = pd.read_csv(os.path.join(base_dir, "dim_location.csv"))
    dim_commodity = pd.read_csv(os.path.join(base_dir, "dim_commodity.csv"))
    fact_mandi_prices = pd.read_csv(os.path.join(base_dir, "fact_mandi_prices.csv"))
    fact_crop_production = pd.read_csv(os.path.join(base_dir, "fact_crop_production.csv"))
    fact_weather_trends = pd.read_csv(os.path.join(base_dir, "fact_weather_trends.csv"))
    
    # Monthly weather fallback
    monthly_path = os.path.join(base_dir, "view_weather_monthly_summary.csv")
    if os.path.exists(monthly_path):
        fact_weather_monthly = pd.read_csv(monthly_path)
    else:
        fact_weather_monthly = fact_weather_trends.copy()
        
    print(f"SUCCESS: Loaded datasets from verified ClickHouse local export cache ({base_dir})!")
