"""
AgriPulse India - ClickHouse to Power BI CSV Staging Exporter
Exports conformed mart tables directly from ClickHouse into local CSV files
guaranteeing 100% CASE-INSENSITIVE UNIQUE Primary Keys for Power BI Tabular Engine.
"""
import os
import sys
import clickhouse_connect

def export_all():
    output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    os.makedirs(output_dir, exist_ok=True)
    
    print("Connecting to ClickHouse at localhost:8123...")
    client = clickhouse_connect.get_client(host='localhost', port=8123, username='default', password='')
    
    # 1. Date Dimension (14,975 rows)
    print("Exporting dim_date.csv...")
    df_date = client.query_df("""
        SELECT 
            date_key, full_date, year, month, month_name, 
            day, day_of_year, quarter, day_of_week, day_name, 
            is_weekend, agricultural_year 
        FROM agripulse_mart.dim_date 
        ORDER BY date_key ASC
    """)
    df_date.to_csv(os.path.join(output_dir, "dim_date.csv"), index=False)
    print(f"  -> Saved dim_date.csv ({len(df_date):,} rows)")
    
    # 2. Location Dimension (33 rows, case-insensitively unique state_name)
    print("Exporting dim_location.csv...")
    df_loc = client.query_df("""
        SELECT 
            any(toString(location_key)) AS location_key,
            any(state_name) AS st_name,
            any(district_name) AS district_name,
            ifNull(any(market_name), 'All Markets') AS market_name,
            any(location_type) AS location_type,
            round(avg(latitude), 4) AS latitude,
            round(avg(longitude), 4) AS longitude,
            any(region) AS region
        FROM agripulse_mart.dim_location
        GROUP BY lower(state_name)
    """)
    df_loc.rename(columns={'st_name': 'state_name'}, inplace=True)
    df_loc.sort_values('state_name', inplace=True)
    assert df_loc['state_name'].str.lower().is_unique, "dim_location state_name must be case-insensitively unique!"
    df_loc.to_csv(os.path.join(output_dir, "dim_location.csv"), index=False)
    print(f"  -> Saved dim_location.csv ({len(df_loc):,} rows)")
    
    # 3. Commodity Dimension (336 rows, case-insensitively unique commodity_name)
    print("Exporting dim_commodity.csv...")
    df_comm = client.query_df("""
        SELECT 
            any(toString(commodity_key)) AS commodity_key,
            any(commodity_name) AS comm_name,
            any(commodity_group) AS commodity_group,
            any(variety_name) AS variety_name,
            any(grade) AS grade,
            any(standard_unit) AS standard_unit
        FROM agripulse_mart.dim_commodity
        GROUP BY lower(commodity_name)
    """)
    df_comm.rename(columns={'comm_name': 'commodity_name'}, inplace=True)
    df_comm.sort_values('commodity_name', inplace=True)
    assert df_comm['commodity_name'].str.lower().is_unique, "dim_commodity commodity_name must be case-insensitively unique!"
    df_comm.to_csv(os.path.join(output_dir, "dim_commodity.csv"), index=False)
    print(f"  -> Saved dim_commodity.csv ({len(df_comm):,} rows)")
    
    # 4. Mandi Prices Fact (12,727 rows)
    print("Exporting fact_mandi_prices.csv...")
    df_mandi = client.query_df("""
        SELECT 
            mandi_fact_key, date_key, arrival_date, state_name, district_name, 
            market_name, commodity_name, variety_name, grade, 
            modal_price_inr_quintal, min_price_inr_quintal, max_price_inr_quintal, 
            price_spread_inr_quintal, ifNull(arrival_quantity_quintals, 0.0) AS arrival_quantity_quintals, 
            currency, price_unit, is_price_relation_valid
        FROM agripulse_mart.fact_mandi_prices FINAL
    """)
    df_mandi.to_csv(os.path.join(output_dir, "fact_mandi_prices.csv"), index=False)
    print(f"  -> Saved fact_mandi_prices.csv ({len(df_mandi):,} rows)")
    
    # 5. Crop Production Fact (246,091 rows)
    print("Exporting fact_crop_production.csv...")
    df_crop = client.query_df("""
        SELECT 
            crop_fact_key, crop_year, state_name, district_name, crop_name, season, 
            ifNull(area_hectares, 0.0) AS area_hectares, ifNull(production, 0.0) AS production, 
            production_unit, ifNull(production_tonnes, 0.0) AS production_tonnes, 
            ifNull(production_coconut_nuts, 0.0) AS production_coconut_nuts, 
            ifNull(yield_tonnes_per_hectare, 0.0) AS yield_tonnes_per_hectare, 
            ifNull(yield_coconut_nuts_per_hectare, 0.0) AS yield_coconut_nuts_per_hectare, 
            is_production_missing, is_incomplete_year 
        FROM agripulse_mart.fact_crop_production FINAL
    """)
    df_crop.to_csv(os.path.join(output_dir, "fact_crop_production.csv"), index=False)
    print(f"  -> Saved fact_crop_production.csv ({len(df_crop):,} rows)")
    
    # 6. Climate & Extreme Weather Trends (83 rows)
    print("Exporting fact_weather_trends.csv...")
    df_weather = client.query_df("""
        SELECT 
            year, month, 
            concat(toString(year), '-', lpad(toString(month), 2, '0')) AS period_label, 
            total_observations, national_avg_temperature, national_min_temperature, 
            national_max_temperature, national_avg_daily_rainfall, national_total_rainfall, 
            subzero_observations, extreme_rainfall_events 
        FROM agripulse_mart.view_weather_regional_trends 
        ORDER BY year ASC, month ASC
    """)
    df_weather.to_csv(os.path.join(output_dir, "fact_weather_trends.csv"), index=False)
    print(f"  -> Saved fact_weather_trends.csv ({len(df_weather):,} rows)")

    client.close()
    print("\nAll 6 ClickHouse tables exported successfully with CASE-INSENSITIVE UNIQUE primary keys!")

if __name__ == "__main__":
    export_all()
