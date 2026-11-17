"""
AgriPulse India - ClickHouse to Power BI CSV Staging Exporter
Exports conformed mart tables directly from ClickHouse into local CSV files
guaranteeing 100% UNIQUE Primary Keys on all dimension tables for Power BI.
"""
import os
import sys
import clickhouse_connect

def export_all():
    output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    os.makedirs(output_dir, exist_ok=True)
    
    print("Connecting to ClickHouse at localhost:8123...")
    client = clickhouse_connect.get_client(host='localhost', port=8123, username='default', password='')
    
    tables_to_export = [
        ("dim_date.csv", """
            SELECT 
                date_key, full_date, year, month, month_name, 
                day, day_of_year, quarter, day_of_week, day_name, 
                is_weekend, agricultural_year 
            FROM agripulse_mart.dim_date 
            ORDER BY date_key ASC
        """),
        ("dim_location.csv", """
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
        """),
        ("dim_commodity.csv", """
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
        """),
        ("fact_mandi_prices.csv", """
            SELECT 
                mandi_fact_key, date_key, arrival_date, state_name, district_name, 
                market_name, commodity_name, variety_name, grade, 
                modal_price_inr_quintal, min_price_inr_quintal, max_price_inr_quintal, 
                price_spread_inr_quintal, ifNull(arrival_quantity_quintals, 0.0) AS arrival_quantity_quintals, 
                currency, price_unit, is_price_relation_valid
            FROM agripulse_mart.fact_mandi_prices FINAL
        """),
        ("fact_crop_production.csv", """
            SELECT 
                crop_fact_key, crop_year, state_name, district_name, crop_name, season, 
                ifNull(area_hectares, 0.0) AS area_hectares, ifNull(production, 0.0) AS production, 
                production_unit, ifNull(production_tonnes, 0.0) AS production_tonnes, 
                ifNull(production_coconut_nuts, 0.0) AS production_coconut_nuts, 
                ifNull(yield_tonnes_per_hectare, 0.0) AS yield_tonnes_per_hectare, 
                ifNull(yield_coconut_nuts_per_hectare, 0.0) AS yield_coconut_nuts_per_hectare, 
                is_production_missing, is_incomplete_year 
            FROM agripulse_mart.fact_crop_production FINAL
        """),
        ("fact_weather_trends.csv", """
            SELECT 
                year, month, 
                concat(toString(year), '-', lpad(toString(month), 2, '0')) AS period_label, 
                total_observations, national_avg_temperature, national_min_temperature, 
                national_max_temperature, national_avg_daily_rainfall, national_total_rainfall, 
                subzero_observations, extreme_rainfall_events 
            FROM agripulse_mart.view_weather_regional_trends 
            ORDER BY year ASC, month ASC
        """)
    ]
    
    for filename, query in tables_to_export:
        dest_path = os.path.join(output_dir, filename)
        print(f"Exporting {filename}...")
        df = client.query_df(query)
        df.to_csv(dest_path, index=False)
        print(f"  -> Saved {dest_path} ({len(df):,} rows)")

    client.close()
    print("\nAll ClickHouse tables exported successfully into powerbi/data/!")

if __name__ == "__main__":
    export_all()
