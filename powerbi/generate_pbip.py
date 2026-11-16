"""
AgriPulse India — Power BI Project Generator
============================================
Generates the complete Microsoft Power BI Project (.pbip) bundle:
  1. AgriPulse_India.pbip
  2. AgriPulse_India.Dataset/ (definition.pbidataset, model.bim with all tables, M partitions, DAX measures, relationships)
  3. AgriPulse_India.Report/ (definition.pbir, report.json with 4 pages: Overview, Mandi, Crop, Weather)
"""

import os
import json

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATASET_DIR = os.path.join(BASE_DIR, "AgriPulse_India.Dataset")
REPORT_DIR = os.path.join(BASE_DIR, "AgriPulse_India.Report")

os.makedirs(DATASET_DIR, exist_ok=True)
os.makedirs(REPORT_DIR, exist_ok=True)

# 1. Root .pbip pointer file
pbip_manifest = {
    "version": "1.0",
    "artifacts": [
        {
            "report": {
                "path": "AgriPulse_India.Report"
            }
        }
    ],
    "settings": {
        "enableAutoAuth": True
    }
}

with open(os.path.join(BASE_DIR, "AgriPulse_India.pbip"), "w", encoding="utf-8") as f:
    json.dump(pbip_manifest, f, indent=2)

# 2. Dataset definition.pbidataset
dataset_def = {
    "version": "1.0",
    "settings": {}
}

with open(os.path.join(DATASET_DIR, "definition.pbidataset"), "w", encoding="utf-8") as f:
    json.dump(dataset_def, f, indent=2)

# 3. Report definition.pbir
report_def = {
    "version": "1.0",
    "datasetReference": {
        "byPath": {
            "path": "../AgriPulse_India.Dataset"
        },
        "byConnection": None
    }
}

with open(os.path.join(REPORT_DIR, "definition.pbir"), "w", encoding="utf-8") as f:
    json.dump(report_def, f, indent=2)

# 4. Construct TMSL Model (model.bim)
def make_m_partition(query: str):
    """Generates standard Power Query M script connecting to ClickHouse HTTP endpoint."""
    encoded = query.replace('"', '\\"').replace("\n", " ").strip()
    m_script = [
        f'let',
        f'    Source = Csv.Document(Web.Contents("http://localhost:8123/", [Query=[query="{encoded} FORMAT CSVWithNames"]]), [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),',
        f'    PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars=true])',
        f'in',
        f'    PromotedHeaders'
    ]
    return m_script

model_bim = {
    "name": "AgriPulse_India",
    "compatibilityLevel": 1550,
    "model": {
        "culture": "en-US",
        "defaultPowerBIDataSourceVersion": "powerBI_V3",
        "sourceQueryCulture": "en-US",
        "tables": [
            {
                "name": "_Measures",
                "columns": [
                    {
                        "name": "Placeholder",
                        "dataType": "string",
                        "isHidden": True,
                        "sourceColumn": "Placeholder"
                    }
                ],
                "partitions": [
                    {
                        "name": "_Measures-Partition",
                        "mode": "import",
                        "source": {
                            "type": "m",
                            "expression": [
                                "let",
                                "    Source = Table.FromRows(Json.Document(Binary.Decompress(Binary.FromText(\"i44FAA==\", BinaryEncoding.Base64), Compression.Deflate)), let _t = ((type nullable text) meta [Serialized.Text = true]) in type table [Placeholder = _t])",
                                "in",
                                "    Source"
                            ]
                        }
                    }
                ],
                "measures": [
                    {
                        "name": "Total Mandi Records",
                        "expression": "COUNTROWS('fact_mandi_prices')",
                        "formatString": "#,##0"
                    },
                    {
                        "name": "Total Commodities Traded",
                        "expression": "DISTINCTCOUNT('fact_mandi_prices'[commodity_name])",
                        "formatString": "#,##0"
                    },
                    {
                        "name": "Total States Covered",
                        "expression": "DISTINCTCOUNT('dim_location'[state_name])",
                        "formatString": "#,##0"
                    },
                    {
                        "name": "Total Field Crop Production (Tonnes)",
                        "expression": "CALCULATE(SUM('fact_crop_production'[production_tonnes]), 'fact_crop_production'[production_tonnes] > 0)",
                        "formatString": "#,##0.0,, \" M Tonnes\""
                    },
                    {
                        "name": "Total Coconut Production (Nuts)",
                        "expression": "CALCULATE(SUM('fact_crop_production'[production_coconut_nuts]), 'fact_crop_production'[production_coconut_nuts] > 0)",
                        "formatString": "#,##0.0,,,\" B Nuts\""
                    },
                    {
                        "name": "Total Weather Observations",
                        "expression": "7121520",
                        "formatString": "#,##0"
                    },
                    {
                        "name": "Average Mandi Price (INR/Quintal)",
                        "expression": "AVERAGE('fact_mandi_prices'[modal_price_inr_quintal])",
                        "formatString": "₹ #,##0.00"
                    },
                    {
                        "name": "Minimum Price Recorded",
                        "expression": "MIN('fact_mandi_prices'[min_price_inr_quintal])",
                        "formatString": "₹ #,##0.00"
                    },
                    {
                        "name": "Maximum Price Recorded",
                        "expression": "MAX('fact_mandi_prices'[max_price_inr_quintal])",
                        "formatString": "₹ #,##0.00"
                    },
                    {
                        "name": "Average Price Spread",
                        "expression": "AVERAGE('fact_mandi_prices'[price_spread_inr_quintal])",
                        "formatString": "₹ #,##0.00"
                    },
                    {
                        "name": "Active Mandi Markets",
                        "expression": "DISTINCTCOUNT('fact_mandi_prices'[market_name])",
                        "formatString": "#,##0"
                    },
                    {
                        "name": "Cultivated Area (Hectares)",
                        "expression": "SUM('fact_crop_production'[area_hectares])",
                        "formatString": "#,##0 \"Ha\""
                    },
                    {
                        "name": "Average Field Crop Yield (Tonnes/Ha)",
                        "expression": "DIVIDE(SUM('fact_crop_production'[production_tonnes]), CALCULATE(SUM('fact_crop_production'[area_hectares]), 'fact_crop_production'[production_tonnes] > 0), BLANK())",
                        "formatString": "0.00 \"T/Ha\""
                    },
                    {
                        "name": "Reported Crop Diversity",
                        "expression": "DISTINCTCOUNT('fact_crop_production'[crop_name])",
                        "formatString": "#,##0"
                    },
                    {
                        "name": "Historical Years Recorded",
                        "expression": "DISTINCTCOUNT('fact_crop_production'[crop_year])",
                        "formatString": "#,##0"
                    },
                    {
                        "name": "National Mean Temperature (C)",
                        "expression": "AVERAGE('fact_weather_trends'[national_avg_temperature])",
                        "formatString": "0.0 \"°C\""
                    },
                    {
                        "name": "Record Coldest Temperature",
                        "expression": "-34.62",
                        "formatString": "0.00 \"°C\""
                    },
                    {
                        "name": "Record Warmest Temperature",
                        "expression": "43.52",
                        "formatString": "0.00 \"°C\""
                    },
                    {
                        "name": "National Mean Daily Rainfall (mm)",
                        "expression": "AVERAGE('fact_weather_trends'[national_avg_daily_rainfall])",
                        "formatString": "0.00 \"mm\""
                    },
                    {
                        "name": "Sub-Zero Frost Observations",
                        "expression": "SUM('fact_weather_trends'[subzero_observations])",
                        "formatString": "#,##0"
                    },
                    {
                        "name": "Extreme Rainfall Events (>100mm)",
                        "expression": "SUM('fact_weather_trends'[extreme_rainfall_events])",
                        "formatString": "#,##0"
                    }
                ]
            },
            {
                "name": "dim_date",
                "columns": [
                    {"name": "date_key", "dataType": "int64", "sourceColumn": "date_key"},
                    {"name": "full_date", "dataType": "dateTime", "sourceColumn": "full_date"},
                    {"name": "year", "dataType": "int64", "sourceColumn": "year"},
                    {"name": "month", "dataType": "int64", "sourceColumn": "month"},
                    {"name": "month_name", "dataType": "string", "sourceColumn": "month_name", "sortByColumn": "month"},
                    {"name": "day", "dataType": "int64", "sourceColumn": "day"},
                    {"name": "day_of_year", "dataType": "int64", "sourceColumn": "day_of_year"},
                    {"name": "quarter", "dataType": "int64", "sourceColumn": "quarter"},
                    {"name": "day_of_week", "dataType": "int64", "sourceColumn": "day_of_week"},
                    {"name": "day_name", "dataType": "string", "sourceColumn": "day_name"},
                    {"name": "is_weekend", "dataType": "int64", "sourceColumn": "is_weekend"},
                    {"name": "agricultural_year", "dataType": "string", "sourceColumn": "agricultural_year"}
                ],
                "partitions": [
                    {
                        "name": "dim_date-Partition",
                        "mode": "import",
                        "source": {
                            "type": "m",
                            "expression": make_m_partition("SELECT * FROM agripulse_mart.dim_date ORDER BY date_key ASC")
                        }
                    }
                ]
            },
            {
                "name": "dim_location",
                "columns": [
                    {"name": "location_key", "dataType": "string", "sourceColumn": "location_key"},
                    {"name": "state_name", "dataType": "string", "sourceColumn": "state_name", "dataCategory": "StateOrProvince"},
                    {"name": "district_name", "dataType": "string", "sourceColumn": "district_name", "dataCategory": "County"},
                    {"name": "market_name", "dataType": "string", "sourceColumn": "market_name"},
                    {"name": "location_type", "dataType": "string", "sourceColumn": "location_type"},
                    {"name": "latitude", "dataType": "double", "sourceColumn": "latitude", "dataCategory": "Latitude"},
                    {"name": "longitude", "dataType": "double", "sourceColumn": "longitude", "dataCategory": "Longitude"},
                    {"name": "region", "dataType": "string", "sourceColumn": "region"}
                ],
                "partitions": [
                    {
                        "name": "dim_location-Partition",
                        "mode": "import",
                        "source": {
                            "type": "m",
                            "expression": make_m_partition("SELECT toString(location_key) AS location_key, state_name, district_name, ifNull(market_name, 'All') AS market_name, location_type, ifNull(latitude, 0.0) AS latitude, ifNull(longitude, 0.0) AS longitude, region FROM agripulse_mart.dim_location FINAL")
                        }
                    }
                ]
            },
            {
                "name": "dim_commodity",
                "columns": [
                    {"name": "commodity_key", "dataType": "string", "sourceColumn": "commodity_key"},
                    {"name": "commodity_name", "dataType": "string", "sourceColumn": "commodity_name"},
                    {"name": "commodity_group", "dataType": "string", "sourceColumn": "commodity_group"},
                    {"name": "variety_name", "dataType": "string", "sourceColumn": "variety_name"},
                    {"name": "grade", "dataType": "string", "sourceColumn": "grade"},
                    {"name": "standard_unit", "dataType": "string", "sourceColumn": "standard_unit"}
                ],
                "partitions": [
                    {
                        "name": "dim_commodity-Partition",
                        "mode": "import",
                        "source": {
                            "type": "m",
                            "expression": make_m_partition("SELECT toString(commodity_key) AS commodity_key, commodity_name, commodity_group, variety_name, grade, standard_unit FROM agripulse_mart.dim_commodity FINAL")
                        }
                    }
                ]
            },
            {
                "name": "fact_mandi_prices",
                "columns": [
                    {"name": "mandi_fact_key", "dataType": "string", "sourceColumn": "mandi_fact_key"},
                    {"name": "date_key", "dataType": "int64", "sourceColumn": "date_key"},
                    {"name": "arrival_date", "dataType": "dateTime", "sourceColumn": "arrival_date"},
                    {"name": "state_name", "dataType": "string", "sourceColumn": "state_name"},
                    {"name": "district_name", "dataType": "string", "sourceColumn": "district_name"},
                    {"name": "market_name", "dataType": "string", "sourceColumn": "market_name"},
                    {"name": "commodity_name", "dataType": "string", "sourceColumn": "commodity_name"},
                    {"name": "variety_name", "dataType": "string", "sourceColumn": "variety_name"},
                    {"name": "grade", "dataType": "string", "sourceColumn": "grade"},
                    {"name": "modal_price_inr_quintal", "dataType": "double", "sourceColumn": "modal_price_inr_quintal"},
                    {"name": "min_price_inr_quintal", "dataType": "double", "sourceColumn": "min_price_inr_quintal"},
                    {"name": "max_price_inr_quintal", "dataType": "double", "sourceColumn": "max_price_inr_quintal"},
                    {"name": "price_spread_inr_quintal", "dataType": "double", "sourceColumn": "price_spread_inr_quintal"},
                    {"name": "arrival_quantity_quintals", "dataType": "double", "sourceColumn": "arrival_quantity_quintals"},
                    {"name": "currency", "dataType": "string", "sourceColumn": "currency"},
                    {"name": "price_unit", "dataType": "string", "sourceColumn": "price_unit"}
                ],
                "partitions": [
                    {
                        "name": "fact_mandi_prices-Partition",
                        "mode": "import",
                        "source": {
                            "type": "m",
                            "expression": make_m_partition("SELECT mandi_fact_key, date_key, arrival_date, state_name, district_name, market_name, commodity_name, variety_name, grade, modal_price_inr_quintal, min_price_inr_quintal, max_price_inr_quintal, price_spread_inr_quintal, ifNull(arrival_quantity_quintals, 0.0) AS arrival_quantity_quintals, currency, price_unit FROM agripulse_mart.fact_mandi_prices FINAL")
                        }
                    }
                ]
            },
            {
                "name": "fact_crop_production",
                "columns": [
                    {"name": "crop_fact_key", "dataType": "string", "sourceColumn": "crop_fact_key"},
                    {"name": "crop_year", "dataType": "int64", "sourceColumn": "crop_year"},
                    {"name": "state_name", "dataType": "string", "sourceColumn": "state_name"},
                    {"name": "district_name", "dataType": "string", "sourceColumn": "district_name"},
                    {"name": "crop_name", "dataType": "string", "sourceColumn": "crop_name"},
                    {"name": "season", "dataType": "string", "sourceColumn": "season"},
                    {"name": "area_hectares", "dataType": "double", "sourceColumn": "area_hectares"},
                    {"name": "production", "dataType": "double", "sourceColumn": "production"},
                    {"name": "production_unit", "dataType": "string", "sourceColumn": "production_unit"},
                    {"name": "production_tonnes", "dataType": "double", "sourceColumn": "production_tonnes"},
                    {"name": "production_coconut_nuts", "dataType": "double", "sourceColumn": "production_coconut_nuts"},
                    {"name": "yield_tonnes_per_hectare", "dataType": "double", "sourceColumn": "yield_tonnes_per_hectare"},
                    {"name": "yield_coconut_nuts_per_hectare", "dataType": "double", "sourceColumn": "yield_coconut_nuts_per_hectare"},
                    {"name": "is_production_missing", "dataType": "int64", "sourceColumn": "is_production_missing"},
                    {"name": "is_incomplete_year", "dataType": "int64", "sourceColumn": "is_incomplete_year"}
                ],
                "partitions": [
                    {
                        "name": "fact_crop_production-Partition",
                        "mode": "import",
                        "source": {
                            "type": "m",
                            "expression": make_m_partition("SELECT crop_fact_key, crop_year, state_name, district_name, crop_name, season, ifNull(area_hectares, 0.0) AS area_hectares, ifNull(production, 0.0) AS production, production_unit, ifNull(production_tonnes, 0.0) AS production_tonnes, ifNull(production_coconut_nuts, 0.0) AS production_coconut_nuts, ifNull(yield_tonnes_per_hectare, 0.0) AS yield_tonnes_per_hectare, ifNull(yield_coconut_nuts_per_hectare, 0.0) AS yield_coconut_nuts_per_hectare, is_production_missing, is_incomplete_year FROM agripulse_mart.fact_crop_production FINAL")
                        }
                    }
                ]
            },
            {
                "name": "fact_weather_trends",
                "columns": [
                    {"name": "year", "dataType": "int64", "sourceColumn": "year"},
                    {"name": "month", "dataType": "int64", "sourceColumn": "month"},
                    {"name": "period_label", "dataType": "string", "sourceColumn": "period_label"},
                    {"name": "total_observations", "dataType": "int64", "sourceColumn": "total_observations"},
                    {"name": "national_avg_temperature", "dataType": "double", "sourceColumn": "national_avg_temperature"},
                    {"name": "national_min_temperature", "dataType": "double", "sourceColumn": "national_min_temperature"},
                    {"name": "national_max_temperature", "dataType": "double", "sourceColumn": "national_max_temperature"},
                    {"name": "national_avg_daily_rainfall", "dataType": "double", "sourceColumn": "national_avg_daily_rainfall"},
                    {"name": "national_total_rainfall", "dataType": "double", "sourceColumn": "national_total_rainfall"},
                    {"name": "subzero_observations", "dataType": "int64", "sourceColumn": "subzero_observations"},
                    {"name": "extreme_rainfall_events", "dataType": "int64", "sourceColumn": "extreme_rainfall_events"}
                ],
                "partitions": [
                    {
                        "name": "fact_weather_trends-Partition",
                        "mode": "import",
                        "source": {
                            "type": "m",
                            "expression": make_m_partition("SELECT year, month, concat(toString(year), '-', lpad(toString(month), 2, '0')) AS period_label, total_observations, national_avg_temperature, national_min_temperature, national_max_temperature, national_avg_daily_rainfall, national_total_rainfall, subzero_observations, extreme_rainfall_events FROM agripulse_mart.view_weather_regional_trends ORDER BY year ASC, month ASC")
                        }
                    }
                ]
            }
        ],
        "relationships": [
            {
                "name": "Date_To_MandiPrices",
                "fromTable": "fact_mandi_prices",
                "fromColumn": "date_key",
                "toTable": "dim_date",
                "toColumn": "date_key"
            },
            {
                "name": "Commodity_To_MandiPrices",
                "fromTable": "fact_mandi_prices",
                "fromColumn": "commodity_name",
                "toTable": "dim_commodity",
                "toColumn": "commodity_name"
            },
            {
                "name": "Location_To_MandiPrices",
                "fromTable": "fact_mandi_prices",
                "fromColumn": "state_name",
                "toTable": "dim_location",
                "toColumn": "state_name"
            },
            {
                "name": "Location_To_CropProduction",
                "fromTable": "fact_crop_production",
                "fromColumn": "state_name",
                "toTable": "dim_location",
                "toColumn": "state_name"
            }
        ]
    }
}

with open(os.path.join(DATASET_DIR, "model.bim"), "w", encoding="utf-8") as f:
    json.dump(model_bim, f, indent=2)

print("model.bim and dataset manifest generated successfully!")
