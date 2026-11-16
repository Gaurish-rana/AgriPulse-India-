// ==============================================================================
// AgriPulse India — Power Query M Queries (Native Driverless HTTP Connector)
// ClickHouse Endpoint: http://localhost:8123 (agripulse_mart)
// ==============================================================================

// Query 1: dim_date
let
    Source = Csv.Document(Web.Contents("http://localhost:8123/", [Query=[query="SELECT * FROM agripulse_mart.dim_date ORDER BY date_key ASC FORMAT CSVWithNames"]]), [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    Typed = Table.TransformColumnTypes(Promoted, {{"date_key", Int64.Type}, {"full_date", type date}, {"year", Int64.Type}, {"month", Int64.Type}, {"month_name", type text}, {"day", Int64.Type}, {"day_of_year", Int64.Type}, {"quarter", Int64.Type}, {"day_of_week", Int64.Type}, {"day_name", type text}, {"is_weekend", Int64.Type}, {"agricultural_year", type text}})
in
    Typed

// Query 2: dim_location
let
    Source = Csv.Document(Web.Contents("http://localhost:8123/", [Query=[query="SELECT toString(location_key) AS location_key, state_name, district_name, ifNull(market_name, 'All') AS market_name, location_type, ifNull(latitude, 0.0) AS latitude, ifNull(longitude, 0.0) AS longitude, region FROM agripulse_mart.dim_location FINAL FORMAT CSVWithNames"]]), [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    Typed = Table.TransformColumnTypes(Promoted, {{"location_key", type text}, {"state_name", type text}, {"district_name", type text}, {"market_name", type text}, {"location_type", type text}, {"latitude", type number}, {"longitude", type number}, {"region", type text}})
in
    Typed

// Query 3: dim_commodity
let
    Source = Csv.Document(Web.Contents("http://localhost:8123/", [Query=[query="SELECT toString(commodity_key) AS commodity_key, commodity_name, commodity_group, variety_name, grade, standard_unit FROM agripulse_mart.dim_commodity FINAL FORMAT CSVWithNames"]]), [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    Typed = Table.TransformColumnTypes(Promoted, {{"commodity_key", type text}, {"commodity_name", type text}, {"commodity_group", type text}, {"variety_name", type text}, {"grade", type text}, {"standard_unit", type text}})
in
    Typed

// Query 4: fact_mandi_prices
let
    Source = Csv.Document(Web.Contents("http://localhost:8123/", [Query=[query="SELECT mandi_fact_key, date_key, arrival_date, state_name, district_name, market_name, commodity_name, variety_name, grade, modal_price_inr_quintal, min_price_inr_quintal, max_price_inr_quintal, price_spread_inr_quintal, ifNull(arrival_quantity_quintals, 0.0) AS arrival_quantity_quintals, currency, price_unit FROM agripulse_mart.fact_mandi_prices FINAL FORMAT CSVWithNames"]]), [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    Typed = Table.TransformColumnTypes(Promoted, {{"mandi_fact_key", type text}, {"date_key", Int64.Type}, {"arrival_date", type date}, {"state_name", type text}, {"district_name", type text}, {"market_name", type text}, {"commodity_name", type text}, {"variety_name", type text}, {"grade", type text}, {"modal_price_inr_quintal", type number}, {"min_price_inr_quintal", type number}, {"max_price_inr_quintal", type number}, {"price_spread_inr_quintal", type number}, {"arrival_quantity_quintals", type number}, {"currency", type text}, {"price_unit", type text}})
in
    Typed

// Query 5: fact_crop_production
let
    Source = Csv.Document(Web.Contents("http://localhost:8123/", [Query=[query="SELECT crop_fact_key, crop_year, state_name, district_name, crop_name, season, ifNull(area_hectares, 0.0) AS area_hectares, ifNull(production, 0.0) AS production, production_unit, ifNull(production_tonnes, 0.0) AS production_tonnes, ifNull(production_coconut_nuts, 0.0) AS production_coconut_nuts, ifNull(yield_tonnes_per_hectare, 0.0) AS yield_tonnes_per_hectare, ifNull(yield_coconut_nuts_per_hectare, 0.0) AS yield_coconut_nuts_per_hectare, is_production_missing, is_incomplete_year FROM agripulse_mart.fact_crop_production FINAL FORMAT CSVWithNames"]]), [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    Typed = Table.TransformColumnTypes(Promoted, {{"crop_fact_key", type text}, {"crop_year", Int64.Type}, {"state_name", type text}, {"district_name", type text}, {"crop_name", type text}, {"season", type text}, {"area_hectares", type number}, {"production", type number}, {"production_unit", type text}, {"production_tonnes", type number}, {"production_coconut_nuts", type number}, {"yield_tonnes_per_hectare", type number}, {"yield_coconut_nuts_per_hectare", type number}, {"is_production_missing", Int64.Type}, {"is_incomplete_year", Int64.Type}})
in
    Typed

// Query 6: fact_weather_trends
let
    Source = Csv.Document(Web.Contents("http://localhost:8123/", [Query=[query="SELECT year, month, concat(toString(year), '-', lpad(toString(month), 2, '0')) AS period_label, total_observations, national_avg_temperature, national_min_temperature, national_max_temperature, national_avg_daily_rainfall, national_total_rainfall, subzero_observations, extreme_rainfall_events FROM agripulse_mart.view_weather_regional_trends ORDER BY year ASC, month ASC FORMAT CSVWithNames"]]), [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    Typed = Table.TransformColumnTypes(Promoted, {{"year", Int64.Type}, {"month", Int64.Type}, {"period_label", type text}, {"total_observations", Int64.Type}, {"national_avg_temperature", type number}, {"national_min_temperature", type number}, {"national_max_temperature", type number}, {"national_avg_daily_rainfall", type number}, {"national_total_rainfall", type number}, {"subzero_observations", Int64.Type}, {"extreme_rainfall_events", Int64.Type}})
in
    Typed
