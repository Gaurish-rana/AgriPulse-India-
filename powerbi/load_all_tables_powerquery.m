// ==============================================================================
// AgriPulse India — Power Query M Queries (Driverless 100% Unique Primary Keys)
// ==============================================================================

// Query 1: dim_date (14,975 rows)
let
    Source = Csv.Document(File.Contents("C:\Users\Gaurish16\OneDrive\Desktop\AgriPulse-India\powerbi\data\dim_date.csv"), [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    Typed = Table.TransformColumnTypes(Promoted, {{"date_key", Int64.Type}, {"full_date", type date}, {"year", Int64.Type}, {"month", Int64.Type}, {"month_name", type text}, {"day", Int64.Type}, {"day_of_year", Int64.Type}, {"quarter", Int64.Type}, {"day_of_week", Int64.Type}, {"day_name", type text}, {"is_weekend", Int64.Type}, {"agricultural_year", type text}})
in
    Typed

// Query 2: dim_location (33 rows, unique state_name)
let
    Source = Csv.Document(File.Contents("C:\Users\Gaurish16\OneDrive\Desktop\AgriPulse-India\powerbi\data\dim_location.csv"), [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    Typed = Table.TransformColumnTypes(Promoted, {{"location_key", type text}, {"state_name", type text}, {"district_name", type text}, {"market_name", type text}, {"location_type", type text}, {"latitude", type number}, {"longitude", type number}, {"region", type text}})
in
    Typed

// Query 3: dim_commodity (341 rows, unique commodity_name)
let
    Source = Csv.Document(File.Contents("C:\Users\Gaurish16\OneDrive\Desktop\AgriPulse-India\powerbi\data\dim_commodity.csv"), [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    Typed = Table.TransformColumnTypes(Promoted, {{"commodity_key", type text}, {"commodity_name", type text}, {"commodity_group", type text}, {"variety_name", type text}, {"grade", type text}, {"standard_unit", type text}})
in
    Typed

// Query 4: fact_mandi_prices (12,727 rows)
let
    Source = Csv.Document(File.Contents("C:\Users\Gaurish16\OneDrive\Desktop\AgriPulse-India\powerbi\data\fact_mandi_prices.csv"), [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    Typed = Table.TransformColumnTypes(Promoted, {{"mandi_fact_key", type text}, {"date_key", Int64.Type}, {"arrival_date", type date}, {"state_name", type text}, {"district_name", type text}, {"market_name", type text}, {"commodity_name", type text}, {"variety_name", type text}, {"grade", type text}, {"modal_price_inr_quintal", type number}, {"min_price_inr_quintal", type number}, {"max_price_inr_quintal", type number}, {"price_spread_inr_quintal", type number}, {"arrival_quantity_quintals", type number}, {"currency", type text}, {"price_unit", type text}, {"is_price_relation_valid", Int64.Type}})
in
    Typed

// Query 5: fact_crop_production (246,091 rows)
let
    Source = Csv.Document(File.Contents("C:\Users\Gaurish16\OneDrive\Desktop\AgriPulse-India\powerbi\data\fact_crop_production.csv"), [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    Typed = Table.TransformColumnTypes(Promoted, {{"crop_fact_key", type text}, {"crop_year", Int64.Type}, {"state_name", type text}, {"district_name", type text}, {"crop_name", type text}, {"season", type text}, {"area_hectares", type number}, {"production", type number}, {"production_unit", type text}, {"production_tonnes", type number}, {"production_coconut_nuts", type number}, {"yield_tonnes_per_hectare", type number}, {"yield_coconut_nuts_per_hectare", type number}, {"is_production_missing", Int64.Type}, {"is_incomplete_year", Int64.Type}})
in
    Typed

// Query 6: fact_weather_trends (83 rows)
let
    Source = Csv.Document(File.Contents("C:\Users\Gaurish16\OneDrive\Desktop\AgriPulse-India\powerbi\data\fact_weather_trends.csv"), [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    Typed = Table.TransformColumnTypes(Promoted, {{"year", Int64.Type}, {"month", Int64.Type}, {"period_label", type text}, {"total_observations", Int64.Type}, {"national_avg_temperature", type number}, {"national_min_temperature", type number}, {"national_max_temperature", type number}, {"national_avg_daily_rainfall", type number}, {"national_total_rainfall", type number}, {"subzero_observations", Int64.Type}, {"extreme_rainfall_events", Int64.Type}})
in
    Typed
