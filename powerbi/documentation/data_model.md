# AgriPulse India — Power BI Data Model Architecture

## 1. Overview
The AgriPulse India Power BI data model implements a conformed **Star Schema** backed directly by the ClickHouse OLAP Data Warehouse (`agripulse_mart`). It bridges streaming market prices, two decades of historical crop yields, and six years of daily NASA POWER satellite meteorology observations.

---

## 2. Dimensional Star Schema Diagram

```
                        +----------------------+
                        |       dim_date       |
                        +----------------------+
                        | PK: date_key         |
                        |     full_date        |
                        |     year             |
                        |     month, month_name|
                        |     quarter_name     |
                        |     day_name         |
                        +----------+-----------+
                                   |
         +-------------------------+--------------------------+
         | (1:N on date_key)                                  | (1:N on date_key)
         v                                                    v
+------------------------+                           +------------------------+
|   fact_mandi_prices    |                           |      fact_weather      |
+------------------------+                           +------------------------+
| PK: mandi_fact_key     |                           | PK: weather_fact_key   |
| FK: date_key           |                           | FK: date_key           |
| FK: state_name         |                           |     latitude, longitude|
| FK: commodity_name     |                           |     rainfall_mm_per_day|
|     market_name        |                           |     temperature_celsius|
|     modal_price_inr_...|                           |     is_negative_temp   |
|     min_price, max_... |                           |     is_extreme_rain    |
+-----------+------------+                           +------------------------+
            |
            | (N:1 on commodity_name)
            v
+------------------------+
|     dim_commodity      |
+------------------------+
| PK: commodity_key      |
|     commodity_name     |
|     commodity_group    |
|     standard_unit      |
+-----------+------------+
            ^
            | (N:1 on state_name)
            |                                        +------------------------+
+-----------+------------+                           | fact_crop_production   |
|      dim_location      |                           +------------------------+
+------------------------+                           | PK: crop_fact_key      |
| PK: location_key       | <--- (N:1 on state_name) -| FK: state_name         |
|     state_name         |                           |     crop_year          |
|     district_name      |                           |     crop_name          |
|     market_name        |                           |     season             |
|     region             |                           |     area_hectares      |
|     latitude, longitude|                           |     production_tonnes  |
+------------------------+                           |     production_nuts    |
                                                     +------------------------+
```

---

## 3. Conformed Dimension Tables

### `dim_date` (Calendar Dimension)
- **Primary Key**: `date_key` (`UInt32`, format `YYYYMMDD`, e.g., `20261116`).
- **Attributes**: `full_date` (Date), `year` (Integer), `month` (Integer 1–12), `month_name` (Text), `quarter` (1–4), `quarter_name` (`Q1`–`Q4`), `day` (1–31), `day_of_year` (1–366), `is_weekend` (0/1), `day_name` (`Monday`–`Sunday`).
- **Sort Configuration**: In Power BI, `month_name` is explicitly sorted by `month` to prevent alphabetical ordering (`April`, `August`...).

### `dim_location` (Geographic Hierarchy)
- **Primary Key**: `location_key` (`String`, MD5 hash of state, district, market).
- **Attributes**: `state_name` (State/UT), `district_name` (District), `market_name` (Mandi Yard), `region` (`North`, `South`, `East`, `West`, `Central`, `North East`), `latitude` (Float), `longitude` (Float).
- **Data Categories**: In Power BI, `state_name` is categorized as `State or Province`, and `latitude`/`longitude` as geographic coordinates for native map rendering.

### `dim_commodity` (Commodity Master)
- **Primary Key**: `commodity_key` (`String`, MD5 hash of commodity, variety, grade).
- **Attributes**: `commodity_name` (e.g. `Wheat`, `Mustard`, `Paddy`, `Coconut`), `commodity_group` (`Cereals`, `Pulses`, `Oilseeds`, `Vegetables`, `Fruits`, `Spices`, `Commercial`), `variety_name`, `grade`, `standard_unit`.

---

## 4. Analytical Fact Tables

### `fact_mandi_prices`
- **Grain**: One record per commodity arrival in a wholesale market per date.
- **Volume**: 12,727 verified records (`FINAL`).
- **Key Metrics**: `modal_price_inr_quintal`, `min_price_inr_quintal`, `max_price_inr_quintal`, `price_spread_inr_quintal`, `arrival_quantity_quintals`.
- **Unit**: ₹ per Quintal (100 kg).

### `fact_crop_production`
- **Grain**: One record per crop per season per district per agricultural year (1997–2015).
- **Volume**: 246,091 verified records (`FINAL`).
- **Key Metrics**:
  - `area_hectares`: Cultivated area.
  - `production_tonnes`: Field crop yield in Metric Tonnes (cereals, pulses, oilseeds, spices, cash crops).
  - `production_coconut_nuts`: Coconut yield in Number of Nuts.
  - `yield_tonnes_per_hectare`: Productivity metric for field crops.
  - `is_production_missing`: Flag for unreported observations (1.52% of dataset; never replaced by zero).

### `fact_weather` / `view_weather_monthly_summary`
- **Grain**: Daily/Monthly grid observations across 2,835 latitude/longitude centroid tiles in India (2020–2026).
- **Volume**: 7,121,520 granular daily rows aggregated into ~195,000 conformed monthly tile snapshots.
- **Key Metrics**:
  - `temperature_celsius`: 2-meter air temperature (°C). Sub-zero values strictly preserved down to -34.62°C.
  - `rainfall_mm_per_day`: Daily precipitation (mm).
  - `subzero_days_count`: Count of frost/sub-zero days per month.
  - `extreme_rainfall_days_count`: Count of days exceeding 100mm/day rainfall.

---

## 5. Relationship Rules & Integrity Guards

1. **Strict 1-to-Many Relationships**: All relationships from dimension tables to fact tables are configured with single-direction filtering (`1:N`). Bidirectional cross-filtering is avoided to eliminate ambiguous filter paths.
2. **Measurement Unit Separation**:
   - `production_tonnes` and `production_coconut_nuts` reside in distinct numeric columns.
   - Measures strictly apply `CALCULATE` filters to ensure Coconut Nuts are never added to Field Crop Tonnes.
3. **Temporal Integrity**:
   - Mandi prices reflect the November 2026 market snapshot.
   - Crop production reflects the 1997–2015 historical baseline.
   - Weather observations span 2020–2026.
   - Slicers are decoupled where cross-dataset time filtering is chronologically disjoint.
