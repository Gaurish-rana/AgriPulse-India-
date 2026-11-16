# AgriPulse India — ClickHouse Warehouse Schema Inspection Report
**Generated for Phase 5: Power BI Business Intelligence Implementation**

---

## 1. Executive Summary

A comprehensive inspection of the ClickHouse OLAP Data Warehouse was conducted across all three database tiers:
- **`agripulse_raw`**: Ingestion landing and execution audit tier.
- **`agripulse_clean`**: Conformed, deduplicated tier (`ReplacingMergeTree`).
- **`agripulse_mart`**: Analytical Dimensional Star Schema tier optimized for Business Intelligence reporting.

All 15 tables and views were validated. Conformed deduplicated row counts were verified with `FINAL` to provide exact source-to-warehouse reconciliation for Power BI reporting.

---

## 2. ClickHouse Database & Table Inventory

### A. Analytical Mart Tier (`agripulse_mart`) — Primary BI Reporting Target

| Table Name | Engine | Partition Key | Sorting Key / PK | Conformed Rows (`FINAL`) | Analytical Purpose |
|---|---|---|---|---:|---|
| `dim_date` | `MergeTree` | `toYear(full_date)` | `date_key` | **14,975** | Conformed Calendar Dimension (1995–2035) with month, quarter, weekend attributes. |
| `dim_location` | `ReplacingMergeTree` | `(state_name)` | `location_key` | **2,443** | Conformed Geographic Hierarchy (State, District, Market, Region, Lat/Lon). |
| `dim_commodity` | `ReplacingMergeTree` | `(commodity_group)` | `commodity_key` | **2,051** | Standardized Commodity Master (Group, Variety, Grade, Standard Unit). |
| `fact_mandi_prices` | `ReplacingMergeTree` | `toYYYYMM(arrival_date)` | `mandi_fact_key` | **12,727** | Wholesale Mandi transaction grain (Min, Max, Modal price ₹/quintal, Arrival volume). |
| `fact_crop_production` | `ReplacingMergeTree` | `crop_year` | `crop_fact_key` | **246,091** | Historical crop yields & production with segregated units (Tonnes vs Nuts). |
| `fact_weather` | `ReplacingMergeTree` | `toYear(weather_date)` | `weather_fact_key` | **7,121,520** | High-resolution NASA POWER daily meteorology (Precipitation & Temperature). |

### B. Conformed Clean Tier (`agripulse_clean`)

| Table Name | Engine | Conformed Rows (`FINAL`) | Role |
|---|---|---:|---|
| `clean_mandi_prices` | `ReplacingMergeTree` | 12,727 | Conformed streaming Mandi prices with standardized state names. |
| `clean_crop_production` | `ReplacingMergeTree` | 246,091 | Conformed crop records with missing-flagged production and unit segregation. |
| `clean_weather` | `ReplacingMergeTree` | 7,121,520 | Conformed daily meteorology with sub-zero values preserved. |
| `kafka_mandi_prices_queue` | `Kafka` | Stream Buffer | Ingestion buffer consuming Kafka topic `mandi-prices-clean`. |
| `mv_kafka_mandi_prices` | `MaterializedView` | Pipeline Router | Materialized View pushing Kafka stream into `clean_mandi_prices`. |

### C. Raw Staging Tier (`agripulse_raw`)

| Table Name | Engine | Rows | Role |
|---|---|---:|---|
| `load_audit` | `MergeTree` | 12 | Pipeline execution tracking, start/end timestamps, batch sizes, and statuses. |
| `raw_mandi_prices` | `MergeTree` | 0 | Raw payload staging for batch/stream ingestion. |
| `raw_crop_production` | `MergeTree` | 0 | Raw CSV staging for crop datasets. |
| `raw_weather` | `MergeTree` | 0 | Raw staging for NASA grid tiles. |

---

## 3. Detailed Column Specifications & Data Types

### 1. `dim_date`
- `date_key` (`UInt32`): Integer surrogate key (YYYYMMDD format, e.g., `20261116`).
- `full_date` (`Date32`): Standard date (`2026-11-16`).
- `year` (`UInt16`): Calendar year (1995 to 2035).
- `month` (`UInt8`): Month number (1 to 12).
- `month_name` (`String`): Full month name (`January`, `February`, etc.).
- `day` (`UInt8`): Day of month (1 to 31).
- `day_of_year` (`UInt16`): Day count in year (1 to 366).
- `quarter` (`UInt8`): Calendar quarter (1 to 4).
- `quarter_name` (`String`): Quarter label (`Q1`, `Q2`, `Q3`, `Q4`).
- `is_weekend` (`UInt8`): 1 for Saturday/Sunday, 0 for weekdays.
- `day_name` (`String`): Name of weekday (`Monday` ... `Sunday`).
- `week_of_year` (`UInt8`): ISO week number (1 to 53).

### 2. `dim_location`
- `location_key` (`String`): Deterministic MD5 key `MD5(state_name | district_name | market_name)`.
- `state_name` (`LowCardinality(String)`): Official standardized Government of India state/UT name.
- `district_name` (`LowCardinality(String)`): District name.
- `market_name` (`String`): Agricultural market/mandi yard name.
- `location_type` (`LowCardinality(String)`): Hierarchy level (`market`, `district`, `state`).
- `latitude` (`Float64`): Representative latitude coordinate.
- `longitude` (`Float64`): Representative longitude coordinate.
- `region` (`LowCardinality(String)`): Agro-climatic geographical zone (`North`, `South`, `East`, `West`, `Central`, `North East`).

### 3. `dim_commodity`
- `commodity_key` (`String`): Deterministic MD5 key `MD5(commodity_name | variety_name | grade)`.
- `commodity_name` (`LowCardinality(String)`): Standardized commodity name (e.g. `Wheat`, `Mustard`, `Paddy`, `Coconut`).
- `commodity_group` (`LowCardinality(String)`): Categorical grouping (`Cereals`, `Pulses`, `Oilseeds`, `Vegetables`, `Fruits`, `Spices`, `Commercial`).
- `variety_name` (`String`): Specific botanical/market variety.
- `grade` (`LowCardinality(String)`): Quality classification (`FAQ`, `Medium`, etc.).
- `standard_unit` (`LowCardinality(String)`): Standard trading measure (`INR/Quintal`, `Metric Tonnes`, `Nuts`).

### 4. `fact_mandi_prices`
- `mandi_fact_key` (`String`): Primary key `MD5(state_name | market_name | commodity_name | arrival_date)`.
- `date_key` (`UInt32`): Foreign key matching `dim_date.date_key`.
- `arrival_date` (`Date32`): Market arrival date (`2026-11-16`).
- `state_name` (`LowCardinality(String)`): State name (links to `dim_location.state_name`).
- `district_name` (`LowCardinality(String)`): District name.
- `market_name` (`String`): Mandi market name.
- `commodity_name` (`LowCardinality(String)`): Commodity name (links to `dim_commodity.commodity_name`).
- `variety_name` (`String`): Variety name.
- `grade` (`LowCardinality(String)`): Grade.
- `min_price_inr_quintal` (`Float64`): Minimum modal transaction rate in ₹/quintal.
- `max_price_inr_quintal` (`Float64`): Maximum modal transaction rate in ₹/quintal.
- `modal_price_inr_quintal` (`Float64`): Prevailing/modal price in ₹/quintal.
- `price_spread_inr_quintal` (`Float64`): Spread (`max_price - min_price`).
- `arrival_quantity_quintals` (`Nullable(Float64)`): Volume arriving in market (where available).
- `currency` (`LowCardinality(String)`): `INR`.
- `price_unit` (`LowCardinality(String)`): `INR/Quintal`.
- `is_price_relation_valid` (`UInt8`): 1 if `min_price <= modal_price <= max_price`.

### 5. `fact_crop_production`
- `crop_fact_key` (`String`): Primary key `MD5(state_name | district_name | crop_year | season | crop_name)`.
- `crop_year` (`UInt16`): Agricultural crop year (1997 to 2015).
- `state_name` (`LowCardinality(String)`): State name (links to `dim_location.state_name`).
- `district_name` (`LowCardinality(String)`): District name.
- `crop_name` (`LowCardinality(String)`): Standardized crop name.
- `season` (`LowCardinality(String)`): Agricultural season (`Kharif`, `Rabi`, `Summer`, `Whole Year`, `Autumn`, `Winter`).
- `area_hectares` (`Float64`): Cultivated land area in hectares.
- `production` (`Nullable(Float64)`): Raw reported production figure.
- `production_tonnes` (`Nullable(Float64)`): Clean segregated production for field crops measured in Metric Tonnes (NULL for Coconut).
- `production_coconut_nuts` (`Nullable(Float64)`): Clean segregated production for Coconut measured in Number of Nuts (NULL for field crops).
- `yield_tonnes_per_hectare` (`Nullable(Float64)`): Computed yield (`production_tonnes / area_hectares`).
- `yield_coconut_nuts_per_hectare` (`Nullable(Float64)`): Computed yield (`production_coconut_nuts / area_hectares`).
- `is_production_missing` (`UInt8`): 1 if production was unreported (1.52% flagged; never coerced to zero).
- `is_incomplete_year` (`UInt8`): 1 for incomplete recording year 2015.

### 6. `fact_weather`
- `weather_fact_key` (`String`): Primary key `MD5(date | latitude | longitude)`.
- `date_key` (`UInt32`): Foreign key matching `dim_date.date_key`.
- `weather_date` (`Date32`): Observation date (2020-01-01 to 2026-11-16).
- `latitude` (`Float64`): Grid centroid latitude (8.0° N to 37.0° N).
- `longitude` (`Float64`): Grid centroid longitude (68.0° E to 97.5° E).
- `rainfall_mm_per_day` (`Nullable(Float64)`): Daily precipitation in millimeters (`PRECTOTCORR`).
- `temperature_celsius` (`Nullable(Float64)`): Mean surface 2-meter temperature in °C (`T2M`).
- `is_negative_temperature` (`UInt8`): 1 if temperature < 0.0°C (854,937 sub-zero readings preserved down to -34.62°C).
- `is_extreme_rainfall` (`UInt8`): 1 if daily precipitation > 100 mm.
- `is_subzero_high_altitude` (`UInt8`): 1 for high altitude Himalayan stations (Ladakh, Kashmir, HP, Uttarakhand).
- `rainfall_unit` (`LowCardinality(String)`): `mm/day`.
- `temperature_unit` (`LowCardinality(String)`): `Celsius`.

---

## 4. Logical Dimensional Model & Entity Relationships

```
                        +----------------------+
                        |       dim_date       |
                        +----------------------+
                        | PK: date_key         |
                        |     full_date        |
                        |     year, month      |
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
|     modal_price_inr_...|                           |     temperature_celsius|
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
|     market_name        |                           |     production_tonnes  |
|     region             |                           |     production_nuts    |
+------------------------+                           +------------------------+
```

---

## 5. Suggested Dashboard Metrics & DAX Measures

### A. Agriculture Overview (Page 1)
- `[Total Mandi Records]` = `COUNTROWS('fact_mandi_prices')` -> Benchmark: 12,727
- `[Total Commodities]` = `DISTINCTCOUNT('fact_mandi_prices'[commodity_name])` -> Benchmark: 253
- `[Total States Covered]` = `DISTINCTCOUNT('dim_location'[state_name])` -> Benchmark: 33
- `[Total Field Crop Production (Tonnes)]` = `SUM('fact_crop_production'[production_tonnes])` -> Benchmark: 11.19 Billion Tonnes
- `[Total Coconut Production (Nuts)]` = `SUM('fact_crop_production'[production_coconut_nuts])` -> Benchmark: 129.98 Billion Nuts
- `[Total Weather Observations]` = `7,121,520` (from aggregate/fact)
- `[Average Mandi Price (₹/Quintal)]` = `AVERAGE('fact_mandi_prices'[modal_price_inr_quintal])` -> Benchmark: ₹5,265.73
- `[Average National Temperature]` = `AVERAGE('fact_weather'[temperature_celsius])` -> Benchmark: 20.10 °C
- `[Average Daily Rainfall]` = `AVERAGE('fact_weather'[rainfall_mm_per_day])` -> Benchmark: 3.44 mm

### B. Mandi Price Analytics (Page 2)
- `[Average Modal Price]` = `AVERAGE('fact_mandi_prices'[modal_price_inr_quintal])`
- `[Min Modal Price]` = `MIN('fact_mandi_prices'[modal_price_inr_quintal])`
- `[Max Modal Price]` = `MAX('fact_mandi_prices'[modal_price_inr_quintal])`
- `[Average Price Spread]` = `AVERAGE('fact_mandi_prices'[price_spread_inr_quintal])`
- `[Active Market Count]` = `DISTINCTCOUNT('fact_mandi_prices'[market_name])`
- `[Commodity Count]` = `DISTINCTCOUNT('fact_mandi_prices'[commodity_name])`

### C. Crop Production Analytics (Page 3)
- `[Total Field Crops (Tonnes)]` = `CALCULATE(SUM('fact_crop_production'[production_tonnes]), 'fact_crop_production'[production_tonnes] > 0)`
- `[Total Coconut (Nuts)]` = `CALCULATE(SUM('fact_crop_production'[production_coconut_nuts]), 'fact_crop_production'[production_coconut_nuts] > 0)`
- `[Average Yield (Tonnes/Ha)]` = `AVERAGE('fact_crop_production'[yield_tonnes_per_hectare])`
- `[Average Coconut Yield (Nuts/Ha)]` = `AVERAGE('fact_crop_production'[yield_coconut_nuts_per_hectare])`
- `[Cultivated Area (Hectares)]` = `SUM('fact_crop_production'[area_hectares])`
- `[Distinct Crops]` = `DISTINCTCOUNT('fact_crop_production'[crop_name])`
- `[Reported Years]` = `DISTINCTCOUNT('fact_crop_production'[crop_year])` -> Benchmark: 19 (1997–2015)

### D. Weather Analytics (Page 4)
- `[Mean Temperature (°C)]` = `AVERAGE('fact_weather'[temperature_celsius])`
- `[Minimum Observed Temp (°C)]` = `MIN('fact_weather'[temperature_celsius])` -> Benchmark: -34.62 °C
- `[Maximum Observed Temp (°C)]` = `MAX('fact_weather'[temperature_celsius])` -> Benchmark: 43.52 °C
- `[Mean Daily Rainfall (mm)]` = `AVERAGE('fact_weather'[rainfall_mm_per_day])`
- `[Total Rainfall (mm)]` = `SUM('fact_weather'[rainfall_mm_per_day])`
- `[Sub-Zero Frost Observations]` = `COUNTROWS(FILTER('fact_weather', 'fact_weather'[temperature_celsius] < 0))` -> Benchmark: 854,937
- `[Extreme Rainfall Days (>100mm)]` = `COUNTROWS(FILTER('fact_weather', 'fact_weather'[is_extreme_rainfall] = 1))`

---

## 6. Performance Optimization for 7.12M Weather Records

Because the detailed `fact_weather` table contains 7,121,520 records, importing every individual raw record into Power BI Desktop would consume excessive memory and slow down interactive dashboard slicing.

### Pre-Aggregated Analytical Mart Views in ClickHouse:
To provide sub-second query response and seamless interactive visual rendering in Power BI, we define two curated analytical views in ClickHouse:
1. `agripulse_mart.view_weather_monthly_summary`:
   Aggregates observations by `year`, `month`, `latitude`, and `longitude`. Computes mean temperature, min/max temperature, total monthly rainfall, sub-zero days, and extreme rainfall occurrences. Reduces 7.12M rows to ~195,000 conformed monthly tile rows.
2. `agripulse_mart.view_weather_annual_state`:
   Associates grid coordinates with regional state zones and computes annual/seasonal climate indicators.

---

## 7. Data Quality Limitations & Boundary Integrity

1. **Mandi Prices Temporal Grain**: The current Mandi dataset is a high-density, multi-state wholesale snapshot for November 2026. Price trends within this snapshot reflect inter-market and cross-state price arbitrage rather than a multi-decade time-series.
2. **Crop Production Year Boundary**: The historical crop production dataset spans 1997–2015. 2015 is an incomplete reporting year (562 records) and is explicitly flagged (`is_incomplete_year = 1`).
3. **Unit Incompatibility Guard**: Coconut production is recorded in *number of nuts* (often millions per district) whereas all other crops are recorded in *metric tonnes*. These must **never** be summed into a single generic number.
4. **Sub-Zero Climate Authenticity**: Temperatures between -0.01°C and -34.62°C occur legitimately in high-altitude northern grid tiles (Ladakh, Jammu & Kashmir, Himachal Pradesh). They are strictly preserved as valid climate phenomena.
