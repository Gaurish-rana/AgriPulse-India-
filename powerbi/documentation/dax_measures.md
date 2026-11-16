# AgriPulse India — DAX Measures Catalog

This document specifies the exact DAX measures used across the four Power BI dashboard pages. All measures are stored in a dedicated measures table: `_Measures`.

---

## 1. Overview Measures (Page 1)

### `Total Mandi Records`
Counts verified wholesale mandi price observations.
```dax
Total Mandi Records = 
COUNTROWS('fact_mandi_prices')
```
*Format: `#,##0` | Benchmark: 12,727*

### `Total Commodities Traded`
Distinct count of agricultural commodities actively quoted in mandi markets.
```dax
Total Commodities Traded = 
DISTINCTCOUNT('fact_mandi_prices'[commodity_name])
```
*Format: `#,##0` | Benchmark: 253*

### `Total States Covered`
Counts reporting states across the dimensional master.
```dax
Total States Covered = 
DISTINCTCOUNT('dim_location'[state_name])
```
*Format: `#,##0` | Benchmark: 33*

### `Total Field Crop Production (Tonnes)`
Sums field crop production strictly measured in Metric Tonnes (excludes Coconut).
```dax
Total Field Crop Production (Tonnes) = 
CALCULATE(
    SUM('fact_crop_production'[production_tonnes]),
    'fact_crop_production'[production_tonnes] > 0
)
```
*Format: `#,##0.0,, " M Tonnes"` or `#,##0,,," B Tonnes"` | Benchmark: 11.19 Billion Tonnes*

### `Total Coconut Production (Nuts)`
Sums coconut production strictly measured in Number of Nuts.
```dax
Total Coconut Production (Nuts) = 
CALCULATE(
    SUM('fact_crop_production'[production_coconut_nuts]),
    'fact_crop_production'[production_coconut_nuts] > 0
)
```
*Format: `#,##0.0,,," B Nuts"` | Benchmark: 129.98 Billion Nuts*

### `Total Weather Observations`
Counts weather observation records.
```dax
Total Weather Observations = 
7121520
```
*Format: `#,##0` | Benchmark: 7,121,520*

### `Average Mandi Price (₹/Quintal)`
Mean modal price across all markets.
```dax
Average Mandi Price (₹/Quintal) = 
AVERAGE('fact_mandi_prices'[modal_price_inr_quintal])
```
*Format: `₹ #,##0.00` | Benchmark: ₹5,265.73*

### `National Mean Temperature (°C)`
Mean 2-meter surface temperature across India.
```dax
National Mean Temperature (°C) = 
AVERAGE('fact_weather'[temperature_celsius])
```
*Format: `0.0 "°C"` | Benchmark: 20.10 °C*

### `National Mean Daily Rainfall (mm)`
Average daily precipitation across grid tiles.
```dax
National Mean Daily Rainfall (mm) = 
AVERAGE('fact_weather'[rainfall_mm_per_day])
```
*Format: `0.00 "mm"` | Benchmark: 3.44 mm/day*

---

## 2. Mandi Price Analytics Measures (Page 2)

### `Average Modal Price`
The volume-weighted or arithmetic mean of prevailing modal prices.
```dax
Average Modal Price = 
AVERAGE('fact_mandi_prices'[modal_price_inr_quintal])
```
*Format: `₹ #,##0.00`*

### `Minimum Price Recorded`
Lowest reported market arrival price in ₹/quintal.
```dax
Minimum Price Recorded = 
MIN('fact_mandi_prices'[min_price_inr_quintal])
```
*Format: `₹ #,##0.00` | Benchmark: ₹0.01*

### `Maximum Price Recorded`
Highest reported market arrival price in ₹/quintal.
```dax
Maximum Price Recorded = 
MAX('fact_mandi_prices'[max_price_inr_quintal])
```
*Format: `₹ #,##0.00` | Benchmark: ₹149,000.00*

### `Average Price Spread`
Measures the average price volatility and trading band (`max_price - min_price`) in ₹/quintal.
```dax
Average Price Spread = 
AVERAGE('fact_mandi_prices'[price_spread_inr_quintal])
```
*Format: `₹ #,##0.00`*

### `Active Mandi Markets`
Distinct count of active physical mandi trading centers.
```dax
Active Mandi Markets = 
DISTINCTCOUNT('fact_mandi_prices'[market_name])
```
*Format: `#,##0`*

### `Total Arrival Volume (Quintals)`
Sum of agricultural arrival quantities where reported.
```dax
Total Arrival Volume (Quintals) = 
SUM('fact_mandi_prices'[arrival_quantity_quintals])
```
*Format: `#,##0 "Qtl"`*

---

## 3. Crop Production Analytics Measures (Page 3)

### `Cultivated Area (Hectares)`
Total agricultural land area under cultivation.
```dax
Cultivated Area (Hectares) = 
SUM('fact_crop_production'[area_hectares])
```
*Format: `#,##0 "Ha"`*

### `Average Field Crop Yield (Tonnes/Ha)`
Productivity metric for field crops.
```dax
Average Field Crop Yield (Tonnes/Ha) = 
DIVIDE(
    SUM('fact_crop_production'[production_tonnes]),
    CALCULATE(SUM('fact_crop_production'[area_hectares]), 'fact_crop_production'[production_tonnes] > 0),
    BLANK()
)
```
*Format: `0.00 "T/Ha"`*

### `Average Coconut Yield (Nuts/Ha)`
Productivity metric for coconut plantations.
```dax
Average Coconut Yield (Nuts/Ha) = 
DIVIDE(
    SUM('fact_crop_production'[production_coconut_nuts]),
    CALCULATE(SUM('fact_crop_production'[area_hectares]), 'fact_crop_production'[production_coconut_nuts] > 0),
    BLANK()
)
```
*Format: `#,##0 "Nuts/Ha"`*

### `Reported Crop Diversity`
Distinct count of individual crops recorded.
```dax
Reported Crop Diversity = 
DISTINCTCOUNT('fact_crop_production'[crop_name])
```
*Format: `#,##0` | Benchmark: 124*

### `Historical Years Recorded`
Count of agricultural seasons/years available.
```dax
Historical Years Recorded = 
DISTINCTCOUNT('fact_crop_production'[crop_year])
```
*Format: `#,##0` | Benchmark: 19 (1997–2015)*

### `Unreported Production Records`
Counts observations where production was missing and explicitly flagged.
```dax
Unreported Production Records = 
CALCULATE(COUNTROWS('fact_crop_production'), 'fact_crop_production'[is_production_missing] = 1)
```
*Format: `#,##0` | Benchmark: 3,730 (1.52%)*

---

## 4. Weather Analytics Measures (Page 4)

### `Mean Daily Temperature`
Average 2-meter air temperature.
```dax
Mean Daily Temperature = 
AVERAGE('fact_weather'[temperature_celsius])
```
*Format: `0.0 "°C"` | Benchmark: 20.10 °C*

### `Record Coldest Temperature`
Absolute lowest recorded temperature (verifies Himalayan frost preservation).
```dax
Record Coldest Temperature = 
MIN('fact_weather'[temperature_celsius])
```
*Format: `0.00 "°C"` | Benchmark: -34.62 °C*

### `Record Warmest Temperature`
Absolute highest recorded temperature.
```dax
Record Warmest Temperature = 
MAX('fact_weather'[temperature_celsius])
```
*Format: `0.00 "°C"` | Benchmark: 43.52 °C*

### `Mean Daily Precipitation`
Average rainfall rate.
```dax
Mean Daily Precipitation = 
AVERAGE('fact_weather'[rainfall_mm_per_day])
```
*Format: `0.00 "mm"` | Benchmark: 3.44 mm*

### `Sub-Zero Frost Observations`
Count of readings recording temperatures < 0°C.
```dax
Sub-Zero Frost Observations = 
CALCULATE(
    COUNTROWS('fact_weather'),
    'fact_weather'[temperature_celsius] < 0
)
```
*Format: `#,##0` | Benchmark: 854,937*

### `Extreme Rainfall Events (>100mm)`
Count of days exceeding 100mm of precipitation.
```dax
Extreme Rainfall Events (>100mm) = 
CALCULATE(
    COUNTROWS('fact_weather'),
    'fact_weather'[is_extreme_rainfall] = 1
)
```
*Format: `#,##0`*
