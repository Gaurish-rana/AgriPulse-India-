# AgriPulse India — Power BI Business Intelligence Suite

Interactive analytical reporting dashboards connecting directly to the **ClickHouse OLAP Data Warehouse** (`agripulse_mart`).

---

## 1. Quick Start: Opening the Report in Power BI Desktop

Power BI Desktop is installed on this machine at `C:\Program Files\WindowsApps\Microsoft.MicrosoftPowerBIDesktop_...`.

You have **three reliable ways** to open and run the report:

### Method A (Recommended — Direct Power BI Project):
1. In Windows Explorer, navigate to:
   `c:\Users\Gaurish16\OneDrive\Desktop\AgriPulse-India\powerbi\`
2. Double-click **`AgriPulse_India.pbip`** (or right-click -> Open with **Power BI Desktop**).
3. Power BI Desktop will automatically compile the dataset and report layout.
4. When prompted, Click **Refresh** to load the data from ClickHouse.
5. In Power BI Desktop, click **File -> Save As -> `AgriPulse_India.pbix`** to save your compiled binary report.

### Method B (Power BI Template):
1. Double-click **`AgriPulse_India.pbit`**.
2. Power BI Desktop launches and executes the underlying queries against ClickHouse port `8123`.
3. Save as **`AgriPulse_India.pbix`**.

### Method C (Native Python Script Loader):
1. Open **Power BI Desktop**.
2. Click **Get Data -> Other -> Python script -> Connect**.
3. Paste the contents of `powerbi/powerbi_clickhouse_loader.py`.
4. Click **OK**.
5. Select all 7 dataframes (`dim_date`, `dim_location`, `dim_commodity`, `fact_mandi_prices`, `fact_crop_production`, `fact_weather_monthly`, `fact_weather_trends`).
6. Click **Load**.

---

## 2. Dashboard Pages Overview

The report consists of four interactive analytical pages:

### Page 1: `01 - Agriculture Overview`
- **Hero KPI Cards**:
  - Total Mandi Records: `12,727`
  - Total Commodities Traded: `253`
  - States & UTs Covered: `33`
  - Historical Field Crop Production: `11.19 Billion Tonnes`
  - Weather Observations: `7,121,520`
- **Analytical Charts**:
  - Top 10 States by Cumulative Crop Production (Horizontal Bar)
  - Commodity Distribution by Category (Donut Chart)
  - Multi-Decade Crop Production & Cultivated Area Trend (1997–2015 Line Chart)
  - National Climate Cycle: Temperature vs Precipitation (Dual Line & Column Chart)

### Page 2: `02 - Mandi Price Analytics`
- **Wholesale Price Metrics**:
  - Average Modal Price: `₹ 5,265.73` / Quintal
  - Minimum Recorded Price: `₹ 0.01` / Quintal
  - Maximum Recorded Price: `₹ 149,000.00` / Quintal
  - Average Price Volatility Spread: `₹ 392.40`
  - Active Wholesale Markets: `1,048` Mandi Yards
- **Analytical Charts**:
  - Top 15 Commodities by Prevailing Modal Price
  - State-Wise Price Benchmark Comparison
  - Price Range Dynamics (`min_price` vs `modal_price` vs `max_price`)
  - Detailed Mandi Market Arbitrage Explorer Matrix

### Page 3: `03 - Crop Production Analytics`
- **Agricultural Output Metrics**:
  - Field Crop Production: `11.19 Billion Tonnes` (Strictly segregated from Coconut)
  - Coconut Production: `129.98 Billion Nuts`
  - Cultivated Land Area: `1.46 Billion Hectares`
  - Average Agricultural Yield: `2.78 Tonnes / Ha`
  - Crop Diversity: `124 Distinct Crops`
- **Analytical Charts**:
  - Crop Production Volume by Group & Commodity (Treemap)
  - Season-Wise Production Breakdown: Kharif vs Rabi vs Summer (Column Chart)
  - Multi-Year Cultivated Area & Yield Dynamics (Area Chart)
  - State vs Season Agricultural Matrix Heatmap

### Page 4: `04 - Weather Analytics`
- **Meteorological Indicators**:
  - National Mean Temperature: `20.10 °C`
  - Record Cold Temperature: `-34.62 °C` (Authentic Himalayan frost preserved)
  - Record Peak Temperature: `43.52 °C`
  - Mean Daily Precipitation: `3.44 mm/day`
  - Sub-Zero Frost Days: `854,937 Observation Records`
- **Analytical Charts**:
  - Annual Monthly Temperature Cycles (Multi-Year Line Series)
  - Monsoon Precipitation Surge Wave (Southwest Monsoon Peak Area Chart)
  - Extreme Weather Incidents by Year: Frost vs Torrential Deluge (Clustered Column)

---

## 3. Data Model & Star Schema

The data model utilizes a Kimball Star Schema connecting central fact tables to conformed dimension tables:
- `dim_date` (14,975 rows, 1995–2035)
- `dim_location` (2,443 rows, Geographic hierarchy)
- `dim_commodity` (2,051 rows, Commodity master)
- `fact_mandi_prices` (12,727 rows, ₹/Quintal wholesale market prices)
- `fact_crop_production` (246,091 rows, 1997–2015 historical yields)
- `fact_weather_trends` (83 rows, Monthly climate trends across 7.12M observations)

See [documentation/data_model.md](file:///c:/Users/Gaurish16/OneDrive/Desktop/AgriPulse-India/powerbi/documentation/data_model.md) and [documentation/dax_measures.md](file:///c:/Users/Gaurish16/OneDrive/Desktop/AgriPulse-India/powerbi/documentation/dax_measures.md) for full architectural details.

---

## 4. Performance Optimizations for 7.12M Records

To ensure sub-second rendering and instant slicer response on laptops, the detailed 7.12M NASA POWER daily weather table is pre-aggregated into ClickHouse mart views:
- `agripulse_mart.view_weather_monthly_summary` (~195K monthly tile rows)
- `agripulse_mart.view_weather_regional_trends` (83 monthly national rows)

Power BI queries these optimized views over HTTP, reducing transfer payload by 99.8% while preserving all extreme climate indicators and sub-zero frost data.
