# AgriPulse India — Power BI Dashboard Design & Implementation Guide

---

## 1. Visual Design System & Theme

The dashboards follow a cohesive, modern agricultural intelligence design system:

| UI Role | Color Name | Hex Code | Purpose |
|---|---|---|---|
| Primary | Deep Green | `#173F35` | Main headers, navigation bars, primary visual titles, hero KPIs. |
| Secondary | Forest Green | `#246B4B` | Primary chart bars, line series, active filter selections. |
| Accent | Light Green | `#A5C96A` | Highlighting top performers, secondary comparisons, accents. |
| Surface Light | Soft Green / Cream | `#DDEBDD` | Slicer panel background, card borders, callout backgrounds. |
| Canvas Background | Off-White | `#F5F7F2` | Clean, high-contrast canvas background for readability. |
| Card Container | Pure White | `#FFFFFF` | Individual visual containers, KPI cards with subtle shadow. |
| Body Typography | Dark Charcoal | `#25332D` | High legibility text, axis labels, data table cells. |

---

## 2. Page 1: 01 - Agriculture Overview

**Goal**: Executive landing page summarizing the three pillar data streams of AgriPulse India.

### Layout & Visual Grid:
- **Header Bar (Top)**:
  - Title: **AgriPulse India — Agricultural Big Data Analytics Platform**
  - Subtitle: *Unified Market Mandi Prices, Historical Crop Production & NASA POWER Satellite Meteorology*
  - Top Navigation Buttons: `[Overview]` (Active), `[Mandi Prices]`, `[Crop Production]`, `[Weather Analytics]`
- **KPI Summary Ribbon (Top Row, 5 Cards)**:
  1. **Total Mandi Records**: `12,727` | Subtitle: *Cleaned & conformed wholesale prices*
  2. **Total Commodities**: `253` | Subtitle: *Agricultural goods tracked*
  3. **States & UTs Covered**: `33` | Subtitle: *Pan-India geographic reach*
  4. **Historical Crop Output**: `11.19 B Tonnes` | Subtitle: *Field crops (1997–2015)*
  5. **Weather Observations**: `7,121,520` | Subtitle: *NASA POWER daily grid tiles*
- **Main Visual Area (2x2 Grid)**:
  - **Visual 1 (Top Left)**: *Clustered Bar Chart* — Top 10 States by Cumulative Crop Production (Tonnes).
    - *X-Axis*: `Total Field Crop Production (Tonnes)`
    - *Y-Axis*: `state_name` (Uttar Pradesh, Maharashtra, Tamil Nadu, etc.)
  - **Visual 2 (Top Right)**: *Donut / Treemap* — Mandi Records by Commodity Group (`Cereals`, `Pulses`, `Oilseeds`, `Vegetables`, `Fruits`).
    - *Values*: `Total Mandi Records`
    - *Category*: `commodity_group`
  - **Visual 3 (Bottom Left)**: *Line Chart* — Historical Crop Production & Cultivated Area Trend (1997–2015).
    - *X-Axis*: `crop_year`
    - *Y-Axis (Line 1)*: `Total Field Crop Production (Tonnes)`
    - *Y-Axis (Line 2)*: `Cultivated Area (Hectares)`
  - **Visual 4 (Bottom Right)**: *Line & Clustered Column Chart* — Monthly Climate Cycle (National Temperature vs Rainfall).
    - *X-Axis*: `month` (1 to 12)
    - *Column (Bar)*: `National Mean Daily Rainfall (mm)` (Forest Green)
    - *Line*: `National Mean Temperature (°C)` (Deep Green)
- **Interactive Slicers (Left Panel or Floating Top)**:
  - `state_name` (Dropdown Slicer)
  - `commodity_group` (Pill / List Slicer)

---

## 3. Page 2: 02 - Mandi Price Analytics

**Goal**: In-depth wholesale price distribution, market arbitrage, and commodity rate comparisons.

### Layout & Visual Grid:
- **Header & Navigation**: Mandi Price Analytics header with interactive navigation links.
- **KPI Cards (Top Ribbon, 5 Cards)**:
  1. **Average Modal Price**: `₹ 5,265.73` / Quintal
  2. **Lowest Price Recorded**: `₹ 0.01` / Quintal
  3. **Highest Price Recorded**: `₹ 149,000.00` / Quintal
  4. **Average Price Spread**: `₹ 392.40` (Max Price - Min Price band)
  5. **Active Mandi Markets**: `1,048` Physical yards
- **Visuals**:
  - **Visual 1 (Top Left, Bar Chart)**: Top 15 Commodities by Average Modal Price (₹/Quintal).
    - Identifies high-value spices, medicinal crops, and cash crops vs staple grains.
  - **Visual 2 (Top Right, Clustered Bar Chart)**: State-Wise Average Modal Price Comparison.
    - Compares prevailing wholesale rates across states.
  - **Visual 3 (Bottom Left, Scatter Plot / High-Low Chart)**: Price Range Dynamics (`min_price` vs `modal_price` vs `max_price`).
    - Visualizes market volatility and trading spreads.
  - **Visual 4 (Bottom Right, Interactive Matrix Table)**: Mandi Market Pricing Explorer.
    - *Rows*: `state_name` > `district_name` > `market_name` > `commodity_name`
    - *Values*: `Average Modal Price`, `Min Price`, `Max Price`, `Price Spread`, `Arrival Volume (Qtl)`
- **Slicers**:
  - `state_name`
  - `commodity_name`
  - `market_name`
  - `grade`

---

## 4. Page 3: 03 - Crop Production Analytics

**Goal**: Historical crop yields, seasonal shifts, and agricultural productivity across Indian states.

### Layout & Visual Grid:
- **Header & Navigation**: Crop Production Analytics header.
- **KPI Cards (Top Ribbon, 5 Cards)**:
  1. **Field Crop Production**: `11.19 B Tonnes`
  2. **Coconut Production**: `129.98 B Nuts` (Strictly segregated from Tonnes)
  3. **Cultivated Area**: `1.46 B Hectares`
  4. **Average Field Yield**: `2.78 Tonnes / Ha`
  5. **Crop Diversity**: `124 Distinct Crops`
- **Visuals**:
  - **Visual 1 (Top Left, Treemap)**: Production by Crop Category & Crop Name.
    - Proportional area showing dominance of Sugarcane, Rice, Wheat, Cotton, Maize.
  - **Visual 2 (Top Right, Column Chart)**: Season-Wise Production Distribution (`Kharif`, `Rabi`, `Summer`, `Whole Year`).
  - **Visual 3 (Bottom Left, Area Chart)**: Multi-Year Cultivated Area & Yield Dynamics (1997–2015).
    - Tracks productivity gains over time.
  - **Visual 4 (Bottom Right, Matrix Heatmap)**: State vs Season Crop Matrix.
    - *Rows*: `state_name`
    - *Columns*: `season`
    - *Values*: `Total Field Crop Production (Tonnes)`
- **Slicers**:
  - `crop_year` (Range Slider: 1997 to 2015)
  - `state_name`
  - `crop_name`
  - `season`

---

## 5. Page 4: 04 - Weather Analytics

**Goal**: Satellite-derived climate observations, regional monsoon patterns, and sub-zero temperature analysis.

### Layout & Visual Grid:
- **Header & Navigation**: Weather Analytics header.
- **KPI Cards (Top Ribbon, 5 Cards)**:
  1. **National Mean Temperature**: `20.10 °C`
  2. **Record Cold Temperature**: `-34.62 °C` (Himalayan sub-zero preservation)
  3. **Record Peak Temperature**: `43.52 °C`
  4. **Mean Daily Rainfall**: `3.44 mm`
  5. **Sub-Zero Frost Observations**: `854,937 Records`
- **Visuals**:
  - **Visual 1 (Top Left, Line Chart)**: Monthly Temperature Cycle (Annual seasonality 2020–2026).
    - *X-Axis*: `month` (Jan to Dec)
    - *Lines*: Distinct years (2020, 2021, 2022, 2023, 2024, 2025, 2026).
  - **Visual 2 (Top Right, Area / Column Chart)**: Monsoon Precipitation Wave.
    - Demonstrates sharp rainfall spike during Southwest Monsoon (June–September) reaching annual peaks.
  - **Visual 3 (Bottom Left, Map / Bubble Chart)**: Geographic Grid Centroids (Latitude vs Longitude).
    - *Size*: `Total Monthly Rainfall (mm)`
    - *Color*: `Mean Temperature (°C)` (Blue for sub-zero Himalayan zones, Green/Amber for tropical plains).
  - **Visual 4 (Bottom Right, Clustered Bar Chart)**: Extreme Weather Incidents by Year.
    - Compares frost days (`temperature < 0°C`) and torrential deluge days (`rainfall > 100mm`).
- **Slicers**:
  - `year` (2020 to 2026)
  - `month` (1 to 12)
  - `subzero_only` toggle

---

## 6. Slicer & Cross-Filtering Interaction Matrix

To ensure that slicers and visual interactions do not cause illogical joins between disjoint datasets:
- **`dim_date`**: Filters `fact_mandi_prices` and `fact_weather` on matching dates. Does not force cross-filtering on historical crop records where the years do not overlap (1997–2015 vs 2020–2026).
- **`dim_location`**: `state_name` serves as a universal conformed filter across all three facts.
- **`dim_commodity`**: Slices `fact_mandi_prices`. Does not filter unrelated meteorological observations.
