# Shipping Nowcast Dashboard — Hormuz Crisis Monitor

Real-time monitoring of the Iran/Strait of Hormuz crisis (Feb 28, 2026) impact on global shipping using satellite-derived vessel tracking data from IMF PortWatch.

**Live dashboard**: [https://limkvn.github.io/shipping-nowcast/](https://limkvn.github.io/shipping-nowcast/)

## Methodology

The system uses STL decomposition + Ridge regression to construct a **counterfactual** — what shipping activity would have looked like without the crisis — and measures deviations from it.

1. **Data**: Daily port calls and chokepoint transits from IMF PortWatch (AIS-based vessel tracking), covering 5 vessel types (tanker, container, dry bulk, general cargo, RoRo) across ~800 ports and 28 chokepoints worldwide.

2. **STL Decomposition** (period=52 weeks, robust): Decomposes each time series into trend, seasonal, and remainder components. Estimated on pre-crisis data only (before Feb 28, 2026).

3. **Ridge Regression on Remainder**: Models the STL remainder using frozen macroeconomic controls (oil prices, VIX, trade data, industrial production) to capture non-seasonal variation. Controls are frozen at their last pre-crisis values when projecting forward.

4. **Counterfactual Projection**: Post-crisis counterfactual = extrapolated trend + seasonal template (via ISO-week mapping) + predicted remainder from frozen controls.

5. **Significance Testing**: Deviations are tested against the standard deviation of pre-crisis residuals. In multi-vessel-type mode, joint sigma is computed empirically from the summed pre-crisis series rather than assuming independence across vessel types.

## Pipeline

```
download_portwatch_data.py         # Fetch latest PortWatch data from ArcGIS API
download_eia_data.py               # Fetch EIA weekly petroleum data
download_futures_data.py           # Fetch Brent futures via yfinance
download_brent_futures_eia.py      # Fetch Brent futures from EIA
download_futures_term_structure.py # Fetch futures term structure
        |
        v
nowcast_pipeline.py                # STL decomposition + Ridge regression
        |
        v
build_nowcast_dashboard.py         # Generate self-contained HTML dashboard
        |
        v
shipping_nowcast_dashboard.html    # Interactive dashboard (dark theme, password-gated)
```

Orchestrated by `run_analysis.py`.

## Files

### Data Ingestion

| File | Description |
|------|-------------|
| `download_portwatch_data.py` | Downloads Daily Ports Data (~5M records) and Daily Chokepoints Data (~4K records) from IMF PortWatch ArcGIS Feature Service API. No API key needed. |
| `download_eia_data.py` | Downloads weekly petroleum series from EIA API v2 (crude inventories, SPR, production, refinery utilization, imports/exports, demand proxies). Requires free EIA API key. |
| `download_futures_data.py` | Downloads Brent crude oil futures at multiple tenors via yfinance. |
| `download_brent_futures_eia.py` | Downloads Brent futures and term structure data from EIA. |
| `download_futures_term_structure.py` | Downloads futures term structure data for contango/backwardation analysis. |

### Core Pipeline

| File | Description |
|------|-------------|
| `nowcast_pipeline.py` | Core nowcasting engine. STL decomposition, Ridge regression on remainders, counterfactual projection, variance decomposition. Covers chokepoints, regions, countries, and individual ports across 5 vessel types. |
| `build_nowcast_dashboard.py` | Generates a self-contained interactive HTML dashboard with KPI cards, time series charts, deviation tables, Leaflet map, vessel type toggles, and significance filtering. |
| `run_analysis.py` | End-to-end orchestration: runs pipeline then builds dashboard. |
| `validate_pipeline.py` | Historical backtest on the 2024 Red Sea/Houthi crisis. Rolling pseudo-out-of-sample evaluation with RMSE, MAE, bias, and coverage metrics. |

### Supporting Files

| File | Description |
|------|-------------|
| `geo_linkage.csv` | Port-to-region geographic mappings. |
| `geo_linkage_chokepoints_regions.csv` | Chokepoint-to-region mappings. |
| `geo_linkage_countries.csv` | Country-level mappings. |
| `shipping_nowcast_dashboard.html` | The deployed dashboard (fully self-contained, ~9MB). |
| `github_pages_deploy_guide.md` | Deployment instructions for GitHub Pages. |

## Data Requirements

### Required (primary data)

The pipeline expects data in `../data/portwatch/`:
- `Daily_Ports_Data.csv` — port-level daily vessel counts and tonnage (download via `download_portwatch_data.py`)
- `Daily_Chokepoints_Data.csv` — chokepoint-level daily transit counts and capacity (download via `download_portwatch_data.py`)

### Optional (macro control variables for Ridge regression)

The Ridge regression step uses macro controls to model the non-seasonal remainder. These improve the counterfactual but the pipeline runs without them (STL-only counterfactual).

- `../data/controls/fred/` — 20+ FRED series downloaded via FRED API: industrial production, commodity indices, VIX, yield curve, trade-weighted USD, consumer sentiment, China trade, oil prices, natural gas, etc. (series IDs: INDPRO, PNRGINDEXM, PCOPPUSDM, BUSLOANS, TOTALSA, TCU, XTIMVA01CNM667S, XTEXVA01CNM667S, UMCSENT, GEPUCURRENT, DTWEXBGS, DFF, DCOILBRENTEU, DCOILWTICO, DHHNGSP, BAMLH0A0HYM2, T10Y2Y, IC4WSA, GASREGW, etc.)
- `../data/controls/eia/` — OPEC crude production by country (`eia_opec_crude_production.csv`)
- `../data/controls/shipping/` — Baltic Dry Index (`shipping_bdry.csv`), Frontline tanker rates (`shipping_fro.csv`)
- `../data/prices/` — Historical Brent spot prices (`oil_prices.csv`)
- `../data/futures/` — Brent futures data (download via `download_futures_data.py` and related scripts)

## Quick Start

```bash
# 1. Download latest PortWatch data (primary — required)
python download_portwatch_data.py

# 2. Download EIA & futures data (optional — improves controls)
python download_eia_data.py YOUR_EIA_API_KEY
python download_futures_data.py

# 3. Run full pipeline + build dashboard
python run_analysis.py
```

## Key Findings (as of late March 2026)

- Strait of Hormuz tanker transits collapsed ~71% post-crisis
- Suez Canal tanker transits down ~17% (rerouting effects)
- Persian Gulf port calls (tanker) down ~44%
- Cape of Good Hope seeing increased rerouting traffic
- Singapore, as a major transshipment hub, shows ~6-8% decline in total port calls

## Tech Stack

- **Data source**: IMF PortWatch (ArcGIS Feature Service REST API)
- **Pipeline**: Python (pandas, statsmodels STL, scikit-learn Ridge)
- **Dashboard**: Self-contained HTML, Chart.js, Leaflet.js
- **Deployment**: GitHub Pages
