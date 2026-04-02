#!/usr/bin/env python3
"""
Oil Trade Flow Nowcasting Pipeline — Hormuz Crisis
====================================================
Methodology (inspired by IMF WP/25/93 "Nowcasting Global Trade from Space"):
  1. Aggregate daily tanker counts/capacity to weekly frequency per chokepoint
  2. STL decomposition (period=52) → trend, seasonal, remainder
  3. Regress remainder on macro controls over the pre-crisis training window
  4. Project counterfactual post-crisis:
       counterfactual = trend_extrapolated + seasonal_same_week + controls_predicted_remainder
  5. Crisis deviation = actual − counterfactual

Control variable strategy:
  - Primary model: slow-moving monthly controls frozen at last pre-crisis value
  - Sensitivity: live daily financial controls (Brent, VIX proxy, etc.)
"""

import os, sys, json, warnings
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from statsmodels.tsa.seasonal import STL
from sklearn.linear_model import Ridge, RidgeCV
from sklearn.preprocessing import StandardScaler

warnings.filterwarnings("ignore")


def _drop_incomplete_trailing_weeks(daily_series, weekly_series, min_days=5):
    """Drop trailing weekly bins that have fewer than `min_days` of raw daily data.

    This prevents phantom zero-weeks (e.g. data ends Friday but the next
    Monday-anchored bin exists with 0 observations) and incomplete final weeks
    from distorting the 'most recent week' deviation calculation.
    """
    if weekly_series.empty or daily_series.empty:
        return weekly_series
    last_daily = daily_series.index.max()
    # Check the last weekly bin
    last_week_start = weekly_series.index[-1]
    last_week_end = last_week_start + pd.Timedelta(days=6)
    # Count daily observations in this bin
    days_in_bin = daily_series.loc[
        (daily_series.index >= last_week_start) & (daily_series.index <= last_week_end)
    ].count()
    if days_in_bin < min_days:
        weekly_series = weekly_series.iloc[:-1]
        # Recurse in case the second-to-last is also incomplete
        if not weekly_series.empty:
            second_last_start = weekly_series.index[-1]
            second_last_end = second_last_start + pd.Timedelta(days=6)
            days_in_second = daily_series.loc[
                (daily_series.index >= second_last_start) & (daily_series.index <= second_last_end)
            ].count()
            if days_in_second < min_days:
                weekly_series = weekly_series.iloc[:-1]
    return weekly_series


def _resample_weekly_split(daily_series, crisis_date=None, agg="mean"):
    """Resample daily series to weekly (W-MON), splitting cleanly at the crisis date.

    Pre-crisis daily data (up to the day before crisis_date) is resampled
    independently so the last pre-crisis bin contains only pre-crisis days.

    Post-crisis daily data (from crisis_date onward) is resampled with W-MON,
    and if the first bin is a short stub (< 5 days, because the crisis doesn't
    start on a Monday), it is merged into the next bin to form a single longer
    first post-crisis bin.

    Returns a single concatenated weekly Series/DataFrame with a clean boundary.
    """
    if crisis_date is None:
        crisis_date = CRISIS_DATE
    crisis_dt = pd.Timestamp(crisis_date)

    pre_daily = daily_series.loc[daily_series.index < crisis_dt]
    post_daily = daily_series.loc[daily_series.index >= crisis_dt]

    agg_func = agg  # "mean" or "sum"

    # Pre-crisis resampling
    if not pre_daily.empty:
        pre_weekly = pre_daily.resample(**_WEEKLY_RESAMPLE).agg(agg_func)
        # Drop any bins that are entirely NaN
        if isinstance(pre_weekly, pd.DataFrame):
            pre_weekly = pre_weekly.dropna(how="all")
        else:
            pre_weekly = pre_weekly.dropna()
    else:
        pre_weekly = daily_series.iloc[:0].resample(**_WEEKLY_RESAMPLE).agg(agg_func)

    # Post-crisis resampling
    if not post_daily.empty:
        post_weekly = post_daily.resample(**_WEEKLY_RESAMPLE).agg(agg_func)
        if isinstance(post_weekly, pd.DataFrame):
            post_weekly = post_weekly.dropna(how="all")
        else:
            post_weekly = post_weekly.dropna()

        # Merge short first stub into second bin if needed
        if len(post_weekly) >= 2:
            first_bin_start = post_weekly.index[0]
            first_bin_end = first_bin_start + pd.Timedelta(days=6)
            days_in_first = post_daily.loc[
                (post_daily.index >= first_bin_start) & (post_daily.index <= first_bin_end)
            ]
            if isinstance(days_in_first, pd.DataFrame):
                n_days = days_in_first.iloc[:, 0].count()
            else:
                n_days = days_in_first.count()

            if n_days < 5:
                # Merge first bin into second: recompute from daily data
                second_bin_start = post_weekly.index[1]
                second_bin_end = second_bin_start + pd.Timedelta(days=6)
                merged_daily = post_daily.loc[
                    (post_daily.index >= first_bin_start) & (post_daily.index <= second_bin_end)
                ]
                if agg_func == "mean":
                    merged_val = merged_daily.mean()
                else:
                    merged_val = merged_daily.sum()

                # Replace second bin with merged value, drop first bin
                if isinstance(post_weekly, pd.DataFrame):
                    post_weekly.iloc[1] = merged_val
                else:
                    post_weekly.iloc[1] = merged_val
                post_weekly = post_weekly.iloc[1:]
    else:
        post_weekly = daily_series.iloc[:0].resample(**_WEEKLY_RESAMPLE).agg(agg_func)

    # Remove any overlap (the first post-crisis bin label might equal a pre-crisis label)
    if not pre_weekly.empty and not post_weekly.empty:
        pre_weekly = pre_weekly.loc[pre_weekly.index < post_weekly.index[0]]

    return pd.concat([pre_weekly, post_weekly])


# ─── Configuration ──────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_DIR = os.path.join(BASE_DIR, "outputs", "nowcast")
os.makedirs(OUTPUT_DIR, exist_ok=True)

CRISIS_DATE = "2026-02-28"  # Hormuz crisis onset
# Weekly resampling: weeks start Monday, end Sunday, labeled by the Monday start date
_WEEKLY_RESAMPLE = dict(rule="W-MON", closed="left", label="left")
TRAIN_START = "2019-06-01"  # allow STL burn-in (data starts 2019-01-01, need ~26 weeks)
TRAIN_END   = "2026-02-27"  # last pre-crisis date

# Chokepoints to analyze
CHOKEPOINTS = [
    # Primary oil-trade chokepoints (original 5)
    "Strait of Hormuz",
    "Cape of Good Hope",
    "Bab el-Mandeb Strait",
    "Suez Canal",
    "Malacca Strait",
    # Major global chokepoints
    "Panama Canal",
    "Bosporus Strait",
    "Gibraltar Strait",
    "Dover Strait",
    "Oresund Strait",
    # East / Southeast Asian straits
    "Taiwan Strait",
    "Korea Strait",
    "Tsugaru Strait",
    "Luzon Strait",
    "Lombok Strait",
    "Sunda Strait",
    "Makassar Strait",
    "Mindoro Strait",
    "Balabac Strait",
    "Ombai Strait",
    "Bohai Strait",
    # Other passages
    "Torres Strait",
    "Magellan Strait",
    "Yucatan Channel",
    "Windward Passage",
    "Mona Passage",
    "Bering Strait",
    "Kerch Strait",
]

# ─── 1. Load and Prepare Chokepoint Data ────────────────────────────────────
def load_chokepoint_data():
    """Load daily chokepoint data, return dict of weekly DataFrames per chokepoint."""
    print("Loading chokepoint data...")
    fp = os.path.join(DATA_DIR, "portwatch", "Daily_Chokepoints_Data.csv")
    df = pd.read_csv(fp)
    df["date"] = pd.to_datetime(df["date"], utc=True).dt.tz_localize(None)

    weekly = {}
    for cp in CHOKEPOINTS:
        cp_cols = ["date",
                   "n_tanker", "capacity_tanker",
                   "n_container", "capacity_container",
                   "n_dry_bulk", "capacity_dry_bulk",
                   "n_general_cargo", "capacity_general_cargo",
                   "n_roro", "capacity_roro",
                   "n_total"]
        sub = df[df["portname"] == cp][[c for c in cp_cols if c in df.columns]].copy()
        sub = sub.sort_values("date").set_index("date")
        # Resample to weekly (Monday start), splitting cleanly at crisis boundary
        w = _resample_weekly_split(sub, crisis_date=CRISIS_DATE, agg="mean")
        w = w.loc[w.index >= "2019-01-07"]  # drop partial first week
        # Drop incomplete trailing weeks
        # Use the first available numeric column for trailing-week check
        first_num_col = [c for c in sub.columns if c != "date"][0]
        w = _drop_incomplete_trailing_weeks(sub[first_num_col], w)
        weekly[cp] = w
        print(f"  {cp}: {len(w)} weeks, {w.index.min().date()} → {w.index.max().date()}")
    return weekly


# ─── 2. Load Control Variables ──────────────────────────────────────────────
def load_fred_series(filename, col_name=None):
    """Load a FRED CSV, return Series indexed by date."""
    fp = os.path.join(DATA_DIR, "controls", "fred", filename)
    if not os.path.exists(fp):
        return None
    df = pd.read_csv(fp)
    # Handle different column naming conventions
    if "observation_date" in df.columns:
        date_col = "observation_date"
        val_col = [c for c in df.columns if c != date_col][0]
    elif "date" in df.columns:
        date_col = "date"
        val_col = [c for c in df.columns if c != date_col][0]
    else:
        date_col = df.columns[0]
        val_col = df.columns[1]

    df[date_col] = pd.to_datetime(df[date_col])
    df[val_col] = pd.to_numeric(df[val_col], errors="coerce")
    s = df.set_index(date_col)[val_col].dropna()
    s.name = col_name or val_col
    return s


def load_controls():
    """
    Load all control variables and organize into:
      - frozen_monthly: slow-moving controls (primary model)
      - frozen_daily: daily controls averaged to monthly then frozen
      - live_daily: fast-moving financial controls (sensitivity model)
    Returns dict of {name: pd.Series} for each group.
    """
    print("\nLoading control variables...")

    # --- Frozen monthly controls ---
    frozen_monthly = {}

    # OPEC / Gulf production
    eia_fp = os.path.join(DATA_DIR, "controls", "eia", "eia_opec_crude_production.csv")
    if os.path.exists(eia_fp):
        eia = pd.read_csv(eia_fp)
        eia["date"] = pd.to_datetime(eia["date"])
        # Gulf Hormuz Total
        gulf = eia[eia["country"] == "Gulf Hormuz Total"].set_index("date")["crude_prod_mbpd"]
        gulf.name = "gulf_hormuz_prod"
        frozen_monthly["gulf_hormuz_prod"] = gulf
        # OPEC Total
        opec = eia[eia["country"] == "OPEC Total"].set_index("date")["crude_prod_mbpd"]
        opec.name = "opec_total_prod"
        frozen_monthly["opec_total_prod"] = opec
        # World
        world = eia[eia["country"] == "World"].set_index("date")["crude_prod_mbpd"]
        world.name = "world_prod"
        frozen_monthly["world_prod"] = world

    monthly_series = {
        "us_ip":            ("fred_us_ip_index.csv", "us_ip"),
        "imf_energy":       ("fred_imf_energy_price_index.csv", "imf_energy"),
        "imf_nonfuel":      ("fred_imf_nonfuel_commodity.csv", "imf_nonfuel"),
        "commercial_loans": ("fred_commercial_loans.csv", "commercial_loans"),
        "vehicle_sales":    ("fred_vehicle_sales.csv", "vehicle_sales"),
        "capacity_util":    ("fred_us_capacity_utilization.csv", "capacity_util"),
        # New: Asian trade & demand proxies
        "china_imports":    ("fred_china_imports.csv", "china_imports"),
        "china_exports":    ("fred_china_exports.csv", "china_exports"),
        "consumer_sent":    ("fred_consumer_sentiment.csv", "consumer_sent"),
        "global_epu":       ("fred_global_epu.csv", "global_epu"),
        "retail_sales":     ("fred_retail_sales_real.csv", "retail_sales"),
    }
    for name, (fname, cname) in monthly_series.items():
        s = load_fred_series(fname, cname)
        if s is not None and len(s) > 12:
            frozen_monthly[name] = s
            print(f"  [monthly] {name}: {len(s)} obs, {s.index.min().date()} → {s.index.max().date()}")

    # --- Frozen daily controls (will be averaged to monthly, then frozen) ---
    frozen_daily = {}
    daily_frozen_series = {
        "usd_broad":      ("fred_usd_broad_daily.csv", "usd_broad"),
        "fed_funds":      ("fred_fed_funds_rate.csv", "fed_funds"),
        "us_epu":         ("fred_us_epu_daily.csv", "us_epu"),
        "breakeven_infl": ("fred_breakeven_inflation_10y.csv", "breakeven_infl"),
        # New: risk & rates
        "vix":            ("fred_vix_daily.csv", "vix"),
        "yield_curve":    ("fred_yield_curve_10y2y.csv", "yield_curve"),
    }
    for name, (fname, cname) in daily_frozen_series.items():
        s = load_fred_series(fname, cname)
        if s is not None and len(s) > 100:
            frozen_daily[name] = s
            print(f"  [daily→frozen] {name}: {len(s)} obs")

    # --- Live daily controls (sensitivity model only) ---
    live_daily = {}
    live_series = {
        "brent":     ("fred_brent_daily.csv", "brent"),
        "wti":       ("fred_wti_daily.csv", "wti"),
        "henry_hub": ("fred_henry_hub_natgas_daily.csv", "henry_hub"),
        "hy_spread": ("fred_high_yield_spread.csv", "hy_spread"),
    }
    for name, (fname, cname) in live_series.items():
        s = load_fred_series(fname, cname)
        if s is not None and len(s) > 100:
            live_daily[name] = s
            print(f"  [live daily] {name}: {len(s)} obs")

    # --- Frozen weekly controls (already at weekly frequency → treat like daily) ---
    weekly_frozen_series = {
        "initial_claims": ("fred_initial_claims_4wk.csv", "initial_claims"),
        "gasoline":       ("fred_gasoline_weekly.csv", "gasoline"),
    }
    for name, (fname, cname) in weekly_frozen_series.items():
        s = load_fred_series(fname, cname)
        if s is not None and len(s) > 50:
            # Add to frozen_daily — resample("W-MON").mean() handles weekly just fine
            frozen_daily[name] = s
            print(f"  [weekly→frozen] {name}: {len(s)} obs, {s.index.min().date()} → {s.index.max().date()}")

    # Shipping proxies (live, sensitivity only)
    for fname, cname in [("shipping_bdry.csv", "bdry"), ("shipping_fro.csv", "fro")]:
        fp = os.path.join(DATA_DIR, "controls", "shipping", fname)
        if os.path.exists(fp):
            df = pd.read_csv(fp)
            df["date"] = pd.to_datetime(df["date"])
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
            s = df.set_index("date")["value"].dropna()
            s.name = cname
            live_daily[cname] = s
            print(f"  [live daily] {cname}: {len(s)} obs")

    return frozen_monthly, frozen_daily, live_daily


# ─── 3. Build Weekly Control Matrix ────────────────────────────────────────
def build_weekly_controls(weekly_index, frozen_monthly, frozen_daily, live_daily, freeze_date):
    """
    Align all controls to the weekly index.
    Monthly controls: forward-fill to weekly, freeze at last value before freeze_date.
    Daily→frozen controls: resample to weekly mean, freeze at last pre-crisis week.
    Live daily controls: resample to weekly mean, keep updating.

    Returns: (X_frozen, X_live) DataFrames aligned to weekly_index
    """
    freeze_dt = pd.Timestamp(freeze_date)

    # --- Frozen monthly → weekly ---
    frozen_cols = {}
    for name, s in frozen_monthly.items():
        # Forward-fill monthly to daily, then resample to weekly
        daily = s.resample("D").ffill()
        wk = daily.reindex(weekly_index, method="ffill")
        # Freeze: values after freeze_date replaced with last pre-crisis value
        last_pre = wk.loc[wk.index < freeze_dt].iloc[-1] if (wk.index < freeze_dt).any() else wk.iloc[0]
        wk.loc[wk.index >= freeze_dt] = last_pre
        frozen_cols[name] = wk

    # --- Frozen daily/weekly → weekly (averaged), then frozen ---
    for name, s in frozen_daily.items():
        wk = s.resample(**_WEEKLY_RESAMPLE).mean().reindex(weekly_index, method="ffill")
        last_pre = wk.loc[wk.index < freeze_dt].iloc[-1] if (wk.index < freeze_dt).any() else wk.iloc[0]
        wk.loc[wk.index >= freeze_dt] = last_pre
        frozen_cols[name] = wk

    X_frozen = pd.DataFrame(frozen_cols, index=weekly_index).ffill().bfill()

    # --- Fourier harmonics (deterministic calendar features) ---
    # Capture annual, semi-annual, and quarterly seasonality
    day_of_year = weekly_index.dayofyear.values.astype(float)
    for period_weeks, tag in [(52, 'annual'), (26, 'semiannual'), (13, 'quarterly')]:
        period_days = period_weeks * 7
        X_frozen[f'fourier_sin_{tag}'] = np.sin(2 * np.pi * day_of_year / period_days)
        X_frozen[f'fourier_cos_{tag}'] = np.cos(2 * np.pi * day_of_year / period_days)

    # --- Live daily → weekly (sensitivity) ---
    live_cols = {}
    for name, s in live_daily.items():
        wk = s.resample(**_WEEKLY_RESAMPLE).mean().reindex(weekly_index, method="ffill")
        live_cols[name] = wk

    X_live = pd.DataFrame(live_cols, index=weekly_index).ffill().bfill()

    return X_frozen, X_live


# Controls that are clearly exogenous to a Hormuz shipping crisis
# (driven by domestic/global macro forces, not by the crisis itself)
EXOGENOUS_CONTROLS = {
    "us_ip", "capacity_util", "vehicle_sales", "commercial_loans",
    "consumer_sent", "retail_sales", "initial_claims", "gasoline",
    "china_imports", "china_exports",
}

# Controls contaminated by the crisis (oil/energy prices, risk, shipping)
ENDOGENOUS_CONTROLS = {
    "gulf_hormuz_prod", "opec_total_prod", "world_prod",
    "imf_energy", "imf_nonfuel",
    "usd_broad", "fed_funds", "us_epu", "breakeven_infl",
    "vix", "yield_curve", "global_epu",
}


def build_weekly_controls_hybrid(weekly_index, frozen_monthly, frozen_daily, live_daily, freeze_date):
    """
    Approach A: Hybrid freeze.
    Exogenous controls stay live (use actual post-crisis values).
    Endogenous/contaminated controls are frozen at last pre-crisis value.
    Returns X_hybrid DataFrame aligned to weekly_index.
    """
    freeze_dt = pd.Timestamp(freeze_date)
    cols = {}

    # Monthly controls
    for name, s in frozen_monthly.items():
        daily = s.resample("D").ffill()
        wk = daily.reindex(weekly_index, method="ffill")
        if name not in EXOGENOUS_CONTROLS:
            last_pre = wk.loc[wk.index < freeze_dt].iloc[-1] if (wk.index < freeze_dt).any() else wk.iloc[0]
            wk.loc[wk.index >= freeze_dt] = last_pre
        cols[name] = wk

    # Daily/weekly frozen controls
    for name, s in frozen_daily.items():
        wk = s.resample(**_WEEKLY_RESAMPLE).mean().reindex(weekly_index, method="ffill")
        if name not in EXOGENOUS_CONTROLS:
            last_pre = wk.loc[wk.index < freeze_dt].iloc[-1] if (wk.index < freeze_dt).any() else wk.iloc[0]
            wk.loc[wk.index >= freeze_dt] = last_pre
        cols[name] = wk

    X_hybrid = pd.DataFrame(cols, index=weekly_index).ffill().bfill()

    # Fourier harmonics (deterministic calendar features)
    day_of_year = weekly_index.dayofyear.values.astype(float)
    for period_weeks, tag in [(52, 'annual'), (26, 'semiannual'), (13, 'quarterly')]:
        period_days = period_weeks * 7
        X_hybrid[f'fourier_sin_{tag}'] = np.sin(2 * np.pi * day_of_year / period_days)
        X_hybrid[f'fourier_cos_{tag}'] = np.cos(2 * np.pi * day_of_year / period_days)

    return X_hybrid


def _forecast_series_arima(s, freeze_date, n_ahead, weekly_index):
    """
    Forecast a single control series post-crisis using simple ARIMA(1,1,0)
    (random walk with drift). Falls back to last value on failure.
    """
    from statsmodels.tsa.arima.model import ARIMA

    freeze_dt = pd.Timestamp(freeze_date)
    pre = s.loc[s.index < freeze_dt].dropna()

    # Align to weekly first
    wk = s.resample(**_WEEKLY_RESAMPLE).mean().reindex(weekly_index, method="ffill")
    pre_wk = wk.loc[wk.index < freeze_dt].dropna()

    if len(pre_wk) < 30:
        # Too short for ARIMA, just freeze
        last_val = pre_wk.iloc[-1] if len(pre_wk) > 0 else 0
        wk.loc[wk.index >= freeze_dt] = last_val
        return wk

    try:
        model = ARIMA(pre_wk.values, order=(1, 1, 0))
        fit = model.fit()
        fc = fit.forecast(steps=n_ahead)
        post_idx = wk.index[wk.index >= freeze_dt][:n_ahead]
        for i, idx in enumerate(post_idx):
            wk.loc[idx] = fc[i]
        # Fill any remaining
        wk = wk.ffill()
    except Exception:
        # Fallback: linear extrapolation from last 13 weeks
        recent = pre_wk.iloc[-13:]
        x = np.arange(len(recent))
        if len(recent) >= 2:
            slope, intercept = np.polyfit(x, recent.values, 1)
            last_val = recent.iloc[-1]
            post_idx = wk.index[wk.index >= freeze_dt]
            for i, idx in enumerate(post_idx):
                wk.loc[idx] = last_val + slope * (i + 1)
        else:
            wk.loc[wk.index >= freeze_dt] = pre_wk.iloc[-1]

    return wk


def build_weekly_controls_arima(weekly_index, frozen_monthly, frozen_daily, live_daily, freeze_date):
    """
    Approach B: ARIMA-forecasted controls.
    All controls are forecasted from their pre-crisis history using ARIMA(1,1,0),
    so the predicted remainder varies week-to-week post-crisis.
    Returns X_arima DataFrame aligned to weekly_index.
    """
    freeze_dt = pd.Timestamp(freeze_date)
    n_ahead = (weekly_index >= freeze_dt).sum()
    cols = {}

    print("    [ARIMA forecasting controls...]")

    # Monthly controls
    for name, s in frozen_monthly.items():
        daily = s.resample("D").ffill()
        wk_raw = daily.reindex(weekly_index, method="ffill")
        wk = _forecast_series_arima(s, freeze_date, n_ahead, weekly_index)
        cols[name] = wk

    # Daily/weekly frozen controls
    for name, s in frozen_daily.items():
        wk = _forecast_series_arima(s, freeze_date, n_ahead, weekly_index)
        cols[name] = wk

    X_arima = pd.DataFrame(cols, index=weekly_index).ffill().bfill()

    # Fourier harmonics (deterministic calendar features)
    day_of_year = weekly_index.dayofyear.values.astype(float)
    for period_weeks, tag in [(52, 'annual'), (26, 'semiannual'), (13, 'quarterly')]:
        period_days = period_weeks * 7
        X_arima[f'fourier_sin_{tag}'] = np.sin(2 * np.pi * day_of_year / period_days)
        X_arima[f'fourier_cos_{tag}'] = np.cos(2 * np.pi * day_of_year / period_days)

    return X_arima


# ─── 4. STL Decomposition ──────────────────────────────────────────────────
def run_stl(series, period=52, seasonal=7, robust=True, crisis_date=None, log_transform=True):
    """
    Run STL decomposition on pre-crisis data only to avoid crisis contamination
    of the seasonal component. Post-crisis seasonal is projected forward by
    matching ISO calendar weeks from the pre-crisis seasonal.

    If log_transform=True (default), applies log1p() before decomposition so that
    the model operates in multiplicative space. Components are returned in log space;
    the caller must expm1() the reassembled counterfactual back to levels.

    Returns (trend, seasonal, remainder, was_logged) aligned to the FULL series index.
    - trend: STL trend for pre-crisis; NaN post-crisis (extrapolated separately)
    - seasonal: STL seasonal for pre-crisis; calendar-week projection post-crisis
    - remainder: STL remainder for pre-crisis; NaN post-crisis (predicted by Ridge)
    - was_logged: bool — True if log1p was applied (caller uses expm1 to invert)
    """
    if crisis_date is None:
        crisis_date = CRISIS_DATE
    crisis_dt = pd.Timestamp(crisis_date)

    full_index = series.index

    # Log-transform: ensures counterfactual is always positive after expm1()
    # Use log1p for any series with at least 10% positive values — log1p(0) = 0 handles zeros
    was_logged = False
    if log_transform and (series >= 0).all() and (series > 0).mean() > 0.1:
        series = np.log1p(series)
        was_logged = True

    # Split: estimate STL on pre-crisis only
    pre_crisis = series.loc[series.index < crisis_dt].copy()
    pre_crisis = pre_crisis.interpolate(method="linear").ffill().bfill()

    if len(pre_crisis) < 2 * period:
        # Fallback: not enough pre-crisis data, use full sample
        s = series.interpolate(method="linear").ffill().bfill()
        stl = STL(s, period=period, seasonal=seasonal, robust=robust)
        res = stl.fit()
        return res.trend, res.seasonal, res.resid, was_logged

    stl = STL(pre_crisis, period=period, seasonal=seasonal, robust=robust)
    res = stl.fit()

    # --- Extend seasonal to post-crisis via calendar-week mapping ---
    # Build lookup: ISO week → seasonal value (average from last 3 years for stability)
    pre_seasonal = res.seasonal
    week_lookup = {}
    recent_years = pre_seasonal.loc[pre_seasonal.index >= crisis_dt - pd.Timedelta(weeks=156)]  # last 3 years
    for dt_val, s_val in recent_years.items():
        iso_wk = dt_val.isocalendar()[1]
        if iso_wk not in week_lookup:
            week_lookup[iso_wk] = []
        week_lookup[iso_wk].append(s_val)
    week_avg_raw = {wk: np.mean(vals) for wk, vals in week_lookup.items()}

    # Smooth the weekly seasonal profile with a 5-week circular moving average
    # to prevent implausible week-on-week spikes in the counterfactual.
    # STL's seasonal can overfit weekly noise (especially for low-traffic series),
    # creating sharp spikes that produce unrealistic counterfactual trajectories.
    _SEASONAL_SMOOTH_WINDOW = 5
    _half = _SEASONAL_SMOOTH_WINDOW // 2
    _weeks_sorted = sorted(week_avg_raw.keys())
    _n_wks = len(_weeks_sorted)
    _raw_vals = np.array([week_avg_raw[w] for w in _weeks_sorted])
    week_avg = {}
    for idx, wk in enumerate(_weeks_sorted):
        _indices = [(idx + j) % _n_wks for j in range(-_half, _half + 1)]
        week_avg[wk] = float(np.mean(_raw_vals[_indices]))

    # Project seasonal to full index
    seasonal_full = pd.Series(index=full_index, dtype=float)
    seasonal_full.loc[pre_seasonal.index] = pre_seasonal.values
    post_idx = full_index[full_index >= crisis_dt]
    for dt_val in post_idx:
        iso_wk = dt_val.isocalendar()[1]
        seasonal_full.loc[dt_val] = week_avg.get(iso_wk, 0.0)

    # --- Extend trend and remainder with NaN post-crisis ---
    # (trend is extrapolated separately by extrapolate_trend;
    #  remainder is predicted by Ridge regression)
    trend_full = pd.Series(np.nan, index=full_index)
    trend_full.loc[res.trend.index] = res.trend.values

    remainder_full = pd.Series(np.nan, index=full_index)
    remainder_full.loc[res.resid.index] = res.resid.values

    return trend_full, seasonal_full, remainder_full, was_logged


# ─── 4b. Geographic Cross-Series Features ─────────────────────────────────

# Map from target entity → list of feeder chokepoint names whose STL remainder
# should be used as predictive features.  Keys can be chokepoint names, region
# group names (matching the "Persian Gulf Exports" style), or ISO3 country codes.
GEO_LINKAGE = {
    # Chokepoint → feeder chokepoints
    "Cape of Good Hope":      ["Strait of Hormuz", "Suez Canal", "Bab el-Mandeb Strait"],
    "Suez Canal":             ["Bab el-Mandeb Strait", "Strait of Hormuz"],
    "Bab el-Mandeb Strait":   ["Strait of Hormuz"],
    "Malacca Strait":         ["Strait of Hormuz", "Bab el-Mandeb Strait"],
    "Panama Canal":           ["Yucatan Channel"],
    "Gibraltar Strait":       ["Suez Canal", "Bab el-Mandeb Strait"],
    "Dover Strait":           ["Gibraltar Strait", "Suez Canal"],
    "Bosporus Strait":        ["Suez Canal"],
    "Taiwan Strait":          ["Malacca Strait", "Korea Strait"],
    "Korea Strait":           ["Malacca Strait", "Taiwan Strait"],
    "Lombok Strait":          ["Malacca Strait"],
    "Sunda Strait":           ["Malacca Strait"],
    "Makassar Strait":        ["Malacca Strait", "Lombok Strait"],

    # Region → feeder chokepoints
    "Persian Gulf":           ["Strait of Hormuz"],
    "East Asia":              ["Malacca Strait", "Taiwan Strait", "Korea Strait"],
    "Southeast Asia":         ["Malacca Strait", "Strait of Hormuz"],
    "Indian Subcontinent":    ["Strait of Hormuz", "Bab el-Mandeb Strait"],
    "Mediterranean":          ["Suez Canal", "Gibraltar Strait", "Bosporus Strait"],
    "Northwest Europe":       ["Dover Strait", "Gibraltar Strait", "Suez Canal"],
    "North America":          ["Panama Canal", "Yucatan Channel"],
    "Latin America":          ["Panama Canal", "Magellan Strait"],
    "West Africa":            ["Cape of Good Hope", "Gibraltar Strait"],
    "Russia":                 ["Bosporus Strait", "Korea Strait"],
    "Oceania":                ["Malacca Strait", "Lombok Strait"],
}

# Country → feeder chokepoints (ISO3 codes)
COUNTRY_GEO_LINKAGE = {
    # Persian Gulf producers
    "SAU": ["Strait of Hormuz"], "IRQ": ["Strait of Hormuz"],
    "IRN": ["Strait of Hormuz"], "ARE": ["Strait of Hormuz"],
    "KWT": ["Strait of Hormuz"], "QAT": ["Strait of Hormuz"],
    "OMN": ["Strait of Hormuz", "Bab el-Mandeb Strait"],
    "BHR": ["Strait of Hormuz"],
    # East Asia
    "CHN": ["Malacca Strait", "Taiwan Strait", "Korea Strait"],
    "JPN": ["Malacca Strait", "Korea Strait", "Taiwan Strait"],
    "KOR": ["Malacca Strait", "Korea Strait"],
    "TWN": ["Malacca Strait", "Taiwan Strait"],
    # Southeast Asia
    "SGP": ["Malacca Strait"],
    "MYS": ["Malacca Strait"],
    "IDN": ["Malacca Strait", "Lombok Strait", "Sunda Strait"],
    "THA": ["Malacca Strait"],
    "VNM": ["Malacca Strait"],
    "PHL": ["Malacca Strait", "Luzon Strait"],
    # Indian Subcontinent
    "IND": ["Strait of Hormuz", "Bab el-Mandeb Strait", "Malacca Strait"],
    "PAK": ["Strait of Hormuz"],
    "BGD": ["Malacca Strait"],
    "LKA": ["Strait of Hormuz", "Malacca Strait"],
    # Mediterranean
    "TUR": ["Suez Canal", "Bosporus Strait"],
    "EGY": ["Suez Canal", "Bab el-Mandeb Strait"],
    "GRC": ["Suez Canal", "Gibraltar Strait"],
    "ISR": ["Suez Canal"],
    "ITA": ["Suez Canal", "Gibraltar Strait"],
    "ESP": ["Gibraltar Strait", "Suez Canal"],
    # Northwest Europe
    "NLD": ["Dover Strait", "Gibraltar Strait"],
    "GBR": ["Dover Strait", "Gibraltar Strait"],
    "DEU": ["Dover Strait"],
    "FRA": ["Gibraltar Strait", "Dover Strait"],
    "BEL": ["Dover Strait"],
    "NOR": ["Dover Strait"],
    # Americas
    "USA": ["Panama Canal", "Yucatan Channel"],
    "CAN": ["Panama Canal"],
    "MEX": ["Panama Canal", "Yucatan Channel"],
    "BRA": ["Cape of Good Hope", "Panama Canal"],
    "COL": ["Panama Canal"],
    "ARG": ["Magellan Strait"],
    # Africa
    "NGA": ["Cape of Good Hope", "Gibraltar Strait"],
    "AGO": ["Cape of Good Hope"],
    "ZAF": ["Cape of Good Hope"],
    # Oceania
    "AUS": ["Malacca Strait", "Lombok Strait"],
    "NZL": ["Malacca Strait"],
    # Russia
    "RUS": ["Bosporus Strait", "Korea Strait"],
}


def _build_cross_features(series_index, feeder_names, chokepoint_remainders, freeze_date):
    """
    Build geographic cross-series features from feeder chokepoint STL remainders.

    For each feeder chokepoint, adds:
      - contemporaneous remainder (frozen post-crisis)
      - lag-1 remainder (frozen post-crisis)

    Returns a DataFrame aligned to series_index, or empty DataFrame if no feeders available.
    """
    crisis_dt = pd.Timestamp(freeze_date)
    cross_features = {}

    for feeder in feeder_names:
        if feeder not in chokepoint_remainders:
            continue
        # Get the feeder's STL remainder (keyed by chokepoint name)
        feeder_rem = chokepoint_remainders[feeder]
        if feeder_rem is None or feeder_rem.empty:
            continue

        # Align to target series index
        aligned = feeder_rem.reindex(series_index)

        # Freeze post-crisis: use last pre-crisis value
        pre_crisis_vals = aligned.loc[aligned.index < crisis_dt].dropna()
        if len(pre_crisis_vals) < 2:
            continue
        frozen_val = float(pre_crisis_vals.iloc[-1])
        frozen_lag1_val = float(pre_crisis_vals.iloc[-2])

        # Contemporaneous (frozen post-crisis)
        feat_name = f"xfeat_{feeder[:12].replace(' ', '_')}_t0"
        feat = aligned.copy()
        feat.loc[feat.index >= crisis_dt] = frozen_val
        cross_features[feat_name] = feat

        # Lag-1 (frozen post-crisis)
        feat_lag = aligned.shift(1).copy()
        feat_lag.loc[feat_lag.index >= crisis_dt] = frozen_lag1_val
        cross_features[f"xfeat_{feeder[:12].replace(' ', '_')}_t1"] = feat_lag

    if not cross_features:
        return pd.DataFrame(index=series_index)

    return pd.DataFrame(cross_features, index=series_index)


def _get_feeder_chokepoints(entity_name, iso3=None):
    """
    Look up feeder chokepoints for a given entity.
    Tries: exact match on GEO_LINKAGE, then region prefix, then ISO3 country code.
    """
    # Exact match (chokepoint name or region group like "Persian Gulf Exports")
    if entity_name in GEO_LINKAGE:
        return GEO_LINKAGE[entity_name]

    # Region prefix match (e.g. "Persian Gulf Exports" → "Persian Gulf")
    for region_key in GEO_LINKAGE:
        if entity_name.startswith(region_key):
            return GEO_LINKAGE[region_key]

    # Country match via ISO3
    if iso3 and iso3 in COUNTRY_GEO_LINKAGE:
        return COUNTRY_GEO_LINKAGE[iso3]

    # Country match via group name "COUNTRY:China Exports" → extract country name
    if entity_name.startswith("COUNTRY:"):
        # The country name is between "COUNTRY:" and " Exports"/" Imports"
        country_part = entity_name.replace("COUNTRY:", "").rsplit(" ", 1)[0]
        # Try to find ISO3 for this country name in COUNTRY_GEO_LINKAGE
        # (we don't have a reverse lookup, so skip — caller should pass iso3)
        pass

    return []


# ─── 5. Regression and Counterfactual ──────────────────────────────────────
def _add_frozen_ar_lags(X_controls, remainder, freeze_date, n_lags=2):
    """
    Add AR lag features of the STL remainder to the control matrix.
    Post-crisis, lags are frozen at the last pre-crisis remainder values
    to avoid contaminating the counterfactual with crisis-period shocks.
    """
    crisis_dt = pd.Timestamp(freeze_date)
    pre_remainders = remainder.loc[remainder.index < crisis_dt]
    if len(pre_remainders) < n_lags:
        return X_controls

    X = X_controls.copy()
    for lag in range(1, n_lags + 1):
        lag_series = remainder.shift(lag).copy()
        # Freeze: post-crisis lag values use last pre-crisis remainders
        frozen_val = float(pre_remainders.iloc[-lag])
        lag_series.loc[lag_series.index >= crisis_dt] = frozen_val
        X[f"remainder_lag{lag}"] = lag_series

    return X


def fit_residual_model(remainder, X_controls, train_mask, freeze_date=None,
                       cross_features=None):
    """
    Fit Ridge regression: remainder ~ controls + AR(2) lags [+ cross-features] on training window.
    AR lags are frozen at last pre-crisis values for post-crisis prediction.
    freeze_date: date at which to freeze AR lags (defaults to CRISIS_DATE).
    cross_features: optional DataFrame of geographic cross-series features (already frozen).
    Returns fitted model, scaler, and predictions over full index.
    """
    # Add frozen AR lags
    if freeze_date is None:
        freeze_date = CRISIS_DATE
    X_with_ar = _add_frozen_ar_lags(X_controls, remainder, freeze_date, n_lags=2)

    # Append cross-features if provided
    if cross_features is not None and not cross_features.empty:
        X_with_ar = pd.concat([X_with_ar, cross_features.reindex(X_with_ar.index)], axis=1)

    # Align
    common = remainder.index.intersection(X_with_ar.index)
    r = remainder.reindex(common)
    X = X_with_ar.reindex(common)

    train_idx = train_mask.reindex(common).fillna(False)

    X_train = X.loc[train_idx]
    y_train = r.loc[train_idx]

    # Drop any remaining NaNs
    valid = X_train.notna().all(axis=1) & y_train.notna()
    X_train = X_train.loc[valid]
    y_train = y_train.loc[valid]

    if len(X_train) < 20:
        print(f"    WARNING: Only {len(X_train)} training observations, skipping controls")
        return None, None, pd.Series(0, index=common)

    scaler = StandardScaler()
    X_train_sc = scaler.fit_transform(X_train)

    # RidgeCV with leave-one-out CV (efficient analytic solution)
    alphas = [0.01, 0.1, 0.5, 1.0, 5.0, 10.0, 50.0, 100.0]
    model = RidgeCV(alphas=alphas, scoring="r2")
    model.fit(X_train_sc, y_train)

    # Predict over full period
    X_all_sc = scaler.transform(X.fillna(0))
    predicted_remainder = pd.Series(model.predict(X_all_sc), index=common)

    r2_train = model.score(X_train_sc, y_train)
    print(f"    Controls R² (train): {r2_train:.3f}  |  alpha={model.alpha_:.2f}  |  Features: {list(X.columns)}")

    return model, scaler, predicted_remainder


def extrapolate_trend(trend, train_end, n_ahead):
    """
    Extrapolate trend beyond training window using last 13-week linear fit.
    """
    pre = trend.loc[trend.index <= train_end].dropna()
    if len(pre) < 13:
        slope = 0
    else:
        recent = pre.iloc[-13:]
        x = np.arange(len(recent))
        slope = np.polyfit(x, recent.values, 1)[0]

    last_val = pre.iloc[-1]
    post_idx = trend.index[trend.index > train_end][:n_ahead]
    extrap = pd.Series(
        [last_val + slope * (i + 1) for i in range(len(post_idx))],
        index=post_idx
    )
    return pd.concat([pre, extrap])


# ─── 6. Main Pipeline ──────────────────────────────────────────────────────
def run_pipeline(seasonal_param=13):
    """Execute the full nowcasting pipeline (two-pass: STL then Ridge with cross-features)."""
    print("=" * 70)
    print("OIL TRADE FLOW NOWCASTING — HORMUZ CRISIS")
    print("=" * 70)

    # Load data
    weekly_data = load_chokepoint_data()
    frozen_monthly, frozen_daily, live_daily = load_controls()

    crisis_dt = pd.Timestamp(CRISIS_DATE)
    train_start_dt = pd.Timestamp(TRAIN_START)
    train_end_dt = pd.Timestamp(TRAIN_END)

    results = {}

    all_cp_metrics = [
        ("n_tanker", "tanker_count"),
        ("capacity_tanker", "tanker_capacity"),
        ("n_container", "container_count"),
        ("capacity_container", "container_capacity"),
        ("n_dry_bulk", "dry_bulk_count"),
        ("capacity_dry_bulk", "dry_bulk_capacity"),
        ("n_general_cargo", "general_cargo_count"),
        ("capacity_general_cargo", "general_cargo_capacity"),
        ("n_roro", "roro_count"),
        ("capacity_roro", "roro_capacity"),
        ("n_total", "total_count"),
    ]

    # ═══════════════════════════════════════════════════════════════════════
    # PASS 1: Run STL on all chokepoints, store remainders for cross-features
    # ═══════════════════════════════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("PASS 1: STL decomposition on all chokepoints")
    print("=" * 70)

    # chokepoint_remainders[cp_name] = remainder Series (from total_count metric,
    # used as the representative series for cross-feature construction)
    chokepoint_remainders = {}
    # Also cache all STL results for pass 2 to avoid re-running
    stl_cache = {}  # key: (cp_name, label) → (series, trend, seasonal, remainder, was_logged)

    for cp_name, wk_df in weekly_data.items():
        print(f"\n  STL pass: {cp_name}")
        for metric, label in all_cp_metrics:
            if metric not in wk_df.columns:
                continue
            series = wk_df[metric].copy()
            if len(series) < 104:
                continue

            trend, seasonal, remainder, was_logged = run_stl(series, seasonal=seasonal_param)
            stl_cache[(cp_name, label)] = (series, trend, seasonal, remainder, was_logged)

            # Use total_count as the representative remainder for cross-features
            if label == "total_count":
                chokepoint_remainders[cp_name] = remainder
                print(f"    → stored remainder for cross-features ({len(remainder)} obs)")

    print(f"\n  Chokepoints with stored remainders: {list(chokepoint_remainders.keys())}")

    # ═══════════════════════════════════════════════════════════════════════
    # PASS 2: Fit Ridge models with cross-features from feeder chokepoints
    # ═══════════════════════════════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("PASS 2: Ridge regression with geographic cross-features")
    print("=" * 70)

    for cp_name, wk_df in weekly_data.items():
        print(f"\n{'─' * 60}")
        print(f"  CHOKEPOINT: {cp_name}")
        print(f"{'─' * 60}")

        # Determine feeder chokepoints for this target
        feeders = _get_feeder_chokepoints(cp_name)
        if feeders:
            print(f"    Feeders: {feeders}")

        for metric, label in all_cp_metrics:
            cache_key = (cp_name, label)
            if cache_key not in stl_cache:
                continue
            series, trend, seasonal, remainder, was_logged = stl_cache[cache_key]

            print(f"\n  → Metric: {label}")
            print(f"    STL ({'log' if was_logged else 'level'}): trend range [{trend.min():.0f}, {trend.max():.0f}], "
                  f"seasonal amp {(seasonal.max() - seasonal.min()):.2f}, "
                  f"remainder std {remainder.std():.2f}")

            # Build control matrices aligned to this series' index
            X_frozen, X_live = build_weekly_controls(
                series.index, frozen_monthly, frozen_daily, live_daily,
                freeze_date=CRISIS_DATE
            )

            # Build geographic cross-features
            cross_feats = _build_cross_features(
                series.index, feeders, chokepoint_remainders, CRISIS_DATE
            )
            n_xfeats = len(cross_feats.columns)
            if n_xfeats > 0:
                print(f"    Cross-features: {n_xfeats} ({list(cross_feats.columns)})")

            # Training mask
            train_mask = pd.Series(False, index=series.index)
            train_mask[(series.index >= train_start_dt) & (series.index <= train_end_dt)] = True

            # Fit primary model (frozen controls + cross-features)
            print("    [Primary model — frozen controls + cross-features]")
            model_p, scaler_p, pred_remainder_p = fit_residual_model(
                remainder, X_frozen, train_mask, cross_features=cross_feats
            )

            # Fit sensitivity model (frozen + live controls + cross-features)
            X_combined = pd.concat([X_frozen, X_live], axis=1)
            print("    [Sensitivity model — all controls + cross-features]")
            model_s, scaler_s, pred_remainder_s = fit_residual_model(
                remainder, X_combined, train_mask, cross_features=cross_feats
            )

            # Approach A: Hybrid (exogenous live, endogenous frozen)
            X_hybrid = build_weekly_controls_hybrid(
                series.index, frozen_monthly, frozen_daily, live_daily,
                freeze_date=CRISIS_DATE
            )
            print("    [Hybrid model — exogenous live, endogenous frozen + cross-features]")
            model_h, scaler_h, pred_remainder_h = fit_residual_model(
                remainder, X_hybrid, train_mask, cross_features=cross_feats
            )

            # Approach B: ARIMA-forecasted controls
            X_arima = build_weekly_controls_arima(
                series.index, frozen_monthly, frozen_daily, live_daily,
                freeze_date=CRISIS_DATE
            )
            print("    [ARIMA model — forecasted controls + cross-features]")
            model_a, scaler_a, pred_remainder_a = fit_residual_model(
                remainder, X_arima, train_mask, cross_features=cross_feats
            )

            # Variance decomposition (sequential R² on training window)
            train_idx = train_mask & train_mask  # boolean
            if was_logged:
                y_train_decomp = np.log1p(series.clip(lower=0)).reindex(trend.index)
            else:
                y_train_decomp = series.reindex(trend.index)
            y_t = y_train_decomp[train_idx.reindex(y_train_decomp.index, fill_value=False)]
            trend_t = trend.reindex(y_t.index, fill_value=0)
            seasonal_t = seasonal.reindex(y_t.index, fill_value=0)
            pred_rem_t = pred_remainder_p.reindex(y_t.index, fill_value=0)

            ss_total = float(np.var(y_t)) if len(y_t) > 1 else 1.0
            if ss_total > 0 and len(y_t) > 1:
                r2_trend = max(0, 1 - float(np.var(y_t - trend_t)) / ss_total)
                r2_trend_seasonal = max(0, 1 - float(np.var(y_t - trend_t - seasonal_t)) / ss_total)
                r2_full = max(0, 1 - float(np.var(y_t - trend_t - seasonal_t - pred_rem_t)) / ss_total)
            else:
                r2_trend = r2_trend_seasonal = r2_full = 0.0
            r2_controls_marginal = r2_full - r2_trend_seasonal
            r2_unexplained = 1.0 - r2_full
            variance_decomp = {
                "r2_trend": round(r2_trend, 4),
                "r2_trend_seasonal": round(r2_trend_seasonal, 4),
                "r2_full": round(r2_full, 4),
                "r2_controls_marginal": round(max(0, r2_controls_marginal), 4),
                "r2_unexplained": round(max(0, r2_unexplained), 4),
            }
            print(f"    Variance: trend={r2_trend:.1%}, +seasonal={r2_trend_seasonal:.1%}, "
                  f"+controls={r2_full:.1%}, unexplained={r2_unexplained:.1%}")

            # Build counterfactual
            n_post = (series.index > train_end_dt).sum()
            trend_extrap = extrapolate_trend(trend, train_end_dt, n_post)

            seasonal_cf = seasonal.copy()
            trend_aligned = trend_extrap.reindex(series.index).ffill()
            seasonal_aligned = seasonal_cf.reindex(series.index, fill_value=0)

            # Primary counterfactual (all frozen) — in log space if was_logged
            cf_primary_log = trend_aligned + seasonal_aligned + \
                         pred_remainder_p.reindex(series.index, fill_value=0)

            # Sensitivity counterfactual (live daily financial controls)
            cf_sensitivity_log = trend_aligned + seasonal_aligned + \
                            pred_remainder_s.reindex(series.index, fill_value=0)

            # Hybrid counterfactual (exogenous live, endogenous frozen)
            cf_hybrid_log = trend_aligned + seasonal_aligned + \
                        pred_remainder_h.reindex(series.index, fill_value=0)

            # ARIMA counterfactual (all controls forecasted)
            cf_arima_log = trend_aligned + seasonal_aligned + \
                       pred_remainder_a.reindex(series.index, fill_value=0)

            # Convert counterfactuals back to level space and clamp to non-negative
            if was_logged:
                cf_primary = np.expm1(cf_primary_log).clip(lower=0)
                cf_sensitivity = np.expm1(cf_sensitivity_log).clip(lower=0)
                cf_hybrid = np.expm1(cf_hybrid_log).clip(lower=0)
                cf_arima = np.expm1(cf_arima_log).clip(lower=0)
            else:
                cf_primary = cf_primary_log.clip(lower=0)
                cf_sensitivity = cf_sensitivity_log.clip(lower=0)
                cf_hybrid = cf_hybrid_log.clip(lower=0)
                cf_arima = cf_arima_log.clip(lower=0)

            # Crisis deviation (in level space)
            deviation_primary = series - cf_primary
            deviation_sensitivity = series - cf_sensitivity

            # Post-crisis stats for all models
            post_mask = series.index >= crisis_dt
            if post_mask.any():
                actual_post = series.loc[post_mask].mean()
                for cf_name, cf in [("frozen", cf_primary), ("sensitivity", cf_sensitivity),
                                     ("hybrid", cf_hybrid), ("arima", cf_arima)]:
                    cf_post = cf.loc[post_mask].mean()
                    if cf_post > 0:
                        pct = (actual_post - cf_post) / cf_post * 100
                    else:
                        pct = 0
                    pct = max(-999, min(999, pct))
                    print(f"    {cf_name:12s}: cf_avg={cf_post:.0f}, dev={pct:+.1f}%")

            # Pre-crisis average (52-week lookback from crisis onset)
            pre_52w_mask = (series.index >= crisis_dt - pd.Timedelta(weeks=52)) & (series.index < crisis_dt)
            pre_crisis_avg = float(series.loc[pre_52w_mask].mean()) if pre_52w_mask.any() else float(series.mean())

            # Store results (all in level space for dashboard consumption)
            key = f"{cp_name}|{label}"
            results[key] = {
                "chokepoint": cp_name,
                "metric": label,
                "dates": [d.strftime("%Y-%m-%d") for d in series.index],
                "actual": series.values.tolist(),
                "trend": trend_aligned.reindex(series.index).values.tolist(),
                "seasonal": seasonal_aligned.reindex(series.index).values.tolist(),
                "remainder": pred_remainder_p.reindex(series.index, fill_value=0).values.tolist(),
                "counterfactual_primary": cf_primary.reindex(series.index).values.tolist(),
                "counterfactual_sensitivity": cf_sensitivity.reindex(series.index).values.tolist(),
                "counterfactual_hybrid": cf_hybrid.reindex(series.index).values.tolist(),
                "counterfactual_arima": cf_arima.reindex(series.index).values.tolist(),
                "deviation_primary": deviation_primary.reindex(series.index).values.tolist(),
                "deviation_sensitivity": deviation_sensitivity.reindex(series.index).values.tolist(),
                "crisis_date": CRISIS_DATE,
                "train_end": TRAIN_END,
                "pre_crisis_avg": round(pre_crisis_avg, 1),
                "variance_decomp": variance_decomp,
            }

    # ─── Regional aggregate port analysis ────────────────────────────────────
    results = run_all_port_group_aggregates(results, frozen_monthly, frozen_daily, live_daily,
                                            seasonal_param=seasonal_param, chokepoint_remainders=chokepoint_remainders)

    # ─── Country-level aggregate port analysis ────────────────────────────
    print(f"\n{'─' * 60}")
    print(f"  COUNTRY-LEVEL AGGREGATE PORT ANALYSIS (Top 50)")
    print(f"{'─' * 60}")
    results = run_country_aggregates(results, frozen_monthly, frozen_daily, live_daily,
                                      seasonal_param=seasonal_param, chokepoint_remainders=chokepoint_remainders)

    # ─── Per-port deviations (all groups, STL + naive) ────────────────────
    print(f"\n{'─' * 60}")
    print(f"  PER-PORT DEVIATION BREAKDOWN (STL + Naive)")
    print(f"{'─' * 60}")
    results = run_per_port_deviations(results, frozen_monthly, frozen_daily, live_daily,
                                       seasonal_param=seasonal_param, chokepoint_remainders=chokepoint_remainders)

    print(f"\n{'─' * 60}")
    print(f"  GLOBAL TOP-50 EXPORT / IMPORT PORT DEVIATIONS")
    print(f"{'─' * 60}")
    results = run_top_ports_global(results, frozen_monthly, frozen_daily, live_daily,
                                    seasonal_param=seasonal_param, chokepoint_remainders=chokepoint_remainders)

    # ─── Save results ───────────────────────────────────────────────────────
    output_fp = os.path.join(OUTPUT_DIR, f"nowcast_results_s{seasonal_param}.json")
    with open(output_fp, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\nResults saved to {output_fp}")

    # Also save a summary CSV
    save_summary_csv(results)

    return results


# ─── 8. Per-Port Deviation Breakdown (STL + Controls + Naive) ────────────────
def _run_naive_port(weekly_series):
    """Compute naive same-calendar-week baseline deviation for comparison."""
    crisis_dt = pd.Timestamp(CRISIS_DATE)
    series = weekly_series.loc[weekly_series.index >= "2019-01-07"].copy()

    pre_crisis = series[(series.index >= "2023-01-01") & (series.index < crisis_dt)]
    post_crisis = series[series.index >= crisis_dt]

    if len(pre_crisis) < 52 or len(post_crisis) == 0:
        return None

    # Calendar-week average baseline
    pre_wk = pre_crisis.copy()
    pre_wk.index = pre_wk.index.isocalendar().week.values
    baseline_by_week = pre_wk.groupby(pre_wk.index).mean()

    # Use most recent week only (post-crisis average is contaminated by onset week)
    latest_week = post_crisis.index[-1]
    latest_iso_week = latest_week.isocalendar()[1]
    expected_latest = float(baseline_by_week.get(latest_iso_week, pre_crisis.mean()))
    actual_latest = float(post_crisis.iloc[-1])
    # Normalize by pre-crisis mean to avoid division by near-zero baselines
    pre_crisis_mean = float(pre_crisis.mean())
    pct_dev = (actual_latest - expected_latest) / pre_crisis_mean * 100 if pre_crisis_mean != 0 else 0

    return {"actual_avg": actual_latest, "cf_avg": expected_latest, "pct_dev": pct_dev}


def _run_stl_port(port_name, weekly_series, frozen_monthly, frozen_daily, live_daily, seasonal_param=7,
                   chokepoint_remainders=None, port_iso3=None):
    """Run STL + controls on a single port's weekly series."""
    crisis_dt = pd.Timestamp(CRISIS_DATE)
    train_start_dt = pd.Timestamp(TRAIN_START)
    train_end_dt = pd.Timestamp(TRAIN_END)

    series = weekly_series.loc[weekly_series.index >= "2019-01-07"].copy()

    if len(series) < 104:
        return None

    nonzero_frac = (series > 0).mean()
    if nonzero_frac < 0.3:
        return None

    try:
        trend, seasonal, remainder, was_logged = run_stl(series, period=52, seasonal=seasonal_param, robust=True)
    except Exception:
        return None

    X_frozen, _ = build_weekly_controls(
        series.index, frozen_monthly, frozen_daily, live_daily,
        freeze_date=CRISIS_DATE
    )

    # Build cross-features using port's country ISO3
    cross_feats = pd.DataFrame(index=series.index)
    if chokepoint_remainders and port_iso3:
        feeders = _get_feeder_chokepoints(port_name, iso3=port_iso3)
        cross_feats = _build_cross_features(
            series.index, feeders, chokepoint_remainders, CRISIS_DATE
        )

    train_mask = pd.Series(False, index=series.index)
    train_mask[(series.index >= train_start_dt) & (series.index <= train_end_dt)] = True

    _, _, pred_remainder_p = fit_residual_model(remainder, X_frozen, train_mask,
                                                 cross_features=cross_feats)

    n_post = (series.index > train_end_dt).sum()
    trend_extrap = extrapolate_trend(trend, train_end_dt, n_post)

    cf_log = trend_extrap.reindex(series.index).fillna(method="ffill") + \
                 seasonal.reindex(series.index, fill_value=0) + \
                 pred_remainder_p.reindex(series.index, fill_value=0)

    # Convert counterfactual back to level space and clamp to non-negative
    cf_primary = (np.expm1(cf_log) if was_logged else cf_log).clip(lower=0)

    post_mask = series.index >= crisis_dt
    if not post_mask.any():
        return None

    # Pre-crisis 52-week average
    pre_52w_mask = (series.index >= crisis_dt - pd.Timedelta(weeks=52)) & (series.index < crisis_dt)
    pre_crisis_avg = float(series.loc[pre_52w_mask].mean()) if pre_52w_mask.any() else float(series.mean())

    # Use most recent week only
    actual_latest = float(series.loc[post_mask].iloc[-1])
    cf_latest = float(cf_primary.loc[post_mask].iloc[-1])
    # Safeguard: if counterfactual is negative or zero (can happen with additive STL
    # when log-space not applied), fall back to pre-crisis average as denominator
    if cf_latest > 0:
        pct_dev = (actual_latest - cf_latest) / cf_latest * 100
    elif pre_crisis_avg > 0:
        pct_dev = (actual_latest - cf_latest) / pre_crisis_avg * 100
    else:
        pct_dev = 0
    # Cap extreme deviations at ±999%
    pct_dev = max(-999, min(999, pct_dev))

    # Variance decomposition (sequential R² on training window)
    if was_logged:
        y_train_decomp = np.log1p(series.clip(lower=0)).reindex(trend.index)
    else:
        y_train_decomp = series.reindex(trend.index)
    y_t = y_train_decomp[train_mask.reindex(y_train_decomp.index, fill_value=False)]
    trend_t = trend.reindex(y_t.index, fill_value=0)
    seasonal_t = seasonal.reindex(y_t.index, fill_value=0)
    pred_rem_t = pred_remainder_p.reindex(y_t.index, fill_value=0)

    ss_total = float(np.var(y_t)) if len(y_t) > 1 else 1.0
    if ss_total > 0 and len(y_t) > 1:
        r2_trend = max(0, 1 - float(np.var(y_t - trend_t)) / ss_total)
        r2_trend_seasonal = max(0, 1 - float(np.var(y_t - trend_t - seasonal_t)) / ss_total)
        r2_full = max(0, 1 - float(np.var(y_t - trend_t - seasonal_t - pred_rem_t)) / ss_total)
    else:
        r2_trend = r2_trend_seasonal = r2_full = 0.0
    r2_controls_marginal = r2_full - r2_trend_seasonal
    r2_unexplained = 1.0 - r2_full
    variance_decomp = {
        "r2_trend": round(r2_trend, 4),
        "r2_trend_seasonal": round(r2_trend_seasonal, 4),
        "r2_full": round(r2_full, 4),
        "r2_controls_marginal": round(max(0, r2_controls_marginal), 4),
        "r2_unexplained": round(max(0, r2_unexplained), 4),
    }

    # Full time series for dashboard charts (last 52 weeks before crisis + all post-crisis)
    chart_start = crisis_dt - pd.Timedelta(weeks=52)
    chart_mask = series.index >= chart_start
    chart_dates = [d.strftime("%Y-%m-%d") for d in series.index[chart_mask]]
    chart_actual = [round(float(v), 1) if pd.notna(v) else None for v in series.loc[chart_mask]]
    chart_cf = [round(float(v), 1) if pd.notna(v) else None for v in cf_primary.reindex(series.index).loc[chart_mask]]

    return {
        "actual_avg": actual_latest, "cf_avg": cf_latest, "pct_dev": pct_dev,
        "pre_crisis_avg": pre_crisis_avg,
        "dates": chart_dates, "actual": chart_actual, "counterfactual": chart_cf,
        "variance_decomp": variance_decomp,
    }


def _analyze_port_group(df, iso_list, metric_col, top_n, group_label,
                        frozen_monthly, frozen_daily, live_daily, seasonal_param=7,
                        chokepoint_remainders=None):
    """
    Generic per-port analysis for a group of countries.
    Returns list of port deviation dicts with both STL and naive results, sorted by STL deviation.
    """
    sub = df[df["ISO3"].isin(iso_list)].copy()
    port_totals = sub.groupby("portname")[metric_col].sum().sort_values(ascending=False)
    top_ports = port_totals.head(top_n).index.tolist()
    print(f"  Top {group_label} ports: {top_ports}")

    port_devs = []
    for port in top_ports:
        port_data = sub[sub["portname"] == port].set_index("date")[metric_col].sort_index()
        daily_raw = port_data.copy()
        weekly = _resample_weekly_split(port_data, crisis_date=CRISIS_DATE, agg="mean")
        weekly = _drop_incomplete_trailing_weeks(daily_raw, weekly)

        print(f"    {port} ({len(weekly)} weeks)...", end=" ")

        port_iso3 = sub[sub["portname"] == port]["ISO3"].iloc[0]
        stl_res = _run_stl_port(port, weekly, frozen_monthly, frozen_daily, live_daily, seasonal_param=seasonal_param,
                                 chokepoint_remainders=chokepoint_remainders, port_iso3=port_iso3)
        naive_res = _run_naive_port(weekly)

        if stl_res is None and naive_res is None:
            print("SKIP")
            continue

        entry = {
            "port": port,
            "iso3": sub[sub["portname"] == port]["ISO3"].iloc[0],
        }

        if stl_res is not None:
            entry["stl_cf"] = round(stl_res["cf_avg"], 1)
            entry["stl_actual"] = round(stl_res["actual_avg"], 1)
            entry["stl_pct"] = round(stl_res["pct_dev"], 1)
            entry["pre_crisis_avg"] = round(stl_res["pre_crisis_avg"], 1)
            entry["post_crisis_avg"] = entry["stl_actual"]
            entry["deviation_pct"] = entry["stl_pct"]
            # Time series for inline charts
            if "dates" in stl_res:
                entry["dates"] = stl_res["dates"]
                entry["actual"] = stl_res["actual"]
                entry["counterfactual"] = stl_res["counterfactual"]
            if "variance_decomp" in stl_res:
                entry["variance_decomp"] = stl_res["variance_decomp"]

        if naive_res is not None:
            entry["naive_cf"] = round(naive_res["cf_avg"], 1)
            entry["naive_actual"] = round(naive_res["actual_avg"], 1)
            entry["naive_pct"] = round(naive_res["pct_dev"], 1)

        port_devs.append(entry)
        stl_str = f"STL {stl_res['pct_dev']:+.1f}%" if stl_res else "STL: N/A"
        naive_str = f"naive {naive_res['pct_dev']:+.1f}%" if naive_res else "naive: N/A"
        print(f"{stl_str} | {naive_str}")

    # Sort by STL deviation (most negative first)
    port_devs.sort(key=lambda x: x.get("stl_pct", x.get("naive_pct", 0)))

    return port_devs


def run_per_port_deviations(results, frozen_monthly, frozen_daily, live_daily, seasonal_param=7,
                             chokepoint_remainders=None):
    """Run STL + naive dual-method per-port analysis for all port groups."""
    fp = os.path.join(DATA_DIR, "portwatch", "Daily_Ports_Data.csv")

    print("  Loading port data for per-port STL breakdown...")
    cols = ["date", "portname", "ISO3",
            "export_tanker", "import_tanker", "portcalls_tanker",
            "export_container", "import_container", "portcalls_container",
            "export_dry_bulk", "import_dry_bulk", "portcalls_dry_bulk",
            "export_general_cargo", "import_general_cargo", "portcalls_general_cargo",
            "export_roro", "import_roro", "portcalls_roro",
            "portcalls"]
    df = pd.read_csv(fp, usecols=lambda c: c in cols)
    df["date"] = pd.to_datetime(df["date"])
    numeric_cols = [c for c in df.columns if c not in ("date", "portname", "ISO3")]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    # Port group definitions
    PER_PORT_REGIONS = [
        ("persian_gulf",        ["SAU", "IRQ", "IRN", "ARE", "KWT", "QAT", "OMN", "BHR"], "Persian Gulf",        10),
        ("east_asia",           ["CHN", "JPN", "KOR", "TWN"],                              "East Asia",           10),
        ("southeast_asia",      ["SGP", "MYS", "IDN", "THA", "VNM", "PHL"],               "Southeast Asia",      10),
        ("indian_subcontinent", ["IND", "PAK", "BGD", "LKA"],                              "Indian Subcontinent",  8),
        ("mediterranean",       ["TUR", "EGY", "GRC", "ISR", "LBN", "ITA", "ESP"],        "Mediterranean",        8),
        ("nw_europe",           ["NLD", "GBR", "DEU", "FRA", "BEL", "NOR", "PRT", "DZA"], "Northwest Europe",     8),
        ("north_america",       ["USA", "CAN", "MEX"],                                     "North America",       10),
        ("latin_america",       ["BRA", "COL", "GUY", "TTO", "ECU", "VEN", "ARG"],        "Latin America",       10),
        ("west_africa",         ["NGA", "AGO", "GHA", "GNQ", "COG", "CMR", "GAB"],        "West Africa",          8),
        ("russia",              ["RUS"],                                                    "Russia",               8),
        ("oceania",             ["AUS", "NZL"],                                             "Oceania",              8),
    ]

    # Define all metric/direction combos to analyze
    METRIC_COMBOS = [
        # (direction, metric_col, result_key_suffix)
        # Tanker
        ("export", "export_tanker", "export_deviations"),
        ("import", "import_tanker", "import_deviations"),
        ("portcalls", "portcalls_tanker", "portcalls_tanker_deviations"),
        # Container
        ("export", "export_container", "export_container_deviations"),
        ("import", "import_container", "import_container_deviations"),
        ("portcalls", "portcalls_container", "portcalls_container_deviations"),
        # Dry Bulk
        ("export", "export_dry_bulk", "export_dry_bulk_deviations"),
        ("import", "import_dry_bulk", "import_dry_bulk_deviations"),
        ("portcalls", "portcalls_dry_bulk", "portcalls_dry_bulk_deviations"),
        # General Cargo
        ("export", "export_general_cargo", "export_general_cargo_deviations"),
        ("import", "import_general_cargo", "import_general_cargo_deviations"),
        ("portcalls", "portcalls_general_cargo", "portcalls_general_cargo_deviations"),
        # RoRo
        ("export", "export_roro", "export_roro_deviations"),
        ("import", "import_roro", "import_roro_deviations"),
        ("portcalls", "portcalls_roro", "portcalls_roro_deviations"),
        # Total (portcalls only)
        ("portcalls", "portcalls", "portcalls_total_deviations"),
    ]

    for slug, iso_list, label, top_n in PER_PORT_REGIONS:
        for direction, metric, suffix in METRIC_COMBOS:
            result_key = f"_{slug}_{suffix}"
            group_label = f"{label} {direction} ({metric})"
            print(f"\n  --- {group_label.upper()} PORTS ---")
            devs = _analyze_port_group(
                df, iso_list, metric, top_n, group_label,
                frozen_monthly, frozen_daily, live_daily, seasonal_param=seasonal_param,
                chokepoint_remainders=chokepoint_remainders
            )
            results[result_key] = devs

    return results


# ─── 8c. Top-50 Global Export/Import Port Deviations ─────────────────────────

# Region mapping: ISO3 -> region label
ISO3_REGION = {
    # Gulf / Middle East
    "SAU": "Gulf", "IRQ": "Gulf", "IRN": "Gulf", "ARE": "Gulf",
    "KWT": "Gulf", "QAT": "Gulf", "OMN": "Gulf", "BHR": "Gulf",
    "YEM": "Gulf", "JOR": "Med",
    # East Asia
    "CHN": "East Asia", "JPN": "East Asia", "KOR": "East Asia",
    "TWN": "East Asia", "HKG": "East Asia", "MNG": "East Asia",
    # SE Asia / Oceania
    "THA": "SE Asia", "SGP": "SE Asia", "IDN": "SE Asia", "MYS": "SE Asia",
    "VNM": "SE Asia", "PHL": "SE Asia", "MMR": "SE Asia", "KHM": "SE Asia",
    "AUS": "Oceania", "NZL": "Oceania",
    # Indian Subcontinent
    "IND": "S. Asia", "PAK": "S. Asia", "LKA": "S. Asia", "BGD": "S. Asia",
    # Mediterranean
    "TUR": "Med", "EGY": "Med", "GRC": "Med", "ISR": "Med", "LBN": "Med",
    "HRV": "Med", "MLT": "Med", "CYP": "Med", "TUN": "Med", "LBY": "Med",
    "DZA": "Med", "MAR": "N. Africa",
    # Europe
    "NLD": "Europe", "GBR": "Europe", "DEU": "Europe", "FRA": "Europe",
    "ITA": "Europe", "ESP": "Europe", "BEL": "Europe", "PRT": "Europe",
    "POL": "Europe", "SWE": "Europe", "NOR": "Europe", "DNK": "Europe",
    "FIN": "Europe", "IRL": "Europe", "ROU": "Europe", "BGR": "Europe",
    "LTU": "Europe", "LVA": "Europe", "EST": "Europe",
    # Americas
    "USA": "N. America", "CAN": "N. America", "MEX": "LatAm",
    "BRA": "LatAm", "COL": "LatAm", "ARG": "LatAm", "VEN": "LatAm",
    "ECU": "LatAm", "PER": "LatAm", "CHL": "LatAm", "GUY": "LatAm",
    "TTO": "LatAm", "SUR": "LatAm",
    # West Africa
    "NGA": "W. Africa", "AGO": "W. Africa", "GHA": "W. Africa",
    "GNQ": "W. Africa", "COG": "W. Africa", "CMR": "W. Africa",
    "GAB": "W. Africa", "CIV": "W. Africa", "SEN": "W. Africa",
    # East / South Africa
    "ZAF": "S. Africa", "MOZ": "E. Africa", "TZA": "E. Africa",
    "KEN": "E. Africa", "SDN": "E. Africa", "DJI": "E. Africa",
    # Russia / CIS
    "RUS": "Russia", "KAZ": "C. Asia", "AZE": "C. Asia", "TKM": "C. Asia",
    "GEO": "C. Asia", "UKR": "Europe",
}


def run_top_ports_global(results, frozen_monthly, frozen_daily, live_daily, seasonal_param=7,
                          chokepoint_remainders=None):
    """Compute deviation for the top 50 export/import/portcalls ports globally (tanker + container)."""
    fp = os.path.join(DATA_DIR, "portwatch", "Daily_Ports_Data.csv")

    print("\n  Loading port data for global top-50 rankings...")
    cols = ["date", "portname", "ISO3",
            "export_tanker", "import_tanker", "portcalls_tanker",
            "export_container", "import_container", "portcalls_container",
            "export_dry_bulk", "import_dry_bulk", "portcalls_dry_bulk",
            "export_general_cargo", "import_general_cargo", "portcalls_general_cargo",
            "export_roro", "import_roro", "portcalls_roro",
            "portcalls"]
    df = pd.read_csv(fp, usecols=lambda c: c in cols)
    df["date"] = pd.to_datetime(df["date"])
    numeric_cols = [c for c in df.columns if c not in ("date", "portname", "ISO3")]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    # Load port coordinates
    ports_meta_fp = os.path.join(DATA_DIR, "portwatch", "Ports.csv")
    ports_meta = pd.read_csv(ports_meta_fp, usecols=["portname", "lat", "lon"])
    port_coords = ports_meta.drop_duplicates("portname").set_index("portname")

    TOP_METRICS = [
        # Tanker
        ("export", "export_tanker", "_top50_export_ports"),
        ("import", "import_tanker", "_top50_import_ports"),
        ("portcalls", "portcalls_tanker", "_top50_portcalls_tanker_ports"),
        # Container
        ("export", "export_container", "_top50_export_container_ports"),
        ("import", "import_container", "_top50_import_container_ports"),
        ("portcalls", "portcalls_container", "_top50_portcalls_container_ports"),
        # Dry Bulk
        ("export", "export_dry_bulk", "_top50_export_dry_bulk_ports"),
        ("import", "import_dry_bulk", "_top50_import_dry_bulk_ports"),
        ("portcalls", "portcalls_dry_bulk", "_top50_portcalls_dry_bulk_ports"),
        # General Cargo
        ("export", "export_general_cargo", "_top50_export_general_cargo_ports"),
        ("import", "import_general_cargo", "_top50_import_general_cargo_ports"),
        ("portcalls", "portcalls_general_cargo", "_top50_portcalls_general_cargo_ports"),
        # RoRo
        ("export", "export_roro", "_top50_export_roro_ports"),
        ("import", "import_roro", "_top50_import_roro_ports"),
        ("portcalls", "portcalls_roro", "_top50_portcalls_roro_ports"),
        # Total (portcalls only)
        ("portcalls", "portcalls", "_top50_portcalls_total_ports"),
    ]

    for direction, metric_col, result_key in TOP_METRICS:
        if metric_col not in df.columns:
            print(f"\n  SKIP top-50 {direction}/{metric_col} — column not in data")
            continue
        print(f"\n  === TOP 50 {metric_col.upper()} PORTS (globally) ===")

        # Rank ports by total volume across entire dataset
        port_totals = df.groupby("portname")[metric_col].sum().sort_values(ascending=False)
        top_ports = port_totals.head(50).index.tolist()
        print(f"  Selected {len(top_ports)} ports by total {direction} tonnage")

        port_devs = []
        for port in top_ports:
            port_sub = df[df["portname"] == port]
            iso3 = port_sub["ISO3"].iloc[0]
            region = ISO3_REGION.get(iso3, iso3)
            port_data = port_sub.set_index("date")[metric_col].sort_index()
            daily_raw = port_data.copy()
            weekly = _resample_weekly_split(port_data, crisis_date=CRISIS_DATE, agg="mean")
            weekly = _drop_incomplete_trailing_weeks(daily_raw, weekly)

            print(f"    {port} [{iso3}/{region}] ({len(weekly)} weeks)...", end=" ")

            stl_res = _run_stl_port(port, weekly, frozen_monthly, frozen_daily, live_daily, seasonal_param=seasonal_param,
                                     chokepoint_remainders=chokepoint_remainders, port_iso3=iso3)
            naive_res = _run_naive_port(weekly)

            if stl_res is None and naive_res is None:
                print("SKIP")
                continue

            entry = {
                "port": port,
                "iso3": iso3,
                "region": region,
            }

            # Add coordinates if available
            if port in port_coords.index:
                entry["lat"] = round(float(port_coords.loc[port, "lat"]), 4)
                entry["lon"] = round(float(port_coords.loc[port, "lon"]), 4)

            if stl_res is not None:
                entry["stl_pct"] = round(stl_res["pct_dev"], 1)
                entry["stl_actual"] = round(stl_res["actual_avg"], 1)
                entry["stl_cf"] = round(stl_res["cf_avg"], 1)
                entry["pre_crisis_avg"] = round(stl_res["pre_crisis_avg"], 1)
                # Time series for inline charts
                if "dates" in stl_res:
                    entry["dates"] = stl_res["dates"]
                    entry["actual"] = stl_res["actual"]
                    entry["counterfactual"] = stl_res["counterfactual"]
                if "variance_decomp" in stl_res:
                    entry["variance_decomp"] = stl_res["variance_decomp"]
            if naive_res is not None:
                entry["naive_pct"] = round(naive_res["pct_dev"], 1)

            port_devs.append(entry)
            stl_str = f"STL {stl_res['pct_dev']:+.1f}%" if stl_res else "STL: N/A"
            naive_str = f"naive {naive_res['pct_dev']:+.1f}%" if naive_res else "naive: N/A"
            print(f"{stl_str} | {naive_str}")

        # Sort by STL deviation (most negative first)
        port_devs.sort(key=lambda x: x.get("stl_pct", x.get("naive_pct", 0)))

        results[result_key] = port_devs
        print(f"  → {len(port_devs)} {metric_col} ports with valid deviations")

    # ── Fill missing port×VT portcalls combos ──────────────────────────────
    # For multi-select aggregation, ports need data for all vessel types they
    # have activity in — not just the VTs where they made the top-50 list.
    # Collect the union of all ports across VT-specific portcalls lists, then
    # for each port missing from a VT list, run STL/naive if viable or use
    # pre-crisis average as counterfactual.
    VT_PORTCALLS_KEYS = [
        ("portcalls_tanker", "_top50_portcalls_tanker_ports"),
        ("portcalls_container", "_top50_portcalls_container_ports"),
        ("portcalls_dry_bulk", "_top50_portcalls_dry_bulk_ports"),
        ("portcalls_general_cargo", "_top50_portcalls_general_cargo_ports"),
        ("portcalls_roro", "_top50_portcalls_roro_ports"),
    ]

    # Build union of all ports that appear in at least one VT portcalls list
    all_port_names = set()
    port_meta = {}  # port -> {iso3, region, lat, lon}
    for _, rk in VT_PORTCALLS_KEYS:
        for entry in results.get(rk, []):
            pname = entry.get("port", "")
            if pname:
                all_port_names.add(pname)
                if pname not in port_meta:
                    port_meta[pname] = {
                        "iso3": entry.get("iso3", ""),
                        "region": entry.get("region", ""),
                        "lat": entry.get("lat"),
                        "lon": entry.get("lon"),
                    }

    # For each VT, find ports that are in the union but not in this VT's list
    fill_count = 0
    for metric_col, result_key in VT_PORTCALLS_KEYS:
        existing_ports = {e["port"] for e in results.get(result_key, [])}
        missing_ports = all_port_names - existing_ports
        if not missing_ports:
            continue

        print(f"\n  === FILL {metric_col.upper()} for {len(missing_ports)} non-top-50 ports ===")

        for port in sorted(missing_ports):
            if metric_col not in df.columns:
                continue
            port_sub = df[df["portname"] == port]
            if port_sub.empty:
                continue

            iso3 = port_sub["ISO3"].iloc[0]
            region = ISO3_REGION.get(iso3, iso3)
            port_data = port_sub.set_index("date")[metric_col].sort_index()

            # Skip ports with zero activity for this VT
            if port_data.sum() == 0:
                continue

            daily_raw = port_data.copy()
            weekly = _resample_weekly_split(port_data, crisis_date=CRISIS_DATE, agg="mean")
            weekly = _drop_incomplete_trailing_weeks(daily_raw, weekly)

            if len(weekly) < 10:
                continue

            # Try STL first, then naive, then pre-crisis-average fallback
            stl_res = _run_stl_port(port, weekly, frozen_monthly, frozen_daily, live_daily, seasonal_param=seasonal_param,
                                     chokepoint_remainders=chokepoint_remainders, port_iso3=iso3)
            naive_res = _run_naive_port(weekly)

            entry = {
                "port": port,
                "iso3": iso3,
                "region": region,
                "fill": True,  # flag: not in original top-50
            }
            # Add coordinates from meta
            meta = port_meta.get(port, {})
            if meta.get("lat") is not None:
                entry["lat"] = meta["lat"]
                entry["lon"] = meta["lon"]
            elif port in port_coords.index:
                entry["lat"] = round(float(port_coords.loc[port, "lat"]), 4)
                entry["lon"] = round(float(port_coords.loc[port, "lon"]), 4)

            method = "SKIP"
            if stl_res is not None:
                entry["stl_pct"] = round(stl_res["pct_dev"], 1)
                entry["stl_actual"] = round(stl_res["actual_avg"], 1)
                entry["stl_cf"] = round(stl_res["cf_avg"], 1)
                entry["pre_crisis_avg"] = round(stl_res["pre_crisis_avg"], 1)
                if "dates" in stl_res:
                    entry["dates"] = stl_res["dates"]
                    entry["actual"] = stl_res["actual"]
                    entry["counterfactual"] = stl_res["counterfactual"]
                if "variance_decomp" in stl_res:
                    entry["variance_decomp"] = stl_res["variance_decomp"]
                method = f"STL {stl_res['pct_dev']:+.1f}%"
            elif naive_res is not None:
                entry["stl_pct"] = round(naive_res["pct_dev"], 1)
                entry["stl_actual"] = round(naive_res["actual_avg"], 1)
                entry["stl_cf"] = round(naive_res["cf_avg"], 1)
                entry["pre_crisis_avg"] = round(float(weekly.loc[weekly.index < pd.Timestamp(CRISIS_DATE)].mean()), 1)
                entry["naive_pct"] = round(naive_res["pct_dev"], 1)
                entry["counterfactual_method"] = "naive"
                # Build time series with naive baseline as counterfactual
                crisis_dt = pd.Timestamp(CRISIS_DATE)
                chart_start = crisis_dt - pd.Timedelta(weeks=52)
                chart_mask = weekly.index >= chart_start
                chart_dates = [d.strftime("%Y-%m-%d") for d in weekly.index[chart_mask]]
                chart_actual = [round(float(v), 1) if pd.notna(v) else None for v in weekly.loc[chart_mask]]
                # Use pre-crisis average as a flat counterfactual line
                pre_avg = float(weekly.loc[weekly.index < crisis_dt].mean())
                chart_cf = [round(pre_avg, 1)] * len(chart_dates)
                entry["dates"] = chart_dates
                entry["actual"] = chart_actual
                entry["counterfactual"] = chart_cf
                method = f"naive {naive_res['pct_dev']:+.1f}%"
            else:
                # Fallback: use pre-crisis average as counterfactual
                crisis_dt = pd.Timestamp(CRISIS_DATE)
                pre_crisis = weekly[weekly.index < crisis_dt]
                post_crisis = weekly[weekly.index >= crisis_dt]
                if len(pre_crisis) == 0 or len(post_crisis) == 0:
                    continue
                pre_avg = float(pre_crisis.mean())
                actual_latest = float(post_crisis.iloc[-1])
                if pre_avg > 0:
                    pct_dev = (actual_latest - pre_avg) / pre_avg * 100
                else:
                    pct_dev = 0
                pct_dev = max(-999, min(999, pct_dev))
                entry["stl_pct"] = round(pct_dev, 1)
                entry["stl_actual"] = round(actual_latest, 1)
                entry["stl_cf"] = round(pre_avg, 1)
                entry["pre_crisis_avg"] = round(pre_avg, 1)
                entry["counterfactual_method"] = "pre_crisis_avg"
                # Build time series
                chart_start = crisis_dt - pd.Timedelta(weeks=52)
                chart_mask = weekly.index >= chart_start
                chart_dates = [d.strftime("%Y-%m-%d") for d in weekly.index[chart_mask]]
                chart_actual = [round(float(v), 1) if pd.notna(v) else None for v in weekly.loc[chart_mask]]
                chart_cf = [round(pre_avg, 1)] * len(chart_dates)
                entry["dates"] = chart_dates
                entry["actual"] = chart_actual
                entry["counterfactual"] = chart_cf
                method = f"fallback {pct_dev:+.1f}%"

            results.get(result_key, []).append(entry)
            fill_count += 1
            print(f"    {port} [{iso3}] → {method}")

    if fill_count > 0:
        print(f"\n  → Filled {fill_count} non-top-50 port×VT combos (STL/naive/fallback)")

    return results


# ─── 8b. Regional Aggregate Port Analysis ───────────────────────────────────
def _run_aggregate_port_group(df, iso_list, import_or_export, group_name,
                              frozen_monthly, frozen_daily, live_daily, results, seasonal_param=7,
                              chokepoint_remainders=None):
    """Run STL + controls on aggregated port group (similar to chokepoint analysis)."""
    crisis_dt = pd.Timestamp(CRISIS_DATE)
    train_start_dt = pd.Timestamp(TRAIN_START)
    train_end_dt = pd.Timestamp(TRAIN_END)

    sub = df[df["ISO3"].isin(iso_list)].copy()

    # Build aggregation dict dynamically based on available columns
    agg_specs = {}
    slug = group_name.lower().replace(' ', '_')
    metric_labels = {}

    # Per-vessel-type metrics: tonnage + portcalls
    VESSEL_TYPES = [
        ("tanker", "tanker"),
        ("container", "container"),
        ("dry_bulk", "dry_bulk"),
        ("general_cargo", "general_cargo"),
        ("roro", "roro"),
    ]
    for vt_key, vt_col in VESSEL_TYPES:
        tonnage_col = f"{import_or_export}_{vt_col}"
        calls_col = f"portcalls_{vt_col}"
        if tonnage_col in df.columns:
            agg_specs[f"{vt_key}_tonnage"] = (tonnage_col, "sum")
            label_suffix = "_tonnage" if vt_key == "tanker" else f"_{vt_key}_tonnage"
            metric_labels[f"{vt_key}_tonnage"] = f"{slug}{label_suffix}"
        if calls_col in df.columns:
            agg_specs[f"{vt_key}_calls"] = (calls_col, "sum")
            metric_labels[f"{vt_key}_calls"] = f"{slug}_{vt_key}_calls"

    # Total portcalls (all vessel types)
    if "portcalls" in df.columns:
        agg_specs["total_calls"] = ("portcalls", "sum")
        metric_labels["total_calls"] = f"{slug}_total_calls"

    daily_agg = sub.groupby("date").agg(**agg_specs).sort_index()

    print(f"  {group_name} daily: {len(daily_agg)} days, "
          f"{daily_agg.index.min().date()} → {daily_agg.index.max().date()}")

    weekly_agg = _resample_weekly_split(daily_agg, crisis_date=CRISIS_DATE, agg="mean")
    weekly_agg = weekly_agg.loc[weekly_agg.index >= "2019-01-07"]
    # Drop incomplete trailing weeks (phantom zero-weeks from data lag)
    first_col = list(agg_specs.keys())[0]
    weekly_agg = _drop_incomplete_trailing_weeks(daily_agg[first_col], weekly_agg)

    for metric_col_w, label in metric_labels.items():
        series = weekly_agg[metric_col_w].dropna()
        if len(series) < 104:
            print(f"    SKIP {label}: only {len(series)} weeks")
            continue

        print(f"\n  → Metric: {label}")
        trend, seasonal, remainder, was_logged = run_stl(series, seasonal=seasonal_param)

        X_frozen, X_live = build_weekly_controls(
            series.index, frozen_monthly, frozen_daily, live_daily,
            freeze_date=CRISIS_DATE
        )

        train_mask = pd.Series(False, index=series.index)
        train_mask[(series.index >= train_start_dt) & (series.index <= train_end_dt)] = True

        # Build geographic cross-features for this region/group
        cross_feats = pd.DataFrame(index=series.index)
        if chokepoint_remainders:
            # For single-country groups, pass iso3 for lookup
            _iso = iso_list[0] if len(iso_list) == 1 else None
            feeders = _get_feeder_chokepoints(group_name, iso3=_iso)
            cross_feats = _build_cross_features(
                series.index, feeders, chokepoint_remainders, CRISIS_DATE
            )
            if not cross_feats.empty:
                print(f"    Cross-features: {len(cross_feats.columns)} ({list(cross_feats.columns)})")

        print("    [Primary model — frozen controls + cross-features]")
        _, _, pred_remainder_p = fit_residual_model(remainder, X_frozen, train_mask,
                                                     cross_features=cross_feats)

        X_combined = pd.concat([X_frozen, X_live], axis=1)
        print("    [Sensitivity model — all controls + cross-features]")
        _, _, pred_remainder_s = fit_residual_model(remainder, X_combined, train_mask,
                                                     cross_features=cross_feats)

        n_post = (series.index > train_end_dt).sum()
        trend_extrap = extrapolate_trend(trend, train_end_dt, n_post)

        cf_primary_log = trend_extrap.reindex(series.index).fillna(method="ffill") + \
                     seasonal.reindex(series.index, fill_value=0) + \
                     pred_remainder_p.reindex(series.index, fill_value=0)

        cf_sensitivity_log = trend_extrap.reindex(series.index).fillna(method="ffill") + \
                        seasonal.reindex(series.index, fill_value=0) + \
                        pred_remainder_s.reindex(series.index, fill_value=0)

        # Convert back to level space and clamp to non-negative
        if was_logged:
            cf_primary = np.expm1(cf_primary_log).clip(lower=0)
            cf_sensitivity = np.expm1(cf_sensitivity_log).clip(lower=0)
        else:
            cf_primary = cf_primary_log.clip(lower=0)
            cf_sensitivity = cf_sensitivity_log.clip(lower=0)

        deviation_primary = series - cf_primary

        # Pre-crisis average (52-week lookback)
        pre_52w_mask = (series.index >= crisis_dt - pd.Timedelta(weeks=52)) & (series.index < crisis_dt)
        pre_crisis_avg = float(series.loc[pre_52w_mask].mean()) if pre_52w_mask.any() else float(series.mean())

        post_mask = series.index >= crisis_dt
        if post_mask.any():
            actual_post = series.loc[post_mask].mean()
            cf_post = cf_primary.loc[post_mask].mean()
            if cf_post > 0:
                pct_dev = (actual_post - cf_post) / cf_post * 100
            elif pre_crisis_avg > 0:
                pct_dev = (actual_post - cf_post) / pre_crisis_avg * 100
            else:
                pct_dev = 0
            pct_dev = max(-999, min(999, pct_dev))
            print(f"    POST-CRISIS: actual avg={actual_post:.0f}, "
                  f"counterfactual avg={cf_post:.0f}, "
                  f"deviation={pct_dev:+.1f}%")

        # Variance decomposition (sequential R² on training window)
        if was_logged:
            y_train_decomp = np.log1p(series.clip(lower=0)).reindex(trend.index)
        else:
            y_train_decomp = series.reindex(trend.index)
        y_t = y_train_decomp[train_mask.reindex(y_train_decomp.index, fill_value=False)]
        trend_t = trend.reindex(y_t.index, fill_value=0)
        seasonal_t = seasonal.reindex(y_t.index, fill_value=0)
        pred_rem_t = pred_remainder_p.reindex(y_t.index, fill_value=0)

        ss_total = float(np.var(y_t)) if len(y_t) > 1 else 1.0
        if ss_total > 0 and len(y_t) > 1:
            r2_trend = max(0, 1 - float(np.var(y_t - trend_t)) / ss_total)
            r2_trend_seasonal = max(0, 1 - float(np.var(y_t - trend_t - seasonal_t)) / ss_total)
            r2_full = max(0, 1 - float(np.var(y_t - trend_t - seasonal_t - pred_rem_t)) / ss_total)
        else:
            r2_trend = r2_trend_seasonal = r2_full = 0.0
        r2_controls_marginal = r2_full - r2_trend_seasonal
        r2_unexplained = 1.0 - r2_full
        variance_decomp = {
            "r2_trend": round(r2_trend, 4),
            "r2_trend_seasonal": round(r2_trend_seasonal, 4),
            "r2_full": round(r2_full, 4),
            "r2_controls_marginal": round(max(0, r2_controls_marginal), 4),
            "r2_unexplained": round(max(0, r2_unexplained), 4),
        }

        key = f"{group_name}|{label}"
        results[key] = {
            "chokepoint": group_name,
            "metric": label,
            "dates": [d.strftime("%Y-%m-%d") for d in series.index],
            "actual": series.values.tolist(),
            "trend": trend_extrap.reindex(series.index).ffill().values.tolist(),
            "seasonal": seasonal.reindex(series.index, fill_value=0).values.tolist(),
            "remainder": pred_remainder_p.reindex(series.index, fill_value=0).values.tolist(),
            "counterfactual_primary": cf_primary.reindex(series.index).values.tolist(),
            "counterfactual_sensitivity": cf_sensitivity.reindex(series.index).values.tolist(),
            "deviation_primary": deviation_primary.reindex(series.index).values.tolist(),
            "crisis_date": CRISIS_DATE,
            "train_end": TRAIN_END,
            "pre_crisis_avg": round(pre_crisis_avg, 1),
            "variance_decomp": variance_decomp,
        }

    return results


def run_all_port_group_aggregates(results, frozen_monthly, frozen_daily, live_daily, seasonal_param=7,
                                   chokepoint_remainders=None):
    """Run aggregate STL analysis for all regional port groups."""
    fp = os.path.join(DATA_DIR, "portwatch", "Daily_Ports_Data.csv")

    print("  Loading port data for regional aggregates...")
    cols = ["date", "portname", "ISO3",
            "export_tanker", "import_tanker", "portcalls_tanker",
            "export_container", "import_container", "portcalls_container",
            "export_dry_bulk", "import_dry_bulk", "portcalls_dry_bulk",
            "export_general_cargo", "import_general_cargo", "portcalls_general_cargo",
            "export_roro", "import_roro", "portcalls_roro",
            "portcalls"]
    df = pd.read_csv(fp, usecols=lambda c: c in cols)
    df["date"] = pd.to_datetime(df["date"])
    # Force numeric for any columns that may have mixed types
    numeric_cols = [c for c in df.columns if c not in ("date", "portname", "ISO3")]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    REGIONS = [
        ("Persian Gulf",          ["SAU", "IRQ", "IRN", "ARE", "KWT", "QAT", "OMN", "BHR"]),
        ("East Asia",             ["CHN", "JPN", "KOR", "TWN"]),
        ("Southeast Asia",        ["SGP", "MYS", "IDN", "THA", "VNM", "PHL"]),
        ("Indian Subcontinent",   ["IND", "PAK", "BGD", "LKA"]),
        ("Mediterranean",         ["TUR", "EGY", "GRC", "ISR", "LBN", "ITA", "ESP"]),
        ("Northwest Europe",      ["NLD", "GBR", "DEU", "FRA", "BEL", "NOR", "PRT", "DZA"]),
        ("North America",         ["USA", "CAN", "MEX"]),
        ("Latin America",         ["BRA", "COL", "GUY", "TTO", "ECU", "VEN", "ARG"]),
        ("West Africa",           ["NGA", "AGO", "GHA", "GNQ", "COG", "CMR", "GAB"]),
        ("Russia",                ["RUS"]),
        ("Oceania",               ["AUS", "NZL"]),
    ]

    for region_name, iso_list in REGIONS:
        for direction in ["export", "import"]:
            group_name = f"{region_name} {direction.title()}s"
            print(f"\n{'─' * 60}")
            print(f"  PORT-LEVEL: {group_name}")
            print(f"{'─' * 60}")
            results = _run_aggregate_port_group(
                df, iso_list, direction, group_name,
                frozen_monthly, frozen_daily, live_daily, results, seasonal_param=seasonal_param,
                chokepoint_remainders=chokepoint_remainders
            )

    return results


# ─── 8c. Country-Level Aggregate Port Analysis ────────────────────────────
def run_country_aggregates(results, frozen_monthly, frozen_daily, live_daily, seasonal_param=7,
                            chokepoint_remainders=None):
    """Run aggregate STL analysis for the top 50 countries by total portcalls."""
    fp = os.path.join(DATA_DIR, "portwatch", "Daily_Ports_Data.csv")

    print("\n  Loading port data for country-level aggregates...")
    cols = ["date", "portname", "ISO3", "country",
            "export_tanker", "import_tanker", "portcalls_tanker",
            "export_container", "import_container", "portcalls_container",
            "export_dry_bulk", "import_dry_bulk", "portcalls_dry_bulk",
            "export_general_cargo", "import_general_cargo", "portcalls_general_cargo",
            "export_roro", "import_roro", "portcalls_roro",
            "portcalls"]
    df = pd.read_csv(fp, usecols=lambda c: c in cols)
    df["date"] = pd.to_datetime(df["date"])
    numeric_cols = [c for c in df.columns if c not in ("date", "portname", "ISO3", "country")]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    # Rank countries by historical average total portcalls
    country_totals = df.groupby(["country", "ISO3"])["portcalls"].sum().reset_index()
    country_totals = country_totals.sort_values("portcalls", ascending=False)
    top_countries = country_totals.head(50)
    print(f"  Selected top {len(top_countries)} countries by total portcalls")

    for _, row in top_countries.iterrows():
        country_name = row["country"]
        iso3 = row["ISO3"]
        for direction in ["export", "import"]:
            group_name = f"COUNTRY:{country_name} {direction.title()}s"
            print(f"\n{'─' * 60}")
            print(f"  COUNTRY-LEVEL: {group_name}")
            print(f"{'─' * 60}")
            results = _run_aggregate_port_group(
                df, [iso3], direction, group_name,
                frozen_monthly, frozen_daily, live_daily, results, seasonal_param=seasonal_param,
                chokepoint_remainders=chokepoint_remainders
            )

    return results


    # Weekly aggregation
    weekly_asia = daily_asia.resample(**_WEEKLY_RESAMPLE).sum()
    weekly_asia = weekly_asia.loc[weekly_asia.index >= "2019-01-07"]

    for metric_col, label in [("tanker_imports", "asia_imports"), ("tanker_calls", "asia_calls")]:
        series = weekly_asia[metric_col].dropna()
        if len(series) < 104:
            print(f"    SKIP {label}: only {len(series)} weeks")
            continue

        print(f"\n  → Metric: {label}")
        trend, seasonal, remainder, was_logged = run_stl(series, seasonal=seasonal_param)

        X_frozen, X_live = build_weekly_controls(
            series.index, frozen_monthly, frozen_daily, live_daily,
            freeze_date=CRISIS_DATE
        )

        train_mask = pd.Series(False, index=series.index)
        train_mask[(series.index >= train_start_dt) & (series.index <= train_end_dt)] = True

        print("    [Primary model — frozen controls]")
        _, _, pred_remainder_p = fit_residual_model(remainder, X_frozen, train_mask)

        X_combined = pd.concat([X_frozen, X_live], axis=1)
        print("    [Sensitivity model — all controls]")
        _, _, pred_remainder_s = fit_residual_model(remainder, X_combined, train_mask)

        n_post = (series.index > train_end_dt).sum()
        trend_extrap = extrapolate_trend(trend, train_end_dt, n_post)

        cf_primary_log = trend_extrap.reindex(series.index).fillna(method="ffill") + \
                     seasonal.reindex(series.index, fill_value=0) + \
                     pred_remainder_p.reindex(series.index, fill_value=0)

        cf_sensitivity_log = trend_extrap.reindex(series.index).fillna(method="ffill") + \
                        seasonal.reindex(series.index, fill_value=0) + \
                        pred_remainder_s.reindex(series.index, fill_value=0)

        cf_primary = (np.expm1(cf_primary_log) if was_logged else cf_primary_log).clip(lower=0)
        cf_sensitivity = (np.expm1(cf_sensitivity_log) if was_logged else cf_sensitivity_log).clip(lower=0)

        deviation_primary = series - cf_primary

        post_mask = series.index >= crisis_dt
        if post_mask.any():
            actual_post = series.loc[post_mask].mean()
            cf_post = cf_primary.loc[post_mask].mean()
            pct_dev = ((actual_post - cf_post) / cf_post * 100) if cf_post != 0 else 0
            print(f"    POST-CRISIS: actual avg={actual_post:.0f}, "
                  f"counterfactual avg={cf_post:.0f}, "
                  f"deviation={pct_dev:+.1f}%")

        key = f"Asian Ports|{label}"
        results[key] = {
            "chokepoint": "Asian Import Ports",
            "metric": label,
            "dates": [d.strftime("%Y-%m-%d") for d in series.index],
            "actual": series.values.tolist(),
            "trend": trend_extrap.reindex(series.index).ffill().values.tolist(),
            "seasonal": seasonal.reindex(series.index, fill_value=0).values.tolist(),
            "remainder": pred_remainder_p.reindex(series.index, fill_value=0).values.tolist(),
            "counterfactual_primary": cf_primary.reindex(series.index).values.tolist(),
            "counterfactual_sensitivity": cf_sensitivity.reindex(series.index).values.tolist(),
            "deviation_primary": deviation_primary.reindex(series.index).values.tolist(),
            "crisis_date": CRISIS_DATE,
            "train_end": TRAIN_END,
        }

    return results


# ─── 9. Summary CSV ─────────────────────────────────────────────────────────
def save_summary_csv(results):
    """Save a summary table of crisis deviations."""
    rows = []
    for key, r in results.items():
        if key.startswith("_"):  # skip per-port breakdown metadata
            continue
        dates = pd.to_datetime(r["dates"])
        actual = np.array(r["actual"])
        cf = np.array(r["counterfactual_primary"])
        crisis_dt = pd.Timestamp(r["crisis_date"])

        post_mask = dates >= crisis_dt
        pre_mask = (dates >= pd.Timestamp(TRAIN_START)) & (dates < crisis_dt)

        if post_mask.any():
            actual_post_avg = actual[post_mask].mean()
            cf_post_avg = cf[post_mask].mean()

            actual_pre_avg = actual[pre_mask].mean() if pre_mask.any() else np.nan
            # Safeguard: fallback to pre-crisis avg if cf is negative/zero
            if cf_post_avg > 0:
                pct_dev = (actual_post_avg - cf_post_avg) / cf_post_avg * 100
            elif actual_pre_avg > 0:
                pct_dev = (actual_post_avg - cf_post_avg) / actual_pre_avg * 100
            else:
                pct_dev = 0
            pct_dev = max(-999, min(999, pct_dev))

            rows.append({
                "chokepoint": r["chokepoint"],
                "metric": r["metric"],
                "pre_crisis_weekly_avg": round(actual_pre_avg, 1),
                "post_crisis_weekly_avg": round(actual_post_avg, 1),
                "counterfactual_avg": round(cf_post_avg, 1),
                "crisis_deviation_abs": round(actual_post_avg - cf_post_avg, 1),
                "crisis_deviation_pct": round(pct_dev, 1),
                "n_post_crisis_weeks": int(post_mask.sum()),
            })

    summary = pd.DataFrame(rows)
    fp = os.path.join(OUTPUT_DIR, "crisis_deviation_summary.csv")
    summary.to_csv(fp, index=False)
    print(f"Summary saved to {fp}")
    print("\n" + summary.to_string(index=False))


if __name__ == "__main__":
    import sys
    if "--seasonal" in sys.argv:
        idx = sys.argv.index("--seasonal")
        seasonal_val = int(sys.argv[idx + 1])
        results = run_pipeline(seasonal_param=seasonal_val)
    else:
        # Default: single run with s=13
        results = run_pipeline()
