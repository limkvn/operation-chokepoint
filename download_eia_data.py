"""
Download EIA Weekly Petroleum Data via API v2
==============================================
Pulls key weekly series for oil price forecasting:
  - Crude oil inventories (commercial, SPR, Cushing)
  - Crude oil production
  - Refinery utilization & inputs
  - Crude imports & exports
  - Product supplied (demand proxy)
  - Gasoline & distillate stocks

Requires: Free API key from https://www.eia.gov/opendata/register.php

Usage:
  python download_eia_data.py              # uses EIA_API_KEY env variable
  python download_eia_data.py YOUR_KEY     # pass key as argument

Output: eia_weekly_petroleum.csv
"""

import json
import os
import sys
import time
from datetime import datetime
from urllib.request import urlopen, Request
from urllib.error import HTTPError

import pandas as pd

# ── Configuration ──────────────────────────────────────────────────────

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Forecasting/
OUT_DIR = os.path.join(BASE_DIR, "data", "eia")
os.makedirs(OUT_DIR, exist_ok=True)

# EIA API v2 base URL
BASE_URL = "https://api.eia.gov/v2/seriesid"

# Key weekly petroleum series
# Format: (series_id, short_name, description)
SERIES = [
    # ── Inventories ──
    ("PET.WCESTUS1.W", "crude_stocks_ex_spr",
     "U.S. commercial crude oil stocks excl. SPR (thousand barrels)"),
    ("PET.WCSSTUS1.W", "crude_stocks_spr",
     "U.S. crude oil stocks in SPR (thousand barrels)"),
    ("PET.WCRSTUS1.W", "crude_stocks_total",
     "U.S. total crude oil stocks incl. SPR (thousand barrels)"),
    ("PET.W_EPC0_SAX_YCUOK_MBBL.W", "crude_stocks_cushing",
     "Cushing OK crude oil stocks (million barrels)"),
    ("PET.WTESTUS1.W", "total_petro_stocks",
     "U.S. total petroleum stocks excl. SPR (thousand barrels)"),
    ("PET.WGTSTUS1.W", "gasoline_stocks",
     "U.S. total motor gasoline stocks (thousand barrels)"),
    ("PET.WDISTUS1.W", "distillate_stocks",
     "U.S. distillate fuel oil stocks (thousand barrels)"),

    # ── Production ──
    ("PET.WCRFPUS2.W", "crude_production",
     "U.S. field production of crude oil (thousand barrels/day)"),

    # ── Refinery ──
    ("PET.WGIRIUS2.W", "refinery_inputs",
     "U.S. gross inputs to refineries (thousand barrels/day)"),
    ("PET.WPULEUS3.W", "refinery_utilization",
     "U.S. percent utilization of refinery operable capacity"),

    # ── Imports / Exports ──
    ("PET.WCRIMUS2.W", "crude_imports",
     "U.S. imports of crude oil (thousand barrels/day)"),
    ("PET.WCREXUS2.W", "crude_exports",
     "U.S. exports of crude oil (thousand barrels/day)"),
    ("PET.WRPIMUS2.W", "product_imports",
     "U.S. imports of total petroleum products (thousand barrels/day)"),
    ("PET.WRPEXUS2.W", "product_exports",
     "U.S. exports of petroleum products (thousand barrels/day)"),
    ("PET.WTTNTUS2.W", "net_imports_total",
     "U.S. net imports of crude oil and products (thousand barrels/day)"),

    # ── Demand (product supplied = proxy for consumption) ──
    ("PET.WRPUPUS2.W", "total_product_supplied",
     "U.S. total product supplied (thousand barrels/day)"),
    ("PET.WGFUPUS2.W", "gasoline_supplied",
     "U.S. finished motor gasoline supplied (thousand barrels/day)"),
    ("PET.WDIUPUS2.W", "distillate_supplied",
     "U.S. distillate fuel oil supplied (thousand barrels/day)"),
    ("PET.WKJUPUS2.W", "jet_fuel_supplied",
     "U.S. kerosene-type jet fuel supplied (thousand barrels/day)"),
]


def fetch_series(api_key, series_id, retries=3):
    """Fetch a single series from EIA API v2 using the SeriesID endpoint."""
    url = f"{BASE_URL}/{series_id}?api_key={api_key}"

    for attempt in range(retries):
        try:
            req = Request(url)
            req.add_header("User-Agent", "OilForecast/1.0")
            with urlopen(req, timeout=30) as response:
                data = json.loads(response.read().decode())

            if "response" in data and "data" in data["response"]:
                return data["response"]["data"]
            elif "data" in data:
                return data["data"]
            else:
                print(f"  Warning: unexpected response structure for {series_id}")
                print(f"  Keys: {list(data.keys())}")
                return []

        except HTTPError as e:
            if e.code == 429:  # rate limited
                wait = 2 ** attempt
                print(f"  Rate limited, waiting {wait}s...")
                time.sleep(wait)
            else:
                print(f"  HTTP error {e.code} for {series_id}: {e.reason}")
                return []
        except Exception as e:
            print(f"  Error fetching {series_id}: {e}")
            if attempt < retries - 1:
                time.sleep(1)
            else:
                return []

    return []


def download_all(api_key):
    """Download all series and merge into a single DataFrame."""
    all_series = {}

    for series_id, short_name, description in SERIES:
        print(f"  Fetching {short_name} ({series_id})...")
        records = fetch_series(api_key, series_id)

        if not records:
            print(f"    ⚠ No data returned")
            continue

        # Parse records: each has 'period' (date string) and 'value'
        dates = []
        values = []
        for rec in records:
            period = rec.get("period")
            value = rec.get("value")
            if period and value is not None:
                try:
                    dates.append(pd.to_datetime(period))
                    values.append(float(value))
                except (ValueError, TypeError):
                    continue

        if dates:
            series = pd.Series(values, index=dates, name=short_name)
            all_series[short_name] = series
            print(f"    ✓ {len(dates)} observations "
                  f"({min(dates).strftime('%Y-%m-%d')} → {max(dates).strftime('%Y-%m-%d')})")
        else:
            print(f"    ⚠ No valid observations")

        # Be nice to the API
        time.sleep(0.3)

    if not all_series:
        print("\nNo data retrieved. Check your API key.")
        return None

    # Merge all series by date
    df = pd.DataFrame(all_series)
    df.index.name = "date"
    df = df.sort_index()

    # Add ISO week columns for merging with shipping data
    df["year"] = df.index.isocalendar().year.astype(int)
    df["week"] = df.index.isocalendar().week.astype(int)

    return df


def main():
    # Get API key
    api_key = None
    if len(sys.argv) > 1:
        api_key = sys.argv[1]
    else:
        api_key = os.environ.get("EIA_API_KEY")

    if not api_key:
        print("=" * 60)
        print("  EIA Weekly Petroleum Data Downloader")
        print("=" * 60)
        print()
        print("To use this script, you need a free EIA API key.")
        print()
        print("Steps:")
        print("  1. Go to: https://www.eia.gov/opendata/register.php")
        print("  2. Enter your email address")
        print("  3. You'll receive an API key immediately by email")
        print("  4. Run this script with your key:")
        print()
        print("     python download_eia_data.py YOUR_API_KEY")
        print()
        print("  Or set the environment variable:")
        print("     export EIA_API_KEY=YOUR_API_KEY")
        print("     python download_eia_data.py")
        print()
        print("The key is free and registration takes ~30 seconds.")
        print("=" * 60)

        # ── Generate sample data so the pipeline works without a key ──
        print("\nGenerating SAMPLE DATA for pipeline testing...")
        print("(Replace with real data by re-running with an API key)\n")
        generate_sample_data()
        return

    print("=" * 60)
    print("  Downloading EIA Weekly Petroleum Data")
    print("=" * 60)
    print(f"\n  Series to download: {len(SERIES)}")
    print()

    df = download_all(api_key)

    if df is not None:
        out_path = f"{OUT_DIR}/eia_weekly_petroleum.csv"
        df.to_csv(out_path)
        print(f"\n  Saved: {out_path}")
        print(f"  Shape: {df.shape}")
        print(f"  Date range: {df.index[0].strftime('%Y-%m-%d')} → "
              f"{df.index[-1].strftime('%Y-%m-%d')}")
        print(f"  Columns: {list(df.columns)}")

        # Print summary stats
        print(f"\n  Coverage summary:")
        for col in df.columns:
            if col in ("year", "week"):
                continue
            valid = df[col].notna().sum()
            total = len(df)
            print(f"    {col:<30s} {valid:>5d}/{total} observations "
                  f"({valid/total*100:.0f}%)")


def generate_sample_data():
    """Generate realistic sample EIA data for pipeline testing.
    Uses approximate historical ranges so the pipeline can run
    without an API key. Replace with real data for actual forecasting."""

    import numpy as np
    np.random.seed(42)

    # Weekly dates from 2010 through 2025
    dates = pd.date_range("2010-01-08", "2026-02-06", freq="W-FRI")

    data = {}

    # Inventories (thousand barrels, approximate ranges)
    data["crude_stocks_ex_spr"] = np.random.normal(430000, 30000, len(dates)).clip(380000, 540000)
    data["crude_stocks_spr"] = np.linspace(726000, 390000, len(dates)) + np.random.normal(0, 5000, len(dates))
    data["crude_stocks_total"] = data["crude_stocks_ex_spr"] + data["crude_stocks_spr"]
    data["crude_stocks_cushing"] = np.random.normal(40, 15, len(dates)).clip(20, 70)
    data["total_petro_stocks"] = np.random.normal(1280000, 50000, len(dates))
    data["gasoline_stocks"] = np.random.normal(230000, 15000, len(dates))
    data["distillate_stocks"] = np.random.normal(140000, 20000, len(dates))

    # Production (thousand barrels/day)
    data["crude_production"] = np.linspace(5500, 13300, len(dates)) + np.random.normal(0, 200, len(dates))

    # Refinery
    data["refinery_inputs"] = np.random.normal(16000, 1000, len(dates)).clip(13000, 18000)
    data["refinery_utilization"] = np.random.normal(88, 5, len(dates)).clip(70, 97)

    # Imports/Exports (thousand barrels/day)
    data["crude_imports"] = np.linspace(9000, 6000, len(dates)) + np.random.normal(0, 500, len(dates))
    data["crude_exports"] = np.linspace(100, 4000, len(dates)) + np.random.normal(0, 300, len(dates))
    data["product_imports"] = np.random.normal(2200, 300, len(dates))
    data["product_exports"] = np.linspace(2000, 6500, len(dates)) + np.random.normal(0, 300, len(dates))
    data["net_imports_total"] = data["crude_imports"] - data["crude_exports"] + data["product_imports"] - data["product_exports"]

    # Demand / product supplied (thousand barrels/day)
    data["total_product_supplied"] = np.random.normal(20000, 1500, len(dates)).clip(15000, 22000)
    data["gasoline_supplied"] = np.random.normal(9000, 500, len(dates))
    data["distillate_supplied"] = np.random.normal(3800, 400, len(dates))
    data["jet_fuel_supplied"] = np.random.normal(1500, 200, len(dates))

    df = pd.DataFrame(data, index=dates)
    df.index.name = "date"
    df["year"] = df.index.isocalendar().year.astype(int)
    df["week"] = df.index.isocalendar().week.astype(int)

    out_path = f"{OUT_DIR}/eia_weekly_petroleum.csv"
    df.to_csv(out_path)
    print(f"  Saved SAMPLE data: {out_path}")
    print(f"  Shape: {df.shape}")
    print(f"  Date range: {df.index[0].strftime('%Y-%m-%d')} → "
          f"{df.index[-1].strftime('%Y-%m-%d')}")
    print(f"  ⚠ This is SIMULATED data for testing only!")
    print(f"  ⚠ Get a free API key at https://www.eia.gov/opendata/register.php")
    print(f"  ⚠ Then re-run: python download_eia_data.py YOUR_KEY")


if __name__ == "__main__":
    main()
