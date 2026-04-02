"""
Download Brent Crude Oil Futures Data
=====================================
Downloads historical Brent futures prices at multiple tenors for use as
baselines in a "futures forecast error" prediction framework.

Uses yfinance to pull:
  - BZ=F   : Front-month (nearest) Brent futures  (~1 month out)
  - BZ2=F  : 2nd month Brent futures               (~2 months out)
  - BZ3=F  : 3rd month Brent futures               (~3 months out)

These map roughly to forecast horizons:
  - 1-week ahead  → front-month futures (BZ=F)
  - 4-week ahead  → front-month futures (BZ=F)
  - 13-week ahead → 3rd-month futures   (BZ3=F)

Requirements:
  pip install yfinance pandas

Usage:
  python download_futures_data.py

Output: brent_futures_daily.csv  (daily data, all tenors)
"""

import os
import sys
from datetime import datetime

import pandas as pd

try:
    import yfinance as yf
except ImportError:
    print("Please install yfinance first:  pip install yfinance")
    sys.exit(1)


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Forecasting/
OUT_DIR = os.path.join(BASE_DIR, "data", "futures")
os.makedirs(OUT_DIR, exist_ok=True)

# Brent futures tickers on Yahoo Finance
# BZ=F is the front-month continuous contract
# BZ{N}=F is the Nth generic month
TICKERS = {
    "brent_front":  "BZ=F",     # front month (~1 month out)
    "brent_2nd":    "BZ2=F",    # 2nd month (~2 months out, not always available)
}

# Date range — match the shipping data range (2019 onwards)
START_DATE = "2017-01-01"
END_DATE   = datetime.now().strftime("%Y-%m-%d")


def download_futures():
    print("=" * 60)
    print("  Downloading Brent Crude Oil Futures Data")
    print("=" * 60)

    all_dfs = []

    for name, ticker in TICKERS.items():
        print(f"\n  Fetching {name} ({ticker})...")
        try:
            data = yf.download(ticker, start=START_DATE, end=END_DATE,
                               auto_adjust=True, progress=False)
            if data.empty:
                print(f"    ⚠ No data returned for {ticker}")
                continue

            # Handle multi-level columns from yfinance
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)

            # Keep only Close price and rename
            df = data[["Close"]].copy()
            df.columns = [name]
            df.index.name = "date"

            print(f"    ✓ {len(df)} daily observations "
                  f"({df.index.min().strftime('%Y-%m-%d')} → "
                  f"{df.index.max().strftime('%Y-%m-%d')})")
            all_dfs.append(df)

        except Exception as e:
            print(f"    ⚠ Error: {e}")
            continue

    if not all_dfs:
        print("\n  ⚠ No futures data retrieved!")
        print("  Falling back to front-month only using alternative approach...")

        # Try alternative: download BZ=F directly with different params
        try:
            data = yf.download("BZ=F", period="max", auto_adjust=True, progress=False)
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)
            df = data[["Close"]].copy()
            df.columns = ["brent_front"]
            df.index.name = "date"
            all_dfs.append(df)
            print(f"    ✓ Got {len(df)} observations")
        except Exception as e:
            print(f"    ⚠ Alternative also failed: {e}")
            print("\n  Please check your internet connection.")
            return

    # Merge all tenors on date
    merged = all_dfs[0]
    for df in all_dfs[1:]:
        merged = merged.join(df, how="outer")

    # Sort by date
    merged = merged.sort_index()

    # Forward-fill small gaps (weekends already excluded; holidays may cause 1-2 day gaps)
    merged = merged.ffill(limit=5)

    # Add year/week columns for easy merging with weekly data
    merged["year"] = merged.index.isocalendar().year.astype(int)
    merged["week"] = merged.index.isocalendar().week.astype(int)

    # Save daily data
    out_path = os.path.join(OUT_DIR, "brent_futures_daily.csv")
    merged.to_csv(out_path)
    print(f"\n  ✓ Saved daily futures data: {out_path}")
    print(f"    Shape: {merged.shape}")
    print(f"    Columns: {list(merged.columns)}")

    # Also create weekly version (Friday close or last available)
    weekly = merged.groupby(["year", "week"]).last()
    weekly = weekly.reset_index()

    # Drop the date index that got grouped
    if "date" in weekly.columns:
        weekly = weekly.drop(columns=["date"])

    out_weekly = os.path.join(OUT_DIR, "brent_futures_weekly.csv")
    weekly.to_csv(out_weekly, index=False)
    print(f"\n  ✓ Saved weekly futures data: {out_weekly}")
    print(f"    Shape: {weekly.shape}")
    print(f"    Date range: {weekly['year'].min()}-W{weekly['week'].min()} → "
          f"{weekly['year'].max()}-W{weekly['week'].max()}")

    # Summary stats
    print(f"\n  Summary:")
    for col in merged.columns:
        if col in ["year", "week"]:
            continue
        valid = merged[col].dropna()
        print(f"    {col}: {len(valid)} obs, "
              f"range ${valid.min():.2f} – ${valid.max():.2f}, "
              f"mean ${valid.mean():.2f}")

    print("\n" + "=" * 60)
    print("  Done! Next steps:")
    print("  1. The forecasting script will use futures prices as")
    print("     the market baseline for each horizon")
    print("  2. Target becomes: spot(t+h) - futures(t)")
    print("     instead of:     spot(t+h) - spot(t)")
    print("=" * 60)


if __name__ == "__main__":
    download_futures()
