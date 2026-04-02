"""
Download Brent Crude Oil Futures Term Structure from Yahoo Finance
=================================================================
Downloads individual contract months (BZ{month_code}{YY}.NYM) and constructs:
  1. Front-month continuous series (already have from BZ=F)
  2. 3-month forward price series (for 13-week forecast error target)

The 3-month forward is built by, for each trading day, selecting the contract
whose expiration is closest to 3 months ahead.

Outputs:
  - brent_futures_term_structure_daily.csv  (all contracts, daily)
  - brent_futures_term_structure_weekly.csv (front + 3mo, weekly)

Usage:
  pip install yfinance pandas
  python download_futures_term_structure.py
"""

import os
import sys
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Forecasting/
OUT_DIR = os.path.join(BASE_DIR, "data", "futures")
os.makedirs(OUT_DIR, exist_ok=True)

# Futures month codes: F=Jan, G=Feb, H=Mar, J=Apr, K=May, M=Jun,
#                      N=Jul, Q=Aug, U=Sep, V=Oct, X=Nov, Z=Dec
MONTH_CODES = {
    1: "F", 2: "G", 3: "H", 4: "J", 5: "K", 6: "M",
    7: "N", 8: "Q", 9: "U", 10: "V", 11: "X", 12: "Z",
}
CODE_TO_MONTH = {v: k for k, v in MONTH_CODES.items()}

# Generate contract tickers from 2017 to 2027
# Each contract is BZ{code}{YY}.NYM  e.g., BZN25.NYM = July 2025
START_YEAR = 2017
END_YEAR = 2027

tickers = []
ticker_to_expiry = {}  # ticker -> approximate expiry date (last business day of prior month)

for year in range(START_YEAR, END_YEAR + 1):
    yy = str(year)[-2:]
    for month, code in MONTH_CODES.items():
        ticker = f"BZ{code}{yy}.NYM"
        tickers.append(ticker)
        # Brent "last day" futures expire at end of month, 2 months before delivery
        # For simplicity, approximate the contract's reference month
        ticker_to_expiry[ticker] = datetime(year, month, 1)

print(f"Attempting to download {len(tickers)} Brent futures contracts...")
print(f"Date range: {START_YEAR} to {END_YEAR}")

# Download in batches to avoid rate limiting
BATCH_SIZE = 20
all_data = {}
failed = []

for i in range(0, len(tickers), BATCH_SIZE):
    batch = tickers[i:i + BATCH_SIZE]
    batch_str = " ".join(batch)
    print(f"\n  Batch {i // BATCH_SIZE + 1}/{(len(tickers) + BATCH_SIZE - 1) // BATCH_SIZE}: "
          f"{batch[0]} to {batch[-1]}")

    try:
        data = yf.download(
            batch_str,
            start=f"{START_YEAR}-01-01",
            end=datetime.now().strftime("%Y-%m-%d"),
            progress=False,
            group_by="ticker",
            auto_adjust=True,
        )

        if isinstance(data.columns, pd.MultiIndex):
            # Multiple tickers returned
            for ticker in batch:
                if ticker in data.columns.get_level_values(0):
                    df_t = data[ticker]["Close"].dropna()
                    if len(df_t) > 0:
                        all_data[ticker] = df_t
                        print(f"    {ticker}: {len(df_t)} days")
                    else:
                        failed.append(ticker)
                else:
                    failed.append(ticker)
        else:
            # Single ticker (shouldn't happen with batches, but handle it)
            if len(batch) == 1 and len(data) > 0:
                all_data[batch[0]] = data["Close"].dropna()
                print(f"    {batch[0]}: {len(data)} days")
            else:
                failed.extend(batch)
    except Exception as e:
        print(f"    Error: {e}")
        failed.extend(batch)

print(f"\n\nDownloaded {len(all_data)} contracts successfully")
print(f"Failed: {len(failed)} contracts")

if len(all_data) < 10:
    print("Too few contracts downloaded. Check your network connection.")
    sys.exit(1)

# Build daily DataFrame with all contracts
print("\nBuilding daily term structure...")
daily_frames = []
for ticker, series in all_data.items():
    df_t = series.reset_index()
    df_t.columns = ["date", "close"]
    df_t["ticker"] = ticker
    df_t["contract_month"] = ticker_to_expiry[ticker]
    daily_frames.append(df_t)

daily_all = pd.concat(daily_frames, ignore_index=True)
daily_all["date"] = pd.to_datetime(daily_all["date"])
daily_all = daily_all.sort_values(["date", "contract_month"]).reset_index(drop=True)

print(f"  Total daily records: {len(daily_all)}")
print(f"  Date range: {daily_all['date'].min().date()} to {daily_all['date'].max().date()}")

# For each date, find:
#   1. Front-month: contract with nearest expiry that hasn't expired yet
#   2. 3-month forward: contract expiring closest to date + 90 days
print("\nConstructing front-month and 3-month forward series...")

dates = sorted(daily_all["date"].unique())
records = []

for dt in dates:
    day_data = daily_all[daily_all["date"] == dt].copy()

    # Only consider contracts whose reference month is >= current month
    future_contracts = day_data[day_data["contract_month"] >= pd.Timestamp(dt.year, dt.month, 1)]

    if len(future_contracts) == 0:
        continue

    # Sort by contract month
    future_contracts = future_contracts.sort_values("contract_month")

    # Front month = nearest contract
    front = future_contracts.iloc[0]

    # 3-month forward = contract closest to date + 90 days
    target_date = pd.Timestamp(dt) + pd.Timedelta(days=90)
    future_contracts["months_to_target"] = abs(
        (future_contracts["contract_month"] - target_date).dt.days
    )
    three_mo = future_contracts.loc[future_contracts["months_to_target"].idxmin()]

    records.append({
        "date": dt,
        "brent_front": front["close"],
        "brent_front_ticker": front["ticker"],
        "brent_3mo": three_mo["close"],
        "brent_3mo_ticker": three_mo["ticker"],
    })

daily_ts = pd.DataFrame(records)
print(f"  Daily term structure: {len(daily_ts)} days")
print(f"  Front-month tickers used: {daily_ts['brent_front_ticker'].nunique()}")
print(f"  3-month tickers used: {daily_ts['brent_3mo_ticker'].nunique()}")

# Save daily
daily_path = os.path.join(OUT_DIR, "brent_futures_term_structure_daily.csv")
daily_ts.to_csv(daily_path, index=False)
print(f"\n  Saved: {daily_path}")

# Aggregate to weekly (ISO week mean)
print("\nAggregating to weekly...")
daily_ts["date"] = pd.to_datetime(daily_ts["date"])
daily_ts["year"] = daily_ts["date"].apply(lambda x: x.isocalendar()[0])
daily_ts["week"] = daily_ts["date"].apply(lambda x: x.isocalendar()[1])

weekly = daily_ts.groupby(["year", "week"]).agg(
    brent_front=("brent_front", "mean"),
    brent_3mo=("brent_3mo", "mean"),
).reset_index()

weekly_path = os.path.join(OUT_DIR, "brent_futures_term_structure_weekly.csv")
weekly.to_csv(weekly_path, index=False)
print(f"  Weekly term structure: {len(weekly)} weeks")
print(f"  Date range: {weekly['year'].min()}-W{weekly['week'].min()} to "
      f"{weekly['year'].max()}-W{weekly['week'].max()}")
print(f"  Saved: {weekly_path}")

# Summary stats
print("\n" + "=" * 60)
print("  SUMMARY")
print("=" * 60)
print(f"  Contracts downloaded: {len(all_data)}")
print(f"  Daily records: {len(daily_ts)}")
print(f"  Weekly records: {len(weekly)}")
print(f"  Front-month range: ${weekly['brent_front'].min():.2f} – ${weekly['brent_front'].max():.2f}")
print(f"  3-month fwd range: ${weekly['brent_3mo'].min():.2f} – ${weekly['brent_3mo'].max():.2f}")
print(f"  Mean front-3mo spread: ${(weekly['brent_3mo'] - weekly['brent_front']).mean():.2f}")
print("=" * 60)
