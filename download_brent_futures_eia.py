"""
Download Brent Crude Oil Futures Prices (Contracts 1-4) from EIA API v2
=======================================================================
Uses the EIA API v2 route-based endpoints:
  - petroleum/pri/spt  (spot prices)     — series: RBRTE
  - petroleum/pri/fut  (futures prices)  — series: RBRTE1..4

The EIA publishes daily NYMEX futures settlement prices for Brent crude:
  - Contract 1 (front month, ~1 month out)
  - Contract 2 (~2 months out)
  - Contract 3 (~3 months out)
  - Contract 4 (~4 months out)

Requires: Free API key from https://www.eia.gov/opendata/register.php

Usage:
  python download_brent_futures_eia.py YOUR_API_KEY

Output: brent_futures_contracts_weekly.csv
"""

import json
import os
import sys
import time
from datetime import datetime
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Forecasting/
OUT_DIR = os.path.join(BASE_DIR, "data", "futures")
os.makedirs(OUT_DIR, exist_ok=True)
BASE_URL = "https://api.eia.gov/v2"

# Different possible series ID patterns for Brent futures
# EIA has changed naming conventions; we try multiple options
FUTURES_ATTEMPTS = [
    # Attempt 1: petroleum/pri/fut with RBRTE + contract number
    {
        "route": "petroleum/pri/fut",
        "series": {
            "RBRTE1": "brent_c1",
            "RBRTE2": "brent_c2",
            "RBRTE3": "brent_c3",
            "RBRTE4": "brent_c4",
        },
    },
    # Attempt 2: petroleum/pri/fut with RBRC + contract number
    {
        "route": "petroleum/pri/fut",
        "series": {
            "RBRC1": "brent_c1",
            "RBRC2": "brent_c2",
            "RBRC3": "brent_c3",
            "RBRC4": "brent_c4",
        },
    },
    # Attempt 3: petroleum/pri/fut with EER_EPJK prefix (another EIA convention)
    {
        "route": "petroleum/pri/fut",
        "series": {
            "EER_EPJK_PF4_RGC_DPB": "brent_c4",
        },
    },
]

# Spot price (known working pattern from existing download script)
SPOT_SERIES = ("PET.RBRTE.D", "brent_spot", "Europe Brent Spot Price FOB ($/bbl)")


def fetch_v2_route(api_key, route, series_ids, frequency="daily", retries=3):
    """Fetch data from EIA API v2 using the route-based endpoint."""
    # Build the series facet: series=ID1;ID2;...
    series_param = ";".join(series_ids) + ";"
    url = (f"{BASE_URL}/{route}/data?"
           f"api_key={api_key}"
           f"&frequency={frequency}"
           f"&data[0]=value"
           f"&facets[series][]={'&facets[series][]='.join(series_ids)}"
           f"&sort[0][column]=period"
           f"&sort[0][direction]=asc"
           f"&length=5000"
           f"&offset=0")

    print(f"  Trying route: {route} with series: {list(series_ids)}")

    all_records = []
    offset = 0

    while True:
        paged_url = url.replace("&offset=0", f"&offset={offset}")
        for attempt in range(retries):
            try:
                req = Request(paged_url)
                req.add_header("User-Agent", "Mozilla/5.0")
                response = urlopen(req, timeout=30)
                data = json.loads(response.read().decode())

                if "response" in data and "data" in data["response"]:
                    records = data["response"]["data"]
                    total = data["response"].get("total", len(records))
                    all_records.extend(records)
                    print(f"    Got {len(records)} records (offset={offset}, total={total})")

                    if len(all_records) >= total or len(records) == 0:
                        return all_records
                    offset += len(records)
                    break
                else:
                    print(f"    Unexpected response: {json.dumps(data)[:300]}")
                    return all_records
            except HTTPError as e:
                print(f"    HTTP {e.code}: {e.reason}")
                if e.code == 404:
                    return []  # Route doesn't exist
                if attempt < retries - 1:
                    time.sleep(2)
            except (URLError, Exception) as e:
                print(f"    Error: {e}")
                if attempt < retries - 1:
                    time.sleep(2)
        else:
            break  # All retries failed

    return all_records


def fetch_v1_series(api_key, series_id, retries=3):
    """Fallback: Fetch using v2 seriesid endpoint (like existing EIA download script)."""
    url = f"{BASE_URL}/seriesid/{series_id}?api_key={api_key}"
    print(f"  Trying seriesid: {series_id}")

    for attempt in range(retries):
        try:
            req = Request(url)
            req.add_header("User-Agent", "Mozilla/5.0")
            response = urlopen(req, timeout=30)
            data = json.loads(response.read().decode())

            if "response" in data and "data" in data["response"]:
                records = data["response"]["data"]
                print(f"    Got {len(records)} records")
                return records
            else:
                print(f"    Unexpected response: {list(data.keys())}")
                return []
        except HTTPError as e:
            print(f"    HTTP {e.code}: {e.reason}")
            if e.code == 404:
                return []
            if attempt < retries - 1:
                time.sleep(2)
        except Exception as e:
            print(f"    Error: {e}")
            if attempt < retries - 1:
                time.sleep(2)

    return []


def main(api_key):
    print("=" * 60)
    print("  DOWNLOADING BRENT FUTURES CONTRACTS FROM EIA API v2")
    print("=" * 60)

    # Step 1: Try to discover what series exist in the futures route
    print("\n[1] Discovering available futures series...")
    discover_url = (f"{BASE_URL}/petroleum/pri/fut?"
                    f"api_key={api_key}")
    try:
        req = Request(discover_url)
        req.add_header("User-Agent", "Mozilla/5.0")
        response = urlopen(req, timeout=30)
        data = json.loads(response.read().decode())
        if "response" in data:
            resp = data["response"]
            # Print available facets
            if "facets" in resp:
                for facet in resp["facets"]:
                    if facet.get("id") == "series":
                        print(f"  Available series facet values:")
                        # Fetch facet values
                        pass
            if "frequency" in resp:
                print(f"  Frequencies: {resp['frequency']}")
            if "data" in resp:
                print(f"  Data keys: {resp.get('data', {})}")
            print(f"  Full metadata keys: {list(resp.keys())}")
            # Print raw response for debugging
            print(f"\n  Raw response (first 1000 chars):")
            print(f"  {json.dumps(data, indent=2)[:1000]}")
    except Exception as e:
        print(f"  Discovery failed: {e}")

    # Step 2: Try to list all Brent series by querying with facet
    print("\n[2] Querying for Brent-related futures series...")
    facet_url = (f"{BASE_URL}/petroleum/pri/fut/facet/series?"
                 f"api_key={api_key}")
    try:
        req = Request(facet_url)
        req.add_header("User-Agent", "Mozilla/5.0")
        response = urlopen(req, timeout=30)
        data = json.loads(response.read().decode())
        if "response" in data and "facets" in data["response"]:
            facets = data["response"]["facets"]
            brent_facets = [f for f in facets if "brent" in str(f).lower() or "rbr" in str(f).lower()]
            print(f"  Total series in petroleum/pri/fut: {len(facets)}")
            print(f"  Brent-related series: {len(brent_facets)}")
            for f in brent_facets:
                print(f"    {f}")
            # Also print all series if not too many
            if len(facets) <= 50:
                print(f"\n  All available series:")
                for f in facets:
                    print(f"    {f}")
    except Exception as e:
        print(f"  Facet query failed: {e}")

    # Step 3: Try each set of series IDs
    all_daily = {}

    print("\n[3] Attempting to download futures data...")
    for attempt_config in FUTURES_ATTEMPTS:
        route = attempt_config["route"]
        series_map = attempt_config["series"]

        records = fetch_v2_route(api_key, route, list(series_map.keys()))
        if records:
            for r in records:
                series = r.get("series", "")
                period = r.get("period", "")
                value = r.get("value")
                if series in series_map and value is not None and value != "":
                    col_name = series_map[series]
                    try:
                        dt = datetime.strptime(period, "%Y-%m-%d")
                        if col_name not in all_daily:
                            all_daily[col_name] = []
                        all_daily[col_name].append({"date": dt, col_name: float(value)})
                    except (ValueError, TypeError):
                        pass
            if all_daily:
                print(f"  Success with route {route}!")
                break
        else:
            print(f"  No data from {route} with {list(series_map.keys())}")

    # Step 4: Also try seriesid-based approach
    if not all_daily:
        print("\n[4] Trying seriesid-based approach...")
        seriesid_attempts = [
            ("PET.RBRC1.D", "brent_c1"),
            ("PET.RBRC2.D", "brent_c2"),
            ("PET.RBRC3.D", "brent_c3"),
            ("PET.RBRC4.D", "brent_c4"),
            ("PET.RBRTE1.D", "brent_c1"),
            ("PET.RBRTE2.D", "brent_c2"),
            ("PET.RBRTE3.D", "brent_c3"),
            ("PET.RBRTE4.D", "brent_c4"),
            # WTI-style naming for reference: PET.RCLC1.D is WTI contract 1
            # Brent equivalent might be:
            ("PET.RBRCE1.D", "brent_c1"),
            ("PET.RBRCE3.D", "brent_c3"),
        ]
        for series_id, col_name in seriesid_attempts:
            records = fetch_v1_series(api_key, series_id)
            if records:
                for r in records:
                    period = r.get("period", "")
                    value = r.get("value")
                    if value is not None and value != "":
                        try:
                            dt = datetime.strptime(period, "%Y-%m-%d")
                            if col_name not in all_daily:
                                all_daily[col_name] = []
                            all_daily[col_name].append({"date": dt, col_name: float(value)})
                        except (ValueError, TypeError):
                            pass
                if col_name in all_daily:
                    print(f"  Found: {series_id} -> {col_name} ({len(all_daily[col_name])} records)")

    # Step 5: Report results
    print("\n" + "=" * 60)
    if not all_daily:
        print("  No futures data found. Printing discovery results above for debugging.")
        print("  You may need to check the EIA API browser at:")
        print("  https://www.eia.gov/opendata/browser/petroleum/pri/fut")
        print("  to find the correct series IDs for Brent futures contracts.")
    else:
        print("  RESULTS")
        print("=" * 60)

        # Merge all series
        merged = None
        for col_name, rows in all_daily.items():
            df = pd.DataFrame(rows)
            if merged is None:
                merged = df
            else:
                merged = merged.merge(df, on="date", how="outer")

        merged = merged.sort_values("date").reset_index(drop=True)
        print(f"  Daily records: {len(merged)}")
        print(f"  Columns: {list(merged.columns)}")
        print(f"  Date range: {merged['date'].min().date()} to {merged['date'].max().date()}")

        # Save daily
        daily_path = os.path.join(OUT_DIR, "brent_futures_contracts_daily.csv")
        merged.to_csv(daily_path, index=False)
        print(f"  Saved: {daily_path}")

        # Aggregate to weekly
        merged["year"] = merged["date"].apply(lambda x: x.isocalendar()[0])
        merged["week"] = merged["date"].apply(lambda x: x.isocalendar()[1])
        price_cols = [c for c in merged.columns if c not in ["date", "year", "week"]]
        weekly = merged.groupby(["year", "week"])[price_cols].mean().reset_index()

        # Add convenience columns
        if "brent_c1" in weekly.columns:
            weekly["brent_front"] = weekly["brent_c1"]
        if "brent_c3" in weekly.columns:
            weekly["brent_3mo"] = weekly["brent_c3"]

        weekly_path = os.path.join(OUT_DIR, "brent_futures_contracts_weekly.csv")
        weekly.to_csv(weekly_path, index=False)
        print(f"  Weekly records: {len(weekly)}")
        print(f"  Saved: {weekly_path}")

        for col in price_cols:
            valid = weekly[col].notna().sum()
            if valid > 0:
                print(f"  {col}: {valid} weeks, "
                      f"${weekly[col].min():.2f} – ${weekly[col].max():.2f}")

    print("=" * 60)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        api_key = sys.argv[1]
    else:
        api_key = os.environ.get("EIA_API_KEY", "")

    if not api_key:
        print("Usage: python download_brent_futures_eia.py YOUR_API_KEY")
        print("Get a free key at: https://www.eia.gov/opendata/register.php")
        sys.exit(1)

    main(api_key)
