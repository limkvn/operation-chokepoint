"""
Download IMF PortWatch Daily Data
=================================
Downloads the latest Daily Ports Data and Daily Chokepoints Data
from IMF PortWatch via the ArcGIS Feature Service REST API.

The data is publicly available at:
  - Ports:       https://portwatch.imf.org/datasets/d51e4539d51a4cc793a91f865de6bf80/about
  - Chokepoints: https://portwatch.imf.org/datasets/42132aa4e2fc4d41bdaf9a445f688931/about

No API key required — the data is public.

Usage:
  python download_portwatch_data.py              # download both datasets
  python download_portwatch_data.py --ports      # download ports only
  python download_portwatch_data.py --chokepoints # download chokepoints only

Output:
  data/portwatch/Daily_Ports_Data.csv
  data/portwatch/Daily_Chokepoints_Data.csv

Notes:
  - Ports data is ~5M+ records; expect ~15-30 minutes for a full download.
  - Chokepoints data is ~3,800 records; downloads in seconds.
  - The script paginates through the ArcGIS Feature Service API (max 2000
    records per request) and writes the result to CSV.
  - Date fields (epoch-ms in the API) are converted to YYYY-MM-DD strings.
  - Existing files are overwritten.
"""

import csv
import io
import json
import os
import sys
import time
from datetime import datetime
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

# ── Configuration ──────────────────────────────────────────────────────

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Forecasting/
OUT_DIR = os.path.join(BASE_DIR, "data", "portwatch")
os.makedirs(OUT_DIR, exist_ok=True)

# ArcGIS Feature Service endpoints
PORTS_URL = (
    "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services"
    "/Daily_Ports_Data/FeatureServer/0"
)
CHOKEPOINTS_URL = (
    "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services"
    "/Daily_Chokepoints_Data/FeatureServer/0"
)

# ArcGIS query parameters
PAGE_SIZE = 2000        # max records per request (ArcGIS limit is often 2000)
MAX_RETRIES = 3         # retries per request on failure
RETRY_DELAY = 5         # seconds between retries
REQUEST_DELAY = 0.5     # seconds between paginated requests (rate-limit courtesy)


# ── Helpers ────────────────────────────────────────────────────────────

def fetch_json(url, params=None, retries=MAX_RETRIES):
    """Fetch JSON from a URL with retry logic."""
    if params:
        query = "&".join(f"{k}={v}" for k, v in params.items())
        full_url = f"{url}?{query}"
    else:
        full_url = url

    for attempt in range(retries):
        try:
            req = Request(full_url, headers={"User-Agent": "PortWatch-Downloader/1.0"})
            with urlopen(req, timeout=120) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError) as e:
            if attempt < retries - 1:
                wait = RETRY_DELAY * (attempt + 1)
                print(f"  Retry {attempt + 1}/{retries} after error: {e} (waiting {wait}s)")
                time.sleep(wait)
            else:
                raise


def get_record_count(service_url):
    """Get the total number of records in a feature service layer."""
    data = fetch_json(f"{service_url}/query", {
        "where": "1=1",
        "returnCountOnly": "true",
        "f": "json",
    })
    return data.get("count", 0)


def get_fields(service_url):
    """Get field names and types from the feature service metadata."""
    data = fetch_json(service_url, {"f": "json"})
    fields = data.get("fields", [])
    return [(f["name"], f["type"]) for f in fields]


def download_feature_service(service_url, out_path, label="data"):
    """
    Download all records from an ArcGIS Feature Service layer to CSV.

    Uses paginated queries with resultOffset/resultRecordCount to handle
    large datasets that exceed the server's maxRecordCount.
    """
    print(f"\n{'='*60}")
    print(f"Downloading {label}")
    print(f"{'='*60}")

    # 1. Get total record count
    total = get_record_count(service_url)
    print(f"  Total records: {total:,}")

    # 2. Get field metadata
    fields_meta = get_fields(service_url)
    field_names = [name for name, _ in fields_meta]
    date_fields = {name for name, ftype in fields_meta if ftype == "esriFieldTypeDate"}

    # Exclude ObjectId/OID from output columns (we'll let it come through
    # in the query but drop it if not in the original CSV)
    print(f"  Fields: {len(field_names)}")

    # 3. Paginated download
    all_rows = []
    offset = 0
    page = 0
    start_time = time.time()

    while offset < total:
        page += 1
        params = {
            "where": "1=1",
            "outFields": "*",
            "orderByFields": "ObjectId",
            "resultOffset": str(offset),
            "resultRecordCount": str(PAGE_SIZE),
            "f": "json",
        }

        data = fetch_json(f"{service_url}/query", params)
        features = data.get("features", [])

        if not features:
            break

        for feat in features:
            attrs = feat.get("attributes", {})
            # Convert epoch-ms dates to readable strings
            for df in date_fields:
                if df in attrs and attrs[df] is not None:
                    try:
                        attrs[df] = datetime.utcfromtimestamp(
                            attrs[df] / 1000
                        ).strftime("%Y-%m-%d")
                    except (ValueError, OSError):
                        pass
            all_rows.append(attrs)

        fetched = len(all_rows)
        elapsed = time.time() - start_time
        rate = fetched / elapsed if elapsed > 0 else 0
        eta = (total - fetched) / rate if rate > 0 else 0
        print(
            f"  Page {page}: {fetched:,}/{total:,} records "
            f"({fetched/total*100:.1f}%) "
            f"[{rate:.0f} rec/s, ETA {eta:.0f}s]"
        )

        offset += len(features)

        # Check if the server indicated there are more records
        if not data.get("exceededTransferLimit", False) and len(features) < PAGE_SIZE:
            break

        time.sleep(REQUEST_DELAY)

    elapsed = time.time() - start_time
    print(f"  Downloaded {len(all_rows):,} records in {elapsed:.1f}s")

    # 4. Write CSV
    if not all_rows:
        print("  WARNING: No records downloaded!")
        return False

    # Use field order from metadata, but only include fields present in data
    data_keys = set(all_rows[0].keys())
    columns = [f for f in field_names if f in data_keys]
    # Add any extra fields not in metadata (shouldn't happen, but be safe)
    for k in all_rows[0].keys():
        if k not in columns:
            columns.append(k)

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_rows)

    file_size = os.path.getsize(out_path)
    print(f"  Saved to: {out_path}")
    print(f"  File size: {file_size / 1024 / 1024:.1f} MB")

    return True


# ── Validation ─────────────────────────────────────────────────────────

def validate_csv(path, expected_min_rows, label):
    """Basic validation of downloaded CSV."""
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        row_count = sum(1 for _ in reader)

    print(f"\n  Validation ({label}):")
    print(f"    Columns: {len(header)}")
    print(f"    Rows: {row_count:,}")

    if row_count < expected_min_rows:
        print(f"    WARNING: Expected at least {expected_min_rows:,} rows!")
        return False

    # Check a few expected columns
    header_lower = [h.lower() for h in header]
    if "date" not in header_lower and "year" not in header_lower:
        print("    WARNING: No 'date' or 'year' column found!")
        return False

    print("    OK")
    return True


# ── Main ───────────────────────────────────────────────────────────────

def main():
    args = sys.argv[1:]
    do_ports = "--ports" in args or not args or "--all" in args
    do_chokepoints = "--chokepoints" in args or not args or "--all" in args

    print(f"IMF PortWatch Data Downloader")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Output directory: {OUT_DIR}")

    success = True

    if do_ports:
        ports_path = os.path.join(OUT_DIR, "Daily_Ports_Data.csv")
        ok = download_feature_service(PORTS_URL, ports_path, "Daily Ports Data")
        if ok:
            validate_csv(ports_path, 1_000_000, "Ports")
        success = success and ok

    if do_chokepoints:
        cp_path = os.path.join(OUT_DIR, "Daily_Chokepoints_Data.csv")
        ok = download_feature_service(CHOKEPOINTS_URL, cp_path, "Daily Chokepoints Data")
        if ok:
            validate_csv(cp_path, 1_000, "Chokepoints")
        success = success and ok

    print(f"\n{'='*60}")
    if success:
        print("All downloads completed successfully.")
    else:
        print("Some downloads failed. Check output above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
