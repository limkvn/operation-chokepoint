#!/usr/bin/env python3
"""
Hormuz Crisis Oil Flow Nowcasting — Run Full Analysis
======================================================
Entry point that runs the complete pipeline end-to-end:
  1. Nowcasting pipeline (STL + controls regression + counterfactual projection)
  2. Dashboard generation (interactive HTML with Chart.js)

Usage:
    python run_analysis.py

Outputs (in outputs/nowcast/):
    - nowcast_results.json          Full pipeline results (all series, all metrics)
    - nowcast_dashboard_data.json   Compact 2-year window for dashboard embedding
    - crisis_deviation_summary.csv  Summary table of post-crisis deviations
    - hormuz_nowcast_dashboard.html Interactive dashboard

Requirements:
    pip install pandas numpy statsmodels scikit-learn
"""

import os, sys, time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.join(BASE_DIR, "scripts")
sys.path.insert(0, SCRIPTS_DIR)

def main():
    t0 = time.time()

    # ─── Step 1: Run nowcasting pipeline ────────────────────────────────
    print("\n" + "=" * 70)
    print("STEP 1: Running nowcasting pipeline")
    print("=" * 70)
    from nowcast_pipeline import run_pipeline
    results = run_pipeline()

    # ─── Step 2: Prepare compact dashboard data ─────────────────────────
    print("\n" + "=" * 70)
    print("STEP 2: Preparing dashboard data (crisis window)")
    print("=" * 70)
    import json
    import numpy as np
    from datetime import datetime

    # Show ~3 months before Iran attack (Feb 28, 2026) through end of data
    # 3 months before Feb 28 → late November 2025
    WINDOW_START = "2025-11-24"

    compact = {}
    for key, val in results.items():
        # Per-port deviation lists — pass through as-is
        if key.startswith("_"):
            compact[key] = val
            continue

        dates = val["dates"]
        # Find the index of the first date >= WINDOW_START
        start = 0
        for i, d in enumerate(dates):
            if d >= WINDOW_START:
                start = i
                break
        compact[key] = {
            "chokepoint": val["chokepoint"],
            "metric": val["metric"],
            "dates": val["dates"][start:],
            "actual": [round(v, 1) if not (isinstance(v, float) and np.isnan(v)) else 0 for v in val["actual"][start:]],
            "counterfactual_primary": [round(v, 1) if not (isinstance(v, float) and np.isnan(v)) else 0 for v in val["counterfactual_primary"][start:]],
            "counterfactual_sensitivity": [round(v, 1) if not (isinstance(v, float) and np.isnan(v)) else 0 for v in val["counterfactual_sensitivity"][start:]],
            "deviation_primary": [round(v, 1) if not (isinstance(v, float) and np.isnan(v)) else 0 for v in val["deviation_primary"][start:]],
            "crisis_date": val["crisis_date"],
            "train_end": val["train_end"],
        }
        # Carry through pre-crisis average if present
        if "pre_crisis_avg" in val:
            compact[key]["pre_crisis_avg"] = val["pre_crisis_avg"]
        # Include STL components for Hormuz (for decomposition panel)
        if "Hormuz" in key:
            for comp in ["trend", "seasonal", "remainder"]:
                if comp in val:
                    compact[key][comp] = [round(v, 1) if not (isinstance(v, float) and np.isnan(v)) else 0 for v in val[comp][start:]]

    output_dir = os.path.join(BASE_DIR, "outputs", "nowcast")
    with open(os.path.join(output_dir, "nowcast_dashboard_data.json"), "w") as f:
        json.dump(compact, f)
    print(f"  Dashboard data: {len(compact)} series, window from {WINDOW_START}")

    # ─── Step 3: Build dashboard HTML ───────────────────────────────────
    print("\n" + "=" * 70)
    print("STEP 3: Building interactive dashboard")
    print("=" * 70)

    # Import and run the dashboard builder
    os.chdir(BASE_DIR)
    from build_nowcast_dashboard import main as build_dashboard
    build_dashboard()

    # ─── Done ───────────────────────────────────────────────────────────
    elapsed = time.time() - t0
    print(f"\n{'=' * 70}")
    print(f"ANALYSIS COMPLETE in {elapsed:.1f}s")
    print(f"{'=' * 70}")
    print(f"\nOutputs in {output_dir}/:")
    for fn in sorted(os.listdir(output_dir)):
        fp = os.path.join(output_dir, fn)
        sz = os.path.getsize(fp)
        print(f"  {fn:45s}  {sz/1024:>8.1f} KB")


if __name__ == "__main__":
    main()
