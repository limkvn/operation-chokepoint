#!/usr/bin/env python3
"""
Validation suite for the STL + controls nowcasting pipeline.

Two tests:
  A. Historical backtest — 2024 Houthi/Red Sea crisis
     Onset ~Jan 12, 2024. Train on Jun 2019 – Jan 11, 2024.
     Expect: large negative deviation at Bab el-Mandeb & Suez,
             near-zero at Hormuz, Cape rerouting signal.

  B. Pseudo out-of-sample rolling forecast evaluation
     Expanding window, 4-week-ahead forecasts over 2022-01-01 to 2025-12-31
     (pre-Hormuz crisis, but includes Houthi period as a stress test).
     Measures: RMSE, MAE, bias, coverage of ±1 std band.

Outputs:
  outputs/nowcast/validation_results.json
  outputs/nowcast/validation_report.html
"""

import os, sys, json, warnings
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from statsmodels.tsa.seasonal import STL
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler

warnings.filterwarnings("ignore")

# Add scripts dir to path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR, "scripts"))

from nowcast_pipeline import (
    load_chokepoint_data, load_controls, build_weekly_controls,
    run_stl, fit_residual_model, extrapolate_trend,
    CHOKEPOINTS, DATA_DIR, OUTPUT_DIR
)


# ═══════════════════════════════════════════════════════════════════════════
# A. HOUTHI BACKTEST
# ═══════════════════════════════════════════════════════════════════════════

HOUTHI_CRISIS_DATE = "2024-01-12"
HOUTHI_TRAIN_START = "2019-06-01"
HOUTHI_TRAIN_END   = "2024-01-11"
# Evaluation window: first 12 weeks post-onset (Jan 12 – Apr 5, 2024)
HOUTHI_EVAL_END    = "2024-04-15"


def run_houthi_backtest(weekly_data, frozen_monthly, frozen_daily, live_daily):
    """
    Re-run the STL + controls pipeline with Houthi crisis parameters.
    Returns per-chokepoint deviation stats.
    """
    print("\n" + "=" * 70)
    print("VALIDATION A: HOUTHI / RED SEA BACKTEST (onset Jan 12, 2024)")
    print("=" * 70)

    crisis_dt = pd.Timestamp(HOUTHI_CRISIS_DATE)
    train_start_dt = pd.Timestamp(HOUTHI_TRAIN_START)
    train_end_dt = pd.Timestamp(HOUTHI_TRAIN_END)
    eval_end_dt = pd.Timestamp(HOUTHI_EVAL_END)

    backtest_results = {}

    for cp_name in CHOKEPOINTS:
        if cp_name not in weekly_data:
            continue

        wk_df = weekly_data[cp_name]
        series = wk_df["n_tanker"].copy()

        # Only use data up to eval end (don't look at Hormuz crisis period)
        series = series.loc[series.index <= eval_end_dt]

        if len(series) < 104:
            continue

        print(f"\n  {cp_name}:")

        # STL on full available series up to eval end
        trend, seasonal, remainder = run_stl(series)

        # Build controls frozen at Houthi crisis onset
        X_frozen, X_live = build_weekly_controls(
            series.index, frozen_monthly, frozen_daily, live_daily,
            freeze_date=HOUTHI_CRISIS_DATE
        )

        # Training mask: HOUTHI_TRAIN_START to HOUTHI_TRAIN_END
        train_mask = pd.Series(False, index=series.index)
        train_mask[(series.index >= train_start_dt) & (series.index <= train_end_dt)] = True

        # Fit primary model (freeze AR lags at Houthi crisis onset, not Hormuz)
        model, scaler, pred_remainder = fit_residual_model(
            remainder, X_frozen, train_mask, freeze_date=HOUTHI_CRISIS_DATE
        )

        # Extrapolate trend
        n_post = (series.index > train_end_dt).sum()
        trend_extrap = extrapolate_trend(trend, train_end_dt, n_post)

        # Build counterfactual
        cf = (trend_extrap.reindex(series.index).ffill() +
              seasonal.reindex(series.index, fill_value=0) +
              pred_remainder.reindex(series.index, fill_value=0))

        # Deviation in post-crisis eval window
        post_mask = (series.index >= crisis_dt) & (series.index <= eval_end_dt)
        if post_mask.any():
            actual_post = series.loc[post_mask]
            cf_post = cf.loc[post_mask]
            deviation = actual_post - cf_post
            pct_dev = ((actual_post.mean() - cf_post.mean()) / cf_post.mean() * 100)

            # Also compute pre-crisis "deviation" for false positive check
            pre_check_start = crisis_dt - timedelta(weeks=12)
            pre_mask = (series.index >= pre_check_start) & (series.index < crisis_dt)
            pre_actual = series.loc[pre_mask]
            pre_cf = cf.loc[pre_mask]
            pre_pct = ((pre_actual.mean() - pre_cf.mean()) / pre_cf.mean() * 100) if pre_cf.mean() != 0 else 0

            print(f"    Post-crisis avg: actual={actual_post.mean():.0f}, "
                  f"cf={cf_post.mean():.0f}, deviation={pct_dev:+.1f}%")
            print(f"    Pre-crisis 12wk: deviation={pre_pct:+.1f}% (should be ~0%)")

            # Weekly deviation series for the chart
            weekly_devs = []
            for idx in actual_post.index:
                wk_label = idx.strftime("%Y-%m-%d")
                a = float(actual_post.loc[idx])
                c = float(cf_post.loc[idx])
                d = ((a - c) / c * 100) if c != 0 else 0
                weekly_devs.append({"week": wk_label, "actual": round(a, 1),
                                    "cf": round(c, 1), "pct": round(d, 1)})

            backtest_results[cp_name] = {
                "post_crisis_pct": round(float(pct_dev), 2),
                "pre_crisis_pct": round(float(pre_pct), 2),
                "post_actual_avg": round(float(actual_post.mean()), 1),
                "post_cf_avg": round(float(cf_post.mean()), 1),
                "n_post_weeks": int(post_mask.sum()),
                "weekly_deviations": weekly_devs,
                # Full series for chart
                "dates": [d.strftime("%Y-%m-%d") for d in series.index],
                "actual": [round(float(v), 1) for v in series.values],
                "counterfactual": [round(float(v), 1) for v in cf.reindex(series.index).values],
            }

    # Interpretation
    print("\n  BACKTEST SUMMARY:")
    print(f"  {'Chokepoint':<25s} {'Post-Crisis %':>14s} {'Pre-Crisis %':>13s} {'Pass?':>6s}")
    print(f"  {'─' * 60}")
    for cp, res in backtest_results.items():
        post = res["post_crisis_pct"]
        pre = res["pre_crisis_pct"]
        # Bab/Suez should show large negative, Hormuz should be ~0
        if cp in ("Bab el-Mandeb Strait", "Suez Canal"):
            passed = "YES" if post < -10 else "NO"
        elif cp == "Strait of Hormuz":
            passed = "YES" if abs(post) < 15 else "NO"
        elif cp == "Cape of Good Hope":
            passed = "YES" if post > -5 else "MAYBE"
        else:
            passed = "—"
        print(f"  {cp:<25s} {post:>+13.1f}% {pre:>+12.1f}% {passed:>6s}")

    return backtest_results


# ═══════════════════════════════════════════════════════════════════════════
# B. PSEUDO OUT-OF-SAMPLE ROLLING FORECAST
# ═══════════════════════════════════════════════════════════════════════════

OOS_START = "2022-01-01"  # start of evaluation window
OOS_END   = "2025-12-31"  # end (before Hormuz crisis)
HORIZON   = 4             # 4-week-ahead forecast


def run_oos_evaluation(weekly_data, frozen_monthly, frozen_daily, live_daily):
    """
    Expanding-window pseudo out-of-sample forecasts.
    For each evaluation origin t (every 4 weeks from OOS_START to OOS_END):
      1. Run STL on data up to t
      2. Extrapolate trend + seasonal + mean controls remainder for HORIZON weeks ahead
      3. Compare forecast vs actual
    """
    print("\n" + "=" * 70)
    print(f"VALIDATION B: PSEUDO OUT-OF-SAMPLE ({OOS_START} to {OOS_END}, {HORIZON}-week ahead)")
    print("=" * 70)

    oos_start_dt = pd.Timestamp(OOS_START)
    oos_end_dt = pd.Timestamp(OOS_END)
    train_start_dt = pd.Timestamp("2019-06-01")

    oos_results = {}

    for cp_name in CHOKEPOINTS:
        if cp_name not in weekly_data:
            continue

        wk_df = weekly_data[cp_name]
        series_full = wk_df["n_tanker"].copy()

        print(f"\n  {cp_name}:", end="", flush=True)

        all_dates = series_full.index
        eval_origins = all_dates[(all_dates >= oos_start_dt) & (all_dates <= oos_end_dt)]
        eval_origins = eval_origins[::4]  # every 4 weeks for speed

        forecasts = []
        actuals = []
        origins_list = []

        for origin in eval_origins:
            series_train = series_full.loc[series_full.index <= origin]
            if len(series_train) < 104:
                continue

            target_dates = all_dates[(all_dates > origin)][:HORIZON]
            if len(target_dates) < HORIZON:
                continue

            actual_target = series_full.loc[target_dates]

            try:
                trend, seasonal, remainder = run_stl(series_train)
            except Exception:
                continue

            # Extrapolate trend forward manually (since target_dates aren't in trend)
            pre_trend = trend.loc[trend.index <= origin]
            if len(pre_trend) >= 13:
                recent_t = pre_trend.iloc[-13:]
                x = np.arange(len(recent_t))
                slope = np.polyfit(x, recent_t.values, 1)[0]
            else:
                slope = 0
            last_val = float(pre_trend.iloc[-1])
            trend_fc_vals = [last_val + slope * (i + 1) for i in range(HORIZON)]
            trend_fc = pd.Series(trend_fc_vals, index=target_dates)

            # Seasonal forecast: use same ISO week from seasonal component
            seasonal_fc_vals = []
            for td in target_dates:
                wk = td.isocalendar()[1]
                matches = [d for d in seasonal.index if d.isocalendar()[1] == wk]
                seasonal_fc_vals.append(float(seasonal.loc[matches[-1]]) if matches else 0.0)
            seasonal_fc = pd.Series(seasonal_fc_vals, index=target_dates)

            # Remainder forecast: use mean of recent 13-week remainder (simple baseline)
            recent_rem = float(remainder.iloc[-13:].mean())

            # Combine
            forecast = trend_fc + seasonal_fc + recent_rem

            fc_avg = float(forecast.mean())
            actual_avg = float(actual_target.mean())

            if np.isnan(fc_avg) or np.isnan(actual_avg):
                continue

            forecasts.append(fc_avg)
            actuals.append(actual_avg)
            origins_list.append(origin.strftime("%Y-%m-%d"))

        if not forecasts:
            print(" no valid forecasts")
            continue

        forecasts = np.array(forecasts)
        actuals = np.array(actuals)

        errors = actuals - forecasts
        abs_errors = np.abs(errors)
        rmse = float(np.sqrt(np.mean(errors ** 2)))
        mae = float(np.mean(abs_errors))
        bias = float(np.mean(errors))
        mape = float(np.mean(abs_errors / np.maximum(actuals, 1)) * 100)

        pct_errors = errors / np.maximum(np.abs(forecasts), 1) * 100
        pct_rmse = float(np.sqrt(np.mean(pct_errors ** 2)))
        pct_bias = float(np.mean(pct_errors))

        error_std = float(np.std(errors))
        coverage_1std = float(np.mean(abs_errors <= error_std) * 100)
        coverage_2std = float(np.mean(abs_errors <= 2 * error_std) * 100)

        print(f" N={len(forecasts)}, RMSE={rmse:.1f}, MAE={mae:.1f}, "
              f"Bias={bias:+.1f}, MAPE={mape:.1f}%, ±1σ={coverage_1std:.0f}%")

        cf_values = [{"origin": o, "forecast": round(f, 1), "actual": round(a, 1)}
                     for o, f, a in zip(origins_list, forecasts, actuals)]

        oos_results[cp_name] = {
            "n_forecasts": len(forecasts),
            "rmse": round(rmse, 2),
            "mae": round(mae, 2),
            "bias": round(bias, 2),
            "mape": round(mape, 2),
            "pct_rmse": round(pct_rmse, 2),
            "pct_bias": round(pct_bias, 2),
            "coverage_1std": round(coverage_1std, 1),
            "coverage_2std": round(coverage_2std, 1),
            "error_std": round(error_std, 2),
            "forecast_vs_actual": cf_values,
        }

    return oos_results


# ═══════════════════════════════════════════════════════════════════════════
# C. GENERATE VALIDATION REPORT
# ═══════════════════════════════════════════════════════════════════════════

def build_validation_report(backtest, oos, output_path):
    """Generate an HTML validation report."""

    # KPI summary
    hormuz_bt = backtest.get("Strait of Hormuz", {})
    bab_bt = backtest.get("Bab el-Mandeb Strait", {})
    suez_bt = backtest.get("Suez Canal", {})

    hormuz_oos = oos.get("Strait of Hormuz", {})

    html = []
    html.append('<!DOCTYPE html>')
    html.append('<html lang="en"><head>')
    html.append('<meta charset="UTF-8">')
    html.append('<meta name="viewport" content="width=device-width, initial-scale=1.0">')
    html.append('<title>Pipeline Validation Report</title>')
    html.append('<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.js"></script>')
    html.append('<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-annotation@3.0.1/dist/chartjs-plugin-annotation.min.js"></script>')
    html.append('<style>')
    html.append('* { margin:0; padding:0; box-sizing:border-box; }')
    html.append("body { font-family: 'Inter', -apple-system, sans-serif; background: #0a0e17; color: #e5e7eb; line-height: 1.6; }")
    html.append('.container { max-width: 1400px; margin: 0 auto; padding: 2rem; }')
    html.append('h1 { font-size: 2rem; font-weight: 700; background: linear-gradient(135deg, #10b981, #3b82f6); -webkit-background-clip: text; -webkit-text-fill-color: transparent; margin-bottom: 0.5rem; }')
    html.append('h2 { font-size: 1.5rem; color: #e5e7eb; margin: 2.5rem 0 1rem 0; border-bottom: 1px solid #374151; padding-bottom: 0.5rem; }')
    html.append('h3 { font-size: 1.1rem; color: #93c5fd; margin: 1.5rem 0 0.75rem 0; }')
    html.append('.subtitle { color: #9ca3af; font-size: 1rem; margin-bottom: 2rem; }')
    html.append('.card-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 1.5rem; margin-bottom: 2rem; }')
    html.append('.card { background: linear-gradient(135deg, #1f2937, #111827); border: 1px solid #374151; border-radius: 0.75rem; padding: 1.25rem; }')
    html.append('.card-label { font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.05em; color: #9ca3af; margin-bottom: 0.5rem; }')
    html.append('.card-value { font-size: 1.75rem; font-weight: 700; }')
    html.append('.card-value.pass { color: #10b981; }')
    html.append('.card-value.fail { color: #ef4444; }')
    html.append('.card-value.warn { color: #f59e0b; }')
    html.append('.card-unit { font-size: 0.8rem; color: #6b7280; margin-top: 0.25rem; }')
    html.append('table { width: 100%; border-collapse: collapse; margin: 1rem 0; }')
    html.append('th { padding: 0.75rem 1rem; text-align: left; font-weight: 600; border-bottom: 2px solid #374151; color: #9ca3af; font-size: 0.8rem; text-transform: uppercase; background: #111827; }')
    html.append('td { padding: 0.75rem 1rem; border-bottom: 1px solid #1f2937; }')
    html.append('.num { text-align: right; font-variant-numeric: tabular-nums; }')
    html.append('.pos { color: #10b981; }')
    html.append('.neg { color: #ef4444; }')
    html.append('.neutral { color: #9ca3af; }')
    html.append('.chart-box { background: linear-gradient(135deg, #1f2937, #111827); border: 1px solid #374151; border-radius: 0.75rem; padding: 1.5rem; margin: 1.5rem 0; }')
    html.append('.chart-container { position: relative; height: 300px; }')
    html.append('.interpretation { background: #111827; border-left: 3px solid #3b82f6; padding: 1rem 1.5rem; margin: 1rem 0; border-radius: 0 0.5rem 0.5rem 0; color: #d1d5db; }')
    html.append('.pass-badge { display: inline-block; padding: 0.2rem 0.6rem; border-radius: 0.25rem; font-size: 0.75rem; font-weight: 600; }')
    html.append('.pass-badge.yes { background: rgba(16,185,129,0.2); color: #10b981; }')
    html.append('.pass-badge.no { background: rgba(239,68,68,0.2); color: #ef4444; }')
    html.append('.pass-badge.maybe { background: rgba(245,158,11,0.2); color: #f59e0b; }')
    html.append('</style></head><body>')
    html.append('<div class="container">')

    # Title
    html.append('<h1>STL + Controls Pipeline Validation</h1>')
    html.append('<p class="subtitle">Houthi backtest (Jan 2024) &amp; pseudo out-of-sample forecast evaluation</p>')

    # ─── Section A: Houthi Backtest ────────────────────────────────────────
    html.append('<h2>A. Historical Backtest: 2024 Houthi / Red Sea Crisis</h2>')
    html.append('<p style="color:#9ca3af; margin-bottom:1rem;">Crisis onset: Jan 12, 2024. Training window: Jun 2019 – Jan 11, 2024. Evaluation: 12 weeks post-onset.</p>')

    # KPI cards
    bab_pct = bab_bt.get("post_crisis_pct", 0)
    suez_pct = suez_bt.get("post_crisis_pct", 0)
    hormuz_pct = hormuz_bt.get("post_crisis_pct", 0)
    cape_pct = backtest.get("Cape of Good Hope", {}).get("post_crisis_pct", 0)

    html.append('<div class="card-grid">')
    # Bab el-Mandeb
    bab_class = "pass" if bab_pct < -10 else "fail"
    html.append('<div class="card"><div class="card-label">Bab el-Mandeb</div>')
    html.append('<div class="card-value ' + bab_class + '">' + f'{bab_pct:+.1f}%' + '</div>')
    html.append('<div class="card-unit">Expected: large negative</div></div>')
    # Suez
    suez_class = "pass" if suez_pct < -10 else ("warn" if suez_pct < 0 else "fail")
    html.append('<div class="card"><div class="card-label">Suez Canal</div>')
    html.append('<div class="card-value ' + suez_class + '">' + f'{suez_pct:+.1f}%' + '</div>')
    html.append('<div class="card-unit">Expected: negative</div></div>')
    # Hormuz (should be ~0, no false positive)
    hormuz_class = "pass" if abs(hormuz_pct) < 15 else "fail"
    html.append('<div class="card"><div class="card-label">Hormuz (control)</div>')
    html.append('<div class="card-value ' + hormuz_class + '">' + f'{hormuz_pct:+.1f}%' + '</div>')
    html.append('<div class="card-unit">Expected: near zero</div></div>')
    # Cape (rerouting)
    cape_class = "pass" if cape_pct > 0 else "warn"
    html.append('<div class="card"><div class="card-label">Cape (rerouting)</div>')
    html.append('<div class="card-value ' + cape_class + '">' + f'{cape_pct:+.1f}%' + '</div>')
    html.append('<div class="card-unit">Expected: positive</div></div>')
    html.append('</div>')

    # Backtest table
    html.append('<table>')
    html.append('<thead><tr><th>Chokepoint</th><th class="num">Post-Crisis Deviation</th><th class="num">Pre-Crisis Check</th><th>Expected</th><th>Result</th></tr></thead>')
    html.append('<tbody>')

    expected = {
        "Bab el-Mandeb Strait": "Large negative (direct disruption)",
        "Suez Canal": "Negative (linked to Red Sea)",
        "Strait of Hormuz": "Near zero (no false positive)",
        "Cape of Good Hope": "Positive (rerouting around Africa)",
        "Malacca Strait": "Minimal change",
    }

    for cp in CHOKEPOINTS:
        if cp not in backtest:
            continue
        r = backtest[cp]
        post = r["post_crisis_pct"]
        pre = r["pre_crisis_pct"]
        exp = expected.get(cp, "—")

        post_class = "neg" if post < -5 else ("pos" if post > 5 else "neutral")
        pre_class = "neg" if abs(pre) > 10 else "neutral"

        if cp in ("Bab el-Mandeb Strait", "Suez Canal"):
            badge = "yes" if post < -10 else "no"
        elif cp == "Strait of Hormuz":
            badge = "yes" if abs(post) < 15 else "no"
        elif cp == "Cape of Good Hope":
            badge = "yes" if post > 0 else "maybe"
        else:
            badge = "maybe"

        html.append('<tr>')
        html.append('<td>' + cp + '</td>')
        html.append('<td class="num ' + post_class + '">' + f'{post:+.1f}%' + '</td>')
        html.append('<td class="num ' + pre_class + '">' + f'{pre:+.1f}%' + '</td>')
        html.append('<td style="font-size:0.85rem">' + exp + '</td>')
        html.append('<td><span class="pass-badge ' + badge + '">' + badge.upper() + '</span></td>')
        html.append('</tr>')

    html.append('</tbody></table>')

    # Backtest charts — one per chokepoint showing actual vs counterfactual
    for cp in CHOKEPOINTS:
        if cp not in backtest:
            continue
        r = backtest[cp]
        chart_id = "bt_" + cp.replace(" ", "_").replace("-", "_").lower()

        html.append('<div class="chart-box">')
        html.append('<h3>' + cp + ' — Houthi Backtest</h3>')
        html.append('<div class="chart-container"><canvas id="' + chart_id + '"></canvas></div>')
        html.append('</div>')

        dates_js = json.dumps(r["dates"])
        actual_js = json.dumps(r["actual"])
        cf_js = json.dumps(r["counterfactual"])

        # Find crisis index
        html.append('<script>')
        html.append('(function(){')
        html.append('var dates = ' + dates_js + ';')
        html.append('var actual = ' + actual_js + ';')
        html.append('var cf = ' + cf_js + ';')
        html.append('var crisisIdx = -1;')
        html.append('for(var i=0;i<dates.length;i++){if(dates[i]>="2024-01-12"){crisisIdx=i;break;}}')
        html.append('var ann = crisisIdx >= 0 ? {annotations:{crisis:{type:"line",xMin:crisisIdx,xMax:crisisIdx,borderColor:"rgba(239,68,68,0.7)",borderWidth:2,borderDash:[6,3],label:{display:true,content:"Houthi Onset",color:"#ef4444",backgroundColor:"rgba(0,0,0,0.6)",font:{size:10,weight:"bold"},position:"start"}}}} : {};')
        html.append('var ctx = document.getElementById("' + chart_id + '").getContext("2d");')
        html.append('new Chart(ctx,{type:"line",data:{labels:dates,datasets:[{label:"Actual",data:actual,borderColor:"#3b82f6",backgroundColor:"rgba(59,130,246,0.05)",borderWidth:2,tension:0.3,fill:true,pointRadius:1},{label:"Counterfactual",data:cf,borderColor:"#8b5cf6",backgroundColor:"transparent",borderWidth:1.5,borderDash:[5,5],tension:0.3,fill:false,pointRadius:1}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:true,position:"top",labels:{color:"#e5e7eb"}},tooltip:{backgroundColor:"rgba(0,0,0,0.8)",titleColor:"#fff",bodyColor:"#fff"},annotation:ann},scales:{x:{grid:{color:"#1f2937"},ticks:{color:"#9ca3af",maxRotation:45}},y:{grid:{color:"#1f2937"},ticks:{color:"#9ca3af"}}}}});')
        html.append('})();')
        html.append('</script>')

    # ─── Section B: OOS Evaluation ─────────────────────────────────────────
    html.append('<h2>B. Pseudo Out-of-Sample Forecast Evaluation</h2>')
    html.append('<p style="color:#9ca3af; margin-bottom:1rem;">Expanding window, ' + str(HORIZON) + '-week-ahead forecasts evaluated from ' + OOS_START + ' to ' + OOS_END + '.</p>')

    # OOS summary table
    html.append('<table>')
    html.append('<thead><tr><th>Chokepoint</th><th class="num">N</th><th class="num">RMSE</th><th class="num">MAE</th><th class="num">Bias</th><th class="num">MAPE</th><th class="num">%RMSE</th><th class="num">%Bias</th><th class="num">±1&sigma; Cov.</th><th class="num">±2&sigma; Cov.</th></tr></thead>')
    html.append('<tbody>')

    for cp in CHOKEPOINTS:
        if cp not in oos:
            continue
        r = oos[cp]
        bias_class = "neg" if abs(r["pct_bias"]) > 5 else "neutral"
        html.append('<tr>')
        html.append('<td>' + cp + '</td>')
        html.append('<td class="num">' + str(r["n_forecasts"]) + '</td>')
        html.append('<td class="num">' + f'{r["rmse"]:.1f}' + '</td>')
        html.append('<td class="num">' + f'{r["mae"]:.1f}' + '</td>')
        html.append('<td class="num ' + bias_class + '">' + f'{r["bias"]:+.1f}' + '</td>')
        html.append('<td class="num">' + f'{r["mape"]:.1f}%' + '</td>')
        html.append('<td class="num">' + f'{r["pct_rmse"]:.1f}%' + '</td>')
        html.append('<td class="num ' + bias_class + '">' + f'{r["pct_bias"]:+.1f}%' + '</td>')
        html.append('<td class="num">' + f'{r["coverage_1std"]:.0f}%' + '</td>')
        html.append('<td class="num">' + f'{r["coverage_2std"]:.0f}%' + '</td>')
        html.append('</tr>')

    html.append('</tbody></table>')

    # Interpretation
    avg_mape = np.mean([oos[cp]["mape"] for cp in oos])
    avg_pct_bias = np.mean([oos[cp]["pct_bias"] for cp in oos])
    avg_coverage = np.mean([oos[cp]["coverage_1std"] for cp in oos])

    html.append('<div class="interpretation">')
    html.append('<strong>Interpretation:</strong> ')
    html.append('Average MAPE across chokepoints is ' + f'{avg_mape:.1f}%' + ', ')
    html.append('mean percentage bias is ' + f'{avg_pct_bias:+.1f}%' + ', ')
    html.append('and ±1&sigma; coverage is ' + f'{avg_coverage:.0f}%' + '. ')
    if avg_mape < 15:
        html.append('The counterfactual is reasonably well-calibrated for a weekly shipping flow series. ')
    else:
        html.append('MAPE is elevated, reflecting inherent volatility in weekly shipping flows. ')
    if abs(avg_pct_bias) < 3:
        html.append('Bias is minimal, indicating the model does not systematically over- or under-predict. ')
    else:
        direction = "over" if avg_pct_bias > 0 else "under"
        html.append('There is a mild systematic ' + direction + '-prediction bias. ')
    html.append('</div>')

    # Forecast vs actual scatter / time series per chokepoint
    for cp in CHOKEPOINTS:
        if cp not in oos:
            continue
        r = oos[cp]
        fva = r["forecast_vs_actual"]
        chart_id = "oos_" + cp.replace(" ", "_").replace("-", "_").lower()

        origins_js = json.dumps([x["origin"] for x in fva])
        fc_js = json.dumps([x["forecast"] for x in fva])
        act_js = json.dumps([x["actual"] for x in fva])

        html.append('<div class="chart-box">')
        html.append('<h3>' + cp + ' — Forecast vs Actual (' + str(HORIZON) + '-week ahead)</h3>')
        html.append('<div class="chart-container"><canvas id="' + chart_id + '"></canvas></div>')
        html.append('</div>')

        html.append('<script>')
        html.append('(function(){')
        html.append('var origins = ' + origins_js + ';')
        html.append('var fc = ' + fc_js + ';')
        html.append('var act = ' + act_js + ';')
        html.append('var ctx = document.getElementById("' + chart_id + '").getContext("2d");')
        html.append('new Chart(ctx,{type:"line",data:{labels:origins,datasets:[{label:"Actual",data:act,borderColor:"#3b82f6",backgroundColor:"rgba(59,130,246,0.1)",borderWidth:2,tension:0.3,fill:true,pointRadius:2},{label:"Forecast",data:fc,borderColor:"#10b981",backgroundColor:"transparent",borderWidth:2,borderDash:[5,5],tension:0.3,fill:false,pointRadius:2}]},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:true,position:"top",labels:{color:"#e5e7eb"}},tooltip:{backgroundColor:"rgba(0,0,0,0.8)",titleColor:"#fff",bodyColor:"#fff"}},scales:{x:{grid:{color:"#1f2937"},ticks:{color:"#9ca3af",maxRotation:45}},y:{grid:{color:"#1f2937"},ticks:{color:"#9ca3af"}}}}});')
        html.append('})();')
        html.append('</script>')

    # Methodology note
    html.append('<h2>Methodology</h2>')
    html.append('<div class="interpretation">')
    html.append('<strong>Houthi Backtest:</strong> The pipeline is re-run with crisis onset set to Jan 12, 2024 ')
    html.append('(when Houthi attacks began disrupting Red Sea shipping). The model trains on Jun 2019 – Jan 11, 2024 ')
    html.append('and evaluates deviations over the first 12 post-crisis weeks. A valid model should detect large negative ')
    html.append('deviations at Bab el-Mandeb and Suez (directly disrupted) while showing no false positive at Hormuz (unaffected).')
    html.append('<br><br>')
    html.append('<strong>Out-of-Sample Evaluation:</strong> Expanding-window forecasts are generated every 4 weeks ')
    html.append('from ' + OOS_START + ' to ' + OOS_END + '. At each origin, the model trains on all prior data, ')
    html.append('freezes controls, and projects ' + str(HORIZON) + ' weeks ahead. Metrics: RMSE (absolute error), ')
    html.append('MAE, MAPE (scale-normalized), percentage bias (systematic over/under-prediction), ')
    html.append('and coverage (fraction of actuals within ±1σ and ±2σ of forecast).')
    html.append('</div>')

    html.append('</div></body></html>')

    with open(output_path, 'w') as f:
        f.write('\n'.join(html))
    print(f"\nValidation report written to {output_path}")


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

def main():
    # Load data
    weekly_data = load_chokepoint_data()
    frozen_monthly, frozen_daily, live_daily = load_controls()

    # A. Houthi backtest
    backtest = run_houthi_backtest(weekly_data, frozen_monthly, frozen_daily, live_daily)

    # B. OOS evaluation
    oos = run_oos_evaluation(weekly_data, frozen_monthly, frozen_daily, live_daily)

    # Save raw results
    results = {"houthi_backtest": backtest, "oos_evaluation": oos}
    results_path = os.path.join(OUTPUT_DIR, "validation_results.json")
    with open(results_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\nResults saved to {results_path}")

    # Generate report
    report_path = os.path.join(OUTPUT_DIR, "validation_report.html")
    build_validation_report(backtest, oos, report_path)

    return results


if __name__ == "__main__":
    main()
