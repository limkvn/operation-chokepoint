#!/usr/bin/env python3
"""
Build the nowcast dashboard HTML from JSON data.

Generates a comprehensive dark-themed dashboard with:
- KPI cards for key metrics
- Chokepoint results table
- Hormuz main chart with toggle
- STL decomposition charts
- Multiple chokepoint comparison charts
- Regional port group charts
- Per-port deviation bar charts with STL vs naive comparison
- Methodology note
"""

import json
import math
import os
import csv
from pathlib import Path
from typing import Any, Dict, List


def load_data(json_path: str) -> Dict[str, Any]:
    """Load the nowcast dashboard data."""
    with open(json_path, 'r') as f:
        return json.load(f)


def escape_js_string(s: str) -> str:
    """Escape a string for safe inclusion in JavaScript."""
    s = s.replace('\\', '\\\\')
    s = s.replace('"', '\\"')
    s = s.replace('\n', '\\n')
    s = s.replace('\r', '\\r')
    s = s.replace('\t', '\\t')
    return s


def format_number(val: float) -> str:
    """Format a number for display."""
    if val is None:
        return 'N/A'
    if abs(val) >= 1e6:
        return f'{val/1e6:.1f}M'
    if abs(val) >= 1e3:
        return f'{val/1e3:.1f}K'
    if abs(val) % 1 == 0:
        return f'{int(val):,}'
    return f'{val:.1f}'


def get_latest_value(data: Dict[str, Any], key: str, field: str) -> float:
    """Get the latest value for a metric."""
    if key not in data:
        return None
    metric_data = data[key]
    field_data = metric_data.get(field, [])
    if field_data:
        return field_data[-1]
    return None


def _compute_yoy_qoq(dates: list, actual: list) -> tuple:
    """Compute year-over-year and quarter-over-quarter naive deviations.

    Looks up the actual value ~52 weeks ago (YoY) and ~13 weeks ago (QoQ)
    relative to the last data point, and returns percentage changes.

    Returns (yoy_pct, qoq_pct) — either may be None if data is unavailable.
    """
    if not dates or not actual or len(dates) < 2:
        return None, None

    n = len(dates)
    latest_val = actual[-1]
    if latest_val is None:
        return None, None

    yoy_pct = None
    qoq_pct = None

    # YoY: ~52 weeks back
    if n > 52:
        yoy_val = actual[-53]  # 52 positions back = 52 weeks
        if yoy_val is not None and yoy_val != 0:
            yoy_pct = (latest_val - yoy_val) / abs(yoy_val) * 100

    # QoQ: ~13 weeks back
    if n > 13:
        qoq_val = actual[-14]  # 13 positions back = 13 weeks
        if qoq_val is not None and qoq_val != 0:
            qoq_pct = (latest_val - qoq_val) / abs(qoq_val) * 100

    return yoy_pct, qoq_pct


def _fmt_naive_pct(val) -> tuple:
    """Format a YoY/QoQ percentage for table display. Returns (html_str, css_class)."""
    if val is None:
        return '—', ''
    cls = 'negative' if val < 0 else 'positive'
    return f'{val:+.1f}%', cls


def get_latest_deviation(data: Dict[str, Any], key: str) -> float:
    """Get the most recent week's deviation as a percentage of the counterfactual.

    With log-space STL, the counterfactual is always positive (expm1 output),
    so (actual - cf) / cf * 100 is always well-defined.
    """
    if key not in data:
        return None
    metric_data = data[key]
    actual = metric_data.get('actual', [])
    cf = metric_data.get('counterfactual_primary', [])
    if actual and cf:
        a_val = actual[-1]
        c_val = cf[-1]
        if a_val is not None and c_val is not None and c_val != 0:
            return (a_val - c_val) / abs(c_val) * 100
    return None


# ── Significance threshold for dimming ──────────────────────────────────────
_SIGNIFICANCE_SIGMA = 2.0   # deviation must exceed this many pre-crisis σ


def is_deviation_significant(data: Dict[str, Any], key: str) -> bool:
    """Check if the latest deviation exceeds the pre-crisis residual noise band.

    Returns True (significant / display normally) when:
      - |latest_deviation| >= _SIGNIFICANCE_SIGMA * pre_crisis_residual_std
      - Or the series has no counterfactual / insufficient pre-crisis data.
    Returns False (should be dimmed) when the deviation is within normal noise.
    """
    if key not in data:
        return True  # no data → nothing to dim
    metric_data = data[key]
    actual = metric_data.get('actual', [])
    cf = metric_data.get('counterfactual_primary', [])
    dates = metric_data.get('dates', [])
    crisis_date = metric_data.get('crisis_date', '2026-02-28')
    if not actual or not cf or len(actual) != len(cf):
        return True

    # Find crisis boundary
    crisis_idx = len(dates)  # default: all pre-crisis
    for i, d in enumerate(dates):
        if d >= crisis_date:
            crisis_idx = i
            break

    if crisis_idx < 20:
        return True  # not enough pre-crisis data to judge

    # Compute pre-crisis percentage deviations
    pre_devs = []
    for i in range(crisis_idx):
        a = actual[i]
        c = cf[i]
        if a is not None and c is not None and c != 0:
            pre_devs.append((a - c) / abs(c) * 100)

    if len(pre_devs) < 20:
        return True  # not enough valid points

    # Compute std of pre-crisis residuals
    import numpy as _np
    std = float(_np.std(pre_devs))
    if std < 0.01:
        return True  # essentially zero noise → any deviation is significant

    # Latest deviation
    dev = get_latest_deviation(data, key)
    if dev is None:
        return True

    return abs(dev) >= _SIGNIFICANCE_SIGMA * std


def get_significance_params(data: Dict[str, Any], key: str) -> tuple:
    """Return (deviation_pct, pre_crisis_std) for a metric.

    Returns (None, None) when there isn't enough data to compute.
    """
    if key not in data:
        return None, None
    metric_data = data[key]
    actual = metric_data.get('actual', [])
    cf = metric_data.get('counterfactual_primary', [])
    dates = metric_data.get('dates', [])
    crisis_date = metric_data.get('crisis_date', '2026-02-28')
    if not actual or not cf or len(actual) != len(cf):
        return None, None
    crisis_idx = len(dates)
    for i, d in enumerate(dates):
        if d >= crisis_date:
            crisis_idx = i
            break
    if crisis_idx < 20:
        return None, None
    pre_devs = []
    for i in range(crisis_idx):
        a = actual[i]
        c = cf[i]
        if a is not None and c is not None and c != 0:
            pre_devs.append((a - c) / abs(c) * 100)
    if len(pre_devs) < 20:
        return None, None
    import numpy as _np
    std = float(_np.std(pre_devs))
    dev = get_latest_deviation(data, key)
    return dev, std


def js_array(values: List[Any]) -> str:
    """Convert Python list to JavaScript array."""
    js_items = []
    for v in values:
        if v is None:
            js_items.append('null')
        elif isinstance(v, str):
            js_items.append('"' + escape_js_string(v) + '"')
        elif isinstance(v, (int, float)):
            if math.isnan(v) or math.isinf(v):
                js_items.append('null')
            elif isinstance(v, int):
                js_items.append(str(v))
            else:
                js_items.append(str(round(v, 1)))
        else:
            js_items.append('null')
    return '[' + ','.join(js_items) + ']'


def _build_table_rows(items: list, data: Dict[str, Any], id_suffix: str = '') -> str:
    """Build table rows for a list of (name, key) pairs, with expandable chart rows.
    Default sort: descending by pre-crisis historical average."""
    # Sort by pre-crisis average (largest first)
    items = sorted(items, key=lambda x: data.get(x[1], {}).get('pre_crisis_avg') or 0, reverse=True)
    rows = []
    for idx, (name, key) in enumerate(items):
        if key in data:
            entry = data[key]
            actual = get_latest_value(data, key, 'actual')
            cf = get_latest_value(data, key, 'counterfactual_primary')
            deviation = get_latest_deviation(data, key)
            pre_avg = entry.get('pre_crisis_avg')

            pre_avg_str = format_number(pre_avg) if pre_avg is not None else '—'
            actual_str = format_number(actual) if actual else '—'
            cf_str = format_number(cf) if cf else '—'
            if deviation is not None:
                deviation_str = f'{deviation:.1f}%'
                deviation_class = 'negative' if deviation < 0 else 'positive'
            else:
                deviation_str = '—'
                deviation_class = ''

            # Significance check — dim row if deviation is within pre-crisis noise
            sig = is_deviation_significant(data, key)
            dim_cls = ' dev-ns' if not sig else ''
            _row_dev, _row_sg = get_significance_params(data, key)
            _row_data_attrs = ''
            if _row_dev is not None and _row_sg is not None:
                _row_data_attrs = f' data-dev="{round(_row_dev, 2)}" data-sg="{round(_row_sg, 2)}"'

            # Post-crisis arrays for week slider
            _row_dates = entry.get('dates', [])
            _row_crisis = entry.get('crisis_date', '2026-02-28')
            _row_actual = entry.get('actual', [])
            _row_cf = entry.get('counterfactual_primary', [])
            _row_ci = len(_row_dates)
            for _ri, _rd in enumerate(_row_dates):
                if _rd >= _row_crisis:
                    _row_ci = _ri
                    break
            if _row_actual and _row_cf:
                _row_ta = [round(_row_actual[i], 2) if i < len(_row_actual) and _row_actual[i] is not None else 0
                           for i in range(_row_ci, len(_row_dates))]
                _row_tc = [round(_row_cf[i], 2) if i < len(_row_cf) and _row_cf[i] is not None else 0
                           for i in range(_row_ci, len(_row_dates))]
                _row_data_attrs += f' data-ta="{",".join(str(v) for v in _row_ta)}" data-tc="{",".join(str(v) for v in _row_tc)}"'

            # YoY and QoQ naive deviations
            yoy_pct, qoq_pct = _compute_yoy_qoq(entry.get('dates', []), entry.get('actual', []))
            yoy_str, yoy_class = _fmt_naive_pct(yoy_pct)
            qoq_str, qoq_class = _fmt_naive_pct(qoq_pct)

            # Embed time series trimmed to 52 weeks pre-crisis (JS trims further for default view)
            row_id = f'row_{key.replace("|","_").replace(" ","_")}_{idx}{id_suffix}'
            crisis_date = entry.get('crisis_date', '2026-03-02')
            _all_dates = entry.get('dates', [])
            _all_actual = entry.get('actual', [])
            _all_cf = entry.get('counterfactual_primary', [])
            # Trim to 52 weeks before crisis + post-crisis
            _trim_start = 0
            if _all_dates and crisis_date:
                from datetime import datetime as _dt, timedelta as _td
                try:
                    _crisis_dt = _dt.strptime(crisis_date, '%Y-%m-%d')
                    _start_dt = (_crisis_dt - _td(weeks=52)).strftime('%Y-%m-%d')
                    for _si, _d in enumerate(_all_dates):
                        if _d >= _start_dt:
                            _trim_start = _si
                            break
                except ValueError:
                    pass
            dates_json = js_array(_all_dates[_trim_start:])
            actual_json = js_array(_all_actual[_trim_start:])
            cf_json = js_array(_all_cf[_trim_start:])

            # Variance decomposition for R² display
            vd = entry.get('variance_decomp', {})
            vd_json = json.dumps(vd) if vd else 'null'

            rows.append(
                '    <tr class="expandable-row sig-dimmable' + dim_cls + '"' + _row_data_attrs + ' data-target="' + row_id + '" data-metric-key="' + escape_js_string(key) + '" style="cursor:pointer;" title="Click to show chart">\n' +
                '      <td class="region-cell">' + escape_js_string(name) + ' <span class="expand-icon">&#9654;</span></td>\n' +
                '      <td class="numeric-cell">' + pre_avg_str + '</td>\n' +
                '      <td class="numeric-cell wk-actual">' + actual_str + '</td>\n' +
                '      <td class="numeric-cell wk-cf">' + cf_str + '</td>\n' +
                '      <td class="numeric-cell deviation-cell wk-dev ' + deviation_class + '">' + deviation_str + '</td>\n' +
                '      <td class="numeric-cell deviation-cell ' + yoy_class + '">' + yoy_str + '</td>\n' +
                '      <td class="numeric-cell deviation-cell ' + qoq_class + '">' + qoq_str + '</td>\n' +
                '    </tr>\n' +
                '    <tr class="chart-row" id="' + row_id + '" style="display:none;">\n' +
                '      <td colspan="7" class="chart-cell">\n' +
                '        <div class="inline-chart-container"><canvas id="canvas_' + row_id + '"></canvas></div>\n' +
                '        <div class="vd-bar-container" id="vd_' + row_id + '"></div>\n' +
                '        <button class="zoom-toggle-btn" onclick="toggleChartZoom(this)" title="Zoom in to recent 3 months">Zoom In</button><button class="export-csv-btn" onclick="exportChartCSV(this)">Export CSV</button>\n' +
                '        <script type="application/json" class="chart-data">{"dates":' + dates_json + ',"actual":' + actual_json + ',"cf":' + cf_json + ',"crisis":"' + crisis_date + '","label":"' + escape_js_string(name) + '","vd":' + vd_json + '}</script>\n' +
                '      </td>\n' +
                '    </tr>'
            )
    return '\n'.join(rows)


def build_chokepoint_table(data: Dict[str, Any], vessel_type: str = 'tanker',
                           metric_type: str = 'capacity', id_suffix: str = '') -> str:
    """Build the chokepoints-only results table rows for one metric type.

    vessel_type: 'tanker' or 'container'
    metric_type: 'capacity' or 'count'
    """
    metric_suffix = f'{vessel_type}_{metric_type}'

    chokepoints = []
    for key in data:
        if key.startswith('_'):
            continue
        parts = key.split('|')
        if len(parts) != 2:
            continue
        name = parts[0]
        metric = parts[1]

        # Only include actual chokepoints (not port groups)
        if any(x in name.lower() for x in ['export', 'import', 'port']):
            continue

        if metric == metric_suffix:
            chokepoints.append((name, key))

    # Sorting handled by _build_table_rows (descending by pre-crisis avg)
    return _build_table_rows(chokepoints, data, id_suffix=id_suffix)


def build_port_group_tables(data: Dict[str, Any], vessel_type: str = 'tanker',
                            metric_type: str = 'tonnage', id_suffix: str = '') -> tuple:
    """Build two separate port group tables: exports and imports.
    Returns (export_rows, import_rows) as HTML strings.

    vessel_type: 'tanker' or 'container'
    metric_type: 'tonnage' or 'calls' (portcalls)
    """

    REGION_ORDER = [
        "Persian Gulf", "East Asia", "Southeast Asia", "Indian Subcontinent",
        "Mediterranean", "Northwest Europe", "North America", "Latin America",
        "West Africa", "Russia", "Oceania",
    ]

    # Build the suffix to match based on vessel_type + metric_type
    if metric_type == 'tonnage':
        if vessel_type == 'tanker':
            suffix_match = '_tonnage'
            # Exclude other vessel types that also end in _tonnage
            suffix_excludes = ['_container_tonnage', '_dry_bulk_tonnage', '_general_cargo_tonnage', '_roro_tonnage']
        else:
            suffix_match = f'_{vessel_type}_tonnage'
            suffix_excludes = []
    elif metric_type == 'calls':
        suffix_match = f'_{vessel_type}_calls'
        suffix_excludes = []
    else:
        return '', ''

    # Discover all port group keys
    group_keys = {}
    for key in data:
        if key.startswith('_'):
            continue
        parts = key.split('|')
        if len(parts) != 2:
            continue
        name = parts[0]
        metric_slug = parts[1]

        if metric_slug.endswith(suffix_match):
            excluded = False
            for excl in suffix_excludes:
                if metric_slug.endswith(excl):
                    excluded = True
                    break
            if not excluded:
                group_keys[name] = key

    # Split into exports and imports, sorted by region order
    export_items = []
    import_items = []
    for region in REGION_ORDER:
        for name, key in sorted(group_keys.items()):
            if name.startswith(region):
                if 'Exports' in name:
                    # For portcalls, strip "Exports" since calls are symmetric
                    display_name = name.replace(' Exports', '') if metric_type == 'calls' else name
                    export_items.append((display_name, key))
                elif 'Imports' in name:
                    import_items.append((name, key))

    export_rows = _build_table_rows(export_items, data, id_suffix=id_suffix)
    import_rows = _build_table_rows(import_items, data, id_suffix=id_suffix)

    return export_rows, import_rows


def build_country_tables(data: Dict[str, Any], vessel_type: str = 'tanker',
                         metric_type: str = 'tonnage', id_suffix: str = '') -> tuple:
    """Build two separate country-level tables: exports and imports.
    Returns (export_rows, import_rows) as HTML strings.

    Discovers keys with 'COUNTRY:' prefix in data dict.
    """
    # Build the suffix to match based on vessel_type + metric_type
    if metric_type == 'tonnage':
        if vessel_type == 'tanker':
            suffix_match = '_tonnage'
            suffix_excludes = ['_container_tonnage', '_dry_bulk_tonnage', '_general_cargo_tonnage', '_roro_tonnage']
        else:
            suffix_match = f'_{vessel_type}_tonnage'
            suffix_excludes = []
    elif metric_type == 'calls':
        suffix_match = f'_{vessel_type}_calls'
        suffix_excludes = []
    else:
        return '', ''

    # Discover all country-level keys (prefixed with COUNTRY:)
    group_keys = {}
    for key in data:
        if key.startswith('_'):
            continue
        parts = key.split('|')
        if len(parts) != 2:
            continue
        name = parts[0]
        metric_slug = parts[1]

        if not name.startswith('COUNTRY:'):
            continue

        if metric_slug.endswith(suffix_match):
            excluded = False
            for excl in suffix_excludes:
                if metric_slug.endswith(excl):
                    excluded = True
                    break
            if not excluded:
                # Display name: strip 'COUNTRY:' prefix
                display_name = name[len('COUNTRY:'):]
                group_keys[display_name] = key

    # Split into exports and imports
    export_items = []
    import_items = []
    for name, key in sorted(group_keys.items()):
        if 'Exports' in name:
            # For portcalls, strip "Exports" since calls are symmetric
            display_name = name.replace(' Exports', '') if metric_type == 'calls' else name
            export_items.append((display_name, key))
        elif 'Imports' in name:
            import_items.append((name, key))

    export_rows = _build_table_rows(export_items, data, id_suffix=id_suffix)
    import_rows = _build_table_rows(import_items, data, id_suffix=id_suffix)

    return export_rows, import_rows


def build_top_port_table(data: Dict[str, Any], data_key: str, title: str, id_suffix: str = '') -> str:
    """Build an HTML table card for top-50 export or import ports with region tags."""
    port_list = data.get(data_key, [])
    if not port_list:
        return ''

    # Region color mapping
    region_colors = {
        'Gulf': '#ef4444', 'East Asia': '#3b82f6', 'SE Asia': '#06b6d4',
        'Oceania': '#06b6d4', 'S. Asia': '#f59e0b', 'Med': '#a855f7',
        'N. Africa': '#a855f7', 'Europe': '#6366f1', 'N. America': '#10b981',
        'LatAm': '#22c55e', 'W. Africa': '#f97316', 'S. Africa': '#f97316',
        'E. Africa': '#f97316', 'Russia': '#64748b', 'C. Asia': '#64748b',
    }

    # Default sort: descending by pre-crisis historical average
    port_list = sorted(port_list, key=lambda x: x.get('pre_crisis_avg') or 0, reverse=True)

    rows = []
    for idx, p in enumerate(port_list):
        port_name = p.get('port', 'Unknown')
        iso3 = p.get('iso3', '')
        region = p.get('region', iso3)
        stl_pct = p.get('stl_pct')
        naive_pct = p.get('naive_pct')
        pre_avg = p.get('pre_crisis_avg')

        # Use STL if available, else naive
        pct = stl_pct if stl_pct is not None else naive_pct
        if pct is None:
            continue

        pct_str = f'{pct:+.1f}%'
        pct_class = 'negative' if pct < -5 else ('positive' if pct > 5 else '')
        pre_avg_str = format_number(pre_avg) if pre_avg is not None else '—'

        # Region tag (inline with port name)
        color = region_colors.get(region, '#94a3b8')
        region_tag = ' <span class="region-tag" style="background:' + color + '22; color:' + color + '; border: 1px solid ' + color + '44;">' + escape_js_string(region) + '</span>'

        # Latest actual and counterfactual values
        actual_arr = p.get('actual', [])
        cf_arr = p.get('counterfactual', [])
        actual_val = actual_arr[-1] if actual_arr else None
        cf_val = cf_arr[-1] if cf_arr else None
        actual_str = format_number(actual_val) if actual_val is not None else '—'
        cf_str = format_number(cf_val) if cf_val is not None else '—'

        # YoY and QoQ naive deviations
        yoy_pct, qoq_pct = _compute_yoy_qoq(p.get('dates', []), actual_arr)
        yoy_str, yoy_class = _fmt_naive_pct(yoy_pct)
        qoq_str, qoq_class = _fmt_naive_pct(qoq_pct)

        # Expandable chart data
        row_id = f'port_{data_key}_{idx}{id_suffix}'
        has_series = 'dates' in p and 'actual' in p and 'counterfactual' in p
        dates_json = js_array(p.get('dates', [])) if has_series else '[]'
        actual_json = js_array(actual_arr) if has_series else '[]'
        cf_json = js_array(cf_arr) if has_series else '[]'

        # Variance decomposition for R² display
        vd = p.get('variance_decomp', {})
        vd_json = json.dumps(vd) if vd else 'null'

        rows.append(
            '    <tr class="expandable-row" data-target="' + row_id + '" style="cursor:pointer;" title="Click to show chart">\n' +
            '      <td class="region-cell">' + escape_js_string(port_name) + ' <span style="color:#6b7280;font-size:0.75rem;">(' + iso3 + ')</span>' + region_tag + ' <span class="expand-icon">&#9654;</span></td>\n' +
            '      <td class="numeric-cell">' + pre_avg_str + '</td>\n' +
            '      <td class="numeric-cell">' + actual_str + '</td>\n' +
            '      <td class="numeric-cell">' + cf_str + '</td>\n' +
            '      <td class="numeric-cell deviation-cell ' + pct_class + '">' + pct_str + '</td>\n' +
            '      <td class="numeric-cell deviation-cell ' + yoy_class + '">' + yoy_str + '</td>\n' +
            '      <td class="numeric-cell deviation-cell ' + qoq_class + '">' + qoq_str + '</td>\n' +
            '    </tr>\n' +
            '    <tr class="chart-row" id="' + row_id + '" style="display:none;">\n' +
            '      <td colspan="7" class="chart-cell">\n' +
            '        <div class="inline-chart-container"><canvas id="canvas_' + row_id + '"></canvas></div>\n' +
            '        <div class="vd-bar-container" id="vd_' + row_id + '"></div>\n' +
            '        <button class="zoom-toggle-btn" onclick="toggleChartZoom(this)" title="Zoom in to recent 3 months">Zoom In</button><button class="export-csv-btn" onclick="exportChartCSV(this)">Export CSV</button>\n' +
            '        <script type="application/json" class="chart-data">{"dates":' + dates_json + ',"actual":' + actual_json + ',"cf":' + cf_json + ',"crisis":"2026-03-02","label":"' + escape_js_string(port_name) + '","vd":' + vd_json + '}</script>\n' +
            '      </td>\n' +
            '    </tr>'
        )

    is_tonnage = 'Tonnage' in title or 'Capacity' in title
    metric_attr = ' data-metric-type="tonnage"' if is_tonnage else ' data-metric-type="counts"'
    section = (
        '        <div class="table-section"' + metric_attr + '>\n' +
        '            <h2>' + escape_js_string(title) + '</h2>\n' +
        '            <table>\n' +
        '                <thead>\n' +
        '                    <tr>\n' +
        '                        <th>Port</th>\n' +
        '                        <th>Hist. Avg</th>\n' +
        '                        <th>Latest</th>\n' +
        '                        <th>Counterfactual</th>\n' +
        '                        <th>Deviation</th>\n' +
        '                        <th>vs 1Y ago</th>\n' +
        '                        <th>vs 1Q ago</th>\n' +
        '                    </tr>\n' +
        '                </thead>\n' +
        '                <tbody>\n' +
        '\n'.join(rows) + '\n' +
        '                </tbody>\n' +
        '            </table>\n' +
        '        </div>'
    )
    return section


def build_leaflet_map(data: Dict[str, Any]) -> str:
    """Build a Leaflet.js interactive world map showing port and chokepoint deviations.
    Embeds data for all vessel types so the map updates when the vessel toggle changes."""

    VESSEL_TYPES = ['tanker', 'container', 'dry_bulk', 'general_cargo', 'roro']

    # --- Chokepoint coordinates ---
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    cp_csv = os.path.join(BASE_DIR, "data", "portwatch", "Chokepoints.csv")
    CHOKEPOINT_COORDS = {}
    try:
        with open(cp_csv, encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                CHOKEPOINT_COORDS[row["portname"]] = (float(row["lat"]), float(row["lon"]))
    except Exception:
        pass

    # --- Compute post-crisis date indices for week slider support ---
    _map_all_dates = []
    _map_crisis_str = ''
    for _dk in data:
        if isinstance(data[_dk], dict) and 'dates' in data[_dk]:
            _map_all_dates = data[_dk]['dates']
            _map_crisis_str = data[_dk].get('crisis_date', '2026-02-28')
            break
    _map_crisis_idx = len(_map_all_dates)
    for _ci, _cd in enumerate(_map_all_dates):
        if _cd >= _map_crisis_str:
            _map_crisis_idx = _ci
            break
    _map_n_post = len(_map_all_dates) - _map_crisis_idx

    def _map_pc_arrays(key):
        """Get post-crisis (ta, tc) arrays for a data key, for map week slider."""
        if key not in data:
            return None, None
        md = data[key]
        actual = md.get('actual', [])
        cf = md.get('counterfactual_primary', [])
        if not actual or not cf:
            return None, None
        ta = [round(actual[i], 4) if i < len(actual) and actual[i] is not None else 0
              for i in range(_map_crisis_idx, _map_crisis_idx + _map_n_post)]
        tc = [round(cf[i], 4) if i < len(cf) and cf[i] is not None else 0
              for i in range(_map_crisis_idx, _map_crisis_idx + _map_n_post)]
        return ta, tc

    def _js_arr_inline(arr):
        if arr is None:
            return 'null'
        return '[' + ','.join(str(v) for v in arr) + ']'

    # --- Build chokepoint data per vessel type ---
    # For each vessel type, the chokepoint metric key is: cp_name|{vt}_capacity
    # Filter out markers for series with negligible baseline traffic to prevent
    # near-zero denominators producing absurd deviation percentages on the map.
    _MAP_MIN_AVG_CAPACITY = 1000   # minimum pre-crisis avg tonnage for map marker
    cp_data_by_vessel = {}
    for vt in VESSEL_TYPES:
        items = []
        for cp_name, (lat, lon) in CHOKEPOINT_COORDS.items():
            key = cp_name + '|' + vt + '_capacity'
            # Skip markers for series with negligible baseline traffic
            series_data = data.get(key, {})
            pre_avg = series_data.get('pre_crisis_avg', 0) if isinstance(series_data, dict) else 0
            if pre_avg < _MAP_MIN_AVG_CAPACITY:
                continue
            dev = get_latest_deviation(data, key)
            if dev is None:
                dev = 0
            _cp_dev, _cp_sg = get_significance_params(data, key)
            _cp_d_js = 'null' if _cp_dev is None else str(round(_cp_dev, 2))
            _cp_sg_js = 'null' if _cp_sg is None else str(round(_cp_sg, 2))
            _cp_ta, _cp_tc = _map_pc_arrays(key)
            items.append(
                '{lat:' + str(lat) + ',lon:' + str(lon) +
                ',name:"' + escape_js_string(cp_name) + '",pct:' + str(round(dev, 1)) +
                ',d:' + _cp_d_js + ',sg:' + _cp_sg_js +
                ',ta:' + _js_arr_inline(_cp_ta) + ',tc:' + _js_arr_inline(_cp_tc) + '}'
            )
        cp_data_by_vessel[vt] = '[' + ','.join(items) + ']'

    # --- Build port data per vessel type ---
    _MAP_MIN_AVG_PORT_TONNAGE = 1000  # minimum pre-crisis avg for port map markers
    def _port_sig_params(p):
        """Compute (dev, sigma) for a port entry from its actual/counterfactual arrays."""
        actual = p.get('actual', [])
        cf = p.get('counterfactual', [])
        dates = p.get('dates', [])
        crisis_date = p.get('crisis_date', '2026-02-28')
        if not actual or not cf or len(actual) != len(cf):
            return None, None
        crisis_idx = len(dates)
        for i, d in enumerate(dates):
            if d >= crisis_date:
                crisis_idx = i
                break
        if crisis_idx < 20:
            return None, None
        pre_devs = []
        for i in range(crisis_idx):
            a, c = actual[i], cf[i]
            if a is not None and c is not None and c != 0:
                pre_devs.append((a - c) / abs(c) * 100)
        if len(pre_devs) < 20:
            return None, None
        import numpy as _np
        std = float(_np.std(pre_devs))
        # Latest deviation
        a_last = actual[-1] if actual else None
        c_last = cf[-1] if cf else None
        if a_last is not None and c_last is not None and c_last != 0:
            dev = (a_last - c_last) / abs(c_last) * 100
        else:
            dev = None
        return dev, std

    def _port_pc_arrays(p):
        """Get post-crisis (ta, tc) arrays for a port entry."""
        actual = p.get('actual', [])
        cf = p.get('counterfactual', [])
        dates = p.get('dates', [])
        crisis_date = p.get('crisis_date', '2026-02-28')
        if not actual or not cf:
            return None, None
        pc_idx = len(dates)
        for i, d in enumerate(dates):
            if d >= crisis_date:
                pc_idx = i
                break
        n = _map_n_post
        ta = [round(actual[pc_idx + i], 4) if pc_idx + i < len(actual) and actual[pc_idx + i] is not None else 0 for i in range(n)]
        tc = [round(cf[pc_idx + i], 4) if pc_idx + i < len(cf) and cf[pc_idx + i] is not None else 0 for i in range(n)]
        return ta, tc

    def build_port_arr(ports):
        items = []
        for p in ports:
            if 'lat' not in p or 'lon' not in p:
                continue
            # Skip ports with negligible baseline traffic
            if p.get('pre_crisis_avg', 0) < _MAP_MIN_AVG_PORT_TONNAGE:
                continue
            pct = p.get('stl_pct', p.get('naive_pct', 0))
            _p_dev, _p_sg = _port_sig_params(p)
            _p_d_js = 'null' if _p_dev is None else str(round(_p_dev, 2))
            _p_sg_js = 'null' if _p_sg is None else str(round(_p_sg, 2))
            _p_ta, _p_tc = _port_pc_arrays(p)
            items.append(
                '{lat:' + str(p['lat']) + ',lon:' + str(p['lon']) +
                ',port:"' + escape_js_string(p.get('port', '')) +
                '",iso3:"' + escape_js_string(p.get('iso3', '')) +
                '",region:"' + escape_js_string(p.get('region', '')) +
                '",pct:' + str(round(pct, 1)) +
                ',d:' + _p_d_js + ',sg:' + _p_sg_js +
                ',ta:' + _js_arr_inline(_p_ta) + ',tc:' + _js_arr_inline(_p_tc) + '}'
            )
        return '[' + ',\n            '.join(items) + ']'

    # Map from vessel type to the top50 key suffixes
    EXPORT_KEYS = {
        'tanker': '_top50_export_ports',
        'container': '_top50_export_container_ports',
        'dry_bulk': '_top50_export_dry_bulk_ports',
        'general_cargo': '_top50_export_general_cargo_ports',
        'roro': '_top50_export_roro_ports',
    }
    IMPORT_KEYS = {
        'tanker': '_top50_import_ports',
        'container': '_top50_import_container_ports',
        'dry_bulk': '_top50_import_dry_bulk_ports',
        'general_cargo': '_top50_import_general_cargo_ports',
        'roro': '_top50_import_roro_ports',
    }

    export_data_by_vessel = {}
    import_data_by_vessel = {}
    for vt in VESSEL_TYPES:
        export_data_by_vessel[vt] = build_port_arr(data.get(EXPORT_KEYS[vt], []))
        import_data_by_vessel[vt] = build_port_arr(data.get(IMPORT_KEYS[vt], []))

    # Build the JS data object with all vessel types
    # Format: window._mapAllData = { tanker: {cp: [...], exp: [...], imp: [...]}, ... }
    all_data_js = '{\n'
    for vt in VESSEL_TYPES:
        all_data_js += (
            '                ' + vt + ': {\n'
            '                    cp: ' + cp_data_by_vessel[vt] + ',\n'
            '                    exp: ' + export_data_by_vessel[vt] + ',\n'
            '                    imp: ' + import_data_by_vessel[vt] + '\n'
            '                },\n'
        )
    all_data_js += '            }'

    # Vessel type labels for tooltips
    VT_LABELS = {
        'tanker': 'Tanker capacity',
        'container': 'Container capacity',
        'dry_bulk': 'Dry bulk capacity',
        'general_cargo': 'General cargo capacity',
        'roro': 'RoRo capacity',
    }
    vt_labels_js = '{'
    for vt in VESSEL_TYPES:
        vt_labels_js += vt + ':"' + VT_LABELS[vt] + '",'
    vt_labels_js += '}'

    section = (
        '        <div class="chart-section" style="margin-bottom:2rem;">\n' +
        '            <div class="chart-header">\n' +
        '                <h3>Global Port &amp; Chokepoint Deviation Map</h3>\n' +
        '                <div class="chart-controls" id="mapControls">\n' +
        '                    <button class="metric-btn active" id="mapBtnChokepoints" onclick="toggleMapLayer(\'chokepoints\')">Chokepoints</button>\n' +
        '                    <button class="metric-btn active" id="mapBtnExport" onclick="toggleMapLayer(\'export\')">Port Exports</button>\n' +
        '                    <button class="metric-btn active" id="mapBtnImport" onclick="toggleMapLayer(\'import\')">Port Imports</button>\n' +
        '                    <button class="metric-btn active" id="mapBtnPorts" onclick="toggleMapLayer(\'ports\')" style="display:none;">Ports</button>\n' +
        '                </div>\n' +
        '            </div>\n' +
        '            <p style="color:#9ca3af;font-size:0.82rem;margin:0.5rem 0 0.75rem 0;line-height:1.5;">'
        'Each marker shows the latest weekly deviation between observed vessel traffic and the STL counterfactual (what traffic would have been absent the crisis). '
        'Color indicates direction (red = decline, green = increase, yellow = stable within &plusmn;5%), and size scales with the magnitude of the deviation. '
        'Toggle layers and vessel types above to explore different slices.</p>\n' +
        '            <div id="portMap" style="height:500px;border-radius:0.5rem;z-index:1;"></div>\n' +
        '            <div style="margin-top:0.75rem;display:flex;flex-wrap:wrap;align-items:center;gap:1.5rem;font-size:0.8rem;color:#9ca3af;">\n' +
        '                <span style="display:flex;align-items:center;gap:0.4rem;"><span style="display:inline-block;width:12px;height:12px;background:#ef4444;"></span> Decline (&lt;-5%)</span>\n' +
        '                <span style="display:flex;align-items:center;gap:0.4rem;"><span style="display:inline-block;width:12px;height:12px;background:#eab308;"></span> Stable (&plusmn;5%)</span>\n' +
        '                <span style="display:flex;align-items:center;gap:0.4rem;"><span style="display:inline-block;width:12px;height:12px;background:#22c55e;"></span> Increase (&gt;+5%)</span>\n' +
        '                <span style="display:flex;align-items:center;gap:0.25rem;">\n' +
        '                    <svg width="16" height="16" viewBox="0 0 16 16"><rect x="3" y="3" width="10" height="10" transform="rotate(45 8 8)" fill="#94a3b8" stroke="#fff" stroke-width="1"/></svg>\n' +
        '                    Chokepoint\n' +
        '                </span>\n' +
        '                <span id="legendExport" style="display:flex;align-items:center;gap:0.25rem;">\n' +
        '                    <svg width="14" height="14" viewBox="0 0 14 14"><polygon points="7,1 13,13 1,13" fill="#94a3b8" stroke="#fff" stroke-width="1"/></svg>\n' +
        '                    Port (exports)\n' +
        '                </span>\n' +
        '                <span id="legendImport" style="display:flex;align-items:center;gap:0.25rem;">\n' +
        '                    <svg width="14" height="14" viewBox="0 0 14 14"><polygon points="1,1 13,1 7,13" fill="#94a3b8" stroke="#fff" stroke-width="1"/></svg>\n' +
        '                    Port (imports)\n' +
        '                </span>\n' +
        '                <span id="legendPorts" style="display:none;align-items:center;gap:0.25rem;">\n' +
        '                    <svg width="14" height="14" viewBox="0 0 14 14"><circle cx="7" cy="7" r="6" fill="#94a3b8" stroke="#fff" stroke-width="1"/></svg>\n' +
        '                    Port (calls)\n' +
        '                </span>\n' +
        '                <span style="color:#6b7280;">Size = |deviation|</span>\n' +
        '            </div>\n' +
        '        </div>\n' +
        '        <script>\n' +
        '        function initPortMap() {\n' +
        '            if (typeof L === "undefined") {\n' +
        '                var s = document.createElement("script");\n' +
        '                s.src = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.js";\n' +
        '                s.onload = initPortMap;\n' +
        '                s.onerror = function() { document.getElementById("portMap").innerHTML = "<p style=\\"color:#f87171;padding:2rem;\\">Map unavailable — could not load Leaflet library.</p>"; };\n' +
        '                document.head.appendChild(s);\n' +
        '                return;\n' +
        '            }\n' +
        '\n' +
        '            // All vessel type data\n' +
        '            window._mapAllData = ' + all_data_js + ';\n' +
        '            window._mapVtLabels = ' + vt_labels_js + ';\n' +
        '            window._mapCurrentVessel = "tanker";\n' +
        '\n' +
        '            var map = L.map("portMap", {\n' +
        '                center: [20, 55],\n' +
        '                zoom: 3,\n' +
        '                zoomControl: true,\n' +
        '                scrollWheelZoom: true,\n' +
        '                worldCopyJump: true,\n' +
        '                maxBounds: [[-85, -Infinity], [85, Infinity]],\n' +
        '                maxBoundsViscosity: 0\n' +
        '            });\n' +
        '\n' +
        '            L.tileLayer("https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png", {\n' +
        '                attribution: "CartoDB",\n' +
        '                subdomains: "abcd",\n' +
        '                maxZoom: 12\n' +
        '            }).addTo(map);\n' +
        '\n' +
        '            function getColor(pct) {\n' +
        '                if (pct < -5) return "#ef4444";\n' +
        '                if (pct > 5) return "#22c55e";\n' +
        '                return "#eab308";\n' +
        '            }\n' +
        '\n' +
        '            function getSize(pct) {\n' +
        '                return Math.min(Math.max(Math.abs(pct) * 0.3, 10), 36);\n' +
        '            }\n' +
        '\n' +
        '            // SVG icon builders\n' +
        '            function diamondIcon(color, size) {\n' +
        '                var half = size / 2;\n' +
        '                var svg = \'<svg width="\' + size + \'" height="\' + size + \'" viewBox="0 0 \' + size + " " + size + \'">\' +\n' +
        '                    \'<rect x="\' + (half * 0.29) + \'" y="\' + (half * 0.29) + \'" width="\' + (half * 1.42) + \'" height="\' + (half * 1.42) + \'" \' +\n' +
        '                    \'transform="rotate(45 \' + half + " " + half + \')" fill="\' + color + \'" fill-opacity="0.7" stroke="#fff" stroke-width="1.5"/></svg>\';\n' +
        '                return L.divIcon({html: svg, className: "", iconSize: [size, size], iconAnchor: [half, half]});\n' +
        '            }\n' +
        '\n' +
        '            function triangleUpIcon(color, size) {\n' +
        '                var svg = \'<svg width="\' + size + \'" height="\' + size + \'" viewBox="0 0 20 20">\' +\n' +
        '                    \'<polygon points="10,1 19,18 1,18" fill="\' + color + \'" fill-opacity="0.7" stroke="#fff" stroke-width="1.5"/></svg>\';\n' +
        '                return L.divIcon({html: svg, className: "", iconSize: [size, size], iconAnchor: [size/2, size/2]});\n' +
        '            }\n' +
        '\n' +
        '            function triangleDownIcon(color, size) {\n' +
        '                var svg = \'<svg width="\' + size + \'" height="\' + size + \'" viewBox="0 0 20 20">\' +\n' +
        '                    \'<polygon points="1,2 19,2 10,19" fill="\' + color + \'" fill-opacity="0.7" stroke="#fff" stroke-width="1.5"/></svg>\';\n' +
        '                return L.divIcon({html: svg, className: "", iconSize: [size, size], iconAnchor: [size/2, size/2]});\n' +
        '            }\n' +
        '\n' +
        '            // Store icon builders globally for vessel switch\n' +
        '            window._mapHelpers = {getColor: getColor, getSize: getSize, diamondIcon: diamondIcon, triangleUpIcon: triangleUpIcon, triangleDownIcon: triangleDownIcon};\n' +
        '\n' +
        '            // Circle icon for multi-select (portcalls) mode\n' +
        '            function circleIcon(color, size) {\n' +
        '                var svg = \'<svg width="\' + size + \'" height="\' + size + \'" viewBox="0 0 20 20">\' +\n' +
        '                    \'<circle cx="10" cy="10" r="8" fill="\' + color + \'" fill-opacity="0.7" stroke="#fff" stroke-width="1.5"/></svg>\';\n' +
        '                return L.divIcon({html: svg, className: "", iconSize: [size, size], iconAnchor: [size/2, size/2]});\n' +
        '            }\n' +
        '\n' +
        '            window._mapHelpers = {getColor: getColor, getSize: getSize, diamondIcon: diamondIcon, triangleUpIcon: triangleUpIcon, triangleDownIcon: triangleDownIcon, circleIcon: circleIcon};\n' +
        '\n' +
        '            // Build layers for a vessel type\n' +
        '            window._buildMapLayers = function(vt) {\n' +
        '                var d = window._mapAllData[vt];\n' +
        '                var h = window._mapHelpers;\n' +
        '                var vtLabel = window._mapVtLabels[vt] || vt;\n' +
        '\n' +
        '                // Week-indexed pct: compute deviation from ta/tc at current week\n' +
        '                function wkPct(item) {\n' +
        '                    var w = (window._selectedWeekIdx != null) ? window._selectedWeekIdx : (window._postCrisisDates ? window._postCrisisDates.length - 1 : -1);\n' +
        '                    if (w >= 0 && item.ta && item.tc && w < item.ta.length) {\n' +
        '                        var a = item.ta[w], cf = item.tc[w];\n' +
        '                        if (Math.abs(cf) < 0.0001) return 0;\n' +
        '                        return Math.round((a - cf) / Math.abs(cf) * 1000) / 10;\n' +
        '                    }\n' +
        '                    return item.pct;\n' +
        '                }\n' +
        '\n' +
        '                // Sigma threshold filter for map markers (week-aware)\n' +
        '                var sigEl = document.getElementById("sigmaSlider");\n' +
        '                var sigT = sigEl ? parseFloat(sigEl.value) : 2;\n' +
        '                function mapSig(item) {\n' +
        '                    if (sigT === 0) return true;\n' +
        '                    if (item.sg == null) return false;\n' +
        '                    var dev = wkPct(item);\n' +
        '                    if (dev == null) return false;\n' +
        '                    return Math.abs(dev) >= sigT * item.sg;\n' +
        '                }\n' +
        '\n' +
        '                var cpLayer = L.layerGroup();\n' +
        '                d.cp.forEach(function(cp) {\n' +
        '                    if (!mapSig(cp)) return;\n' +
        '                    var pct = wkPct(cp);\n' +
        '                    var c = h.getColor(pct);\n' +
        '                    var sz = Math.min(Math.max(Math.abs(pct) * 0.4, 14), 40);\n' +
        '                    var marker = L.marker([cp.lat, cp.lon], {icon: h.diamondIcon(c, sz)});\n' +
        '                    marker.bindTooltip("<b style=\\"font-size:1rem\\">" + cp.name + "</b><br>" + vtLabel + " deviation: <b style=\\"color:" + c + ";font-size:1rem\\">" + (pct > 0 ? "+" : "") + pct + "%</b>", {className: "dark-tooltip"});\n' +
        '                    cpLayer.addLayer(marker);\n' +
        '                });\n' +
        '\n' +
        '                var exportLayer = L.layerGroup();\n' +
        '                d.exp.forEach(function(p) {\n' +
        '                    if (!mapSig(p)) return;\n' +
        '                    var pct = wkPct(p);\n' +
        '                    var c = h.getColor(pct);\n' +
        '                    var sz = h.getSize(pct);\n' +
        '                    var marker = L.marker([p.lat, p.lon - 0.4], {icon: h.triangleUpIcon(c, sz)});\n' +
        '                    marker.bindTooltip("<b>" + p.port + "</b> (" + p.iso3 + ")<br>Region: " + p.region + "<br>" + vtLabel + " export deviation: <b style=color:" + c + ">" + (pct > 0 ? "+" : "") + pct + "%</b>", {className: "dark-tooltip"});\n' +
        '                    exportLayer.addLayer(marker);\n' +
        '                });\n' +
        '\n' +
        '                var importLayer = L.layerGroup();\n' +
        '                d.imp.forEach(function(p) {\n' +
        '                    if (!mapSig(p)) return;\n' +
        '                    var pct = wkPct(p);\n' +
        '                    var c = h.getColor(pct);\n' +
        '                    var sz = h.getSize(pct);\n' +
        '                    var marker = L.marker([p.lat, p.lon + 0.4], {icon: h.triangleDownIcon(c, sz)});\n' +
        '                    marker.bindTooltip("<b>" + p.port + "</b> (" + p.iso3 + ")<br>Region: " + p.region + "<br>" + vtLabel + " import deviation: <b style=color:" + c + ">" + (pct > 0 ? "+" : "") + pct + "%</b>", {className: "dark-tooltip"});\n' +
        '                    importLayer.addLayer(marker);\n' +
        '                });\n' +
        '\n' +
        '                return {cp: cpLayer, exp: exportLayer, imp: importLayer, ports: null};\n' +
        '            };\n' +
        '\n' +
        '            // Initial build with tanker\n' +
        '            var layers = window._buildMapLayers("tanker");\n' +
        '            layers.cp.addTo(map);\n' +
        '            layers.exp.addTo(map);\n' +
        '            layers.imp.addTo(map);\n' +
        '            window._mapCpLayer = layers.cp;\n' +
        '            window._mapExportLayer = layers.exp;\n' +
        '            window._mapImportLayer = layers.imp;\n' +
        '            window._mapPortsLayer = null;\n' +
        '            window._mapObj = map;\n' +
        '            window._mapIsMulti = false;\n' +
        '\n' +
        '            // Track which layer types are visible\n' +
        '            window._mapLayerVisible = {chokepoints: true, "export": true, "import": true, ports: true};\n' +
        '\n' +
        '            // Signal that map is ready; if a pending multi-VT sync was queued, run it\n' +
        '            window._mapReady = true;\n' +
        '            if (typeof window._pendingMapSync === "function") {\n' +
        '                window._pendingMapSync();\n' +
        '                delete window._pendingMapSync;\n' +
        '            }\n' +
        '        }\n' +
        '\n' +
        '        // Switch map to a different vessel type (called from vessel toggle)\n' +
        '        window.updateMapForVessel = function(vt) {\n' +
        '            var map = window._mapObj;\n' +
        '            if (!map) return;\n' +
        '            window._mapCurrentVessel = vt;\n' +
        '\n' +
        '            // Remove all old layers\n' +
        '            if (window._mapCpLayer) map.removeLayer(window._mapCpLayer);\n' +
        '            if (window._mapExportLayer) map.removeLayer(window._mapExportLayer);\n' +
        '            if (window._mapImportLayer) map.removeLayer(window._mapImportLayer);\n' +
        '            if (window._mapPortsLayer) map.removeLayer(window._mapPortsLayer);\n' +
        '\n' +
        '            // Build new layers\n' +
        '            var layers = window._buildMapLayers(vt);\n' +
        '            window._mapCpLayer = layers.cp;\n' +
        '            window._mapExportLayer = layers.exp;\n' +
        '            window._mapImportLayer = layers.imp;\n' +
        '            window._mapPortsLayer = layers.ports;\n' +
        '\n' +
        '            // When switching from multi-select back to single, restore export/import buttons\n' +
        '            var wasMulti = window._mapIsMulti;\n' +
        '            window._mapIsMulti = false;\n' +
        '            if (wasMulti) {\n' +
        '                window._mapLayerVisible["export"] = true;\n' +
        '                window._mapLayerVisible["import"] = true;\n' +
        '            }\n' +
        '            var btnExp = document.getElementById("mapBtnExport");\n' +
        '            var btnImp = document.getElementById("mapBtnImport");\n' +
        '            var btnPorts = document.getElementById("mapBtnPorts");\n' +
        '            var legExp = document.getElementById("legendExport");\n' +
        '            var legImp = document.getElementById("legendImport");\n' +
        '            var legPorts = document.getElementById("legendPorts");\n' +
        '            btnExp.style.display = "";\n' +
        '            btnImp.style.display = "";\n' +
        '            btnPorts.style.display = "none";\n' +
        '            legExp.style.display = "flex";\n' +
        '            legImp.style.display = "flex";\n' +
        '            legPorts.style.display = "none";\n' +
        '            if (window._mapLayerVisible["export"]) btnExp.classList.add("active");\n' +
        '            else btnExp.classList.remove("active");\n' +
        '            if (window._mapLayerVisible["import"]) btnImp.classList.add("active");\n' +
        '            else btnImp.classList.remove("active");\n' +
        '\n' +
        '            // Add layers based on visibility\n' +
        '            var vis = window._mapLayerVisible;\n' +
        '            if (vis.chokepoints) layers.cp.addTo(map);\n' +
        '            if (vis["export"] && layers.exp) layers.exp.addTo(map);\n' +
        '            if (vis["import"] && layers.imp) layers.imp.addTo(map);\n' +
        '        };\n' +
        '\n' +
        '        window.toggleMapLayer = function(layer) {\n' +
        '            var map = window._mapObj;\n' +
        '            var layers = {\n' +
        '                chokepoints: window._mapCpLayer,\n' +
        '                "export": window._mapExportLayer,\n' +
        '                "import": window._mapImportLayer,\n' +
        '                ports: window._mapPortsLayer\n' +
        '            };\n' +
        '            var btnIds = {chokepoints: "mapBtnChokepoints", "export": "mapBtnExport", "import": "mapBtnImport", ports: "mapBtnPorts"};\n' +
        '            var btn = document.getElementById(btnIds[layer]);\n' +
        '            var lyr = layers[layer];\n' +
        '            if (!lyr) return;\n' +
        '            if (btn.classList.contains("active")) {\n' +
        '                btn.classList.remove("active");\n' +
        '                map.removeLayer(lyr);\n' +
        '                window._mapLayerVisible[layer] = false;\n' +
        '            } else {\n' +
        '                btn.classList.add("active");\n' +
        '                lyr.addTo(map);\n' +
        '                window._mapLayerVisible[layer] = true;\n' +
        '            }\n' +
        '        };\n' +
        '        // Defer map init until dashboard-content is visible (password gate)\n' +
        '        // initPortMap() is called after auth succeeds\n' +
        '        if (!document.getElementById("auth-gate")) {\n' +
        '            // No password gate — init immediately\n' +
        '            if (document.readyState === "loading") {\n' +
        '                document.addEventListener("DOMContentLoaded", initPortMap);\n' +
        '            } else {\n' +
        '                initPortMap();\n' +
        '            }\n' +
        '        }\n' +
        '        </script>'
    )
    return section


def build_port_deviation_bars(data: Dict[str, Any]) -> str:
    """Build the per-port deviation bar charts section, dynamically from data keys."""

    # Geographic region ordering
    REGION_ORDER = [
        "persian_gulf", "east_asia", "southeast_asia", "indian_subcontinent",
        "mediterranean", "nw_europe", "north_america", "latin_america",
        "west_africa", "russia", "oceania",
    ]
    REGION_LABELS = {
        "persian_gulf": "Persian Gulf", "east_asia": "East Asia",
        "southeast_asia": "Southeast Asia", "indian_subcontinent": "Indian Subcontinent",
        "mediterranean": "Mediterranean", "nw_europe": "Northwest Europe",
        "north_america": "North America", "latin_america": "Latin America",
        "west_africa": "West Africa", "russia": "Russia", "oceania": "Oceania",
    }

    # Discover all per-port deviation keys (format: _slug_direction_deviations)
    port_groups = []
    for key in data:
        if not key.startswith('_') or not key.endswith('_deviations'):
            continue
        if not isinstance(data[key], list):
            continue
        # Parse: _slug_export_deviations or _slug_import_deviations
        inner = key[1:-len('_deviations')]  # strip leading _ and trailing _deviations
        if inner.endswith('_export'):
            slug = inner[:-len('_export')]
            direction = 'export'
        elif inner.endswith('_import'):
            slug = inner[:-len('_import')]
            direction = 'import'
        else:
            continue
        region_label = REGION_LABELS.get(slug, slug.replace('_', ' ').title())
        group_name = f"{region_label} {direction.title()}s"
        port_groups.append((key, group_name, slug, direction))

    # Sort by region order, then export before import
    def sort_key(item):
        slug = item[2]
        direction = item[3]
        try:
            idx = REGION_ORDER.index(slug)
        except ValueError:
            idx = 999
        return (idx, 0 if direction == 'export' else 1)

    port_groups.sort(key=sort_key)

    sections = []
    last_slug = None

    for data_key, group_name, slug, direction in port_groups:
        if data_key not in data:
            continue

        # Insert geographic region header
        if slug != last_slug:
            region_label = REGION_LABELS.get(slug, slug.replace('_', ' ').title())
            sections.append(
                f'    <h3 style="font-size: 1.25rem; margin: 2rem 0 1rem 0; color: #93c5fd; border-top: 1px solid #374151; padding-top: 1.5rem;">{region_label}</h3>'
            )
            last_slug = slug

        port_list = data[data_key]

        # Filter and sort by STL deviation
        filtered = [p for p in port_list if p.get('stl_pct', 0) > -200]
        sorted_ports = sorted(filtered, key=lambda x: x.get('stl_pct', 0))

        if not sorted_ports:
            continue

        port_names = [p.get('port', 'Unknown') for p in sorted_ports]
        stl_values = [p.get('stl_pct', 0) for p in sorted_ports]
        naive_values = [p.get('naive_pct', 0) for p in sorted_ports]

        chart_id = 'chart_' + data_key.replace('_', '')

        sections.append(
            '    <div class="chart-section">\n' +
            '      <h3 class="chart-title">' + escape_js_string(group_name) + ' - Per-Port Deviation</h3>\n' +
            '      <div class="chart-container">\n' +
            '        <canvas id="' + chart_id + '"></canvas>\n' +
            '      </div>\n' +
            '      <p class="chart-note">Bars sorted by STL deviation (most negative first). Solid bars = STL method, outline bars = Naive method.</p>\n' +
            '    </div>'
        )

        # JavaScript to render the chart
        port_names_js = js_array(port_names)
        stl_values_js = js_array(stl_values)
        naive_values_js = js_array(naive_values)

        sections.append(
            '    <script>\n' +
            '    (function() {\n' +
            '      const ctx = document.getElementById(\'' + chart_id + '\').getContext(\'2d\');\n' +
            '      const portNames = ' + port_names_js + ';\n' +
            '      const stlValues = ' + stl_values_js + ';\n' +
            '      const naiveValues = ' + naive_values_js + ';\n' +
            '\n' +
            '      new Chart(ctx, {\n' +
            '        type: \'bar\',\n' +
            '        data: {\n' +
            '          labels: portNames,\n' +
            '          datasets: [\n' +
            '            {\n' +
            '              label: \'STL Deviation\',\n' +
            '              data: stlValues,\n' +
            '              backgroundColor: \'rgba(220, 38, 38, 0.8)\',\n' +
            '              borderColor: \'rgba(220, 38, 38, 1)\',\n' +
            '              borderWidth: 2,\n' +
            '              barThickness: \'flex\',\n' +
            '              maxBarThickness: 20\n' +
            '            },\n' +
            '            {\n' +
            '              label: \'Naive Deviation\',\n' +
            '              data: naiveValues,\n' +
            '              backgroundColor: \'rgba(220, 38, 38, 0.2)\',\n' +
            '              borderColor: \'rgba(220, 38, 38, 0.6)\',\n' +
            '              borderWidth: 1,\n' +
            '              borderDash: [3, 3],\n' +
            '              barThickness: \'flex\',\n' +
            '              maxBarThickness: 20\n' +
            '            }\n' +
            '          ]\n' +
            '        },\n' +
            '        options: {\n' +
            '          indexAxis: \'y\',\n' +
            '          responsive: true,\n' +
            '          maintainAspectRatio: false,\n' +
            '          plugins: {\n' +
            '            legend: {\n' +
            '              display: true,\n' +
            '              position: \'top\',\n' +
            '              labels: { color: \'#e5e7eb\', font: { size: 12 } }\n' +
            '            },\n' +
            '            tooltip: {\n' +
            '              callbacks: {\n' +
            '                label: function(context) {\n' +
            '                  return context.dataset.label + \': \' + context.parsed.x.toFixed(1) + \'%\';\n' +
            '                }\n' +
            '              },\n' +
            '              backgroundColor: \'rgba(0, 0, 0, 0.8)\',\n' +
            '              titleColor: \'#fff\',\n' +
            '              bodyColor: \'#fff\',\n' +
            '              borderColor: \'#666\',\n' +
            '              borderWidth: 1\n' +
            '            }\n' +
            '          },\n' +
            '          scales: {\n' +
            '            x: {\n' +
            '              type: \'linear\',\n' +
            '              position: \'bottom\',\n' +
            '              grid: { color: \'#1f2937\', drawBorder: false },\n' +
            '              ticks: { color: \'#9ca3af\', callback: function(v) { return v.toFixed(0) + \'%\'; } },\n' +
            '              title: { display: true, text: \'Deviation (%)\', color: \'#e5e7eb\' }\n' +
            '            },\n' +
            '            y: {\n' +
            '              grid: { display: false },\n' +
            '              ticks: { color: \'#9ca3af\', font: { size: 11 } }\n' +
            '            }\n' +
            '          }\n' +
            '        }\n' +
            '      });\n' +
            '    })();\n' +
            '    </script>'
        )

    return '\n'.join(sections)


def build_hormuz_main_chart(data: Dict[str, Any]) -> str:
    """Build the main Hormuz chart with count/capacity toggle."""
    hormuz_count = data.get('Strait of Hormuz|count', {})
    hormuz_capacity = data.get('Strait of Hormuz|capacity', {})

    dates_count = hormuz_count.get('dates', [])
    actual_count = hormuz_count.get('actual', [])
    cf_primary_count = hormuz_count.get('counterfactual_primary', [])
    cf_sensitivity_count = hormuz_count.get('counterfactual_sensitivity', [])

    dates_capacity = hormuz_capacity.get('dates', [])
    actual_capacity = hormuz_capacity.get('actual', [])
    cf_primary_capacity = hormuz_capacity.get('counterfactual_primary', [])
    cf_sensitivity_capacity = hormuz_capacity.get('counterfactual_sensitivity', [])

    dates_count_js = js_array(dates_count)
    actual_count_js = js_array(actual_count)
    cf_primary_count_js = js_array(cf_primary_count)
    cf_sensitivity_count_js = js_array(cf_sensitivity_count)

    dates_capacity_js = js_array(dates_capacity)
    actual_capacity_js = js_array(actual_capacity)
    cf_primary_capacity_js = js_array(cf_primary_capacity)
    cf_sensitivity_capacity_js = js_array(cf_sensitivity_capacity)

    return (
        '    <div class="chart-section">\n' +
        '      <div class="chart-header">\n' +
        '        <h3 class="chart-title">Strait of Hormuz - Actual vs Counterfactual</h3>\n' +
        '        <div class="toggle-group">\n' +
        '          <button class="toggle-btn active" onclick="switchHormuzMetric(this, \'capacity\')">Tonnage</button>\n' +
        '          <button class="toggle-btn" onclick="switchHormuzMetric(this, \'count\')">Count</button>\n' +
        '        </div>\n' +
        '      </div>\n' +
        '      <div class="chart-container">\n' +
        '        <canvas id="hormuzChart"></canvas>\n' +
        '      </div>\n' +
        '    </div>\n' +
        '\n' +
        '    <script>\n' +
        '    let hormuzChart = null;\n' +
        '    const hormuzCountData = {\n' +
        '      dates: ' + dates_count_js + ',\n' +
        '      actual: ' + actual_count_js + ',\n' +
        '      cf_primary: ' + cf_primary_count_js + ',\n' +
        '      cf_sensitivity: ' + cf_sensitivity_count_js + '\n' +
        '    };\n' +
        '    const hormuzCapacityData = {\n' +
        '      dates: ' + dates_capacity_js + ',\n' +
        '      actual: ' + actual_capacity_js + ',\n' +
        '      cf_primary: ' + cf_primary_capacity_js + ',\n' +
        '      cf_sensitivity: ' + cf_sensitivity_capacity_js + '\n' +
        '    };\n' +
        '\n' +
        '    function createHormuzChart(metricData, metricLabel) {\n' +
        '      const ctx = document.getElementById(\'hormuzChart\').getContext(\'2d\');\n' +
        '\n' +
        '      if (hormuzChart) {\n' +
        '        hormuzChart.destroy();\n' +
        '      }\n' +
        '\n' +
        '      hormuzChart = new Chart(ctx, {\n' +
        '        type: \'line\',\n' +
        '        data: {\n' +
        '          labels: metricData.dates,\n' +
        '          datasets: [\n' +
        '            {\n' +
        '              label: \'Actual\',\n' +
        '              data: metricData.actual,\n' +
        '              borderColor: \'#3b82f6\',\n' +
        '              backgroundColor: \'rgba(59, 130, 246, 0.05)\',\n' +
        '              borderWidth: 2.5,\n' +
        '              tension: 0,\n' +
        '              fill: true,\n' +
        '              pointRadius: 2,\n' +
        '              pointBackgroundColor: \'#3b82f6\',\n' +
        '              pointBorderColor: \'#1e3a8a\'\n' +
        '            },\n' +
        '            {\n' +
        '              label: \'Counterfactual (Primary)\',\n' +
        '              data: metricData.cf_primary,\n' +
        '              borderColor: \'#8b5cf6\',\n' +
        '              backgroundColor: \'transparent\',\n' +
        '              borderWidth: 2,\n' +
        '              borderDash: [5, 5],\n' +
        '              tension: 0,\n' +
        '              fill: false,\n' +
        '              pointRadius: 1.5,\n' +
        '              pointBackgroundColor: \'#8b5cf6\'\n' +
        '            },\n' +
        '            {\n' +
        '              label: \'Counterfactual (Sensitivity)\',\n' +
        '              data: metricData.cf_sensitivity,\n' +
        '              borderColor: \'#ec4899\',\n' +
        '              backgroundColor: \'transparent\',\n' +
        '              borderWidth: 1.5,\n' +
        '              borderDash: [2, 2],\n' +
        '              tension: 0,\n' +
        '              fill: false,\n' +
        '              pointRadius: 1,\n' +
        '              pointBackgroundColor: \'#ec4899\'\n' +
        '            }\n' +
        '          ]\n' +
        '        },\n' +
        '        options: {\n' +
        '          responsive: true,\n' +
        '          maintainAspectRatio: false,\n' +
        '          interaction: { mode: \'index\', intersect: false },\n' +
        '          plugins: {\n' +
        '            legend: {\n' +
        '              display: true,\n' +
        '              position: \'top\',\n' +
        '              labels: { color: \'#e5e7eb\', font: { size: 12 } }\n' +
        '            },\n' +
        '            tooltip: {\n' +
        '              backgroundColor: \'rgba(0, 0, 0, 0.8)\',\n' +
        '              titleColor: \'#fff\',\n' +
        '              bodyColor: \'#fff\',\n' +
        '              borderColor: \'#666\',\n' +
        '              borderWidth: 1,\n' +
        '              padding: 10,\n' +
        '              displayColors: true\n' +
        '            },\n' +
        '            annotation: getCrisisAnnotation(metricData.dates)\n' +
        '          },\n' +
        '          scales: {\n' +
        '            x: {\n' +
        '              grid: { color: \'#1f2937\', drawBorder: false },\n' +
        '              ticks: { color: \'#9ca3af\', maxRotation: 45, minRotation: 0 }\n' +
        '            },\n' +
        '            y: {\n' +
        '              grid: { color: \'#1f2937\', drawBorder: false },\n' +
        '              ticks: { color: \'#9ca3af\' },\n' +
        '              title: { display: true, text: metricLabel, color: \'#e5e7eb\' }\n' +
        '            }\n' +
        '          }\n' +
        '        }\n' +
        '      });\n' +
        '    }\n' +
        '\n' +
        '    function switchHormuzMetric(el, metric) {\n' +
        '      const buttons = el.parentElement.querySelectorAll(\'.toggle-btn\');\n' +
        '      buttons.forEach(btn => btn.classList.remove(\'active\'));\n' +
        '      el.classList.add(\'active\');\n' +
        '\n' +
        '      if (metric === \'count\') {\n' +
        '        createHormuzChart(hormuzCountData, \'Transit Count\');\n' +
        '      } else {\n' +
        '        createHormuzChart(hormuzCapacityData, \'Tonnage\');\n' +
        '      }\n' +
        '    }\n' +
        '\n' +
        '    createHormuzChart(hormuzCapacityData, \'Tonnage\');\n' +
        '    </script>'
    )


def build_stl_decomposition(data: Dict[str, Any]) -> str:
    """Build the STL decomposition charts for Hormuz."""
    hormuz = data.get('Strait of Hormuz|capacity', {})

    dates = hormuz.get('dates', [])
    trend = hormuz.get('trend', [])
    seasonal = hormuz.get('seasonal', [])
    remainder = hormuz.get('remainder', [])
    actual = hormuz.get('actual', [])

    dates_js = js_array(dates)
    trend_js = js_array(trend)
    seasonal_js = js_array(seasonal)
    remainder_js = js_array(remainder)
    actual_js = js_array(actual)

    return (
        '    <div class="stl-grid">\n' +
        '      <div class="chart-section">\n' +
        '        <h3 class="chart-title">Trend</h3>\n' +
        '        <div class="chart-container small-chart">\n' +
        '          <canvas id="stlTrend"></canvas>\n' +
        '        </div>\n' +
        '      </div>\n' +
        '      <div class="chart-section">\n' +
        '        <h3 class="chart-title">Seasonal</h3>\n' +
        '        <div class="chart-container small-chart">\n' +
        '          <canvas id="stlSeasonal"></canvas>\n' +
        '        </div>\n' +
        '      </div>\n' +
        '      <div class="chart-section">\n' +
        '        <h3 class="chart-title">Remainder</h3>\n' +
        '        <div class="chart-container small-chart">\n' +
        '          <canvas id="stlRemainder"></canvas>\n' +
        '        </div>\n' +
        '      </div>\n' +
        '      <div class="chart-section">\n' +
        '        <h3 class="chart-title">Actual vs Trend</h3>\n' +
        '        <div class="chart-container small-chart">\n' +
        '          <canvas id="stlDeviation"></canvas>\n' +
        '        </div>\n' +
        '      </div>\n' +
        '    </div>\n' +
        '\n' +
        '    <script>\n' +
        '    (function() {\n' +
        '      const dates = ' + dates_js + ';\n' +
        '      const trend = ' + trend_js + ';\n' +
        '      const seasonal = ' + seasonal_js + ';\n' +
        '      const remainder = ' + remainder_js + ';\n' +
        '      const actual = ' + actual_js + ';\n' +
        '\n' +
        '      const chartConfigs = [\n' +
        '        {\n' +
        '          id: \'stlTrend\',\n' +
        '          data: trend,\n' +
        '          color: \'#3b82f6\',\n' +
        '          label: \'Trend\'\n' +
        '        },\n' +
        '        {\n' +
        '          id: \'stlSeasonal\',\n' +
        '          data: seasonal,\n' +
        '          color: \'#10b981\',\n' +
        '          label: \'Seasonal\'\n' +
        '        },\n' +
        '        {\n' +
        '          id: \'stlRemainder\',\n' +
        '          data: remainder,\n' +
        '          color: \'#f59e0b\',\n' +
        '          label: \'Remainder\'\n' +
        '        },\n' +
        '        {\n' +
        '          id: \'stlDeviation\',\n' +
        '          data: actual,\n' +
        '          color: \'#ef4444\',\n' +
        '          label: \'Actual\',\n' +
        '          reference: trend\n' +
        '        }\n' +
        '      ];\n' +
        '\n' +
        '      function hexToRgb(hex) {\n' +
        '        const result = /^#?([a-f\\d]{2})([a-f\\d]{2})([a-f\\d]{2})$/i.exec(hex);\n' +
        '        return result ? [\n' +
        '          parseInt(result[1], 16),\n' +
        '          parseInt(result[2], 16),\n' +
        '          parseInt(result[3], 16)\n' +
        '        ] : [59, 130, 246];\n' +
        '      }\n' +
        '\n' +
        '      chartConfigs.forEach(cfg => {\n' +
        '        const ctx = document.getElementById(cfg.id).getContext(\'2d\');\n' +
        '        const datasets = [{\n' +
        '          label: cfg.label,\n' +
        '          data: cfg.data,\n' +
        '          borderColor: cfg.color,\n' +
        '          backgroundColor: \'rgba(\' + hexToRgb(cfg.color).join(\',\') + \', 0.05)\',\n' +
        '          borderWidth: 1.5,\n' +
        '          tension: 0,\n' +
        '          fill: cfg.id !== \'stlRemainder\',\n' +
        '          pointRadius: 1,\n' +
        '          pointBackgroundColor: cfg.color\n' +
        '        }];\n' +
        '\n' +
        '        if (cfg.reference) {\n' +
        '          datasets.push({\n' +
        '            label: \'Trend\',\n' +
        '            data: cfg.reference,\n' +
        '            borderColor: \'#8b5cf6\',\n' +
        '            backgroundColor: \'transparent\',\n' +
        '            borderWidth: 1,\n' +
        '            borderDash: [3, 3],\n' +
        '            tension: 0,\n' +
        '            fill: false,\n' +
        '            pointRadius: 0.5,\n' +
        '            pointBackgroundColor: \'#8b5cf6\'\n' +
        '          });\n' +
        '        }\n' +
        '\n' +
        '        new Chart(ctx, {\n' +
        '          type: \'line\',\n' +
        '          data: { labels: dates, datasets: datasets },\n' +
        '          options: {\n' +
        '            responsive: true,\n' +
        '            maintainAspectRatio: false,\n' +
        '            plugins: {\n' +
        '              legend: {\n' +
        '                display: cfg.reference ? true : false,\n' +
        '                labels: { color: \'#e5e7eb\', font: { size: 10 } }\n' +
        '              },\n' +
        '              tooltip: {\n' +
        '                backgroundColor: \'rgba(0, 0, 0, 0.8)\',\n' +
        '                titleColor: \'#fff\',\n' +
        '                bodyColor: \'#fff\',\n' +
        '                borderColor: \'#666\',\n' +
        '                borderWidth: 1\n' +
        '              }\n' +
        '            },\n' +
        '            scales: {\n' +
        '              x: {\n' +
        '                display: false,\n' +
        '                grid: { display: false }\n' +
        '              },\n' +
        '              y: {\n' +
        '                grid: { color: \'#1f2937\' },\n' +
        '                ticks: { color: \'#9ca3af\', font: { size: 9 } }\n' +
        '              }\n' +
        '            }\n' +
        '          }\n' +
        '        });\n' +
        '      });\n' +
        '    })();\n' +
        '    </script>'
    )


def build_comparison_charts(data: Dict[str, Any]) -> str:
    """Build Cape/Bab el-Mandeb and Suez/Malacca comparison charts."""
    pairs = [
        ('Cape of Good Hope', 'Bab el-Mandeb Strait', ['Cape of Good Hope|capacity', 'Bab el-Mandeb Strait|capacity']),
        ('Suez Canal', 'Malacca Strait', ['Suez Canal|capacity', 'Malacca Strait|capacity']),
    ]

    sections = []

    for pair_idx, (name1, name2, keys) in enumerate(pairs):
        data1 = data.get(keys[0], {})
        data2 = data.get(keys[1], {})

        dates = data1.get('dates', [])
        actual1 = data1.get('actual', [])
        cf1 = data1.get('counterfactual_primary', [])
        actual2 = data2.get('actual', [])
        cf2 = data2.get('counterfactual_primary', [])

        dates_js = js_array(dates)
        actual1_js = js_array(actual1)
        cf1_js = js_array(cf1)
        actual2_js = js_array(actual2)
        cf2_js = js_array(cf2)

        sections.append(
            '    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 2rem; margin: 2rem 0;">\n' +
            '      <div class="chart-section">\n' +
            '        <h3 class="chart-title">' + escape_js_string(name1) + '</h3>\n' +
            '        <div class="chart-container">\n' +
            '          <canvas id="chart_' + str(pair_idx) + '_0"></canvas>\n' +
            '        </div>\n' +
            '      </div>\n' +
            '      <div class="chart-section">\n' +
            '        <h3 class="chart-title">' + escape_js_string(name2) + '</h3>\n' +
            '        <div class="chart-container">\n' +
            '          <canvas id="chart_' + str(pair_idx) + '_1"></canvas>\n' +
            '        </div>\n' +
            '      </div>\n' +
            '    </div>'
        )

        # JavaScript to render both charts
        sections.append(
            '    <script>\n' +
            '    (function() {\n' +
            '      const dates = ' + dates_js + ';\n' +
            '\n' +
            '      const ctx1 = document.getElementById(\'chart_' + str(pair_idx) + '_0\').getContext(\'2d\');\n' +
            '      new Chart(ctx1, {\n' +
            '        type: \'line\',\n' +
            '        data: {\n' +
            '          labels: dates,\n' +
            '          datasets: [\n' +
            '            {\n' +
            '              label: \'Actual\',\n' +
            '              data: ' + actual1_js + ',\n' +
            '              borderColor: \'#3b82f6\',\n' +
            '              backgroundColor: \'rgba(59, 130, 246, 0.05)\',\n' +
            '              borderWidth: 2,\n' +
            '              tension: 0,\n' +
            '              fill: true,\n' +
            '              pointRadius: 1,\n' +
            '              pointBackgroundColor: \'#3b82f6\'\n' +
            '            },\n' +
            '            {\n' +
            '              label: \'Counterfactual\',\n' +
            '              data: ' + cf1_js + ',\n' +
            '              borderColor: \'#8b5cf6\',\n' +
            '              backgroundColor: \'transparent\',\n' +
            '              borderWidth: 1.5,\n' +
            '              borderDash: [5, 5],\n' +
            '              tension: 0,\n' +
            '              fill: false,\n' +
            '              pointRadius: 1\n' +
            '            }\n' +
            '          ]\n' +
            '        },\n' +
            '        options: {\n' +
            '          responsive: true,\n' +
            '          maintainAspectRatio: false,\n' +
            '          plugins: {\n' +
            '            legend: { display: true, position: \'top\', labels: { color: \'#e5e7eb\' } },\n' +
            '            tooltip: {\n' +
            '              backgroundColor: \'rgba(0, 0, 0, 0.8)\',\n' +
            '              titleColor: \'#fff\',\n' +
            '              bodyColor: \'#fff\',\n' +
            '              borderColor: \'#666\',\n' +
            '              borderWidth: 1\n' +
            '            },\n' +
            '            annotation: getCrisisAnnotation(dates)\n' +
            '          },\n' +
            '          scales: {\n' +
            '            x: { grid: { color: \'#1f2937\' }, ticks: { color: \'#9ca3af\', maxRotation: 45 } },\n' +
            '            y: { grid: { color: \'#1f2937\' }, ticks: { color: \'#9ca3af\' } }\n' +
            '          }\n' +
            '        }\n' +
            '      });\n' +
            '\n' +
            '      const ctx2 = document.getElementById(\'chart_' + str(pair_idx) + '_1\').getContext(\'2d\');\n' +
            '      new Chart(ctx2, {\n' +
            '        type: \'line\',\n' +
            '        data: {\n' +
            '          labels: dates,\n' +
            '          datasets: [\n' +
            '            {\n' +
            '              label: \'Actual\',\n' +
            '              data: ' + actual2_js + ',\n' +
            '              borderColor: \'#3b82f6\',\n' +
            '              backgroundColor: \'rgba(59, 130, 246, 0.05)\',\n' +
            '              borderWidth: 2,\n' +
            '              tension: 0,\n' +
            '              fill: true,\n' +
            '              pointRadius: 1,\n' +
            '              pointBackgroundColor: \'#3b82f6\'\n' +
            '            },\n' +
            '            {\n' +
            '              label: \'Counterfactual\',\n' +
            '              data: ' + cf2_js + ',\n' +
            '              borderColor: \'#8b5cf6\',\n' +
            '              backgroundColor: \'transparent\',\n' +
            '              borderWidth: 1.5,\n' +
            '              borderDash: [5, 5],\n' +
            '              tension: 0,\n' +
            '              fill: false,\n' +
            '              pointRadius: 1\n' +
            '            }\n' +
            '          ]\n' +
            '        },\n' +
            '        options: {\n' +
            '          responsive: true,\n' +
            '          maintainAspectRatio: false,\n' +
            '          plugins: {\n' +
            '            legend: { display: true, position: \'top\', labels: { color: \'#e5e7eb\' } },\n' +
            '            tooltip: {\n' +
            '              backgroundColor: \'rgba(0, 0, 0, 0.8)\',\n' +
            '              titleColor: \'#fff\',\n' +
            '              bodyColor: \'#fff\',\n' +
            '              borderColor: \'#666\',\n' +
            '              borderWidth: 1\n' +
            '            },\n' +
            '            annotation: getCrisisAnnotation(dates)\n' +
            '          },\n' +
            '          scales: {\n' +
            '            x: { grid: { color: \'#1f2937\' }, ticks: { color: \'#9ca3af\', maxRotation: 45 } },\n' +
            '            y: { grid: { color: \'#1f2937\' }, ticks: { color: \'#9ca3af\' } }\n' +
            '          }\n' +
            '        }\n' +
            '      });\n' +
            '    })();\n' +
            '    </script>'
        )

    return '\n'.join(sections)


def build_port_group_charts(data: Dict[str, Any]) -> str:
    """Build regional port group charts with toggle for calls/tonnage, dynamically from data keys."""

    REGION_ORDER = [
        "Persian Gulf", "East Asia", "Southeast Asia", "Indian Subcontinent",
        "Mediterranean", "Northwest Europe", "North America", "Latin America",
        "West Africa", "Russia", "Oceania",
    ]

    # Discover all port group tonnage keys and pair with calls keys
    # Keys look like: "Persian Gulf Exports|persian_gulf_exports_tonnage"
    port_groups = []
    seen_names = set()
    for key in data:
        if key.startswith('_'):
            continue
        parts = key.split('|')
        if len(parts) != 2:
            continue
        name = parts[0]
        metric_slug = parts[1]
        if not metric_slug.endswith('_tonnage'):
            continue
        # Find matching calls key
        calls_slug = metric_slug.replace('_tonnage', '_calls')
        calls_key = f"{name}|{calls_slug}"
        if name not in seen_names:
            seen_names.add(name)
            port_groups.append((name, calls_key, key))

    # Sort by region order, then export before import
    def sort_key(item):
        name = item[0]
        region_idx = 999
        for i, r in enumerate(REGION_ORDER):
            if name.startswith(r):
                region_idx = i
                break
        direction_idx = 0 if 'Export' in name else 1
        return (region_idx, direction_idx)

    port_groups.sort(key=sort_key)

    sections = []
    last_region = None

    for idx, (name, calls_key, tonnage_key) in enumerate(port_groups):
        # Insert geographic region header
        current_region = None
        for r in REGION_ORDER:
            if name.startswith(r):
                current_region = r
                break
        if current_region and current_region != last_region:
            sections.append(
                f'    <h3 style="font-size: 1.25rem; margin: 2rem 0 1rem 0; color: #93c5fd; border-top: 1px solid #374151; padding-top: 1.5rem;">{current_region}</h3>'
            )
            last_region = current_region

        tonnage_data = data.get(tonnage_key, {})
        if not tonnage_data:
            continue

        dates_js = js_array(tonnage_data.get('dates', []))
        actual_js = js_array(tonnage_data.get('actual', []))
        cf_js = js_array(tonnage_data.get('counterfactual_primary', []))

        chart_id = 'portGroupChart' + str(idx)

        sections.append(
            '    <div class="chart-section">\n' +
            '      <h3 class="chart-title">' + escape_js_string(name) + ' (Tonnage)</h3>\n' +
            '      <div class="chart-container">\n' +
            '        <canvas id="' + chart_id + '"></canvas>\n' +
            '      </div>\n' +
            '    </div>'
        )

        # JavaScript
        sections.append(
            '    <script>\n' +
            '    (function() {\n' +
            '      const ctx = document.getElementById(\'' + chart_id + '\').getContext(\'2d\');\n' +
            '      const dates = ' + dates_js + ';\n' +
            '      const actual = ' + actual_js + ';\n' +
            '      const cf = ' + cf_js + ';\n' +
            '\n' +
            '      new Chart(ctx, {\n' +
            '        type: \'line\',\n' +
            '        data: {\n' +
            '          labels: dates,\n' +
            '          datasets: [\n' +
            '            {\n' +
            '              label: \'Actual\',\n' +
            '              data: actual,\n' +
            '              borderColor: \'#3b82f6\',\n' +
            '              backgroundColor: \'rgba(59, 130, 246, 0.05)\',\n' +
            '              borderWidth: 2,\n' +
            '              tension: 0,\n' +
            '              fill: true,\n' +
            '              pointRadius: 1,\n' +
            '              pointBackgroundColor: \'#3b82f6\'\n' +
            '            },\n' +
            '            {\n' +
            '              label: \'Counterfactual\',\n' +
            '              data: cf,\n' +
            '              borderColor: \'#8b5cf6\',\n' +
            '              backgroundColor: \'transparent\',\n' +
            '              borderWidth: 1.5,\n' +
            '              borderDash: [5, 5],\n' +
            '              tension: 0,\n' +
            '              fill: false,\n' +
            '              pointRadius: 1\n' +
            '            }\n' +
            '          ]\n' +
            '        },\n' +
            '        options: {\n' +
            '          responsive: true,\n' +
            '          maintainAspectRatio: false,\n' +
            '          plugins: {\n' +
            '            legend: { display: true, position: \'top\', labels: { color: \'#e5e7eb\' } },\n' +
            '            tooltip: {\n' +
            '              backgroundColor: \'rgba(0, 0, 0, 0.8)\',\n' +
            '              titleColor: \'#fff\',\n' +
            '              bodyColor: \'#fff\',\n' +
            '              borderColor: \'#666\',\n' +
            '              borderWidth: 1\n' +
            '            },\n' +
            '            annotation: getCrisisAnnotation(dates)\n' +
            '          },\n' +
            '          scales: {\n' +
            '            x: { grid: { color: \'#1f2937\' }, ticks: { color: \'#9ca3af\', maxRotation: 45 } },\n' +
            '            y: { grid: { color: \'#1f2937\' }, ticks: { color: \'#9ca3af\' } }\n' +
            '          }\n' +
            '        }\n' +
            '      });\n' +
            '    })();\n' +
            '    </script>'
        )

    return '\n'.join(sections)


def _get_port_deviation(data: Dict[str, Any], deviation_list_key: str, port_name: str) -> float:
    """Extract a specific port's STL deviation from a per-port deviation list."""
    ports = data.get(deviation_list_key, [])
    for p in ports:
        if p.get('port', '') == port_name:
            return p.get('stl_pct', p.get('naive_pct', 0))
    return 0


def get_kpi_values(data: Dict[str, Any], vessel_type: str = 'tanker') -> Dict[str, Any]:
    """Extract KPI values for the cards.

    Returns:
        hormuz: Hormuz capacity deviation
        gulf_exp: Persian Gulf exports deviation
        sgp_exp, sgp_imp, sgp_calls: Singapore export/import/portcalls deviations
        sea_exp, sea_imp, sea_calls: Southeast Asia export/import/portcalls deviations
    """
    _safe = lambda v: v if v is not None else 0
    vt = vessel_type

    # ── Chokepoint keys (capacity + count) ──
    CHOKEPOINTS = ['Strait of Hormuz', 'Suez Canal', 'Panama Canal', 'Cape of Good Hope', 'Malacca Strait']
    cp_prefixes = {'Strait of Hormuz': 'hormuz', 'Suez Canal': 'suez', 'Panama Canal': 'panama',
                   'Cape of Good Hope': 'cape', 'Malacca Strait': 'malacca'}
    result = {}
    sig = {}  # significance flags: True = significant, False = within noise
    sig_params = {}  # (deviation, sigma) tuples for client-side threshold slider
    kpi_ts = {}  # post-crisis time series: {kpi_prefix: (ta, tc)}

    # Helper to extract post-crisis arrays for a data key
    def _get_pc(key):
        if key not in data:
            return None, None
        metric_data = data[key]
        actual = metric_data.get('actual', [])
        cf = metric_data.get('counterfactual_primary', [])
        dates = metric_data.get('dates', [])
        cd = metric_data.get('crisis_date', '2026-02-28')
        ci = len(dates)
        for i, d in enumerate(dates):
            if d >= cd:
                ci = i
                break
        ta = [round(actual[i], 4) if i < len(actual) and actual[i] is not None else 0 for i in range(ci, len(dates))]
        tc = [round(cf[i], 4) if i < len(cf) and cf[i] is not None else 0 for i in range(ci, len(dates))]
        return ta, tc

    for cp_name, prefix in cp_prefixes.items():
        cap_key = f'{cp_name}|{vt}_capacity'
        cnt_key = f'{cp_name}|{vt}_count'
        result[f'{prefix}_cap'] = _safe(get_latest_deviation(data, cap_key))
        result[f'{prefix}_cnt'] = _safe(get_latest_deviation(data, cnt_key))
        sig[f'{prefix}_cap'] = is_deviation_significant(data, cap_key)
        sig[f'{prefix}_cnt'] = is_deviation_significant(data, cnt_key)
        sig_params[f'{prefix}_cap'] = get_significance_params(data, cap_key)
        sig_params[f'{prefix}_cnt'] = get_significance_params(data, cnt_key)
        kpi_ts[f'{prefix}_cap'] = _get_pc(cap_key)
        kpi_ts[f'{prefix}_cnt'] = _get_pc(cnt_key)

    # ── Region keys (exports tonnage, imports tonnage, port calls) ──
    REGIONS = [
        ('gulf', 'Persian Gulf', 'persian_gulf'),
        ('na', 'North America', 'north_america'),
        ('ea', 'East Asia', 'east_asia'),
        ('sea', 'Southeast Asia', 'southeast_asia'),
        ('latam', 'Latin America', 'latin_america'),
    ]
    for prefix, display, slug in REGIONS:
        if vt == 'tanker':
            exp_key = f'{display} Exports|{slug}_exports_tonnage'
            imp_key = f'{display} Imports|{slug}_imports_tonnage'
            calls_key = f'{display} Exports|{slug}_exports_tanker_calls'
        else:
            exp_key = f'{display} Exports|{slug}_exports_{vt}_tonnage'
            imp_key = f'{display} Imports|{slug}_imports_{vt}_tonnage'
            calls_key = f'{display} Exports|{slug}_exports_{vt}_calls'
        result[f'{prefix}_exp'] = _safe(get_latest_deviation(data, exp_key))
        result[f'{prefix}_imp'] = _safe(get_latest_deviation(data, imp_key))
        result[f'{prefix}_calls'] = _safe(get_latest_deviation(data, calls_key))
        sig[f'{prefix}_exp'] = is_deviation_significant(data, exp_key)
        sig[f'{prefix}_imp'] = is_deviation_significant(data, imp_key)
        sig[f'{prefix}_calls'] = is_deviation_significant(data, calls_key)
        sig_params[f'{prefix}_exp'] = get_significance_params(data, exp_key)
        sig_params[f'{prefix}_imp'] = get_significance_params(data, imp_key)
        sig_params[f'{prefix}_calls'] = get_significance_params(data, calls_key)
        kpi_ts[f'{prefix}_exp'] = _get_pc(exp_key)
        kpi_ts[f'{prefix}_imp'] = _get_pc(imp_key)
        kpi_ts[f'{prefix}_calls'] = _get_pc(calls_key)

    # ── Country keys (exports tonnage, imports tonnage, port calls) ──
    COUNTRIES = [
        ('sgp', 'Singapore', 'singapore'),
        ('my', 'Malaysia', 'malaysia'),
        ('th', 'Thailand', 'thailand'),
        ('id', 'Indonesia', 'indonesia'),
        ('ph', 'Philippines', 'philippines'),
    ]
    for prefix, display, slug in COUNTRIES:
        if vt == 'tanker':
            exp_key = f'COUNTRY:{display} Exports|country:{slug}_exports_tonnage'
            imp_key = f'COUNTRY:{display} Imports|country:{slug}_imports_tonnage'
            calls_key = f'COUNTRY:{display} Exports|country:{slug}_exports_tanker_calls'
        else:
            exp_key = f'COUNTRY:{display} Exports|country:{slug}_exports_{vt}_tonnage'
            imp_key = f'COUNTRY:{display} Imports|country:{slug}_imports_{vt}_tonnage'
            calls_key = f'COUNTRY:{display} Exports|country:{slug}_exports_{vt}_calls'
        result[f'{prefix}_exp'] = _safe(get_latest_deviation(data, exp_key))
        result[f'{prefix}_imp'] = _safe(get_latest_deviation(data, imp_key))
        result[f'{prefix}_calls'] = _safe(get_latest_deviation(data, calls_key))
        sig[f'{prefix}_exp'] = is_deviation_significant(data, exp_key)
        sig[f'{prefix}_imp'] = is_deviation_significant(data, imp_key)
        sig[f'{prefix}_calls'] = is_deviation_significant(data, calls_key)
        sig_params[f'{prefix}_exp'] = get_significance_params(data, exp_key)
        sig_params[f'{prefix}_imp'] = get_significance_params(data, imp_key)
        sig_params[f'{prefix}_calls'] = get_significance_params(data, calls_key)
        kpi_ts[f'{prefix}_exp'] = _get_pc(exp_key)
        kpi_ts[f'{prefix}_imp'] = _get_pc(imp_key)
        kpi_ts[f'{prefix}_calls'] = _get_pc(calls_key)

    return result, sig, sig_params, kpi_ts


def _get_latest_actual_cf(data: Dict[str, Any], key: str):
    """Get the latest actual and counterfactual values for a data key."""
    if key not in data:
        return None, None
    metric_data = data[key]
    actual = metric_data.get('actual', [])
    cf = metric_data.get('counterfactual_primary', [])
    a_val = actual[-1] if actual else None
    c_val = cf[-1] if cf else None
    return a_val, c_val


def _get_port_actual_cf(data: Dict[str, Any], deviation_list_key: str, port_name: str):
    """Extract a specific port's latest actual and cf from a per-port deviation list."""
    ports = data.get(deviation_list_key, [])
    for p in ports:
        if p.get('port', '') == port_name:
            return p.get('stl_actual', None), p.get('stl_cf', None)
    return None, None


def build_aggregation_data_js(data: Dict[str, Any]) -> str:
    """Build JavaScript objects with per-vessel-type actual/cf values for multi-select aggregation.

    Returns a JS string defining:
    - window._kpiCountData: per-vessel KPI count metrics (actual + cf)
    - window._mapCountData: per-vessel chokepoint counts and port portcalls (actual + cf)
    """
    VESSEL_TYPES = ['tanker', 'container', 'dry_bulk', 'general_cargo', 'roro']

    # ── KPI count data ──
    # Chokepoints (ship count): hormuz, suez, panama, cape, malacca
    # Regions (port calls): gulf, na, ea, sea, latam
    # Countries (port calls): sgp, my, th, id, ph
    CP_DEFS = [('hormuz', 'Strait of Hormuz'), ('suez', 'Suez Canal'), ('panama', 'Panama Canal'),
               ('cape', 'Cape of Good Hope'), ('malacca', 'Malacca Strait')]
    REGION_DEFS = [('gulf', 'Persian Gulf', 'persian_gulf'), ('na', 'North America', 'north_america'),
                   ('ea', 'East Asia', 'east_asia'), ('sea', 'Southeast Asia', 'southeast_asia'),
                   ('latam', 'Latin America', 'latin_america')]
    COUNTRY_DEFS = [('sgp', 'Singapore', 'singapore'), ('my', 'Malaysia', 'malaysia'),
                    ('th', 'Thailand', 'thailand'), ('id', 'Indonesia', 'indonesia'),
                    ('ph', 'Philippines', 'philippines')]

    def _jn(v):
        if v is None:
            return '0'
        return str(round(v, 4))

    # ── Determine post-crisis date indices ──
    _all_dates = []
    _crisis_date_str = ''
    for _dk in data:
        if isinstance(data[_dk], dict) and 'dates' in data[_dk]:
            _all_dates = data[_dk]['dates']
            _crisis_date_str = data[_dk].get('crisis_date', '2026-02-28')
            break
    _crisis_idx = len(_all_dates)
    for _ci, _cd in enumerate(_all_dates):
        if _cd >= _crisis_date_str:
            _crisis_idx = _ci
            break
    _post_crisis_dates = _all_dates[_crisis_idx:]
    _n_post = len(_post_crisis_dates)

    def _get_post_crisis_arrays(data, key):
        """Get post-crisis actual and cf arrays for a key."""
        if key not in data:
            return [0] * _n_post, [0] * _n_post
        metric_data = data[key]
        actual = metric_data.get('actual', [])
        cf = metric_data.get('counterfactual_primary', [])
        a_arr = [round(actual[i], 4) if i < len(actual) and actual[i] is not None else 0
                 for i in range(_crisis_idx, _crisis_idx + _n_post)]
        c_arr = [round(cf[i], 4) if i < len(cf) and cf[i] is not None else 0
                 for i in range(_crisis_idx, _crisis_idx + _n_post)]
        return a_arr, c_arr

    def _js_arr(arr):
        return '[' + ','.join(str(v) for v in arr) + ']'

    kpi_js_parts = []
    for vt in VESSEL_TYPES:
        fields = []
        # Chokepoint ship counts
        for prefix, cp_name in CP_DEFS:
            key = f'{cp_name}|{vt}_count'
            a, cf = _get_latest_actual_cf(data, key)
            _dev, _std = get_significance_params(data, key)
            _d_js = 'null' if _dev is None else str(round(_dev, 2))
            _s_js = 'null' if _std is None else str(round(_std, 2))
            _pa, _pc = _get_post_crisis_arrays(data, key)
            fields.append(f'                {prefix}: {{a:{_jn(a)},cf:{_jn(cf)},d:{_d_js},sg:{_s_js},ta:{_js_arr(_pa)},tc:{_js_arr(_pc)}}}')
        # Region port calls
        for prefix, display, slug in REGION_DEFS:
            key = f'{display} Exports|{slug}_exports_{vt}_calls'
            a, cf = _get_latest_actual_cf(data, key)
            _dev, _std = get_significance_params(data, key)
            _d_js = 'null' if _dev is None else str(round(_dev, 2))
            _s_js = 'null' if _std is None else str(round(_std, 2))
            _pa, _pc = _get_post_crisis_arrays(data, key)
            fields.append(f'                {prefix}: {{a:{_jn(a)},cf:{_jn(cf)},d:{_d_js},sg:{_s_js},ta:{_js_arr(_pa)},tc:{_js_arr(_pc)}}}')
        # Country port calls
        for prefix, display, slug in COUNTRY_DEFS:
            key = f'COUNTRY:{display} Exports|country:{slug}_exports_{vt}_calls'
            a, cf = _get_latest_actual_cf(data, key)
            _dev, _std = get_significance_params(data, key)
            _d_js = 'null' if _dev is None else str(round(_dev, 2))
            _s_js = 'null' if _std is None else str(round(_std, 2))
            _pa, _pc = _get_post_crisis_arrays(data, key)
            fields.append(f'                {prefix}: {{a:{_jn(a)},cf:{_jn(cf)},d:{_d_js},sg:{_s_js},ta:{_js_arr(_pa)},tc:{_js_arr(_pc)}}}')

        kpi_js_parts.append(
            f'            {vt}: {{\n' + ',\n'.join(fields) + '\n            }'
        )

    # Add post-crisis dates metadata
    _pc_dates_js = '[' + ','.join(f'"{d}"' for d in _post_crisis_dates) + ']'
    kpi_js = (
        f'        window._postCrisisDates = {_pc_dates_js};\n'
        f'        window._selectedWeekIdx = {_n_post - 1};\n'  # default to latest
        '        window._kpiCountData = {\n' + ',\n'.join(kpi_js_parts) + '\n        };\n'
    )

    # ── Map chokepoint count data ──
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    cp_csv = os.path.join(BASE_DIR, "data", "portwatch", "Chokepoints.csv")
    CHOKEPOINT_COORDS = {}
    try:
        with open(cp_csv, encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                CHOKEPOINT_COORDS[row["portname"]] = (float(row["lat"]), float(row["lon"]))
    except Exception:
        pass

    # Filter out VT entries with negligible baseline count traffic (same rationale
    # as _MAP_MIN_AVG_CAPACITY above — prevents absurd map marker deviations).
    _MAP_MIN_AVG_COUNT = 2  # minimum pre-crisis avg ship count for map data
    cp_items = []
    for cp_name, (lat, lon) in CHOKEPOINT_COORDS.items():
        vt_data = []
        has_any = False
        for vt in VESSEL_TYPES:
            key = cp_name + '|' + vt + '_count'
            series_data = data.get(key, {})
            pre_avg = series_data.get('pre_crisis_avg', 0) if isinstance(series_data, dict) else 0
            if pre_avg < _MAP_MIN_AVG_COUNT:
                # Zero out negligible series so aggregation ignores them
                _z = _js_arr([0] * _n_post)
                vt_data.append(f'{vt}:{{a:0,cf:0,d:null,sg:null,ta:{_z},tc:{_z}}}')
            else:
                has_any = True
                a, cf = _get_latest_actual_cf(data, key)
                a_s = str(round(a, 4)) if a is not None else '0'
                cf_s = str(round(cf, 4)) if cf is not None else '0'
                _cp_dev, _cp_sg = get_significance_params(data, key)
                _cp_d_js = 'null' if _cp_dev is None else str(round(_cp_dev, 2))
                _cp_sg_js = 'null' if _cp_sg is None else str(round(_cp_sg, 2))
                _pa, _pc = _get_post_crisis_arrays(data, key)
                vt_data.append(f'{vt}:{{a:{a_s},cf:{cf_s},d:{_cp_d_js},sg:{_cp_sg_js},ta:{_js_arr(_pa)},tc:{_js_arr(_pc)}}}')
        if not has_any:
            continue  # skip chokepoint entirely if no VT has meaningful traffic
        cp_items.append(
            '{lat:' + str(lat) + ',lon:' + str(lon) +
            ',name:"' + escape_js_string(cp_name) + '",' +
            ','.join(vt_data) + '}'
        )

    # ── Map port portcalls data ──
    # Collect all unique ports across all vessel types' portcalls top-50
    all_ports = {}  # port_name -> {lat, lon, iso3, region, vt: {a, cf, d, sg}}

    def _map_port_sig(p):
        """Compute (dev, sigma) for a port entry from its actual/counterfactual arrays."""
        import numpy as _np
        actual = p.get('actual', [])
        cf = p.get('counterfactual', [])
        dates = p.get('dates', [])
        crisis_date = p.get('crisis_date', '2026-02-28')
        if not actual or not cf or len(actual) != len(cf):
            return None, None
        crisis_idx = len(dates)
        for i, d in enumerate(dates):
            if d >= crisis_date:
                crisis_idx = i
                break
        if crisis_idx < 20:
            return None, None
        pre_devs = []
        for i in range(crisis_idx):
            a, c = actual[i], cf[i]
            if a is not None and c is not None and c != 0:
                pre_devs.append((a - c) / abs(c) * 100)
        if len(pre_devs) < 20:
            return None, None
        std = float(_np.std(pre_devs))
        a_last = actual[-1] if actual else None
        c_last = cf[-1] if cf else None
        if a_last is not None and c_last is not None and c_last != 0:
            dev = (a_last - c_last) / abs(c_last) * 100
        else:
            dev = None
        return dev, std

    def _port_post_crisis_arrays(p):
        """Extract post-crisis actual/cf arrays from a port entry."""
        actual = p.get('actual', [])
        cf = p.get('counterfactual', [])
        dates = p.get('dates', [])
        crisis_date = p.get('crisis_date', '2026-02-28')
        pc_idx = len(dates)
        for i, d in enumerate(dates):
            if d >= crisis_date:
                pc_idx = i
                break
        n = _n_post  # use global post-crisis count for consistency
        pa = [round(actual[pc_idx + i], 4) if pc_idx + i < len(actual) and actual[pc_idx + i] is not None else 0
              for i in range(n)]
        pc = [round(cf[pc_idx + i], 4) if pc_idx + i < len(cf) and cf[pc_idx + i] is not None else 0
              for i in range(n)]
        return pa, pc

    for vt in VESSEL_TYPES:
        key = f'_top50_portcalls_{vt}_ports'
        ports = data.get(key, [])
        for p in ports:
            pname = p.get('port', '')
            if not pname or 'lat' not in p or 'lon' not in p:
                continue
            if pname not in all_ports:
                all_ports[pname] = {
                    'lat': p['lat'], 'lon': p['lon'],
                    'iso3': p.get('iso3', ''), 'region': p.get('region', ''),
                }
            a_val = p.get('stl_actual', 0) or 0
            cf_val = p.get('stl_cf', 0) or 0
            _pd, _psg = _map_port_sig(p)
            _pta, _ptc = _port_post_crisis_arrays(p)
            all_ports[pname][vt] = {
                'a': round(a_val, 4), 'cf': round(cf_val, 4),
                'd': round(_pd, 2) if _pd is not None else None,
                'sg': round(_psg, 2) if _psg is not None else None,
                'ta': _pta, 'tc': _ptc,
            }

    port_items = []
    for pname, pdata in all_ports.items():
        vt_parts = []
        for vt in VESSEL_TYPES:
            if vt in pdata:
                d_js = 'null' if pdata[vt]['d'] is None else str(pdata[vt]['d'])
                sg_js = 'null' if pdata[vt]['sg'] is None else str(pdata[vt]['sg'])
                ta_js = _js_arr(pdata[vt]['ta'])
                tc_js = _js_arr(pdata[vt]['tc'])
                vt_parts.append(f'{vt}:{{a:{pdata[vt]["a"]},cf:{pdata[vt]["cf"]},d:{d_js},sg:{sg_js},ta:{ta_js},tc:{tc_js}}}')
        port_items.append(
            '{lat:' + str(pdata['lat']) + ',lon:' + str(pdata['lon']) +
            ',port:"' + escape_js_string(pname) +
            '",iso3:"' + escape_js_string(pdata['iso3']) +
            '",region:"' + escape_js_string(pdata['region']) + '",' +
            ','.join(vt_parts) + '}'
        )

    map_js = (
        '        window._mapCountData = {\n'
        '            cp: [' + ',\n                '.join(cp_items) + '],\n'
        '            ports: [' + ',\n                '.join(port_items) + ']\n'
        '        };\n'
    )

    return kpi_js + '\n' + map_js


def build_table_aggregation_data_js(data: Dict[str, Any]) -> str:
    """Extract per-vessel-type count/portcalls data for multi-select table aggregation.

    Returns a JS string defining window._tableAggData with actual/counterfactual values
    per vessel type for chokepoints, ports, regional flows, and country flows.

    The pipeline now includes fill entries for non-top-50 port×VT combos directly
    in the _top50_portcalls_{vt}_ports lists, so no additional CSV reading is needed here.
    """
    VESSEL_TYPES = ['tanker', 'container', 'dry_bulk', 'general_cargo', 'roro']

    # Extract dates and crisis_date from any available data entry
    _full_dates = []
    global_crisis_date = ''
    for data_key in data.keys():
        if 'dates' in data[data_key]:
            _full_dates = data[data_key].get('dates', [])
            global_crisis_date = data[data_key].get('crisis_date', '')
            break

    # Trim global dates to 52 weeks before crisis + post-crisis
    _agg_trim_start = 0
    if _full_dates and global_crisis_date:
        from datetime import datetime as _dt, timedelta as _td
        try:
            _crisis_dt = _dt.strptime(global_crisis_date, '%Y-%m-%d')
            _start_dt = (_crisis_dt - _td(weeks=52)).strftime('%Y-%m-%d')
            for _si, _d in enumerate(_full_dates):
                if _d >= _start_dt:
                    _agg_trim_start = _si
                    break
        except ValueError:
            pass
    global_dates = _full_dates[_agg_trim_start:]

    def _get_vt_values(key: str, include_ts: bool = True) -> dict:
        """Extract a, cf, a1y, a1q, avg, s (significance), vd from a data key. Optionally includes ts_a and ts_c."""
        if key not in data:
            result = {'a': 0, 'cf': 0, 'a1y': 0, 'a1q': 0, 'avg': 0, 's': 1}
            if include_ts:
                result['ts_a'] = []
                result['ts_c'] = []
            return result
        metric_data = data[key]
        actual = metric_data.get('actual', [])
        cf = metric_data.get('counterfactual_primary', [])
        a_val = round(actual[-1], 2) if actual else 0
        cf_val = round(cf[-1], 2) if cf else 0
        a1y_val = round(actual[-53], 2) if len(actual) > 52 else 0
        a1q_val = round(actual[-14], 2) if len(actual) > 13 else 0
        avg_val = round(metric_data.get('pre_crisis_avg', 0) or 0, 2)
        _dev_val, _std_val = get_significance_params(data, key)
        result = {'a': a_val, 'cf': cf_val, 'a1y': a1y_val, 'a1q': a1q_val, 'avg': avg_val,
                  'd': round(_dev_val, 2) if _dev_val is not None else None,
                  'sg': round(_std_val, 2) if _std_val is not None else None}
        # Variance decomposition
        vd = metric_data.get('variance_decomp')
        if vd:
            result['vd'] = {k: round(v, 4) for k, v in vd.items()}
        if include_ts:
            ts_a_full = [round(v, 2) for v in actual] if actual else []
            ts_c_full = [round(v, 2) for v in cf] if cf else []
            # Trim to match global_dates (52 weeks pre-crisis + post-crisis)
            result['ts_a'] = ts_a_full[_agg_trim_start:]
            result['ts_c'] = ts_c_full[_agg_trim_start:]
        return result

    def _dict_to_js(vt_dict: dict) -> str:
        """Convert vessel type values dict to JS object string, handling lists, nested dicts, as arrays/objects."""
        parts = []
        for k, v in vt_dict.items():
            if v is None:
                parts.append(f'{k}:null')
            elif isinstance(v, dict):
                # Nested dict (e.g. variance decomposition)
                inner = ','.join(f'{ik}:{round(iv, 4) if isinstance(iv, float) else iv}' for ik, iv in v.items())
                parts.append(f'{k}:{{{inner}}}')
            elif isinstance(v, list):
                # Convert list to JS array with rounded values
                js_array_str = '[' + ','.join(
                    str(round(x, 1)) if isinstance(x, float) else str(x) for x in v
                ) + ']'
                parts.append(f'{k}:{js_array_str}')
            elif isinstance(v, str):
                parts.append(f'{k}:"{v}"')
            elif isinstance(v, float):
                parts.append(f'{k}:{round(v, 2)}')
            else:
                parts.append(f'{k}:{v}')
        return '{' + ','.join(parts) + '}'

    # ── Chokepoints ──
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    cp_csv = os.path.join(BASE_DIR, "data", "portwatch", "Chokepoints.csv")
    chokepoint_names = []
    try:
        with open(cp_csv, encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                chokepoint_names.append(row["portname"])
    except Exception:
        pass

    cp_items = []
    for cp_name in chokepoint_names:
        vt_parts = []
        for vt in VESSEL_TYPES:
            key = cp_name + '|' + vt + '_count'
            vt_vals = _get_vt_values(key, include_ts=True)
            vt_parts.append(f'{vt}:{_dict_to_js(vt_vals)}')
        cp_obj = '{name:"' + escape_js_string(cp_name) + '",' + ','.join(vt_parts) + '}'
        cp_items.append(cp_obj)

    # ── Ports ──
    # Port time series are shorter than global dates (e.g. 55 vs 376 weeks).
    # We must pad them to align with the global dates grid.
    # Find the offset: where port dates start in the global dates array.
    _port_offset = 0
    _sample_port_dates = None
    for vt in VESSEL_TYPES:
        _sample_ports = data.get(f'_top50_portcalls_{vt}_ports', [])
        if _sample_ports and 'dates' in _sample_ports[0]:
            _sample_port_dates = _sample_ports[0]['dates']
            break
    if _sample_port_dates and global_dates:
        try:
            _port_offset = global_dates.index(_sample_port_dates[0])
        except ValueError:
            _port_offset = len(global_dates) - len(_sample_port_dates)
    _global_len = len(global_dates)

    all_ports = {}  # port_name -> {lat, lon, iso3, region, vt_values}
    for vt in VESSEL_TYPES:
        key = f'_top50_portcalls_{vt}_ports'
        ports = data.get(key, [])
        for p in ports:
            pname = p.get('port', '')
            if not pname or 'lat' not in p or 'lon' not in p:
                continue
            if pname not in all_ports:
                all_ports[pname] = {
                    'lat': p['lat'], 'lon': p['lon'],
                    'iso3': p.get('iso3', ''), 'region': p.get('region', ''),
                }
            # Use actual and counterfactual arrays from port data
            actual_arr = p.get('actual', [])
            cf_arr = p.get('counterfactual', [])
            a_val = round(actual_arr[-1], 2) if actual_arr else 0
            cf_val = round(cf_arr[-1], 2) if cf_arr else 0
            a1y_val = round(actual_arr[-53], 2) if len(actual_arr) > 52 else 0
            a1q_val = round(actual_arr[-14], 2) if len(actual_arr) > 13 else 0
            avg_val = round(p.get('pre_crisis_avg', 0) or 0, 2)
            # Pad port time series to align with global dates grid
            ts_a_raw = [round(v, 2) for v in actual_arr] if actual_arr else []
            ts_c_raw = [round(v, 2) for v in cf_arr] if cf_arr else []
            ts_a = [0] * _port_offset + ts_a_raw + [0] * max(0, _global_len - _port_offset - len(ts_a_raw))
            ts_c = [0] * _port_offset + ts_c_raw + [0] * max(0, _global_len - _port_offset - len(ts_c_raw))
            # Significance params: compute dev and pre-crisis std for this port×VT
            _port_d_val = None
            _port_sg_val = None
            _port_actual_arr = p.get('actual', [])
            _port_cf_arr = p.get('counterfactual', [])
            _port_crisis = p.get('crisis_date', global_crisis_date)
            if _port_actual_arr and _port_cf_arr and len(_port_actual_arr) == len(_port_cf_arr):
                _port_dates = p.get('dates', [])
                _port_ci = len(_port_dates)
                for _pi, _pd in enumerate(_port_dates):
                    if _pd >= _port_crisis:
                        _port_ci = _pi
                        break
                if _port_ci >= 20:
                    import numpy as _np
                    _port_pre_devs = []
                    for _pi in range(_port_ci):
                        _pa = _port_actual_arr[_pi]
                        _pc = _port_cf_arr[_pi]
                        if _pa is not None and _pc is not None and _pc != 0:
                            _port_pre_devs.append((_pa - _pc) / abs(_pc) * 100)
                    if len(_port_pre_devs) >= 20:
                        _port_sg_val = float(_np.std(_port_pre_devs))
                        _port_d_val = (a_val - cf_val) / abs(cf_val) * 100 if abs(cf_val) > 0.01 else 0
            port_vt_entry = {'a': a_val, 'cf': cf_val, 'a1y': a1y_val, 'a1q': a1q_val, 'avg': avg_val,
                             'd': round(_port_d_val, 2) if _port_d_val is not None else None,
                             'sg': round(_port_sg_val, 2) if _port_sg_val is not None else None,
                             'ts_a': ts_a, 'ts_c': ts_c}
            vd = p.get('variance_decomp')
            if vd:
                port_vt_entry['vd'] = {k: round(v, 4) for k, v in vd.items()}
            all_ports[pname][vt] = port_vt_entry

    port_items = []
    for pname, pdata in all_ports.items():
        vt_parts = []
        for vt in VESSEL_TYPES:
            if vt in pdata:
                vt_vals = pdata[vt]
                vt_parts.append(f'{vt}:{_dict_to_js(vt_vals)}')
        port_obj = (
            '{name:"' + escape_js_string(pname) + '",' +
            'lat:' + str(pdata['lat']) + ',' +
            'lon:' + str(pdata['lon']) + ',' +
            'iso3:"' + escape_js_string(pdata['iso3']) + '",' +
            'region:"' + escape_js_string(pdata['region']) + '",' +
            ','.join(vt_parts) + '}'
        )
        port_items.append(port_obj)

    # ── Regional Exports and Imports ──
    # Scan for keys matching {RegionName} Exports|{region_id}_{vt}_calls pattern
    regional_exp = {}  # region_name -> {vt -> values}
    regional_imp = {}  # region_name -> {vt -> values}

    for data_key in data.keys():
        if not data_key or data_key.startswith('_') or 'COUNTRY:' in data_key:
            continue
        if '|' not in data_key:
            continue
        name_part, key_part = data_key.split('|', 1)

        # Check if it's a regional export/import key
        if ' Exports' in name_part and '_calls' in key_part:
            # This is a regional export
            if name_part not in regional_exp:
                regional_exp[name_part] = {}
            # Try to extract vessel type from key_part
            for vt in VESSEL_TYPES:
                if f'_{vt}_calls' in key_part:
                    vt_vals = _get_vt_values(data_key, include_ts=True)
                    regional_exp[name_part][vt] = vt_vals
                    break
        elif ' Imports' in name_part and '_calls' in key_part:
            # This is a regional import
            if name_part not in regional_imp:
                regional_imp[name_part] = {}
            # Try to extract vessel type from key_part
            for vt in VESSEL_TYPES:
                if f'_{vt}_calls' in key_part:
                    vt_vals = _get_vt_values(data_key, include_ts=True)
                    regional_imp[name_part][vt] = vt_vals
                    break

    regional_exp_items = []
    for region_name, vt_dict in regional_exp.items():
        vt_parts = []
        for vt in VESSEL_TYPES:
            if vt in vt_dict:
                vt_parts.append(f'{vt}:{_dict_to_js(vt_dict[vt])}')
        if vt_parts:  # Only add if we have data
            region_obj = '{name:"' + escape_js_string(region_name) + '",' + ','.join(vt_parts) + '}'
            regional_exp_items.append(region_obj)

    regional_imp_items = []
    for region_name, vt_dict in regional_imp.items():
        vt_parts = []
        for vt in VESSEL_TYPES:
            if vt in vt_dict:
                vt_parts.append(f'{vt}:{_dict_to_js(vt_dict[vt])}')
        if vt_parts:  # Only add if we have data
            region_obj = '{name:"' + escape_js_string(region_name) + '",' + ','.join(vt_parts) + '}'
            regional_imp_items.append(region_obj)

    # ── Country Exports and Imports ──
    country_exp = {}  # country_name -> {vt -> values}
    country_imp = {}  # country_name -> {vt -> values}

    for data_key in data.keys():
        if not data_key or not data_key.startswith('COUNTRY:'):
            continue
        if '|' not in data_key:
            continue
        name_part, key_part = data_key.split('|', 1)

        # Extract country name (remove "COUNTRY:" prefix)
        if name_part.startswith('COUNTRY:'):
            country_name = name_part[8:]  # Remove "COUNTRY:" prefix
        else:
            continue

        # Check if it's a country export/import key
        if ' Exports' in country_name and '_calls' in key_part:
            # This is a country export
            if country_name not in country_exp:
                country_exp[country_name] = {}
            # Try to extract vessel type from key_part
            for vt in VESSEL_TYPES:
                if f'_{vt}_calls' in key_part:
                    vt_vals = _get_vt_values(data_key, include_ts=True)
                    country_exp[country_name][vt] = vt_vals
                    break
        elif ' Imports' in country_name and '_calls' in key_part:
            # This is a country import
            if country_name not in country_imp:
                country_imp[country_name] = {}
            # Try to extract vessel type from key_part
            for vt in VESSEL_TYPES:
                if f'_{vt}_calls' in key_part:
                    vt_vals = _get_vt_values(data_key, include_ts=True)
                    country_imp[country_name][vt] = vt_vals
                    break

    country_exp_items = []
    for country_name, vt_dict in country_exp.items():
        vt_parts = []
        for vt in VESSEL_TYPES:
            if vt in vt_dict:
                vt_parts.append(f'{vt}:{_dict_to_js(vt_dict[vt])}')
        if vt_parts:  # Only add if we have data
            country_obj = '{name:"' + escape_js_string(country_name) + '",' + ','.join(vt_parts) + '}'
            country_exp_items.append(country_obj)

    country_imp_items = []
    for country_name, vt_dict in country_imp.items():
        vt_parts = []
        for vt in VESSEL_TYPES:
            if vt in vt_dict:
                vt_parts.append(f'{vt}:{_dict_to_js(vt_dict[vt])}')
        if vt_parts:  # Only add if we have data
            country_obj = '{name:"' + escape_js_string(country_name) + '",' + ','.join(vt_parts) + '}'
            country_imp_items.append(country_obj)

    # ── Assemble final JS object ──
    # Convert dates to JS array
    dates_js_array = '[' + ','.join(f'"{d}"' for d in global_dates) + ']'

    js_str = (
        '        window._tableAggData = {\n'
        '            dates: ' + dates_js_array + ',\n'
        '            crisis: "' + escape_js_string(global_crisis_date) + '",\n'
        '            chokepoints: [' + ',\n                '.join(cp_items) + '],\n'
        '            ports: [' + ',\n                '.join(port_items) + '],\n'
        '            regional_exp: [' + ',\n                '.join(regional_exp_items) + '],\n'
        '            regional_imp: [' + ',\n                '.join(regional_imp_items) + '],\n'
        '            country_exp: [' + ',\n                '.join(country_exp_items) + '],\n'
        '            country_imp: [' + ',\n                '.join(country_imp_items) + ']\n'
        '        };\n'
    )

    return js_str


def _kpi_val_span(val: float) -> str:
    """Format a single KPI value as a colored span."""
    cls = 'negative' if val < 0 else 'positive'
    sign = '+' if val > 0 else ''
    return f'<span class="kpi-value {cls}" style="font-size:1.5rem;">{sign}{round(val, 1)}%</span>'


def _kpi_sub_row(label: str, val: float, dim: bool = False, dev: float = None, sigma: float = None,
                 ta: list = None, tc: list = None) -> str:
    """Format a sub-metric row inside a multi-value KPI card."""
    cls = 'negative' if val < 0 else 'positive'
    sign = '+' if val > 0 else ''
    dim_cls = ' dev-ns' if dim else ''
    # Embed dev/sigma as data attrs so JS slider can recompute dimming
    data_attrs = ''
    if dev is not None and sigma is not None:
        data_attrs = f' data-dev="{round(dev, 2)}" data-sg="{round(sigma, 2)}"'
    # Embed post-crisis time series for week slider
    if ta is not None and tc is not None:
        data_attrs += f' data-ta="{",".join(str(round(v, 4)) for v in ta)}" data-tc="{",".join(str(round(v, 4)) for v in tc)}"'
    return (f'<div class="sig-dimmable" style="display:flex;justify-content:space-between;align-items:center;padding:0.15rem 0;"{data_attrs}>'
            f'<span style="color:#9ca3af;font-size:0.75rem;">{label}</span>'
            f'<span class="kpi-value {cls}{dim_cls}" style="font-size:0.95rem;">{sign}{round(val, 1)}%</span>'
            f'</div>')


def build_kpi_html(kpis: Dict[str, Any], vessel_type: str, vt_label: str,
                   sig: Dict[str, bool] = None, sig_params: Dict[str, tuple] = None,
                   kpi_ts: Dict[str, tuple] = None) -> str:
    """Build KPI cards HTML for given vessel type."""
    if sig is None:
        sig = {}
    if sig_params is None:
        sig_params = {}
    if kpi_ts is None:
        kpi_ts = {}
    _is_dim = lambda k: not sig.get(k, True)  # dim when significance is False
    def _sp(k):
        """Return (dev, sigma) for a metric key, or (None, None)."""
        return sig_params.get(k, (None, None))
    def _ts(k):
        """Return (ta, tc) post-crisis arrays, or (None, None)."""
        return kpi_ts.get(k, (None, None))

    # Helper: chokepoint card (ship count + capacity)
    def _cp_card(label, prefix):
        inner = (
            _kpi_sub_row('Ship Count', kpis[f'{prefix}_cnt'], dim=_is_dim(f'{prefix}_cnt'), dev=_sp(f'{prefix}_cnt')[0], sigma=_sp(f'{prefix}_cnt')[1], ta=_ts(f'{prefix}_cnt')[0], tc=_ts(f'{prefix}_cnt')[1]) +
            _kpi_sub_row('Capacity', kpis[f'{prefix}_cap'], dim=_is_dim(f'{prefix}_cap'), dev=_sp(f'{prefix}_cap')[0], sigma=_sp(f'{prefix}_cap')[1], ta=_ts(f'{prefix}_cap')[0], tc=_ts(f'{prefix}_cap')[1])
        )
        return (
            '            <div class="kpi-card" data-kpi-section="chokepoints">\n'
            '                <div class="kpi-label">' + label + '</div>\n'
            '                <div style="margin-top:0.25rem;">' + inner + '</div>\n'
            '            </div>\n'
        )

    # Helper: region card (port calls, exports, imports)
    def _region_card(label, prefix):
        inner = (
            _kpi_sub_row('Port Calls', kpis[f'{prefix}_calls'], dim=_is_dim(f'{prefix}_calls'), dev=_sp(f'{prefix}_calls')[0], sigma=_sp(f'{prefix}_calls')[1], ta=_ts(f'{prefix}_calls')[0], tc=_ts(f'{prefix}_calls')[1]) +
            _kpi_sub_row('Exports', kpis[f'{prefix}_exp'], dim=_is_dim(f'{prefix}_exp'), dev=_sp(f'{prefix}_exp')[0], sigma=_sp(f'{prefix}_exp')[1], ta=_ts(f'{prefix}_exp')[0], tc=_ts(f'{prefix}_exp')[1]) +
            _kpi_sub_row('Imports', kpis[f'{prefix}_imp'], dim=_is_dim(f'{prefix}_imp'), dev=_sp(f'{prefix}_imp')[0], sigma=_sp(f'{prefix}_imp')[1], ta=_ts(f'{prefix}_imp')[0], tc=_ts(f'{prefix}_imp')[1])
        )
        return (
            '            <div class="kpi-card" data-kpi-section="regional">\n'
            '                <div class="kpi-label">' + label + '</div>\n'
            '                <div style="margin-top:0.25rem;">' + inner + '</div>\n'
            '            </div>\n'
        )

    # Helper: country card (port calls, exports, imports)
    def _country_card(label, prefix):
        inner = (
            _kpi_sub_row('Port Calls', kpis[f'{prefix}_calls'], dim=_is_dim(f'{prefix}_calls'), dev=_sp(f'{prefix}_calls')[0], sigma=_sp(f'{prefix}_calls')[1], ta=_ts(f'{prefix}_calls')[0], tc=_ts(f'{prefix}_calls')[1]) +
            _kpi_sub_row('Exports', kpis[f'{prefix}_exp'], dim=_is_dim(f'{prefix}_exp'), dev=_sp(f'{prefix}_exp')[0], sigma=_sp(f'{prefix}_exp')[1], ta=_ts(f'{prefix}_exp')[0], tc=_ts(f'{prefix}_exp')[1]) +
            _kpi_sub_row('Imports', kpis[f'{prefix}_imp'], dim=_is_dim(f'{prefix}_imp'), dev=_sp(f'{prefix}_imp')[0], sigma=_sp(f'{prefix}_imp')[1], ta=_ts(f'{prefix}_imp')[0], tc=_ts(f'{prefix}_imp')[1])
        )
        return (
            '            <div class="kpi-card" data-kpi-section="countries">\n'
            '                <div class="kpi-label">' + label + '</div>\n'
            '                <div style="margin-top:0.25rem;">' + inner + '</div>\n'
            '            </div>\n'
        )

    # Row 1: Chokepoints
    row1 = (
        '        <div class="kpi-row-label">Chokepoints</div>\n'
        '        <div class="kpi-grid">\n' +
        _cp_card('Strait of Hormuz', 'hormuz') +
        _cp_card('Suez Canal', 'suez') +
        _cp_card('Panama Canal', 'panama') +
        _cp_card('Cape of Good Hope', 'cape') +
        _cp_card('Malacca Strait', 'malacca') +
        '        </div>\n'
    )

    # Row 2: Regions
    row2 = (
        '        <div class="kpi-row-label">Regions</div>\n'
        '        <div class="kpi-grid">\n' +
        _region_card('Persian Gulf', 'gulf') +
        _region_card('North America', 'na') +
        _region_card('East Asia', 'ea') +
        _region_card('Southeast Asia', 'sea') +
        _region_card('Latin America', 'latam') +
        '        </div>\n'
    )

    # Row 3: Countries
    row3 = (
        '        <div class="kpi-row-label">Countries</div>\n'
        '        <div class="kpi-grid">\n' +
        _country_card('Singapore', 'sgp') +
        _country_card('Malaysia', 'my') +
        _country_card('Thailand', 'th') +
        _country_card('Indonesia', 'id') +
        _country_card('Philippines', 'ph') +
        '        </div>\n'
    )

    return row1 + row2 + row3


def build_html(data: Dict[str, Any], output_path: str) -> None:
    """Build and write the complete HTML dashboard."""

    kpis, _kpi_sig, _kpi_sp, _kpi_ts = get_kpi_values(data)

    # Build separate tables
    chokepoint_rows = build_chokepoint_table(data)
    port_group_export_rows, port_group_import_rows = build_port_group_tables(data)

    # Build top-50 port tables
    top50_export_table = build_top_port_table(data, '_top50_export_ports', 'Top Export Ports — Crisis Deviation')
    top50_import_table = build_top_port_table(data, '_top50_import_ports', 'Top Import Ports — Crisis Deviation')

    # Build Leaflet map
    port_map = build_leaflet_map(data)

    # Build main Hormuz chart
    hormuz_main = build_hormuz_main_chart(data)

    # (Standalone chart sections removed — charts are now inline in expandable table rows)

    # Build KPI cards HTML
    # KPI section — this acts as a placeholder/marker for build_html_multi() splicing
    kpi_html = (
        '        <!-- KPI Cards -->\n' +
        build_kpi_html(kpis, 'tanker', 'Tanker')
    )

    html_content = (
        '<!DOCTYPE html>\n' +
        '<html lang="en">\n' +
        '<head>\n' +
        '    <meta charset="UTF-8">\n' +
        '    <meta name="viewport" content="width=device-width, initial-scale=1.0">\n' +
        '    <title>Global Shipping Nowcast</title>\n' +
        '    <link rel="preconnect" href="https://fonts.googleapis.com">\n' +
        '    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>\n' +
        '    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">\n' +
        '    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.js"></script>\n' +
        '    <script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-annotation@3.0.1/dist/chartjs-plugin-annotation.min.js"></script>\n' +
        '    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.css" />\n' +
        '    <script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.js"></script>\n' +
        '    <style>\n' +
        '        * {\n' +
        '            margin: 0;\n' +
        '            padding: 0;\n' +
        '            box-sizing: border-box;\n' +
        '        }\n' +
        '\n' +
        '        body {\n' +
        '            font-family: \'Inter\', -apple-system, BlinkMacSystemFont, \'Segoe UI\', sans-serif;\n' +
        '            background-color: #0a0e17;\n' +
        '            color: #e5e7eb;\n' +
        '            line-height: 1.6;\n' +
        '        }\n' +
        '\n' +
        '        .container {\n' +
        '            max-width: 1400px;\n' +
        '            margin: 0 auto;\n' +
        '            padding: 2rem;\n' +
        '        }\n' +
        '\n' +
        '        /* Header */\n' +
        '        .header {\n' +
        '            display: flex;\n' +
        '            justify-content: space-between;\n' +
        '            align-items: center;\n' +
        '            margin-bottom: 3rem;\n' +
        '            border-bottom: 1px solid #1f2937;\n' +
        '            padding-bottom: 2rem;\n' +
        '        }\n' +
        '\n' +
        '        .header h1 {\n' +
        '            font-size: 2.5rem;\n' +
        '            font-weight: 700;\n' +
        '            background: linear-gradient(135deg, #3b82f6, #8b5cf6);\n' +
        '            -webkit-background-clip: text;\n' +
        '            -webkit-text-fill-color: transparent;\n' +
        '            background-clip: text;\n' +
        '        }\n' +
        '\n' +
        '        .view-toggle {\n' +
        '            display: flex; gap: 0.25rem; background: #1f2937; border-radius: 0.5rem; padding: 0.25rem;\n' +
        '            border: 1px solid #374151;\n' +
        '        }\n' +
        '        .view-toggle-btn {\n' +
        '            padding: 0.45rem 1rem; border-radius: 0.375rem; font-size: 0.8rem; font-weight: 600;\n' +
        '            cursor: pointer; border: none; background: transparent; color: #9ca3af;\n' +
        '            transition: all 0.2s; font-family: "Inter", sans-serif; letter-spacing: 0.02em;\n' +
        '        }\n' +
        '        .view-toggle-btn:hover { color: #e5e7eb; }\n' +
        '        .view-toggle-btn.active { background: linear-gradient(135deg, #3b82f6, #6366f1); color: #fff; }\n' +
        '        .view-dashboard { display: block; }\n' +
        '        .view-methodology { display: none; }\n' +
        '        body.show-methodology .view-dashboard { display: none; }\n' +
        '        body.show-methodology .view-methodology { display: block; }\n' +
        '\n' +
        '        /* KPI Cards */\n' +
        '        .kpi-row-label {\n' +
        '            font-size: 0.7rem;\n' +
        '            text-transform: uppercase;\n' +
        '            letter-spacing: 0.12em;\n' +
        '            color: #6b7280;\n' +
        '            font-weight: 600;\n' +
        '            margin-bottom: 0.5rem;\n' +
        '            padding-left: 0.25rem;\n' +
        '        }\n' +
        '        .kpi-grid {\n' +
        '            display: grid;\n' +
        '            grid-template-columns: repeat(5, 1fr);\n' +
        '            gap: 1rem;\n' +
        '            margin-bottom: 1.75rem;\n' +
        '        }\n' +
        '        @media (max-width: 900px) {\n' +
        '            .kpi-grid {\n' +
        '                grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));\n' +
        '            }\n' +
        '        }\n' +
        '\n' +
        '        .kpi-card {\n' +
        '            background: linear-gradient(135deg, #1f2937, #111827);\n' +
        '            border: 1px solid #374151;\n' +
        '            border-radius: 0.75rem;\n' +
        '            padding: 1rem;\n' +
        '            transition: all 0.3s ease;\n' +
        '            cursor: pointer;\n' +
        '        }\n' +
        '\n' +
        '        .kpi-card:hover {\n' +
        '            border-color: #4b5563;\n' +
        '            transform: translateY(-2px);\n' +
        '            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.3);\n' +
        '        }\n' +
        '\n' +
        '        .kpi-label {\n' +
        '            font-size: 0.875rem;\n' +
        '            text-transform: uppercase;\n' +
        '            letter-spacing: 0.05em;\n' +
        '            color: #9ca3af;\n' +
        '            margin-bottom: 0.75rem;\n' +
        '            font-weight: 500;\n' +
        '        }\n' +
        '\n' +
        '        .kpi-value {\n' +
        '            font-size: 2.5rem;\n' +
        '            font-weight: 700;\n' +
        '            margin-bottom: 0.5rem;\n' +
        '        }\n' +
        '\n' +
        '        .kpi-value.negative {\n' +
        '            color: #ef4444;\n' +
        '        }\n' +
        '\n' +
        '        .kpi-value.positive {\n' +
        '            color: #10b981;\n' +
        '        }\n' +
        '\n' +
        '        /* Dim non-significant deviations (within pre-crisis noise) */\n' +
        '        .dev-ns {\n' +
        '            opacity: 0.35;\n' +
        '        }\n' +
        '        .dev-ns-note {\n' +
        '            font-size: 0.75rem;\n' +
        '            color: #6b7280;\n' +
        '            font-style: italic;\n' +
        '            margin-top: 0.25rem;\n' +
        '        }\n' +
        '\n' +
        '        .kpi-unit {\n' +
        '            font-size: 0.875rem;\n' +
        '            color: #6b7280;\n' +
        '        }\n' +
        '\n' +
        '        /* Table */\n' +
        '        .table-section {\n' +
        '            background: linear-gradient(135deg, #1f2937, #111827);\n' +
        '            border: 1px solid #374151;\n' +
        '            border-radius: 0.75rem;\n' +
        '            overflow-x: auto;\n' +
        '            margin-bottom: 3rem;\n' +
        '        }\n' +
        '\n' +
        '        .table-section h2 {\n' +
        '            padding: 1.5rem;\n' +
        '            border-bottom: 1px solid #374151;\n' +
        '            font-size: 1.25rem;\n' +
        '        }\n' +
        '\n' +
        '        table {\n' +
        '            width: 100%;\n' +
        '            min-width: 500px;\n' +
        '            border-collapse: collapse;\n' +
        '        }\n' +
        '\n' +
        '        thead {\n' +
        '            background-color: #111827;\n' +
        '        }\n' +
        '\n' +
        '        th {\n' +
        '            padding: 1rem 1.5rem;\n' +
        '            text-align: left;\n' +
        '            font-weight: 600;\n' +
        '            border-bottom: 2px solid #374151;\n' +
        '            color: #9ca3af;\n' +
        '            font-size: 0.875rem;\n' +
        '            text-transform: uppercase;\n' +
        '            letter-spacing: 0.05em;\n' +
        '            cursor: pointer;\n' +
        '            user-select: none;\n' +
        '            position: relative;\n' +
        '            padding-right: 1.8rem;\n' +
        '        }\n' +
        '        th:hover { color: #e5e7eb; }\n' +
        '        th::after {\n' +
        '            content: "\\2195";\n' +
        '            position: absolute;\n' +
        '            right: 0.5rem;\n' +
        '            opacity: 0.35;\n' +
        '            font-size: 0.75rem;\n' +
        '        }\n' +
        '        th.sort-asc::after { content: "\\25B2"; opacity: 0.8; }\n' +
        '        th.sort-desc::after { content: "\\25BC"; opacity: 0.8; }\n' +
        '\n' +
        '        td {\n' +
        '            padding: 1rem 1.5rem;\n' +
        '            border-bottom: 1px solid #1f2937;\n' +
        '        }\n' +
        '\n' +
        '        tbody tr:hover {\n' +
        '            background-color: rgba(59, 130, 246, 0.05);\n' +
        '        }\n' +
        '\n' +
        '        .region-cell {\n' +
        '            font-weight: 500;\n' +
        '        }\n' +
        '\n' +
        '        .numeric-cell {\n' +
        '            text-align: right;\n' +
        '            font-family: \'Monaco\', \'Courier New\', monospace;\n' +
        '            font-size: 0.9rem;\n' +
        '            color: #d1d5db;\n' +
        '            white-space: nowrap;\n' +
        '        }\n' +
        '\n' +
        '        .deviation-cell {\n' +
        '            font-weight: 600;\n' +
        '        }\n' +
        '\n' +
        '        .deviation-cell.negative {\n' +
        '            color: #ef4444;\n' +
        '        }\n' +
        '\n' +
        '        .deviation-cell.positive {\n' +
        '            color: #10b981;\n' +
        '        }\n' +
        '\n' +
        '        .category-header td {\n' +
        '            background-color: #1e3a5f;\n' +
        '            color: #93c5fd;\n' +
        '            font-weight: 600;\n' +
        '            font-size: 0.8rem;\n' +
        '            text-transform: uppercase;\n' +
        '            letter-spacing: 0.1em;\n' +
        '            padding: 0.75rem 1.5rem;\n' +
        '        }\n' +
        '\n' +
        '        .region-tag {\n' +
        '            display: inline-block;\n' +
        '            padding: 0.15rem 0.5rem;\n' +
        '            border-radius: 0.25rem;\n' +
        '            font-size: 0.7rem;\n' +
        '            font-weight: 600;\n' +
        '            text-transform: uppercase;\n' +
        '            letter-spacing: 0.05em;\n' +
        '            white-space: nowrap;\n' +
        '        }\n' +
        '\n' +
        '        /* Expandable rows */\n' +
        '        .expandable-row:hover { background: #1f2937; }\n' +
        '        .expandable-row.expanded { background: #1e293b; }\n' +
        '        .expand-icon {\n' +
        '            display: inline-block;\n' +
        '            font-size: 0.6rem;\n' +
        '            color: #6b7280;\n' +
        '            transition: transform 0.2s;\n' +
        '            margin-left: 0.3rem;\n' +
        '        }\n' +
        '        .expandable-row.expanded .expand-icon { transform: rotate(90deg); color: #93c5fd; }\n' +
        '        .chart-row .chart-cell {\n' +
        '            padding: 0.75rem 1rem;\n' +
        '            background: #0f172a;\n' +
        '            border-top: 1px solid #1e293b;\n' +
        '        }\n' +
        '        .inline-chart-container {\n' +
        '            height: 200px;\n' +
        '            position: relative;\n' +
        '        }\n' +
        '\n' +
        '        .export-csv-btn {\n' +
        '            display: inline-block; margin-top: 0.5rem; padding: 0.3rem 0.75rem;\n' +
        '            background: transparent; border: 1px solid #374151; border-radius: 0.375rem;\n' +
        '            color: #9ca3af; font-size: 0.75rem; font-weight: 500; cursor: pointer;\n' +
        '            font-family: "Inter", sans-serif; transition: all 0.2s;\n' +
        '        }\n' +
        '        .export-csv-btn:hover { color: #e5e7eb; border-color: #6b7280; background: #1f2937; }\n' +
        '        .zoom-toggle-btn {\n' +
        '            display: inline-block; margin-top: 0.5rem; margin-right: 0.4rem; padding: 0.3rem 0.75rem;\n' +
        '            background: transparent; border: 1px solid #374151; border-radius: 0.375rem;\n' +
        '            color: #9ca3af; font-size: 0.75rem; font-weight: 500; cursor: pointer;\n' +
        '            font-family: "Inter", sans-serif; transition: all 0.2s;\n' +
        '        }\n' +
        '        .zoom-toggle-btn:hover { color: #e5e7eb; border-color: #6b7280; background: #1f2937; }\n' +
        '        .vd-bar-container { margin: 0.4rem 0 0.2rem 0; }\n' +
        '        .vd-bar-label { color: #9ca3af; font-size: 0.7rem; font-family: "Inter",sans-serif; margin-bottom: 0.2rem; }\n' +
        '        .vd-bar { display: flex; height: 6px; border-radius: 3px; overflow: hidden; background: #1f2937; }\n' +
        '        .vd-seg { height: 100%; transition: width 0.3s; }\n' +
        '        .vd-legend { display: flex; gap: 0.7rem; flex-wrap: wrap; margin-top: 0.2rem; }\n' +
        '        .vd-legend-item { color: #9ca3af; font-size: 0.65rem; font-family: "Inter",sans-serif; display: flex; align-items: center; gap: 3px; }\n' +
        '        .vd-dot { width: 7px; height: 7px; border-radius: 50%; display: inline-block; }\n' +
        '\n' +
        '        .tables-grid {\n' +
        '            display: grid;\n' +
        '            grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));\n' +
        '            gap: 1.5rem;\n' +
        '            margin-bottom: 2rem;\n' +
        '        }\n' +
        '        .tables-grid > .table-section {\n' +
        '            min-width: 0;\n' +
        '        }\n' +
        '\n' +
        '        @media (max-width: 1000px) {\n' +
        '            .tables-grid {\n' +
        '                grid-template-columns: 1fr;\n' +
        '            }\n' +
        '        }\n' +
        '\n' +
        '        /* Charts */\n' +
        '        .chart-section {\n' +
        '            background: linear-gradient(135deg, #1f2937, #111827);\n' +
        '            border: 1px solid #374151;\n' +
        '            border-radius: 0.75rem;\n' +
        '            padding: 1.5rem;\n' +
        '            margin-bottom: 2rem;\n' +
        '        }\n' +
        '\n' +
        '        .chart-header {\n' +
        '            display: flex;\n' +
        '            justify-content: space-between;\n' +
        '            align-items: center;\n' +
        '            margin-bottom: 1.5rem;\n' +
        '        }\n' +
        '\n' +
        '        .chart-title {\n' +
        '            font-size: 1.125rem;\n' +
        '            font-weight: 600;\n' +
        '            color: #e5e7eb;\n' +
        '            margin: 0;\n' +
        '        }\n' +
        '\n' +
        '        .chart-container {\n' +
        '            position: relative;\n' +
        '            height: 400px;\n' +
        '            margin-bottom: 1rem;\n' +
        '        }\n' +
        '\n' +
        '        .chart-container.small-chart {\n' +
        '            height: 250px;\n' +
        '        }\n' +
        '\n' +
        '        .chart-note {\n' +
        '            font-size: 0.875rem;\n' +
        '            color: #9ca3af;\n' +
        '            margin: 0;\n' +
        '            padding-top: 0.5rem;\n' +
        '            border-top: 1px solid #374151;\n' +
        '        }\n' +
        '\n' +
        '        /* Toggle buttons */\n' +
        '        .toggle-group {\n' +
        '            display: flex;\n' +
        '            gap: 0.5rem;\n' +
        '            background-color: #111827;\n' +
        '            padding: 0.25rem;\n' +
        '            border-radius: 0.5rem;\n' +
        '        }\n' +
        '\n' +
        '        .toggle-btn {\n' +
        '            padding: 0.4rem 0.8rem;\n' +
        '            background-color: transparent;\n' +
        '            border: 1px solid #374151;\n' +
        '            border-radius: 0.375rem;\n' +
        '            color: #9ca3af;\n' +
        '            font-size: 0.8rem;\n' +
        '            font-weight: 600;\n' +
        '            cursor: pointer;\n' +
        '            transition: all 0.2s ease;\n' +
        '            font-family: \'Inter\', sans-serif;\n' +
        '        }\n' +
        '\n' +
        '        .toggle-btn:hover {\n' +
        '            color: #e5e7eb;\n' +
        '            border-color: #4b5563;\n' +
        '        }\n' +
        '\n' +
        '        .toggle-btn.active {\n' +
        '            background-color: #3b82f6;\n' +
        '            border-color: #3b82f6;\n' +
        '            color: #fff;\n' +
        '        }\n' +
        '\n' +
        '        /* STL Grid */\n' +
        '        .stl-grid {\n' +
        '            display: grid;\n' +
        '            grid-template-columns: repeat(auto-fit, minmax(350px, 1fr));\n' +
        '            gap: 2rem;\n' +
        '            margin-bottom: 2rem;\n' +
        '        }\n' +
        '\n' +
        '        /* Methodology */\n' +
        '        .methodology {\n' +
        '            background: linear-gradient(135deg, #1f2937, #111827);\n' +
        '            border: 1px solid #374151;\n' +
        '            border-radius: 0.75rem;\n' +
        '            padding: 2rem;\n' +
        '            margin-top: 3rem;\n' +
        '        }\n' +
        '\n' +
        '        .methodology h2 {\n' +
        '            font-size: 1.25rem;\n' +
        '            margin-bottom: 1rem;\n' +
        '            color: #e5e7eb;\n' +
        '        }\n' +
        '\n' +
        '        .methodology p {\n' +
        '            color: #d1d5db;\n' +
        '            margin-bottom: 1rem;\n' +
        '            line-height: 1.8;\n' +
        '        }\n' +
        '\n' +
        '        .methodology ul {\n' +
        '            color: #d1d5db;\n' +
        '            margin-left: 1.5rem;\n' +
        '            margin-bottom: 1rem;\n' +
        '        }\n' +
        '\n' +
        '        .methodology li {\n' +
        '            margin-bottom: 0.5rem;\n' +
        '        }\n' +
        '\n' +
        '        @media (max-width: 768px) {\n' +
        '            .container {\n' +
        '                padding: 1rem;\n' +
        '            }\n' +
        '\n' +
        '            .header {\n' +
        '                flex-direction: column;\n' +
        '                align-items: flex-start;\n' +
        '                gap: 1rem;\n' +
        '            }\n' +
        '\n' +
        '            .header h1 {\n' +
        '                font-size: 1.75rem;\n' +
        '            }\n' +
        '\n' +
        '            .chart-header {\n' +
        '                flex-direction: column;\n' +
        '                align-items: flex-start;\n' +
        '                gap: 1rem;\n' +
        '            }\n' +
        '\n' +
        '            .stl-grid {\n' +
        '                grid-template-columns: 1fr;\n' +
        '            }\n' +
        '\n' +
        '            .kpi-grid {\n' +
        '                grid-template-columns: repeat(2, 1fr);\n' +
        '            }\n' +
        '        }\n' +
        '        .dark-tooltip {\n' +
        '            background: #1f2937 !important;\n' +
        '            color: #e5e7eb !important;\n' +
        '            border: 1px solid #374151 !important;\n' +
        '            border-radius: 0.5rem !important;\n' +
        '            padding: 0.5rem 0.75rem !important;\n' +
        '            font-family: Inter, sans-serif !important;\n' +
        '            font-size: 0.8rem !important;\n' +
        '            box-shadow: 0 4px 12px rgba(0,0,0,0.4) !important;\n' +
        '        }\n' +
        '        .dark-tooltip .leaflet-tooltip-tip {\n' +
        '            display: none;\n' +
        '        }\n' +
        '    </style>\n' +
        '</head>\n' +
        '<body>\n' +
        '    <!-- Password gate -->\n' +
        '    <style>\n' +
        '        #auth-gate { display:flex; align-items:center; justify-content:center; min-height:100vh; background:linear-gradient(135deg, #1e293b 0%, #334155 100%); }\n' +
        '        .auth-box { background:white; border-radius:12px; padding:2.5rem; width:360px; box-shadow:0 4px 24px rgba(0,0,0,0.2); text-align:center; }\n' +
        '        .auth-box h1 { font-size:1.2rem; color:#1e293b; margin-bottom:0.3rem; font-family:Inter,sans-serif; }\n' +
        '        .auth-box .subtitle { font-size:0.85rem; color:#64748b; margin-bottom:1.5rem; font-family:Inter,sans-serif; }\n' +
        '        .auth-box input { width:100%; padding:0.7rem 1rem; border:1px solid #cbd5e1; border-radius:8px; font-size:0.95rem; outline:none; transition:border-color 0.15s; font-family:Inter,sans-serif; }\n' +
        '        .auth-box input:focus { border-color:#3b82f6; }\n' +
        '        .auth-box button { width:100%; padding:0.7rem; margin-top:0.75rem; border:none; border-radius:8px; background:#1e293b; color:white; font-size:0.95rem; font-weight:600; cursor:pointer; transition:background 0.15s; font-family:Inter,sans-serif; }\n' +
        '        .auth-box button:hover { background:#334155; }\n' +
        '        .auth-error { color:#ef4444; font-size:0.85rem; margin-top:0.75rem; display:none; font-family:Inter,sans-serif; }\n' +
        '    </style>\n' +
        '    <div id="auth-gate">\n' +
        '        <div class="auth-box">\n' +
        '            <h1>Shipping Nowcast Dashboard</h1>\n' +
        '            <div class="subtitle">Enter the password to continue</div>\n' +
        '            <input type="password" id="auth-pw" placeholder="Password" autofocus\n' +
        '                onkeydown="if(event.key===\'Enter\')document.getElementById(\'auth-btn\').click()">\n' +
        '            <button id="auth-btn" onclick="checkAuth()">Enter</button>\n' +
        '            <div class="auth-error" id="auth-err">Incorrect password</div>\n' +
        '        </div>\n' +
        '    </div>\n' +
        '    <div id="dashboard-content" style="display:none;">\n' +
        '    <script>\n' +
        '    async function checkAuth() {\n' +
        '        var pw = document.getElementById("auth-pw").value;\n' +
        '        var enc = new TextEncoder();\n' +
        '        var hash = await crypto.subtle.digest("SHA-256", enc.encode(pw));\n' +
        '        var hex = Array.from(new Uint8Array(hash)).map(function(b){return b.toString(16).padStart(2,"0")}).join("");\n' +
        '        if (hex === "96869035ed72106a7d2d9eabd7c5b46ca832d3c51a5b2f524c36f224d870eb8b") {\n' +
        '            document.getElementById("auth-gate").style.display = "none";\n' +
        '            document.getElementById("dashboard-content").style.display = "block";\n' +
        '            // Initialize map now that container is visible\n' +
        '            setTimeout(function() { if (typeof initPortMap === "function") initPortMap(); }, 100);\n' +
        '        } else {\n' +
        '            document.getElementById("auth-err").style.display = "block";\n' +
        '        }\n' +
        '    }\n' +
        '    </script>\n' +
        '    <script>\n' +
        '    function getCrisisAnnotation(labels) {\n' +
        '      var crisisDate = "2026-03-02";\n' +
        '      var idx = -1;\n' +
        '      for (var i = 0; i < labels.length; i++) {\n' +
        '        if (labels[i] >= crisisDate) { idx = i; break; }\n' +
        '      }\n' +
        '      if (idx < 0) return {};\n' +
        '      return {\n' +
        '        annotations: {\n' +
        '          crisisLine: {\n' +
        '            type: "line",\n' +
        '            xMin: idx, xMax: idx,\n' +
        '            borderColor: "rgba(239, 68, 68, 0.7)",\n' +
        '            borderWidth: 2,\n' +
        '            borderDash: [6, 3],\n' +
        '            label: {\n' +
        '              display: true,\n' +
        '              content: "Crisis Onset",\n' +
        '              color: "#ef4444",\n' +
        '              backgroundColor: "rgba(0,0,0,0.6)",\n' +
        '              font: { size: 10, weight: "bold" },\n' +
        '              position: "start"\n' +
        '            }\n' +
        '          }\n' +
        '        }\n' +
        '      };\n' +
        '    }\n' +
        '    </script>\n' +
        '    <div class="container">\n' +
        '        <!-- Header -->\n' +
        '        <div class="header">\n' +
        '            <h1>Global Shipping Nowcast</h1>\n' +
        '            <div class="view-toggle">\n' +
        '                <button class="view-toggle-btn active" data-view="dashboard">Dashboard</button>\n' +
        '                <button class="view-toggle-btn" data-view="methodology">Methodology</button>\n' +
        '            </div>\n' +
        '        </div>\n' +
        '\n' +
        '        <div class="view-dashboard">\n' +
        kpi_html + '\n' +
        '\n' +
        '        <!-- Interactive Map -->\n' +
        port_map + '\n' +
        '\n' +
        '        <!-- Chokepoints -->\n' +
        '        <div class="table-section">\n' +
        '            <h2>Chokepoint Deviations</h2>\n' +
        '            <table>\n' +
        '                <thead>\n' +
        '                    <tr>\n' +
        '                        <th>Chokepoint</th>\n' +
        '                        <th>Hist. Avg</th>\n' +
        '                        <th>Latest</th>\n' +
        '                        <th>Counterfactual</th>\n' +
        '                        <th>Deviation</th>\n' +
        '                        <th>vs 1Y ago</th>\n' +
        '                        <th>vs 1Q ago</th>\n' +
        '                    </tr>\n' +
        '                </thead>\n' +
        '                <tbody>\n' +
        chokepoint_rows + '\n' +
        '                </tbody>\n' +
        '            </table>\n' +
        '        </div>\n' +
        '        <div class="table-section">\n' +
        '            <h2>Regional Exports</h2>\n' +
        '            <table>\n' +
        '                <thead>\n' +
        '                    <tr>\n' +
        '                        <th>Region</th>\n' +
        '                        <th>Hist. Avg</th>\n' +
        '                        <th>Latest</th>\n' +
        '                        <th>Counterfactual</th>\n' +
        '                        <th>Deviation</th>\n' +
        '                        <th>vs 1Y ago</th>\n' +
        '                        <th>vs 1Q ago</th>\n' +
        '                    </tr>\n' +
        '                </thead>\n' +
        '                <tbody>\n' +
        port_group_export_rows + '\n' +
        '                </tbody>\n' +
        '            </table>\n' +
        '        </div>\n' +
        '        <div class="table-section">\n' +
        '            <h2>Regional Imports</h2>\n' +
        '            <table>\n' +
        '                <thead>\n' +
        '                    <tr>\n' +
        '                        <th>Region</th>\n' +
        '                        <th>Hist. Avg</th>\n' +
        '                        <th>Latest</th>\n' +
        '                        <th>Counterfactual</th>\n' +
        '                        <th>Deviation</th>\n' +
        '                        <th>vs 1Y ago</th>\n' +
        '                        <th>vs 1Q ago</th>\n' +
        '                    </tr>\n' +
        '                </thead>\n' +
        '                <tbody>\n' +
        port_group_import_rows + '\n' +
        '                </tbody>\n' +
        '            </table>\n' +
        '        </div>\n' +
        '\n' +
        '        <!-- Top 50 Export & Import Ports -->\n' +
        top50_export_table + '\n' +
        top50_import_table + '\n'
        '\n' +
        '        <!-- Hormuz Main Chart -->\n' +
        hormuz_main + '\n' +
        '        </div><!-- /view-dashboard -->\n' +
        '\n' +
        '        <!-- Methodology -->\n' +
        '        <div class="view-methodology">\n' +
        '        <div class="methodology" id="methodology">\n' +
        '            <h2>Methodology</h2>\n' +
        '\n' +
        '            <h3 style="color:#93c5fd; margin: 1.5rem 0 0.75rem 0; font-size: 1.1rem;">Overview</h3>\n' +
        '            <p>\n' +
        '                This dashboard estimates the causal impact of the Iran war crisis (onset February 28, 2026) on global\n' +
        '                shipping and trade flows. For each series (chokepoint, regional port group, country aggregate, or\n' +
        '                individual port), a <em>counterfactual</em> path is constructed representing what traffic would have\n' +
        '                looked like absent the crisis. The <em>deviation</em> between the actual observed data and the\n' +
        '                counterfactual is attributed to the crisis.\n' +
        '            </p>\n' +
        '            <p>\n' +
        '                Data comes from IMF PortWatch, which aggregates AIS (Automatic Identification System) satellite vessel\n' +
        '                tracking data. Daily counts and capacity (deadweight tonnage) for five vessel types (tanker, container,\n' +
        '                dry bulk, general cargo, and RoRo) are aggregated to weekly frequency to smooth day-of-week effects\n' +
        '                while preserving timely signal. Weeks run Monday through Sunday, with each week labeled by its Monday\n' +
        '                start date. Within each week, values represent the daily average (not the sum), and incomplete trailing\n' +
        '                weeks (fewer than 5 days of data) are dropped to avoid partial-week distortions.\n' +
        '            </p>\n' +
        '            <p>\n' +
        '                A clean resampling boundary is enforced at the crisis date: pre-crisis and post-crisis daily data are\n' +
        '                resampled to weekly frequency independently, ensuring no week blends pre- and post-crisis days. If the\n' +
        '                first post-crisis week is a short stub (crisis onset mid-week), it is merged into the following full\n' +
        '                week to avoid a noisy partial bin.\n' +
        '            </p>\n' +
        '\n' +
        '            <h3 style="color:#93c5fd; margin: 1.5rem 0 0.75rem 0; font-size: 1.1rem;">Counterfactual Construction (Multiplicative via Log-Space STL)</h3>\n' +
        '            <p>\n' +
        '                The decomposition is estimated in <strong>log space</strong> to ensure counterfactuals are always\n' +
        '                non-negative (trade flows cannot be negative). Each series is first transformed via\n' +
        '                <code>log1p(x)</code>, then STL is applied additively in log space, and the counterfactual is converted\n' +
        '                back via <code>expm1()</code>. This is equivalent to a multiplicative model in level space:\n' +
        '            </p>\n' +
        '            <p style="text-align:center; font-family: monospace; font-size: 1rem; color: #f9fafb; margin: 1rem 0;">\n' +
        '                counterfactual(t) = expm1( trend(t) + seasonal(t) + predicted_remainder(t) )\n' +
        '            </p>\n' +
        '            <p>\n' +
        '                The <code>log1p</code>/<code>expm1</code> pair handles zeros gracefully (<code>log1p(0) = 0</code>,\n' +
        '                <code>expm1(0) = 0</code>). Log-space is applied when all values are non-negative and at least 10%\n' +
        '                of the series is strictly positive. A final <code>clip(lower=0)</code> is applied as a hard floor.\n' +
        '            </p>\n' +
        '\n' +
        '            <h3 style="color:#93c5fd; margin: 1.5rem 0 0.75rem 0; font-size: 1.1rem;">Step 1: STL Decomposition</h3>\n' +
        '            <p>\n' +
        '                Each log-transformed weekly series is decomposed using STL (Seasonal-Trend decomposition using LOESS)\n' +
        '                with <code>period=52</code> (annual seasonality), <code>seasonal=13</code> (quarterly-scale seasonal\n' +
        '                smoothing window), and <code>robust=True</code> to downweight outliers. The seasonal smoother at\n' +
        '                s=13 is responsive enough to capture evolving seasonal patterns without overfitting year-to-year noise.\n' +
        '            </p>\n' +
        '            <p>\n' +
        '                <strong>Critically, the STL is estimated on pre-crisis data only</strong> (June 2019 through February\n' +
        '                27, 2026) to prevent the crisis-period collapse from contaminating the seasonal and trend estimates.\n' +
        '                Post-crisis seasonal values are projected forward using a seasonal week-lookup table: for each ISO\n' +
        '                calendar week, the table stores the average of that week&rsquo;s seasonal values from the most recent\n' +
        '                3 years of pre-crisis data. This raw lookup is then smoothed with a <strong>5-week circular moving\n' +
        '                average</strong> (half-window=2, wrapping around the 52-week boundary) to prevent implausible\n' +
        '                week-on-week spikes in the counterfactual caused by STL seasonal overfitting to individual years.\n' +
        '            </p>\n' +
        '            <p>\n' +
        '                This produces three components (all in log space):\n' +
        '            </p>\n' +
        '            <ul>\n' +
        '                <li><strong>Trend:</strong> The slow-moving structural level of the series (e.g., gradual growth in\n' +
        '                    Hormuz traffic as Asian demand rises, or the step-down in Bab el-Mandeb after the 2024 Houthi\n' +
        '                    disruption).</li>\n' +
        '                <li><strong>Seasonal:</strong> The repeating annual cycle (e.g., higher tanker traffic in winter months\n' +
        '                    due to heating oil demand, lower in spring shoulder season).</li>\n' +
        '                <li><strong>Remainder:</strong> Everything left over &mdash; short-run fluctuations driven by economic\n' +
        '                    conditions, oil market dynamics, weather events, and noise.</li>\n' +
        '            </ul>\n' +
        '\n' +
        '            <h3 style="color:#93c5fd; margin: 1.5rem 0 0.75rem 0; font-size: 1.1rem;">Step 2: Trend Extrapolation</h3>\n' +
        '            <p>\n' +
        '                To project the trend into the post-crisis period, a linear regression (<code>np.polyfit</code>, degree 1)\n' +
        '                is fitted to the last 13 weeks of pre-crisis trend values in log space. This captures the recent\n' +
        '                trajectory (rising, falling, or flat) and extends it forward at the same slope. The 13-week window\n' +
        '                balances responsiveness to recent shifts against stability. For the short post-crisis horizons here\n' +
        '                (~4 weeks), the choice of extrapolation method has minimal impact.\n' +
        '            </p>\n' +
        '\n' +
        '            <h3 style="color:#93c5fd; margin: 1.5rem 0 0.75rem 0; font-size: 1.1rem;">Step 3: Seasonal Projection</h3>\n' +
        '            <p>\n' +
        '                The smoothed seasonal week-lookup table (described in Step 1) maps each post-crisis calendar week to\n' +
        '                its expected seasonal value in log space. Because the lookup is built from a 3-year average and then\n' +
        '                smoothed with a 5-week circular moving average, week-to-week transitions are gentle and the\n' +
        '                counterfactual avoids jagged seasonal artifacts.\n' +
        '            </p>\n' +
        '\n' +
        '            <h3 style="color:#93c5fd; margin: 1.5rem 0 0.75rem 0; font-size: 1.1rem;">Step 4: Remainder Prediction (RidgeCV + AR Lags + Fourier Harmonics + Geographic Cross-Features)</h3>\n' +
        '            <p>\n' +
        '                After removing trend and seasonal, the leftover remainder captures short-run dynamics. A RidgeCV\n' +
        '                regression (leave-one-out cross-validation over alphas [0.01, 0.1, 0.5, 1, 5, 10, 50, 100], R&sup2;\n' +
        '                scoring, features standardized to zero mean and unit variance) is fitted on the pre-crisis training\n' +
        '                window to predict the remainder from five groups of features:\n' +
        '            </p>\n' +
        '            <ul>\n' +
        '                <li><strong>Frozen monthly macro controls (14):</strong> Gulf/Hormuz oil production, OPEC total\n' +
        '                    production, world petroleum production (all EIA); US industrial production; US capacity utilization;\n' +
        '                    vehicle sales; commercial &amp; industrial loans; IMF energy price index; IMF non-fuel commodity\n' +
        '                    index; China imports; China exports; consumer sentiment; retail sales; and Global Economic Policy\n' +
        '                    Uncertainty (EPU) index. All forward-filled to weekly frequency and frozen at their last pre-crisis\n' +
        '                    values.</li>\n' +
        '                <li><strong>Frozen daily/weekly controls (8):</strong> USD broad index, federal funds rate, US EPU,\n' +
        '                    10-year breakeven inflation, VIX, yield curve (10Y&minus;2Y), 4-week average initial jobless claims,\n' +
        '                    and weekly gasoline supplied. Averaged to weekly frequency and frozen at last pre-crisis values.</li>\n' +
        '                <li><strong>Fourier harmonics (6 features):</strong> Sine/cosine pairs at annual (52-week), semi-annual\n' +
        '                    (26-week), and quarterly (13-week) periods, computed as\n' +
        '                    <code>sin/cos(2&pi; &times; day_of_year / period_days)</code>. These deterministic calendar features\n' +
        '                    give the remainder model a second chance to capture sub-annual periodicities that the single STL\n' +
        '                    seasonal pass may miss (e.g., quarterly contract-timing cycles, semi-annual refinery turnaround\n' +
        '                    schedules).</li>\n' +
        '                <li><strong>AR(2) lags (2 features):</strong> The remainder&rsquo;s own values at lag-1 and lag-2,\n' +
        '                    capturing week-to-week momentum (autocorrelation) in the residual. Frozen at their last pre-crisis\n' +
        '                    values post-crisis.</li>\n' +
        '                <li><strong>Geographic cross-features (0&ndash;6 features):</strong> STL remainder of upstream\n' +
        '                    &ldquo;feeder&rdquo; chokepoints, both contemporaneous (t) and lagged (t&minus;1). These capture\n' +
        '                    the propagation of shipping flow shocks through the global network. For example, the Strait of\n' +
        '                    Hormuz remainder is used as a predictor for Suez Canal, Cape of Good Hope, and downstream Asian\n' +
        '                    ports; the Malacca Strait remainder feeds into Singapore, East Asian, and Southeast Asian models.\n' +
        '                    A geographic linkage map defines which chokepoints inform each target (up to 3 feeders &times; 2\n' +
        '                    lags = up to 6 cross-features). Like AR lags, all cross-features are frozen at their last\n' +
        '                    pre-crisis values to prevent crisis-period contamination. See &ldquo;Geographic Cross-Series\n' +
        '                    Features&rdquo; below for details.</li>\n' +
        '            </ul>\n' +
        '            <p>\n' +
        '                Total feature count per model: ~30&ndash;36 (14 monthly + 8 daily + 6 Fourier + 2 AR lags + 0&ndash;6\n' +
        '                geographic cross-features). A minimum of 20 pre-crisis observations is required for the Ridge\n' +
        '                regression to be fitted.\n' +
        '            </p>\n' +
        '\n' +
        '            <h3 style="color:#93c5fd; margin: 1.5rem 0 0.75rem 0; font-size: 1.1rem;">Step 5: Freezing Controls Post-Crisis</h3>\n' +
        '            <p>\n' +
        '                The central design choice: all control variables, AR lags, and geographic cross-features are\n' +
        '                <strong>frozen</strong> at their last pre-crisis values when projecting the counterfactual forward.\n' +
        '                This means the counterfactual answers &ldquo;what would have happened if economic conditions and\n' +
        '                shipping momentum had continued as they were immediately before the crisis?&rdquo; Without freezing,\n' +
        '                crisis-period shocks (collapsing tanker counts, surging oil prices) would feed back through the AR\n' +
        '                lags and live controls into the counterfactual, shrinking the estimated deviation toward zero.\n' +
        '            </p>\n' +
        '            <p>\n' +
        '                The sensitivity model relaxes this for a subset of daily financial variables, allowing Brent crude,\n' +
        '                WTI, Henry Hub natural gas, high-yield credit spreads, and BDRY/FRO shipping indices to update live.\n' +
        '                This captures an alternative scenario where financial market movements since the crisis would have\n' +
        '                affected shipping volumes even absent the physical disruption.\n' +
        '            </p>\n' +
        '\n' +
        '            <h3 style="color:#93c5fd; margin: 1.5rem 0 0.75rem 0; font-size: 1.1rem;">Geographic Cross-Series Features</h3>\n' +
        '            <p>\n' +
        '                Global shipping flows are interconnected: a disruption at one chokepoint propagates downstream through\n' +
        '                the network. To capture this, the pipeline uses a <strong>two-pass architecture</strong>. In the first\n' +
        '                pass, STL decomposition is run on all 28 chokepoints and the <code>total_count</code> remainder is\n' +
        '                stored. In the second pass, when fitting the Ridge model for any target series (chokepoint, region,\n' +
        '                country, or individual port), the remainders of geographically upstream &ldquo;feeder&rdquo;\n' +
        '                chokepoints are included as additional predictors.\n' +
        '            </p>\n' +
        '            <p>\n' +
        '                The geographic linkage map encodes domain knowledge about shipping network topology:\n' +
        '            </p>\n' +
        '            <ul>\n' +
        '                <li><strong>Persian Gulf &amp; Indian Ocean chain:</strong> Hormuz &rarr; Bab el-Mandeb &rarr; Suez/Cape</li>\n' +
        '                <li><strong>Asian trade routes:</strong> Hormuz + Malacca &rarr; East Asia, Southeast Asia, Indian Subcontinent</li>\n' +
        '                <li><strong>European routes:</strong> Suez + Gibraltar &rarr; Mediterranean, Dover &rarr; Northwest Europe</li>\n' +
        '                <li><strong>Americas:</strong> Panama + Yucatan &rarr; North America, Panama + Magellan &rarr; Latin America</li>\n' +
        '                <li><strong>Chokepoint-to-chokepoint:</strong> e.g., Malacca feeds Taiwan Strait, Korea Strait, Lombok, Sunda, Makassar</li>\n' +
        '            </ul>\n' +
        '            <p>\n' +
        '                For each feeder, two features are added: the contemporaneous remainder (at time t) and the 1-week lag\n' +
        '                (t&minus;1). Both are frozen at their last pre-crisis values post-crisis, identical to the AR lag\n' +
        '                treatment. Country-level models use the same map via ISO3 codes (e.g., CHN &rarr; Malacca + Taiwan\n' +
        '                Strait + Korea Strait).\n' +
        '            </p>\n' +
        '            <p>\n' +
        '                In A/B testing, adding cross-features improved the controls R&sup2; in <strong>96% of series</strong>\n' +
        '                across regions and countries, with a mean &Delta;R&sup2; of +0.08. The largest gains were in downstream\n' +
        '                regions furthest from the crisis origin (Northwest Europe: +0.35, East Asia: +0.25), where\n' +
        '                intermediary chokepoint flows carry the strongest propagation signal.\n' +
        '            </p>\n' +
        '\n' +
        '            <h3 style="color:#93c5fd; margin: 1.5rem 0 0.75rem 0; font-size: 1.1rem;">Deviation Calculation</h3>\n' +
        '            <p>\n' +
        '                The &ldquo;deviation&rdquo; reported throughout this dashboard measures the percentage gap between\n' +
        '                <em>what actually happened</em> and <em>what the model predicts would have happened absent the\n' +
        '                crisis</em>. The formula depends on the counterfactual value:\n' +
        '            </p>\n' +
        '            <ul>\n' +
        '                <li>If counterfactual &gt; 0:\n' +
        '                    <span style="font-family:monospace; color:#f9fafb;">deviation = (actual &minus; counterfactual) / counterfactual &times; 100%</span></li>\n' +
        '                <li>If counterfactual &le; 0 but pre-crisis average &gt; 0:\n' +
        '                    <span style="font-family:monospace; color:#f9fafb;">deviation = (actual &minus; counterfactual) / pre_crisis_avg &times; 100%</span></li>\n' +
        '                <li>Otherwise: deviation = 0 (no meaningful baseline)</li>\n' +
        '            </ul>\n' +
        '            <p>\n' +
        '                The &ldquo;pre-crisis average&rdquo; (pre_crisis_avg) is defined as the mean weekly value over the\n' +
        '                52 weeks immediately preceding the crisis onset. If fewer than 52 pre-crisis weeks are available,\n' +
        '                the full pre-crisis mean is used instead.\n' +
        '            </p>\n' +
        '            <p>\n' +
        '                A deviation of &minus;20% means the observed value is 20% below the counterfactual &mdash; i.e., 20%\n' +
        '                of expected activity is &ldquo;missing&rdquo; and attributed to the crisis. A positive deviation means\n' +
        '                activity exceeded the no-crisis baseline (e.g., rerouted cargo arriving at alternative ports).\n' +
        '                All deviations are capped at &plusmn;999% to prevent extreme values from distorting comparisons.\n' +
        '            </p>\n' +
        '            <p>\n' +
        '                All deviation figures in the summary tables and KPI cards refer to the <strong>most recent complete\n' +
        '                week</strong> of data, not an average over the full post-crisis period. This ensures the dashboard\n' +
        '                reflects the current state of disruption rather than being diluted by the crisis onset period (when\n' +
        '                ships already loaded or in transit could distort the picture). The time series charts show the full\n' +
        '                weekly trajectory for readers who want to see the evolution.\n' +
        '            </p>\n' +
        '            <p>\n' +
        '                Deviations are computed separately for each of five vessel types: tanker, container, dry bulk,\n' +
        '                general cargo, and RoRo. The vessel type toggle at the top of the dashboard controls which type is\n' +
        '                displayed; selecting multiple types shows aggregated port call results where actual and counterfactual\n' +
        '                values are summed across selected types before computing the deviation percentage. Capacity (deadweight\n' +
        '                tonnage) is used for chokepoint metrics; import/export tonnage is used for regional port groups.\n' +
        '            </p>\n' +
        '\n' +
        '            <h3 style="color:#93c5fd; margin: 1.5rem 0 0.75rem 0; font-size: 1.1rem;">Map Marker Filtering</h3>\n' +
        '            <p>\n' +
        '                The global deviation map suppresses markers for chokepoint&times;vessel-type combinations with\n' +
        '                negligible baseline traffic to prevent misleading extreme-percentage markers. Specifically:\n' +
        '            </p>\n' +
        '            <ul>\n' +
        '                <li><strong>Capacity markers:</strong> Suppressed if the pre-crisis average weekly capacity is below\n' +
        '                    1,000 deadweight tonnes (e.g., RoRo capacity through the Bering Strait).</li>\n' +
        '                <li><strong>Ship count markers:</strong> Suppressed if the pre-crisis average weekly count is below\n' +
        '                    2 ships (e.g., container vessels through the Ombai Strait).</li>\n' +
        '                <li><strong>Port markers:</strong> Suppressed if the pre-crisis average weekly tonnage is below\n' +
        '                    1,000 tonnes.</li>\n' +
        '            </ul>\n' +
        '            <p>\n' +
        '                When a vessel type has negligible traffic at a chokepoint, the multi-vessel-type aggregation treats\n' +
        '                it as zero (both actual and counterfactual), so it does not pollute the combined deviation for that\n' +
        '                location. This filtering only affects map markers &mdash; the KPI summary cards and detail tables\n' +
        '                continue to show all series.\n' +
        '            </p>\n' +
        '\n' +
        '            <h3 style="color:#93c5fd; margin: 1.5rem 0 0.75rem 0; font-size: 1.1rem;">Multi-Vessel-Type Port Aggregation</h3>\n' +
        '            <p>\n' +
        '                When multiple vessel types are selected, the dashboard aggregates port call data across those types.\n' +
        '                Each port&rsquo;s per-vessel-type modeled series (actual and counterfactual) is held separately;\n' +
        '                the aggregation sums actual values and counterfactual values across the selected types, then computes\n' +
        '                deviation as (sum_actual &minus; sum_cf) / |sum_cf| &times; 100%.\n' +
        '            </p>\n' +
        '            <p>\n' +
        '                Port-level models are initially run for the top 50 ports by volume for each vessel type independently.\n' +
        '                However, a port may rank in the top 50 for some vessel types but not others (e.g., Singapore is a\n' +
        '                top-50 tanker port but not a top-50 dry bulk port). To ensure accurate multi-type aggregation, the\n' +
        '                pipeline runs a second pass that models all missing port&times;vessel-type combinations using a\n' +
        '                tiered approach:\n' +
        '            </p>\n' +
        '            <ul>\n' +
        '                <li><strong>STL + Ridge</strong> (primary): If the port&rsquo;s weekly series for the missing vessel\n' +
        '                    type has &ge;104 weeks of data with &ge;30% non-zero values, the full STL decomposition and Ridge\n' +
        '                    regression pipeline is applied, identical to the top-50 methodology.</li>\n' +
        '                <li><strong>Naive baseline</strong> (fallback): If the series is too short or sparse for STL, a naive\n' +
        '                    calendar-week-average baseline is used as the counterfactual, with the deviation normalized by the\n' +
        '                    pre-crisis mean.</li>\n' +
        '                <li><strong>Pre-crisis average</strong> (last resort): If neither STL nor naive is viable (e.g., fewer\n' +
        '                    than 10 pre-crisis weeks of data), the pre-crisis weekly average is used as a flat counterfactual.</li>\n' +
        '            </ul>\n' +
        '            <p>\n' +
        '                This ensures that when you select all five vessel types, every port&rsquo;s total portcall count\n' +
        '                includes contributions from all vessel types it serves &mdash; not just the types where it ranked\n' +
        '                in the top 50.\n' +
        '            </p>\n' +
        '\n' +
        '            <h3 style="color:#93c5fd; margin: 1.5rem 0 0.75rem 0; font-size: 1.1rem;">Naive Baseline &amp; YoY/QoQ Comparisons</h3>\n' +
        '            <p>\n' +
        '                As a robustness check, a simple naive baseline is also computed for each series. It uses the average\n' +
        '                of the same ISO calendar weeks from 2023 onward as the counterfactual, and normalizes the deviation\n' +
        '                by the pre-crisis mean to avoid division-by-zero issues in sparse series.\n' +
        '            </p>\n' +
        '            <p>\n' +
        '                The detail tables also include two additional naive comparisons: <strong>vs 1Y ago</strong>\n' +
        '                (year-over-year, comparing the latest week to the value 52 weeks earlier) and <strong>vs 1Q ago</strong>\n' +
        '                (quarter-over-quarter, 13 weeks earlier). These simple look-back comparisons provide context\n' +
        '                independent of any model.\n' +
        '            </p>\n' +
        '\n' +
        '            <h3 style="color:#93c5fd; margin: 1.5rem 0 0.75rem 0; font-size: 1.1rem;">Signal-to-Noise Dimming</h3>\n' +
        '            <p>\n' +
        '                Not all deviations shown on the dashboard are statistically meaningful. Some series have high\n' +
        '                pre-crisis residual variance, meaning the model&rsquo;s fit is noisy and a large-looking deviation\n' +
        '                may fall within the range of normal fluctuation. To help distinguish genuine crisis signals from\n' +
        '                noise, the dashboard dims values that do not pass a significance threshold.\n' +
        '            </p>\n' +
        '            <p>\n' +
        '                <strong>How it works:</strong> For each series, the standard deviation (&sigma;) of the pre-crisis\n' +
        '                residuals (percentage deviations between actual and counterfactual during the training window) is\n' +
        '                computed. A deviation is considered significant if |deviation| &ge; <em>t</em>&sigma;, where\n' +
        '                <em>t</em> is the threshold set by the <strong>Noise filter</strong> slider in the info bar.\n' +
        '                Values below this threshold are displayed at reduced opacity (dimmed) in both the KPI summary\n' +
        '                cards and detail tables.\n' +
        '            </p>\n' +
        '            <p>\n' +
        '                <strong>Interactive threshold:</strong> The slider ranges from 0&sigma; to 4&sigma; in steps of\n' +
        '                0.5, defaulting to 2&sigma;. At 2&sigma;, roughly the top 5% of pre-crisis fluctuations would\n' +
        '                exceed the threshold under a normal distribution. Lower values (e.g. 1&sigma;) show more values\n' +
        '                as significant; higher values (e.g. 3&sigma;) are stricter and dim more aggressively. Setting the\n' +
        '                slider to 0 disables dimming entirely, showing all deviations at full opacity. Changes apply\n' +
        '                instantly to both the pre-rendered single-vessel panels and the dynamically aggregated\n' +
        '                multi-vessel panels.\n' +
        '            </p>\n' +
        '            <p>\n' +
        '                <strong>Multi-vessel-type mode:</strong> When multiple vessel types are selected, the dashboard\n' +
        '                aggregates actual and counterfactual values across the selected types before computing the deviation.\n' +
        '                The significance test, however, is applied per vessel type individually. A metric is dimmed in\n' +
        '                multi-vessel mode only if <em>all</em> selected vessel types are below the <em>t</em>&sigma;\n' +
        '                threshold for that metric. If any single vessel type shows a significant deviation, the aggregated\n' +
        '                value remains fully visible, since the signal from that vessel type is considered meaningful even\n' +
        '                if other types are noisy.\n' +
        '            </p>\n' +
        '            <p>\n' +
        '                <strong>Edge cases:</strong> Series with fewer than 20 pre-crisis observations, or where the\n' +
        '                pre-crisis residual standard deviation is near zero (&lt;0.01%), default to significant (undimmed),\n' +
        '                since there is insufficient history to estimate the noise band.\n' +
        '            </p>\n' +
        '\n' +
        '            <h3 style="color:#93c5fd; margin: 1.5rem 0 0.75rem 0; font-size: 1.1rem;">Regional Aggregates vs. Per-Port Estimates</h3>\n' +
        '            <p>\n' +
        '                The dashboard presents two levels of port analysis that are estimated independently and should not\n' +
        '                be expected to be arithmetically consistent. <strong>Regional port group aggregates</strong> (e.g.,\n' +
        '                &ldquo;East Asian Imports&rdquo;) sum all daily activity across every port in the region, then run a\n' +
        '                single STL decomposition and Ridge regression on that aggregate series. <strong>Per-port\n' +
        '                deviations</strong> run STL and Ridge independently on each individual port&rsquo;s weekly series\n' +
        '                &mdash; each port gets its own trend, seasonal pattern, and remainder regression. Summing the per-port\n' +
        '                counterfactuals would not reproduce the regional aggregate counterfactual because STL decomposition\n' +
        '                is nonlinear (the trend of a sum is not the sum of the trends). Additionally, the per-port tables show\n' +
        '                only the top ports by volume, omitting the long tail of smaller ports that are included in the\n' +
        '                aggregate. The two levels answer different questions: the aggregate captures total regional disruption,\n' +
        '                while the per-port breakdown identifies which individual ports are most affected.\n' +
        '            </p>\n' +
        '\n' +
        '            <h3 style="color:#93c5fd; margin: 1.5rem 0 0.75rem 0; font-size: 1.1rem;">Data Sources</h3>\n' +
        '            <ul>\n' +
        '                <li><strong>Vessel tracking:</strong> IMF PortWatch (AIS satellite data), daily vessel counts and\n' +
        '                    capacity for 28 chokepoints and ~2,000 ports globally, covering five vessel types.</li>\n' +
        '                <li><strong>Frozen monthly controls (14):</strong> Gulf/Hormuz oil production, OPEC total production,\n' +
        '                    world petroleum production (EIA); US industrial production; US capacity utilization; vehicle sales;\n' +
        '                    commercial &amp; industrial loans; IMF energy price index; IMF non-fuel commodity index;\n' +
        '                    China imports; China exports; consumer sentiment; retail sales; Global EPU index.</li>\n' +
        '                <li><strong>Frozen daily/weekly controls (8):</strong> USD broad index, federal funds rate, US EPU,\n' +
        '                    10-year breakeven inflation, VIX, yield curve (10Y&minus;2Y), 4-week average initial jobless\n' +
        '                    claims, weekly gasoline supplied.</li>\n' +
        '                <li><strong>Live daily controls (sensitivity model only, 6):</strong> Brent crude, WTI, Henry Hub\n' +
        '                    natural gas, high-yield credit spread, BDRY shipping index, FRO shipping index.</li>\n' +
        '                <li><strong>Fourier harmonics (6):</strong> Sine/cosine pairs at annual (52-week), semi-annual\n' +
        '                    (26-week), and quarterly (13-week) periods.</li>\n' +
        '                <li><strong>AR lags (2):</strong> Lag-1 and lag-2 of the STL remainder (frozen at last pre-crisis\n' +
        '                    values post-crisis).</li>\n' +
        '                <li><strong>Geographic cross-features (0&ndash;6):</strong> STL remainder (total vessel count) from\n' +
        '                    upstream feeder chokepoints, contemporaneous + 1-week lag, frozen post-crisis. Up to 3 feeders\n' +
        '                    per target (&times; 2 lags = up to 6 cross-features per model).</li>\n' +
        '            </ul>\n' +
        '\n' +
        '            <h3 style="color:#93c5fd; margin: 1.5rem 0 0.75rem 0; font-size: 1.1rem;">Key Parameters</h3>\n' +
        '            <ul>\n' +
        '                <li><strong>STL:</strong> period=52 weeks (annual), seasonal smoother window s=13 (quarterly),\n' +
        '                    robust=True. Estimated on pre-crisis data only in log space. Seasonal projected forward by\n' +
        '                    ISO-week matching (3-year average), smoothed with 5-week circular moving average\n' +
        '                    (half-window=2, wrapping at week 52&rarr;1 boundary).</li>\n' +
        '                <li><strong>Log-space:</strong> <code>log1p(x)</code> applied when all values &ge;0 and &ge;10% are\n' +
        '                    strictly positive. Counterfactual inverted via <code>expm1()</code> then clamped to &ge;0.</li>\n' +
        '                <li><strong>RidgeCV:</strong> Alpha selected per series via leave-one-out cross-validation\n' +
        '                    (candidates: 0.01, 0.1, 0.5, 1, 5, 10, 50, 100; R&sup2; scoring). ~30&ndash;36 features\n' +
        '                    (14 monthly + 8 daily + 6 Fourier + 2 AR + 0&ndash;6 cross), standardized (zero mean, unit\n' +
        '                    variance). Minimum 20 training observations required.</li>\n' +
        '                <li><strong>Trend extrapolation:</strong> 13-week linear fit (degree-1 polynomial) on the log-space\n' +
        '                    trend.</li>\n' +
        '                <li><strong>AR lags:</strong> 2 (frozen at last pre-crisis values post-crisis).</li>\n' +
        '                <li><strong>Crisis onset:</strong> February 28, 2026. Training window: June 2019 (after STL burn-in)\n' +
        '                    through February 27, 2026.</li>\n' +
        '                <li><strong>Weekly aggregation:</strong> Daily average within Monday-to-Sunday weeks, with separate\n' +
        '                    resampling at the crisis boundary to prevent blending pre- and post-crisis days. Weeks labeled by\n' +
        '                    Monday start date. Trailing weeks with fewer than 5 days of data are dropped.</li>\n' +
        '                <li><strong>Vessel types:</strong> Tanker, container, dry bulk, general cargo, and RoRo. Select one\n' +
        '                    for per-metric detail (ship count + capacity/tonnage); select multiple for aggregated port call\n' +
        '                    view.</li>\n' +
        '                <li><strong>Minimum data:</strong> 104 weeks (~2 years) and &ge;30% non-zero for STL; 52 pre-crisis\n' +
        '                    weeks for naive baseline; 10 weeks for pre-crisis-average fallback. Minimum 20 observations for\n' +
        '                    Ridge regression.</li>\n' +
        '                <li><strong>Port fill:</strong> Top 50 ports per vessel type receive full STL+Ridge models. Ports\n' +
        '                    appearing in any vessel type&rsquo;s top-50 list receive modeled counterfactuals for all other\n' +
        '                    vessel types via the STL &rarr; naive &rarr; pre-crisis-average fallback hierarchy.</li>\n' +
        '                <li><strong>Deviation cap:</strong> &plusmn;999%. Map markers additionally filtered by minimum\n' +
        '                    pre-crisis average traffic (capacity &ge;1,000 DWT; count &ge;2 ships/week; port tonnage\n' +
        '                    &ge;1,000).</li>\n' +
        '            </ul>\n' +
        '\n' +
        '            <h3 style="color:#93c5fd; margin: 1.5rem 0 0.75rem 0; font-size: 1.1rem;">Model Fit: Variance Decomposition</h3>\n' +
        '            <p>\n' +
        '                For each modeled series the pipeline computes a variance decomposition on the pre-crisis training\n' +
        '                window using sequential R&sup2;, displayed as a stacked bar beneath each chart. This measures how\n' +
        '                much of the training-period variance in the log-transformed series is explained at each stage:\n' +
        '                <strong>Trend</strong> (R&sup2; of trend alone vs. the original series),\n' +
        '                <strong>+Seasonal</strong> (R&sup2; of trend + STL seasonal),\n' +
        '                <strong>+Controls</strong> (R&sup2; of the full model including Ridge-predicted remainder from Fourier\n' +
        '                harmonics, macro controls, AR lags, and geographic cross-features),\n' +
        '                and <strong>Unexplained</strong> (1 &minus; full R&sup2;). Higher unexplained variance means the\n' +
        '                counterfactual has wider implicit confidence bands. When multiple vessel types are selected, the bar\n' +
        '                shows a weighted-average decomposition across vessel types, with weights proportional to each\n' +
        '                type&rsquo;s pre-crisis historical average level.\n' +
        '            </p>\n' +
        '\n' +
        '            <h4 style="color:#d1d5db; margin: 1rem 0 0.5rem 0; font-size: 0.95rem;">Full R&sup2; Distribution by Aggregation Level</h4>\n' +
        '            <table style="width:100%; border-collapse:collapse; font-size:0.8rem; color:#d1d5db; margin-bottom:1rem;">\n' +
        '                <thead><tr style="border-bottom:1px solid #374151;">\n' +
        '                    <th style="text-align:left;padding:0.3rem;">Level</th>\n' +
        '                    <th style="text-align:right;padding:0.3rem;">n</th>\n' +
        '                    <th style="text-align:right;padding:0.3rem;">p10</th>\n' +
        '                    <th style="text-align:right;padding:0.3rem;">p25</th>\n' +
        '                    <th style="text-align:right;padding:0.3rem;">Median</th>\n' +
        '                    <th style="text-align:right;padding:0.3rem;">p75</th>\n' +
        '                    <th style="text-align:right;padding:0.3rem;">p90</th>\n' +
        '                    <th style="text-align:right;padding:0.3rem;">R&sup2;&lt;0.3</th>\n' +
        '                    <th style="text-align:right;padding:0.3rem;">R&sup2;&lt;0.5</th>\n' +
        '                </tr></thead>\n' +
        '                <tbody>\n' +
        '                    <tr><td style="padding:0.3rem;">Chokepoints</td><td style="text-align:right;padding:0.3rem;">308</td><td style="text-align:right;padding:0.3rem;">0.24</td><td style="text-align:right;padding:0.3rem;">0.35</td><td style="text-align:right;padding:0.3rem;">0.49</td><td style="text-align:right;padding:0.3rem;">0.65</td><td style="text-align:right;padding:0.3rem;">0.82</td><td style="text-align:right;padding:0.3rem;">17%</td><td style="text-align:right;padding:0.3rem;">52%</td></tr>\n' +
        '                    <tr><td style="padding:0.3rem;">Regional Aggregates</td><td style="text-align:right;padding:0.3rem;">198</td><td style="text-align:right;padding:0.3rem;">0.37</td><td style="text-align:right;padding:0.3rem;">0.48</td><td style="text-align:right;padding:0.3rem;">0.63</td><td style="text-align:right;padding:0.3rem;">0.76</td><td style="text-align:right;padding:0.3rem;">0.84</td><td style="text-align:right;padding:0.3rem;">4%</td><td style="text-align:right;padding:0.3rem;">29%</td></tr>\n' +
        '                    <tr><td style="padding:0.3rem;">Countries</td><td style="text-align:right;padding:0.3rem;">1,100</td><td style="text-align:right;padding:0.3rem;">0.25</td><td style="text-align:right;padding:0.3rem;">0.34</td><td style="text-align:right;padding:0.3rem;">0.47</td><td style="text-align:right;padding:0.3rem;">0.63</td><td style="text-align:right;padding:0.3rem;">0.74</td><td style="text-align:right;padding:0.3rem;">16%</td><td style="text-align:right;padding:0.3rem;">56%</td></tr>\n' +
        '                </tbody>\n' +
        '            </table>\n' +
        '            <p style="font-size:0.8rem; color:#9ca3af; margin-top:-0.5rem;">\n' +
        '                Note: R&sup2; statistics cover only series with full STL+Ridge models (variance_decomp available).\n' +
        '                Port-level series using the naive or pre-crisis-average fallback do not produce a variance\n' +
        '                decomposition and are excluded from this table.\n' +
        '            </p>\n' +
        '\n' +
        '            <p>\n' +
        '                Fit varies systematically by metric type and aggregation level. Port-call series fit substantially\n' +
        '                better than tonnage series because tonnage depends on vessel size mix, which introduces high-frequency\n' +
        '                variance that no trend+seasonal model can explain. Aggregated series (regional, median R&sup2;=0.63;\n' +
        '                country, median 0.47) fit better than individual chokepoint-level metrics (median 0.49) because\n' +
        '                aggregation smooths idiosyncratic noise. The controls (macro variables, Fourier harmonics, AR lags,\n' +
        '                and geographic cross-features) contribute a median of ~5&ndash;7pp of marginal R&sup2; on top of\n' +
        '                trend and seasonal components, with the geographic cross-features contributing the largest gains for\n' +
        '                downstream entities (mean &Delta;R&sup2;=+0.08 for regions and countries).\n' +
        '            </p>\n' +
        '\n' +
        '        </div>\n' +
        '        </div><!-- /view-methodology -->\n' +
        '    </div>\n' +
        '<script>\n' +
        '// ── Export CSV from chart row ──\n' +
        'function exportChartCSV(btn) {\n' +
        '    var td = btn.closest("td");\n' +
        '    var script = td.querySelector("script.chart-data");\n' +
        '    if (!script) return;\n' +
        '    var d = JSON.parse(script.textContent);\n' +
        '    var label = (d.label || "series").replace(/[^a-zA-Z0-9_\\- ]/g, "").replace(/\\s+/g, "_");\n' +
        '    var rows = ["date,actual,counterfactual"];\n' +
        '    for (var i = 0; i < d.dates.length; i++) {\n' +
        '        var a = d.actual[i] != null ? d.actual[i] : "";\n' +
        '        var c = d.cf[i] != null ? d.cf[i] : "";\n' +
        '        rows.push(d.dates[i] + "," + a + "," + c);\n' +
        '    }\n' +
        '    var csv = rows.join("\\n");\n' +
        '    var blob = new Blob([csv], {type: "text/csv"});\n' +
        '    var url = URL.createObjectURL(blob);\n' +
        '    var link = document.createElement("a");\n' +
        '    link.href = url;\n' +
        '    link.download = label + ".csv";\n' +
        '    link.click();\n' +
        '    URL.revokeObjectURL(url);\n' +
        '}\n' +
        '// ── R² Variance Decomposition Bar ──\n' +
        'function renderVdBar(containerId, vd) {\n' +
        '    var el = document.getElementById(containerId);\n' +
        '    if (!el || !vd) return;\n' +
        '    var trend = Math.round((vd.r2_trend || 0) * 100);\n' +
        '    var seasonal = Math.round(((vd.r2_trend_seasonal || 0) - (vd.r2_trend || 0)) * 100);\n' +
        '    var controls = Math.round((vd.r2_controls_marginal || 0) * 100);\n' +
        '    var unexplained = Math.round((vd.r2_unexplained || 0) * 100);\n' +
        '    // Clamp to ensure segments add to 100\n' +
        '    var total = trend + seasonal + controls + unexplained;\n' +
        '    if (total !== 100 && total > 0) { unexplained += (100 - total); }\n' +
        '    var fullR2 = ((vd.r2_full || 0) * 100).toFixed(0);\n' +
        '    var segs = [\n' +
        '        {w: trend, c: "#3b82f6", l: "Trend " + trend + "%"},\n' +
        '        {w: seasonal, c: "#06b6d4", l: "Seasonal " + seasonal + "%"},\n' +
        '        {w: controls, c: "#f59e0b", l: "Controls " + controls + "%"},\n' +
        '        {w: unexplained, c: "#374151", l: "Unexplained " + unexplained + "%"}\n' +
        '    ];\n' +
        '    var bar = \'<div class="vd-bar-label">Training R\\u00b2: \' + fullR2 + \'%</div><div class="vd-bar">\';\n' +
        '    segs.forEach(function(s) {\n' +
        '        if (s.w > 0) bar += \'<div class="vd-seg" style="width:\' + s.w + \'%;background:\' + s.c + \'" title="\' + s.l + \'"></div>\';\n' +
        '    });\n' +
        '    bar += \'</div><div class="vd-legend">\';\n' +
        '    segs.forEach(function(s) {\n' +
        '        bar += \'<span class="vd-legend-item"><span class="vd-dot" style="background:\' + s.c + \'"></span>\' + s.l + \'</span>\';\n' +
        '    });\n' +
        '    bar += "</div>";\n' +
        '    el.innerHTML = bar;\n' +
        '}\n' +
        '// ── Dashboard / Methodology view toggle ──\n' +
        '(function() {\n' +
        '    document.querySelectorAll(".view-toggle-btn").forEach(function(btn) {\n' +
        '        btn.addEventListener("click", function() {\n' +
        '            var view = btn.getAttribute("data-view");\n' +
        '            document.querySelectorAll(".view-toggle-btn").forEach(function(b) { b.classList.remove("active"); });\n' +
        '            btn.classList.add("active");\n' +
        '            if (view === "methodology") {\n' +
        '                document.body.classList.add("show-methodology");\n' +
        '            } else {\n' +
        '                document.body.classList.remove("show-methodology");\n' +
        '            }\n' +
        '        });\n' +
        '    });\n' +
        '})();\n' +
        '(function() {\n' +
        '    document.querySelectorAll("table").forEach(function(table) {\n' +
        '        var headers = table.querySelectorAll("th");\n' +
        '        headers.forEach(function(th, colIdx) {\n' +
        '            th.addEventListener("click", function() {\n' +
        '                var tbody = table.querySelector("tbody");\n' +
        '                if (!tbody) return;\n' +
        '                var rows = Array.from(tbody.querySelectorAll("tr.expandable-row"));\n' +
        '                if (rows.length === 0) rows = Array.from(tbody.querySelectorAll("tr:not(.chart-row)"));\n' +
        '                var asc = !th.classList.contains("sort-asc");\n' +
        '                headers.forEach(function(h) { h.classList.remove("sort-asc", "sort-desc"); });\n' +
        '                th.classList.add(asc ? "sort-asc" : "sort-desc");\n' +
        '                function parseSuffix(s) {\n' +
        '                    s = s.replace(/[,%]/g, "").trim();\n' +
        '                    var m = s.match(/^([\\-]?[0-9.]+)\\s*([KMBkmb]?)$/);\n' +
        '                    if (!m) return NaN;\n' +
        '                    var num = parseFloat(m[1]);\n' +
        '                    var suffix = m[2].toUpperCase();\n' +
        '                    if (suffix === "K") num *= 1e3;\n' +
        '                    else if (suffix === "M") num *= 1e6;\n' +
        '                    else if (suffix === "B") num *= 1e9;\n' +
        '                    return num;\n' +
        '                }\n' +
        '                rows.sort(function(a, b) {\n' +
        '                    var cellA = a.children[colIdx];\n' +
        '                    var cellB = b.children[colIdx];\n' +
        '                    if (!cellA || !cellB) return 0;\n' +
        '                    var tA = cellA.textContent.trim();\n' +
        '                    var tB = cellB.textContent.trim();\n' +
        '                    var nA = parseSuffix(tA);\n' +
        '                    var nB = parseSuffix(tB);\n' +
        '                    if (!isNaN(nA) && !isNaN(nB)) {\n' +
        '                        return asc ? nA - nB : nB - nA;\n' +
        '                    }\n' +
        '                    if (tA === "\\u2014") return 1;\n' +
        '                    if (tB === "\\u2014") return -1;\n' +
        '                    return asc ? tA.localeCompare(tB) : tB.localeCompare(tA);\n' +
        '                });\n' +
        '                rows.forEach(function(r) {\n' +
        '                    tbody.appendChild(r);\n' +
        '                    // Re-append the chart-row sibling if it exists\n' +
        '                    var chartRowId = r.getAttribute("data-target");\n' +
        '                    if (chartRowId) {\n' +
        '                        var chartRow = document.getElementById(chartRowId);\n' +
        '                        if (chartRow) tbody.appendChild(chartRow);\n' +
        '                    }\n' +
        '                });\n' +
        '            });\n' +
        '        });\n' +
        '    });\n' +
        '})();\n' +
        '\n' +
        '// ── Shared chart creation ──\n' +
        'var chartInstances = {};\n' +
        'var chartZoomState = {};  // targetId -> true if zoomed in (3 months)\n' +
        'function createInlineChart(targetId, cd, zoomedIn) {\n' +
        '    // Destroy existing chart if any\n' +
        '    if (chartInstances[targetId]) { chartInstances[targetId].destroy(); delete chartInstances[targetId]; }\n' +
        '    var canvas = document.getElementById("canvas_" + targetId);\n' +
        '    if (!canvas) return;\n' +
        '    var dates, actual, cf;\n' +
        '    if (zoomedIn) {\n' +
        '        // Trim to ~3 months (13 weeks) before crisis\n' +
        '        var crisisDate = cd.crisis;\n' +
        '        var d = new Date(crisisDate);\n' +
        '        d.setDate(d.getDate() - 91);\n' +
        '        var startDate = d.toISOString().slice(0, 10);\n' +
        '        var startIdx = 0;\n' +
        '        for (var i = 0; i < cd.dates.length; i++) {\n' +
        '            if (cd.dates[i] >= startDate) { startIdx = i; break; }\n' +
        '        }\n' +
        '        dates = cd.dates.slice(startIdx);\n' +
        '        actual = cd.actual.slice(startIdx);\n' +
        '        cf = cd.cf.slice(startIdx);\n' +
        '    } else {\n' +
        '        // Full ~52 weeks pre-crisis + post-crisis\n' +
        '        dates = cd.dates;\n' +
        '        actual = cd.actual;\n' +
        '        cf = cd.cf;\n' +
        '    }\n' +
        '    var crisisIdx = dates.indexOf(cd.crisis);\n' +
        '    if (crisisIdx < 0) {\n' +
        '        for (var i = 0; i < dates.length; i++) {\n' +
        '            if (dates[i] >= cd.crisis) { crisisIdx = i; break; }\n' +
        '        }\n' +
        '    }\n' +
        '    chartInstances[targetId] = new Chart(canvas, {\n' +
        '        type: "line",\n' +
        '        data: {\n' +
        '            labels: dates,\n' +
        '            datasets: [\n' +
        '                { label: "Actual", data: actual, borderColor: "#3b82f6", backgroundColor: "rgba(59,130,246,0.1)", borderWidth: 1.5, pointRadius: 0, fill: false, tension: 0 },\n' +
        '                { label: "Counterfactual", data: cf, borderColor: "#f59e0b", borderDash: [5,3], backgroundColor: "transparent", borderWidth: 1.5, pointRadius: 0, fill: false, tension: 0 }\n' +
        '            ]\n' +
        '        },\n' +
        '        options: {\n' +
        '            responsive: true, maintainAspectRatio: false,\n' +
        '            plugins: {\n' +
        '                legend: { display: true, position: "top", labels: { color: "#9ca3af", font: { size: 10 }, boxWidth: 12, padding: 8 } },\n' +
        '                title: { display: true, text: cd.label, color: "#e5e7eb", font: { size: 12 } },\n' +
        '                annotation: { annotations: crisisIdx >= 0 ? { crisisLine: { type: "line", xMin: crisisIdx, xMax: crisisIdx, borderColor: "#ef4444", borderWidth: 1.5, borderDash: [4,3], label: { display: true, content: "Crisis", color: "#ef4444", font: { size: 9 }, position: "start" } } } : {} }\n' +
        '            },\n' +
        '            scales: {\n' +
        '                x: { ticks: { color: "#6b7280", maxTicksLimit: zoomedIn ? 8 : 12, font: { size: 9 } }, grid: { color: "#1f2937" } },\n' +
        '                y: { ticks: { color: "#6b7280", font: { size: 9 } }, grid: { color: "#1f2937" } }\n' +
        '            }\n' +
        '        }\n' +
        '    });\n' +
        '    // Render R² variance decomposition bar\n' +
        '    if (cd.vd) renderVdBar("vd_" + targetId, cd.vd);\n' +
        '}\n' +
        'function toggleChartZoom(btn) {\n' +
        '    var td = btn.closest("td");\n' +
        '    var chartRow = td.closest("tr.chart-row");\n' +
        '    if (!chartRow) return;\n' +
        '    var targetId = chartRow.id;\n' +
        '    var dataScript = td.querySelector("script.chart-data");\n' +
        '    if (!dataScript) return;\n' +
        '    var cd;\n' +
        '    try { cd = JSON.parse(dataScript.textContent); } catch(e) { return; }\n' +
        '    var zoomedIn = !chartZoomState[targetId];\n' +
        '    chartZoomState[targetId] = zoomedIn;\n' +
        '    createInlineChart(targetId, cd, zoomedIn);\n' +
        '    btn.textContent = zoomedIn ? "Zoom Out" : "Zoom In";\n' +
        '    btn.title = zoomedIn ? "Show full 52-week view" : "Zoom in to recent 3 months";\n' +
        '}\n' +
        '\n' +
        '// ── Expandable row click-to-chart ──\n' +
        '(function() {\n' +
        '    window.chartInstances = chartInstances;\n' +
        '    document.querySelectorAll(".expandable-row").forEach(function(row) {\n' +
        '        row.addEventListener("click", function() {\n' +
        '            var targetId = row.getAttribute("data-target");\n' +
        '            if (!targetId) return;\n' +
        '            var chartRow = document.getElementById(targetId);\n' +
        '            if (!chartRow) return;\n' +
        '            var isOpen = chartRow.style.display !== "none";\n' +
        '            if (isOpen) {\n' +
        '                chartRow.style.display = "none";\n' +
        '                row.classList.remove("expanded");\n' +
        '                return;\n' +
        '            }\n' +
        '            chartRow.style.display = "table-row";\n' +
        '            row.classList.add("expanded");\n' +
        '            if (chartInstances[targetId]) return;\n' +
        '            var dataScript = chartRow.querySelector(".chart-data");\n' +
        '            if (!dataScript) return;\n' +
        '            var cd;\n' +
        '            try { cd = JSON.parse(dataScript.textContent); } catch(e) { return; }\n' +
        '            createInlineChart(targetId, cd, false);\n' +
        '        });\n' +
        '    });\n' +
        '\n' +
        '    window.bindMultiExpandableRows = function() {\n' +
        '        document.querySelectorAll(".expandable-row.multi-expandable").forEach(function(row) {\n' +
        '            if (row._multiHandlerBound) return;\n' +
        '            row._multiHandlerBound = true;\n' +
        '            row.addEventListener("click", function() {\n' +
        '                var targetId = row.getAttribute("data-target");\n' +
        '                if (!targetId) return;\n' +
        '                var chartRow = document.getElementById(targetId);\n' +
        '                if (!chartRow) return;\n' +
        '                var isOpen = chartRow.style.display !== "none";\n' +
        '                if (isOpen) {\n' +
        '                    chartRow.style.display = "none";\n' +
        '                    row.classList.remove("expanded");\n' +
        '                    return;\n' +
        '                }\n' +
        '                chartRow.style.display = "table-row";\n' +
        '                row.classList.add("expanded");\n' +
        '                if (window.chartInstances[targetId]) return;\n' +
        '                var dataScript = chartRow.querySelector(".chart-data");\n' +
        '                if (!dataScript) return;\n' +
        '                var cd;\n' +
        '                try { cd = JSON.parse(dataScript.textContent); } catch(e) { return; }\n' +
        '                createInlineChart(targetId, cd, false);\n' +
        '            });\n' +
        '        });\n' +
        '    };\n' +
        '\n' +
        '})();\n' +
        '</script>\n' +
        '    </div><!-- /dashboard-content -->\n' +
        '</body>\n' +
        '</html>'
    )

    with open(output_path, 'w') as f:
        f.write(html_content)


def _build_seasonal_panel(data: Dict[str, Any], s_val: int) -> tuple:
    """Build tables for one seasonal setting, with vessel type toggle.

    Returns (panel_html, kpi_dict) where kpi_dict maps vessel_type -> kpi_html.
    KPI cards are returned separately so they can be placed above the map.
    """
    suffix = f'_s{s_val}'

    # All vessel types to build panels for
    VESSEL_DEFS = [
        ('tanker',        'Tanker',        True),
        ('container',     'Container',     True),
        ('dry_bulk',      'Dry Bulk',      True),
        ('general_cargo', 'General Cargo', True),
        ('roro',          'RoRo',          True),
    ]
    vessel_types = [vd[0] for vd in VESSEL_DEFS]
    vessel_panels = {}

    std_thead = '                <thead><tr><th>Chokepoint</th><th>Hist. Avg</th><th>Latest</th><th>Counterfactual</th><th>Deviation</th><th>vs 1Y ago</th><th>vs 1Q ago</th></tr></thead>\n'
    reg_thead = '                <thead><tr><th>Region</th><th>Hist. Avg</th><th>Latest</th><th>Counterfactual</th><th>Deviation</th><th>vs 1Y ago</th><th>vs 1Q ago</th></tr></thead>\n'

    def _table_section(title, thead, rows):
        # Tag tonnage/capacity sections so JS can hide them in multi-select mode
        is_tonnage = 'Tonnage' in title or 'Capacity' in title
        metric_attr = ' data-metric-type="tonnage"' if is_tonnage else ' data-metric-type="counts"'
        return (
            '        <div class="table-section"' + metric_attr + '>\n' +
            '            <h2>' + title + '</h2>\n' +
            '            <table>\n' + thead +
            '                <tbody>\n' + rows + '\n                </tbody>\n' +
            '            </table>\n' +
            '        </div>\n'
        )

    for vessel_type, vt_label, has_tonnage in VESSEL_DEFS:
        kpis, kpi_sig, kpi_sp, kpi_ts = get_kpi_values(data, vessel_type=vessel_type)
        id_sfx = suffix + f'_{vessel_type}'

        # KPI cards
        kpi_html = build_kpi_html(kpis, vessel_type, vt_label, sig=kpi_sig, sig_params=kpi_sp, kpi_ts=kpi_ts)

        # Build four separate table groups: chokepoints, regional, top ports, countries
        chokepoint_html = ''
        regional_html = ''
        topport_html = ''
        country_html = ''

        if has_tonnage:
            # Chokepoint tables: count first, then capacity
            cp_count_rows = build_chokepoint_table(data, vessel_type=vessel_type, metric_type='count', id_suffix=id_sfx)
            cp_capacity_rows = build_chokepoint_table(data, vessel_type=vessel_type, metric_type='capacity', id_suffix=id_sfx)
            chokepoint_html += _table_section(f'Chokepoint {vt_label} — Count Deviation (vessels)', std_thead, cp_count_rows)
            chokepoint_html += _table_section(f'Chokepoint {vt_label} — Capacity Deviation (DWT)', std_thead, cp_capacity_rows)

            # Regional tables: port calls first, then tonnage
            pg_tonnage_exp, pg_tonnage_imp = build_port_group_tables(data, vessel_type=vessel_type, metric_type='tonnage', id_suffix=id_sfx)
            pg_calls_exp, _pg_calls_imp = build_port_group_tables(data, vessel_type=vessel_type, metric_type='calls', id_suffix=id_sfx)
            if pg_calls_exp:
                regional_html += _table_section(f'Regional {vt_label} — Port Calls Deviation', reg_thead, pg_calls_exp)
            regional_html += _table_section(f'Regional {vt_label} Exports — Tonnage Deviation (DWT)', reg_thead, pg_tonnage_exp)
            regional_html += _table_section(f'Regional {vt_label} Imports — Tonnage Deviation (DWT)', reg_thead, pg_tonnage_imp)

            # Country-level tables: port calls first, then tonnage
            ct_tonnage_exp, ct_tonnage_imp = build_country_tables(data, vessel_type=vessel_type, metric_type='tonnage', id_suffix=id_sfx)
            ct_calls_exp, _ct_calls_imp = build_country_tables(data, vessel_type=vessel_type, metric_type='calls', id_suffix=id_sfx)
            if ct_calls_exp:
                country_html += _table_section(f'Country {vt_label} — Port Calls Deviation', reg_thead, ct_calls_exp)
            country_html += _table_section(f'Country {vt_label} Exports — Tonnage Deviation (DWT)', reg_thead, ct_tonnage_exp)
            country_html += _table_section(f'Country {vt_label} Imports — Tonnage Deviation (DWT)', reg_thead, ct_tonnage_imp)

            # Top-50 port tables: port calls first, then tonnage
            top50_keys = []
            if vessel_type == 'tanker':
                top50_keys = [
                    (f'_top50_portcalls_{vessel_type}_ports', f'Top {vt_label} Ports — Port Calls Deviation'),
                    ('_top50_export_ports', f'Top {vt_label} Export Ports — Tonnage Deviation (DWT)'),
                    ('_top50_import_ports', f'Top {vt_label} Import Ports — Tonnage Deviation (DWT)'),
                ]
            else:
                top50_keys = [
                    (f'_top50_portcalls_{vessel_type}_ports', f'Top {vt_label} Ports — Port Calls Deviation'),
                    (f'_top50_export_{vessel_type}_ports', f'Top {vt_label} Export Ports — Tonnage Deviation (DWT)'),
                    (f'_top50_import_{vessel_type}_ports', f'Top {vt_label} Import Ports — Tonnage Deviation (DWT)'),
                ]

        for data_key, title in top50_keys:
            table = build_top_port_table(data, data_key, title, id_suffix=id_sfx)
            if table:
                topport_html += table + '\n'

        # Combine into section-toggled layout
        tables_html = (
            '        <div class="table-group table-group-ports active">\n' + topport_html + '        </div>\n' +
            '        <div class="table-group table-group-regional">\n' + regional_html + '        </div>\n' +
            '        <div class="table-group table-group-countries">\n' + country_html + '        </div>\n' +
            '        <div class="table-group table-group-chokepoints">\n' + chokepoint_html + '        </div>\n'
        )

        vessel_panels[vessel_type] = {
            'kpi_html': kpi_html,
            'tables_html': tables_html
        }

    # Build vessel-specific panels (no vessel toggle here — it's at the top level)
    # KPI cards are placed separately above the map; only tables go here
    vessel_content = ''
    for vessel_type in vessel_types:
        vessel_content += (
            '        <div class="vessel-panel" data-vessel="' + vessel_type + '">\n' +
            vessel_panels[vessel_type]['tables_html'] +
            '        </div>\n'
        )

    # Add a hidden "multi" vessel panel for dynamically aggregated tables
    vessel_content += (
        '        <div class="vessel-panel" data-vessel="multi">\n'
        '        <div class="table-group table-group-ports active" id="multiTablePorts_s' + str(s_val) + '"></div>\n'
        '        <div class="table-group table-group-regional" id="multiTableRegional_s' + str(s_val) + '"></div>\n'
        '        <div class="table-group table-group-countries" id="multiTableCountries_s' + str(s_val) + '"></div>\n'
        '        <div class="table-group table-group-chokepoints" id="multiTableChokepoints_s' + str(s_val) + '"></div>\n'
        '        </div>\n'
    )

    # Collect KPI HTML per vessel type for top-level placement
    kpi_dict = {vt: vessel_panels[vt]['kpi_html'] for vt in vessel_types}

    panel_html = (
        '    <div class="seasonal-panel" data-seasonal="' + str(s_val) + '">\n' +
        vessel_content +
        '    </div>\n'
    )
    return panel_html, kpi_dict


def build_html_multi(all_data: Dict[int, Dict[str, Any]], output_path: str) -> None:
    """Build dashboard with seasonal toggle from multiple data sets."""
    # Use the first available dataset for shared elements (map, Hormuz chart, methodology)
    default_s = 13 if 13 in all_data else list(all_data.keys())[0]
    data_default = all_data[default_s]

    port_map = build_leaflet_map(data_default)
    hormuz_main = build_hormuz_main_chart(data_default)

    # Extract the deviation reference period from the data
    # Find the latest week date and crisis onset from any entry
    _crisis_date_str = ''
    _latest_week_str = ''
    for _key, _entry in data_default.items():
        if isinstance(_entry, dict) and 'dates' in _entry and 'crisis_date' in _entry:
            _crisis_date_str = _entry['crisis_date']
            _latest_week_str = _entry['dates'][-1] if _entry['dates'] else ''
            break
    # Format the dates nicely
    from datetime import datetime, timedelta
    if _latest_week_str:
        _week_start = datetime.strptime(_latest_week_str, '%Y-%m-%d')
        _week_end = _week_start + timedelta(days=6)
        _week_label = f'{_week_start.strftime("%b %d")} – {_week_end.strftime("%b %d, %Y")}'
    else:
        _week_label = 'N/A'
    if _crisis_date_str:
        _crisis_dt = datetime.strptime(_crisis_date_str, '%Y-%m-%d')
        _crisis_label = _crisis_dt.strftime('%b %d, %Y')
    else:
        _crisis_label = 'N/A'

    # Compute post-crisis dates for week slider
    _post_crisis_dates = []
    for _key, _entry in data_default.items():
        if isinstance(_entry, dict) and 'dates' in _entry and 'crisis_date' in _entry:
            _all_dates = _entry['dates']
            _cd = _entry['crisis_date']
            _post_crisis_dates = [d for d in _all_dates if d >= _cd]
            break

    # Build seasonal panels and collect KPI data
    seasonal_panels = ''
    all_kpi_data = {}  # {s_val: {vessel_type: kpi_html}}
    for s_val in sorted(all_data.keys()):
        panel_html, kpi_dict = _build_seasonal_panel(all_data[s_val], s_val)
        seasonal_panels += panel_html
        all_kpi_data[s_val] = kpi_dict

    # Build top-level KPI section (above the map, responsive to both toggles)
    kpi_section = ''
    for s_val in sorted(all_kpi_data.keys()):
        active_s = ' active' if s_val == default_s else ''
        s_content = ''
        for vt in ['tanker', 'container', 'dry_bulk', 'general_cargo', 'roro']:
            if vt not in all_kpi_data[s_val]:
                continue
            s_content += (
                '        <div class="vessel-panel" data-vessel="' + vt + '">\n' +
                all_kpi_data[s_val][vt] + '\n' +
                '        </div>\n'
            )
        # Add a hidden "multi" vessel panel for dynamic JS-populated aggregated KPI
        s_content += (
            '        <div class="vessel-panel" data-vessel="multi">\n'
            '        <div id="multiKpiGrid_s' + str(s_val) + '">\n'
            '            <div class="kpi-grid"><div class="kpi-card"><div class="kpi-label">Loading...</div></div></div>\n'
            '        </div>\n'
            '        </div>\n'
        )
        kpi_section += (
            '    <div class="seasonal-panel' + active_s + '" data-seasonal="' + str(s_val) + '">\n' +
            s_content +
            '    </div>\n'
        )

    # Toggle buttons — vessel type + seasonal, combined in one bar at the top
    toggle_html = '        <div class="top-toggles">\n'
    toggle_html += '        <div class="vessel-toggle">\n'
    toggle_html += '            <span class="toggle-label">Vessel Type:</span>\n'
    toggle_html += '            <button class="toggle-btn vessel-btn active" data-vessel="tanker">Tanker</button>\n'
    toggle_html += '            <button class="toggle-btn vessel-btn active" data-vessel="container">Container</button>\n'
    toggle_html += '            <button class="toggle-btn vessel-btn active" data-vessel="dry_bulk">Dry Bulk</button>\n'
    toggle_html += '            <button class="toggle-btn vessel-btn active" data-vessel="general_cargo">General Cargo</button>\n'
    toggle_html += '            <button class="toggle-btn vessel-btn active" data-vessel="roro">RoRo</button>\n'
    # "Total" removed — viewer selects all five types for aggregate view
    toggle_html += '        </div>\n'
    # Seasonal toggle removed — using fixed s=13 (quarterly smoothing window)
    toggle_html += '        </div>\n'

    # Time period banner
    period_html = (
        '        <div class="period-banner">'
        '<span class="period-item"><strong>Crisis onset:</strong> ' + _crisis_label + '</span>'
        '<span class="period-sep">|</span>'
        '<span class="period-item" style="display:flex;align-items:center;gap:0.5rem;">'
        '<span style="font-weight:600;color:#e2e8f0;">Week:</span> '
        '<input type="range" id="weekSlider" min="0" max="' + str(len(_post_crisis_dates) - 1) + '" step="1" value="' + str(len(_post_crisis_dates) - 1) + '" '
        'style="width:90px;accent-color:#f59e0b;cursor:pointer;" '
        'oninput="var d=window._postCrisisDates;document.getElementById(\'weekLabel\').textContent=d?d[this.value]:\'\';window._selectedWeekIdx=parseInt(this.value);window._recomputeWeek&&window._recomputeWeek();">'
        ' <span id="weekLabel" style="font-weight:700;color:#fbbf24;min-width:5em;text-align:center;font-size:0.8rem;">' + _post_crisis_dates[-1] + '</span>'
        '</span>'
        '<span class="period-sep">|</span>'
        '<span class="period-item">Deviation = % gap between observed and counterfactual (no-crisis) for the selected week</span>'
        '<span class="period-sep">|</span>'
        '<span class="period-item" style="display:flex;align-items:center;gap:0.5rem;">'
        '<span style="font-weight:600;color:#e2e8f0;">Noise filter:</span> '
        '<input type="range" id="sigmaSlider" min="0" max="4" step="0.5" value="2" '
        'style="width:90px;accent-color:#3b82f6;cursor:pointer;" '
        'oninput="document.getElementById(\'sigmaLabel\').textContent=this.value; window._recomputeDimming && window._recomputeDimming();">'
        ' <span id="sigmaLabel" style="font-weight:700;color:#93c5fd;min-width:1.5em;text-align:center;">2</span>&sigma;'
        '</span>'
        '<span class="period-sep">|</span>'
        '<span class="period-item" style="opacity:0.6;font-style:italic;font-size:0.72rem;">'
        'Each series has its own &sigma; (std. dev. of pre-crisis model residuals). '
        'Deviations smaller than the slider threshold &times; &sigma; are dimmed as likely noise. '
        'Slide right to be stricter, left to show more. 0 = no dimming.'
        '</span>'
        '</div>\n'
    )

    # Read the CSS and JS from the original build_html
    # We need to get the full style block — call build_html's style portion
    # Instead of duplicating, we extract the style from a temporary build
    # Actually, let's just build the HTML directly using the style from build_html

    # Get style content from the original function's structure
    # We'll read the file itself to grab it... or just inline the additions

    # Build full HTML using the same style as build_html plus seasonal toggle CSS/JS
    html = build_html.__code__  # can't do this; let's just build from scratch using original as base

    # Call original build_html to get the base HTML, then splice in the toggle
    # Actually the cleanest: build with default data, then inject panels + toggle

    # Let me just reconstruct the HTML properly
    from io import StringIO

    # Get the CSS and boilerplate from build_html by generating with default data
    # and then replacing the body content. This is hacky but avoids duplicating 800 lines of CSS.
    tmp_path = '/tmp/_nowcast_dashboard_tmp.html'
    build_html(data_default, tmp_path)
    with open(tmp_path, 'r') as f:
        base_html = f.read()
    os.remove(tmp_path)

    # Insert the toggle CSS before </style>
    style_insert = (
        '        .top-toggles { display: flex; flex-wrap: wrap; gap: 0.75rem; margin-bottom: 1.5rem; align-items: stretch; }\n'
        '        .seasonal-toggle, .vessel-toggle { display: flex; align-items: center; gap: 0.5rem; '
        'background: linear-gradient(135deg, #1f2937, #111827); padding: 0.75rem 1rem; border-radius: 0.5rem; '
        'border: 1px solid #374151; box-sizing: border-box; }\n'
        '        .toggle-label { color: #9ca3af; font-size: 0.85rem; font-weight: 500; margin-right: 0.5rem; white-space: nowrap; }\n'
        '        .seasonal-btn, .vessel-btn { padding: 0.4rem 0.8rem; background: transparent; border: 1px solid #374151; '
        'border-radius: 0.375rem; color: #9ca3af; font-size: 0.8rem; font-weight: 600; cursor: pointer; '
        'transition: all 0.2s; font-family: "Inter", sans-serif; white-space: nowrap; }\n'
        '        .seasonal-btn:hover, .vessel-btn:hover { color: #e5e7eb; border-color: #4b5563; }\n'
        '        .seasonal-btn.active { background: #3b82f6; border-color: #3b82f6; color: #fff; }\n'
        '        .vessel-btn.active { background: #8b5cf6; border-color: #8b5cf6; color: #fff; }\n'
        '        .vessel-btn:focus { outline: none; }\n'
        '        .seasonal-panel { display: none; }\n'
        '        .seasonal-panel.active { display: block; }\n'
        '        .vessel-panel { display: none; }\n'
        '        .vessel-panel.active { display: block; }\n'
        '        /* Map filter buttons */\n'
        '        .chart-controls { display: flex; gap: 0.4rem; align-items: center; }\n'
        '        .metric-btn { padding: 0.35rem 0.75rem; background: #1f2937; border: 1px solid #374151; '
        'border-radius: 0.375rem; color: #9ca3af; font-size: 0.75rem; font-weight: 600; cursor: pointer; '
        'transition: all 0.2s; font-family: "Inter", sans-serif; }\n'
        '        .metric-btn:hover { color: #e5e7eb; border-color: #4b5563; background: #283548; }\n'
        '        .metric-btn.active { background: #3b82f6; border-color: #3b82f6; color: #fff; }\n'
        '        /* Period banner */\n'
        '        .period-banner { display: flex; flex-wrap: wrap; align-items: center; gap: 0.5rem; '
        'background: linear-gradient(135deg, #1e293b, #0f172a); padding: 0.6rem 1rem; border-radius: 0.5rem; '
        'border: 1px solid #334155; margin-bottom: 1.5rem; font-size: 0.8rem; color: #94a3b8; '
        'font-family: "Inter", sans-serif; }\n'
        '        .period-banner strong { color: #e2e8f0; font-weight: 600; }\n'
        '        .period-sep { color: #475569; margin: 0 0.25rem; }\n'
        '        /* Table section toggle */\n'
        '        .table-section-toggle { display: flex; gap: 0.5rem; margin: 1.5rem 0; padding: 0.5rem; '
        'background: linear-gradient(135deg, #1f2937, #111827); border-radius: 0.5rem; '
        'border: 1px solid #374151; }\n'
        '        .table-section-btn { padding: 0.5rem 1.2rem; background: transparent; border: 1px solid #374151; '
        'border-radius: 0.375rem; color: #9ca3af; font-size: 0.85rem; font-weight: 600; cursor: pointer; '
        'transition: all 0.2s; font-family: "Inter", sans-serif; flex: 1; text-align: center; }\n'
        '        .table-section-btn:hover { color: #e5e7eb; border-color: #4b5563; }\n'
        '        .table-section-btn.active { background: #059669; border-color: #059669; color: #fff; }\n'
        '        .table-group { display: none; }\n'
        '        .table-group.active { display: block; }\n'
    )
    base_html = base_html.replace('    </style>', style_insert + '    </style>', 1)

    # Find insertion points AFTER CSS insertion so positions are correct
    kpi_marker = '<!-- KPI Cards -->'
    kpi_start = base_html.find(kpi_marker)
    hormuz_marker = '<!-- Hormuz Main Chart -->'
    hormuz_start = base_html.find(hormuz_marker)

    if kpi_start < 0 or hormuz_start < 0:
        print("WARNING: Could not find insertion points, falling back to single-seasonal dashboard")
        build_html(data_default, output_path)
        return

    # Extract the map section (it's between KPI and the first table section)
    map_start = base_html.find('<!-- Interactive Map -->')
    map_end_marker = '<div class="table-section">'
    map_end = base_html.find(map_end_marker, map_start) if map_start >= 0 else -1

    map_section = ''
    if map_start >= 0 and map_end >= 0:
        map_section = base_html[map_start:map_end].strip()

    # Find where the Hormuz chart section ends (at the Methodology section)
    methodology_marker = '<!-- Methodology -->'
    methodology_start = base_html.find(methodology_marker, hormuz_start)
    if methodology_start < 0:
        methodology_start = hormuz_start  # fallback

    # Layout: toggle → map → seasonal panels → methodology
    # Map placed just after the seasonal toggle, before the vessel-type panels
    # Hormuz chart is dropped entirely
    before = base_html[:kpi_start]
    after = base_html[methodology_start:]

    # Multi-vessel selection banner (hidden by default)
    multi_banner_html = (
        '        <div class="multi-vessel-banner" style="display:none; background: linear-gradient(135deg, #422006, #1c1917); '
        'border: 1px solid #92400e; border-radius: 0.5rem; padding: 0.6rem 1rem; margin-bottom: 1rem; '
        'color: #fbbf24; font-size: 0.82rem; font-family: \'Inter\', sans-serif;"></div>\n'
    )

    # Table section toggle bar (below the map)
    table_toggle_html = (
        '        <div class="table-section-toggle">\n'
        '            <button class="toggle-btn table-section-btn active" data-table-section="ports">Ports</button>\n'
        '            <button class="toggle-btn table-section-btn" data-table-section="countries">Countries</button>\n'
        '            <button class="toggle-btn table-section-btn" data-table-section="regional">Regions</button>\n'
        '            <button class="toggle-btn table-section-btn" data-table-section="chokepoints">Chokepoints</button>\n'
        '        </div>\n'
    )

    final_html = (
        before +
        toggle_html + '\n' +
        period_html +
        kpi_section + '\n' +
        '        ' + map_section + '\n\n' +
        multi_banner_html +
        table_toggle_html + '\n' +
        seasonal_panels + '\n' +
        '        </div><!-- /view-dashboard -->\n\n' +
        '        ' + after
    )

    # Add seasonal and vessel type toggle JS before the closing </script></body>
    seasonal_js = (
        '\n// ── Seasonal toggle ──\n'
        '(function() {\n'
        '    var defaultS = ' + str(default_s) + ';\n'
        '    // Show default panel\n'
        '    document.querySelectorAll(".seasonal-panel").forEach(function(p) {\n'
        '        p.classList.toggle("active", p.getAttribute("data-seasonal") == defaultS);\n'
        '    });\n'
        '    // Seasonal toggle removed — fixed s=13\n'
        '})();\n'
        '\n// ── Vessel type toggle (multi-select, global — applies to all seasonal panels) ──\n'
        '(function() {\n'
        '    var VT_LABELS = {tanker:"Tanker",container:"Container",dry_bulk:"Dry Bulk",general_cargo:"General Cargo",roro:"RoRo"};\n'
        '    window._selectedVessels = ["tanker","container","dry_bulk","general_cargo","roro"];\n'
        '    window._displayVessel = "tanker";\n'
        '\n'
        '    function getSelectedVessels() { return window._selectedVessels; }\n'
        '\n'
        '    // Get current week index from slider (default to latest)\n'
        '    function getWeekIdx() {\n'
        '        return window._selectedWeekIdx != null ? window._selectedWeekIdx : (window._postCrisisDates ? window._postCrisisDates.length - 1 : 0);\n'
        '    }\n'
        '\n'
        '    // Read actual/cf for an entry at the selected week index\n'
        '    function weekA(entry) {\n'
        '        var w = getWeekIdx();\n'
        '        if (entry && entry.ta && w < entry.ta.length) return entry.ta[w];\n'
        '        return entry ? (entry.a || 0) : 0;\n'
        '    }\n'
        '    function weekCf(entry) {\n'
        '        var w = getWeekIdx();\n'
        '        if (entry && entry.tc && w < entry.tc.length) return entry.tc[w];\n'
        '        return entry ? (entry.cf || 0) : 0;\n'
        '    }\n'
        '    // Compute deviation at the selected week for an entry\n'
        '    function weekDev(entry) {\n'
        '        var a = weekA(entry), cf = weekCf(entry);\n'
        '        if (Math.abs(cf) < 0.0001) return null;\n'
        '        return (a - cf) / Math.abs(cf) * 100;\n'
        '    }\n'
        '\n'
        '    // Compute aggregated deviation % from actual/cf across selected vessel types\n'
        '    function aggPct(dataByVt, field, selected) {\n'
        '        var sumA = 0, sumCf = 0;\n'
        '        selected.forEach(function(vt) {\n'
        '            if (dataByVt[vt] && dataByVt[vt][field]) {\n'
        '                sumA += weekA(dataByVt[vt][field]);\n'
        '                sumCf += weekCf(dataByVt[vt][field]);\n'
        '            }\n'
        '        });\n'
        '        if (Math.abs(sumCf) < 0.0001) return 0;\n'
        '        return (sumA - sumCf) / Math.abs(sumCf) * 100;\n'
        '    }\n'
        '    // Get current sigma threshold from slider (default 2)\n'
        '    function getSigmaThreshold() {\n'
        '        var sl = document.getElementById("sigmaSlider");\n'
        '        return sl ? parseFloat(sl.value) : 2;\n'
        '    }\n'
        '    // Check significance of a single metric: |d| >= threshold * sg\n'
        '    // Uses week-specific deviation if ta/tc arrays are available\n'
        '    function isSig(entry) {\n'
        '        if (!entry || entry.sg == null) return true;\n'
        '        var t = getSigmaThreshold();\n'
        '        if (t === 0) return true;\n'
        '        var d = weekDev(entry);\n'
        '        if (d == null) return true;\n'
        '        return Math.abs(d) >= t * entry.sg;\n'
        '    }\n'
        '    function aggSig(dataByVt, field, selected) {\n'
        '        // Significant if ANY selected VT is significant for this metric\n'
        '        var anySig = false;\n'
        '        selected.forEach(function(vt) {\n'
        '            if (dataByVt[vt] && dataByVt[vt][field] && isSig(dataByVt[vt][field])) anySig = true;\n'
        '        });\n'
        '        return anySig;\n'
        '    }\n'
        '\n'
        '    function fmtPct(val, big, sig) {\n'
        '        var cls = val < 0 ? "negative" : "positive";\n'
        '        if (sig === false) cls += " dev-ns";\n'
        '        var sign = val > 0 ? "+" : "";\n'
        '        var sz = big ? "1.5rem" : "0.95rem";\n'
        '        return \'<span class="kpi-value \' + cls + \'" style="font-size:\' + sz + \';">\' + sign + val.toFixed(1) + "%</span>";\n'
        '    }\n'
        '\n'
        '    function fmtSubRow(label, val, sig) {\n'
        '        return \'<div style="display:flex;justify-content:space-between;align-items:center;padding:0.15rem 0;">\' +\n'
        '            \'<span style="color:#9ca3af;font-size:0.75rem;">\' + label + "</span>" + fmtPct(val, false, sig) + "</div>";\n'
        '    }\n'
        '\n'
        '    function buildMultiKpi(selected) {\n'
        '        var kd = window._kpiCountData;\n'
        '        if (!kd) return "";\n'
        '        function p(f) { return aggPct(kd, f, selected); }\n'
        '        function s(f) { return aggSig(kd, f, selected); }\n'
        '        function countCard(label, field) {\n'
        '            return \'<div class="kpi-card" data-kpi-section="chokepoints"><div class="kpi-label">\' + label + \'</div><div style="margin-top:0.25rem;">\' + fmtSubRow("Ship Count", p(field), s(field)) + "</div></div>";\n'
        '        }\n'
        '        function regCard(label, field) {\n'
        '            return \'<div class="kpi-card" data-kpi-section="regional"><div class="kpi-label">\' + label + \'</div><div style="margin-top:0.25rem;">\' + fmtSubRow("Port Calls", p(field), s(field)) + "</div></div>";\n'
        '        }\n'
        '        function ctyCard(label, field) {\n'
        '            return \'<div class="kpi-card" data-kpi-section="countries"><div class="kpi-label">\' + label + \'</div><div style="margin-top:0.25rem;">\' + fmtSubRow("Port Calls", p(field), s(field)) + "</div></div>";\n'
        '        }\n'
        '        return \'<div class="kpi-row-label">Chokepoints</div><div class="kpi-grid">\' + countCard("Strait of Hormuz","hormuz") + countCard("Suez Canal","suez") + countCard("Panama Canal","panama") + countCard("Cape of Good Hope","cape") + countCard("Malacca Strait","malacca") + "</div>" +\n'
        '            \'<div class="kpi-row-label">Regions</div><div class="kpi-grid">\' + regCard("Persian Gulf","gulf") + regCard("North America","na") + regCard("East Asia","ea") + regCard("Southeast Asia","sea") + regCard("Latin America","latam") + "</div>" +\n'
        '            \'<div class="kpi-row-label">Countries</div><div class="kpi-grid">\' + ctyCard("Singapore","sgp") + ctyCard("Malaysia","my") + ctyCard("Thailand","th") + ctyCard("Indonesia","id") + ctyCard("Philippines","ph") + "</div>";\n'
        '    }\n'
        '\n'
        '    // Build aggregated map layers for multi-select\n'
        '    function updateMapForMulti(selected) {\n'
        '        var map = window._mapObj;\n'
        '        if (!map) return;\n'
        '        var cd = window._mapCountData;\n'
        '        if (!cd) return;\n'
        '        window._mapIsMulti = true;  // multi-select uses combined layout (chokepoints + ports)\n'
        '\n'
        '        // Remove existing layers\n'
        '        if (window._mapCpLayer) map.removeLayer(window._mapCpLayer);\n'
        '        if (window._mapExportLayer) map.removeLayer(window._mapExportLayer);\n'
        '        if (window._mapImportLayer) map.removeLayer(window._mapImportLayer);\n'
        '        if (window._mapPortsLayer) map.removeLayer(window._mapPortsLayer);\n'
        '\n'
        '        function devColor(pct) {\n'
        '            if (pct <= -5) return "#ef4444";\n'
        '            if (pct >= 5) return "#22c55e";\n'
        '            return "#eab308";\n'
        '        }\n'
        '        function markerSize(pct) { return Math.max(4, Math.min(18, 4 + Math.abs(pct) / 8)); }\n'
        '\n'
        '        // Sigma threshold filter: marker is significant if ANY selected VT is significant\n'
        '        function multiMapSig(item, selected) {\n'
        '            var t = getSigmaThreshold();\n'
        '            if (t === 0) return true;\n'
        '            var anySig = false, anyTestable = false;\n'
        '            selected.forEach(function(vt) {\n'
        '                var e = item[vt];\n'
        '                if (!e || e.sg == null) return;\n'
        '                var d = weekDev(e);\n'
        '                if (d == null) return;\n'
        '                anyTestable = true;\n'
        '                if (Math.abs(d) >= t * e.sg) anySig = true;\n'
        '            });\n'
        '            return anyTestable ? anySig : false;\n'
        '        }\n'
        '\n'
        '        // Aggregate chokepoints\n'
        '        var cpMarkers = [];\n'
        '        cd.cp.forEach(function(cp) {\n'
        '            if (!multiMapSig(cp, selected)) return;\n'
        '            var sumA = 0, sumCf = 0;\n'
        '            selected.forEach(function(vt) {\n'
        '                if (cp[vt]) { sumA += weekA(cp[vt]); sumCf += weekCf(cp[vt]); }\n'
        '            });\n'
        '            var pct = Math.abs(sumCf) < 0.0001 ? 0 : (sumA - sumCf) / Math.abs(sumCf) * 100;\n'
        '            var sz = markerSize(pct);\n'
        '            var m = L.marker([cp.lat, cp.lon], {\n'
        '                icon: L.divIcon({\n'
        '                    className: "",\n'
        '                    html: \'<svg width="\' + (sz*2) + \'" height="\' + (sz*2) + \'" viewBox="0 0 \' + (sz*2) + " " + (sz*2) + \'"><rect x="\' + (sz*0.3) + \'" y="\' + (sz*0.3) + \'" width="\' + (sz*1.4) + \'" height="\' + (sz*1.4) + \'" transform="rotate(45 \' + sz + " " + sz + \')" fill="\' + devColor(pct) + \'" stroke="#fff" stroke-width="1.5" opacity="0.85"/></svg>\',\n'
        '                    iconSize: [sz*2, sz*2],\n'
        '                    iconAnchor: [sz, sz]\n'
        '                })\n'
        '            }).bindTooltip("<b>" + cp.name + "</b><br>Ship count: " + pct.toFixed(1) + "%", {className:"map-tooltip"});\n'
        '            cpMarkers.push(m);\n'
        '        });\n'
        '        window._mapCpLayer = L.layerGroup(cpMarkers);\n'
        '\n'
        '        // Aggregate ports\n'
        '        var portMarkers = [];\n'
        '        cd.ports.forEach(function(p) {\n'
        '            if (!multiMapSig(p, selected)) return;\n'
        '            var sumA = 0, sumCf = 0, hasData = false;\n'
        '            selected.forEach(function(vt) {\n'
        '                if (p[vt]) { sumA += weekA(p[vt]); sumCf += weekCf(p[vt]); hasData = true; }\n'
        '            });\n'
        '            if (!hasData) return;\n'
        '            var pct = Math.abs(sumCf) < 0.0001 ? 0 : (sumA - sumCf) / Math.abs(sumCf) * 100;\n'
        '            var sz = markerSize(pct);\n'
        '            var m = L.circleMarker([p.lat, p.lon], {\n'
        '                radius: sz, fillColor: devColor(pct), color: "#fff", weight: 1.5,\n'
        '                opacity: 0.9, fillOpacity: 0.7\n'
        '            }).bindTooltip("<b>" + p.port + "</b> (" + p.iso3 + ")<br>Port calls: " + pct.toFixed(1) + "%", {className:"map-tooltip"});\n'
        '            portMarkers.push(m);\n'
        '        });\n'
        '        window._mapPortsLayer = L.layerGroup(portMarkers);\n'
        '        window._mapExportLayer = L.layerGroup([]);\n'
        '        window._mapImportLayer = L.layerGroup([]);\n'
        '\n'
        '        // Switch buttons to multi-select style (chokepoints + ports)\n'
        '        var btnExp = document.getElementById("mapBtnExport");\n'
        '        var btnImp = document.getElementById("mapBtnImport");\n'
        '        var btnPorts = document.getElementById("mapBtnPorts");\n'
        '        var legExp = document.getElementById("legendExport");\n'
        '        var legImp = document.getElementById("legendImport");\n'
        '        var legPorts = document.getElementById("legendPorts");\n'
        '        btnExp.style.display = "none"; btnImp.style.display = "none";\n'
        '        btnPorts.style.display = ""; btnPorts.classList.add("active");\n'
        '        legExp.style.display = "none"; legImp.style.display = "none";\n'
        '        legPorts.style.display = "flex";\n'
        '        window._mapLayerVisible.ports = true;\n'
        '\n'
        '        // Add layers\n'
        '        if (window._mapLayerVisible.chokepoints) window._mapCpLayer.addTo(map);\n'
        '        if (window._mapLayerVisible.ports) window._mapPortsLayer.addTo(map);\n'
        '    }\n'
        '\n'
        '    // Build aggregated tables for multi-select mode\n'
        '    function fmtNum(v) {\n'
        '        if (v == null) return "\\u2014";\n'
        '        if (Math.abs(v) >= 1e6) return (v/1e6).toFixed(1) + "M";\n'
        '        if (Math.abs(v) >= 1e3) return (v/1e3).toFixed(1) + "K";\n'
        '        if (v % 1 === 0) return v.toLocaleString();\n'
        '        return v.toFixed(1);\n'
        '    }\n'
        '    function devCell(pct) {\n'
        '        if (pct == null) return \'<td class="numeric-cell deviation-cell">\\u2014</td>\';\n'
        '        var cls = pct < 0 ? "negative" : "positive";\n'
        '        return \'<td class="numeric-cell deviation-cell \' + cls + \'">\' + (pct > 0 ? "+" : "") + pct.toFixed(1) + "%</td>";\n'
        '    }\n'
        '    // Compute the table crisis index (index into _tableAggData.dates where crisis starts)\n'
        '    function getTableCrisisIdx() {\n'
        '        var td = window._tableAggData;\n'
        '        if (!td || !td.dates || !td.crisis) return td ? td.dates.length - 1 : 0;\n'
        '        for (var i = 0; i < td.dates.length; i++) {\n'
        '            if (td.dates[i] >= td.crisis) return i;\n'
        '        }\n'
        '        return td.dates.length - 1;\n'
        '    }\n'
        '\n'
        '    function aggRow(entries, field, selected) {\n'
        '        // Sum a/cf/a1y/a1q/avg across selected vessel types for an entry\n'
        '        // Uses selected week index to pick values from time series arrays\n'
        '        var w = getWeekIdx();\n'
        '        var tsIdx = getTableCrisisIdx() + w;  // index into ts_a/ts_c arrays\n'
        '        var sumA = 0, sumCf = 0, sumA1y = 0, sumA1q = 0, sumAvg = 0, anySig = false;\n'
        '        selected.forEach(function(vt) {\n'
        '            var d = entries[vt];\n'
        '            if (!d) return;\n'
        '            // Read actual/cf from time series at selected week, falling back to scalars\n'
        '            var a = (d.ts_a && tsIdx < d.ts_a.length) ? d.ts_a[tsIdx] : (d.a || 0);\n'
        '            var cf = (d.ts_c && tsIdx < d.ts_c.length) ? d.ts_c[tsIdx] : (d.cf || 0);\n'
        '            sumA += a; sumCf += cf;\n'
        '            sumA1y += d.a1y||0; sumA1q += d.a1q||0; sumAvg += d.avg||0;\n'
        '            if (isSig(d)) anySig = true;\n'
        '        });\n'
        '        var dev = Math.abs(sumCf) < 0.001 ? null : (sumA - sumCf) / Math.abs(sumCf) * 100;\n'
        '        var yoy = Math.abs(sumA1y) < 0.001 ? null : (sumA - sumA1y) / Math.abs(sumA1y) * 100;\n'
        '        var qoq = Math.abs(sumA1q) < 0.001 ? null : (sumA - sumA1q) / Math.abs(sumA1q) * 100;\n'
        '        return {a: sumA, cf: sumCf, avg: sumAvg, dev: dev, yoy: yoy, qoq: qoq, sig: anySig};\n'
        '    }\n'
        '\n'
        '    function aggTimeSeries(item, selected) {\n'
        '        // Aggregate time series data across selected vessel types\n'
        '        // Returns full date range; createInlineChart handles zoom trimming\n'
        '        if (!window._tableAggData || !window._tableAggData.dates) {\n'
        '            return {dates: [], actual: [], cf: [], crisis: "", label: item.name || ""};\n'
        '        }\n'
        '        var dates = window._tableAggData.dates || [];\n'
        '        var crisisDate = window._tableAggData.crisis || "";\n'
        '        var actualArr = [];\n'
        '        var cfArr = [];\n'
        '        dates.forEach(function(d, idx) {\n'
        '            var sumA = 0, sumCf = 0;\n'
        '            selected.forEach(function(vt) {\n'
        '                var d_vt = item[vt];\n'
        '                if (d_vt && d_vt.ts_a && d_vt.ts_c) {\n'
        '                    sumA += d_vt.ts_a[idx] || 0;\n'
        '                    sumCf += d_vt.ts_c[idx] || 0;\n'
        '                }\n'
        '            });\n'
        '            actualArr.push(Math.round(sumA * 10) / 10);\n'
        '            cfArr.push(Math.round(sumCf * 10) / 10);\n'
        '        });\n'
        '        // Compute weighted-average variance decomposition across selected VTs\n'
        '        var vd = null;\n'
        '        var totalWeight = 0;\n'
        '        var vdAccum = {r2_trend:0, r2_trend_seasonal:0, r2_full:0, r2_controls_marginal:0, r2_unexplained:0};\n'
        '        selected.forEach(function(vt) {\n'
        '            var d_vt = item[vt];\n'
        '            if (d_vt && d_vt.vd) {\n'
        '                var w = Math.abs(d_vt.avg) || 1;\n'
        '                totalWeight += w;\n'
        '                for (var k in vdAccum) { vdAccum[k] += (d_vt.vd[k] || 0) * w; }\n'
        '            }\n'
        '        });\n'
        '        if (totalWeight > 0) {\n'
        '            vd = {};\n'
        '            for (var k in vdAccum) { vd[k] = vdAccum[k] / totalWeight; }\n'
        '        }\n'
        '        return {dates: dates, actual: actualArr, cf: cfArr, crisis: crisisDate, label: item.name || "", vd: vd};\n'
        '    }\n'
        '\n'
        '    var REGION_COLORS = {"Gulf":"#ef4444","East Asia":"#3b82f6","SE Asia":"#06b6d4","Oceania":"#06b6d4","S. Asia":"#f59e0b","Med":"#a855f7","N. Africa":"#a855f7","Europe":"#6366f1","N. America":"#10b981","LatAm":"#22c55e","W. Africa":"#f97316","S. Africa":"#f97316","E. Africa":"#f97316","Russia":"#64748b","C. Asia":"#64748b"};\n'
        '    function buildMultiTableSection(title, items, selected, maxRows, isPort) {\n'
        '        // items: array of {name, ...vt data}\n'
        '        // Compute aggregated values and sort by avg descending\n'
        '        var rows = [];\n'
        '        items.forEach(function(item) {\n'
        '            var r = aggRow(item, null, selected);\n'
        '            r.name = item.name;\n'
        '            r.iso3 = item.iso3 || "";\n'
        '            r.region = item.region || "";\n'
        '            r.ts_items = item;  // Store original item for time series\n'
        '            rows.push(r);\n'
        '        });\n'
        '        rows.sort(function(a, b) { return b.avg - a.avg; });\n'
        '        if (maxRows) rows = rows.slice(0, maxRows);\n'
        '        var thead = \'<thead><tr><th>Name</th><th>Hist. Avg</th><th>Latest</th><th>Counterfactual</th><th>Deviation</th><th>vs 1Y ago</th><th>vs 1Q ago</th></tr></thead>\';\n'
        '        var tbody = "<tbody>";\n'
        '        rows.forEach(function(r) {\n'
        '            var rowId = "multi_row_" + (window._multiRowCounter++);\n'
        '            var chartData = aggTimeSeries(r.ts_items, selected);\n'
        '            var nameHtml = r.name;\n'
        '            if (isPort && r.iso3) {\n'
        '                nameHtml += \' <span style="color:#6b7280;font-size:0.75rem;">(\' + r.iso3 + \')</span>\';\n'
        '            }\n'
        '            if (isPort && r.region) {\n'
        '                var rc = REGION_COLORS[r.region] || "#94a3b8";\n'
        '                nameHtml += \' <span class="region-tag" style="background:\' + rc + \'22; color:\' + rc + \'; border: 1px solid \' + rc + \'44;">\' + r.region + \'</span>\';\n'
        '            }\n'
        '            var dimCls = r.sig ? "" : " dev-ns";\n'
        '            tbody += \'<tr class="expandable-row multi-expandable\' + dimCls + \'" data-target="\' + rowId + \'" style="cursor:pointer;" title="Click to show chart">\';\n'
        '            tbody += \'<td class="region-cell">\' + nameHtml + \' <span class="expand-icon">&#9654;</span></td>\';\n'
        '            tbody += \'<td class="numeric-cell">\' + fmtNum(r.avg) + "</td>";\n'
        '            tbody += \'<td class="numeric-cell">\' + fmtNum(r.a) + "</td>";\n'
        '            tbody += \'<td class="numeric-cell">\' + fmtNum(r.cf) + "</td>";\n'
        '            tbody += devCell(r.dev);\n'
        '            tbody += devCell(r.yoy);\n'
        '            tbody += devCell(r.qoq);\n'
        '            tbody += "</tr>";\n'
        '            tbody += \'<tr class="chart-row" id="\' + rowId + \'" style="display:none;">\';\n'
        '            tbody += \'<td colspan="7" class="chart-cell">\';\n'
        '            tbody += \'<div class="inline-chart-container"><canvas id="canvas_\' + rowId + \'"></canvas></div>\';\n'
        '            tbody += \'<div class="vd-bar-container" id="vd_\' + rowId + \'"></div>\';\n'
        '            tbody += \'<button class="zoom-toggle-btn" onclick="toggleChartZoom(this)" title="Zoom in to recent 3 months">Zoom In</button>\';\n' +
        '            tbody += \'<button class="export-csv-btn" onclick="exportChartCSV(this)">Export CSV</button>\';\n'
        '            tbody += \'<scr\' + \'ipt type="application/json" class="chart-data">\' + JSON.stringify(chartData) + \'</scr\' + \'ipt>\';\n'
        '            tbody += \'</td></tr>\';\n'
        '        });\n'
        '        tbody += "</tbody>";\n'
        '        return \'<div class="table-section" data-metric-type="counts"><h2>\' + title + \'</h2><table>\' + thead + tbody + "</table></div>";\n'
        '    }\n'
        '\n'
        '    function buildMultiTables(selected) {\n'
        '        var td = window._tableAggData;\n'
        '        if (!td) return;\n'
        '        var labels = selected.map(function(v) { return VT_LABELS[v] || v; }).join(" + ");\n'
        '        window._multiRowCounter = 0;\n'
        '\n'
        '        // Get active seasonal panels\n'
        '        document.querySelectorAll("[id^=multiTablePorts_]").forEach(function(el) {\n'
        '            el.innerHTML = buildMultiTableSection("Port Calls Deviation (" + labels + ")", td.ports, selected, 50, true);\n'
        '        });\n'
        '        document.querySelectorAll("[id^=multiTableRegional_]").forEach(function(el) {\n'
        '            var h = "";\n'
        '            var regionalItems = [];\n'
        '            if (td.regional_exp) {\n'
        '                td.regional_exp.forEach(function(item) {\n'
        '                    var cleanItem = {};\n'
        '                    for (var key in item) {\n'
        '                        if (key === "name") {\n'
        '                            cleanItem.name = item.name.replace(" Exports", "");\n'
        '                        } else {\n'
        '                            cleanItem[key] = item[key];\n'
        '                        }\n'
        '                    }\n'
        '                    regionalItems.push(cleanItem);\n'
        '                });\n'
        '            }\n'
        '            h += buildMultiTableSection("Regional Port Calls Deviation (" + labels + ")", regionalItems, selected);\n'
        '            el.innerHTML = h;\n'
        '        });\n'
        '        document.querySelectorAll("[id^=multiTableCountries_]").forEach(function(el) {\n'
        '            var h = "";\n'
        '            var countryItems = [];\n'
        '            if (td.country_exp) {\n'
        '                td.country_exp.forEach(function(item) {\n'
        '                    var cleanItem = {};\n'
        '                    for (var key in item) {\n'
        '                        if (key === "name") {\n'
        '                            cleanItem.name = item.name.replace(" Exports", "");\n'
        '                        } else {\n'
        '                            cleanItem[key] = item[key];\n'
        '                        }\n'
        '                    }\n'
        '                    countryItems.push(cleanItem);\n'
        '                });\n'
        '            }\n'
        '            h += buildMultiTableSection("Country Port Calls Deviation (" + labels + ")", countryItems, selected);\n'
        '            el.innerHTML = h;\n'
        '        });\n'
        '        document.querySelectorAll("[id^=multiTableChokepoints_]").forEach(function(el) {\n'
        '            el.innerHTML = buildMultiTableSection("Chokepoint Ship Counts Deviation (" + labels + ")", td.chokepoints, selected);\n'
        '        });\n'
        '    }\n'
        '\n'
        '    function updateVesselView() {\n'
        '        var selected = getSelectedVessels();\n'
        '        var isMulti = selected.length > 1;\n'
        '        // Destroy existing inline charts\n'
        '        if (typeof chartInstances !== "undefined") {\n'
        '            Object.keys(chartInstances).forEach(function(k) {\n'
        '                chartInstances[k].destroy();\n'
        '                delete chartInstances[k];\n'
        '            });\n'
        '        }\n'
        '        document.querySelectorAll(".chart-row").forEach(function(r) { r.style.display = "none"; });\n'
        '        document.querySelectorAll(".expandable-row.expanded").forEach(function(r) { r.classList.remove("expanded"); });\n'
        '\n'
        '        var displayVessel = window._displayVessel || selected[0];\n'
        '\n'
        '        if (isMulti) {\n'
        '            // MULTI-SELECT: show aggregated KPI, map, and tables\n'
        '            // Show "multi" vessel panel for both KPI and tables\n'
        '            document.querySelectorAll(".vessel-panel").forEach(function(p) {\n'
        '                p.classList.toggle("active", p.getAttribute("data-vessel") === "multi");\n'
        '            });\n'
        '            // Populate dynamic multi KPI grids\n'
        '            var multiHtml = buildMultiKpi(selected);\n'
        '            document.querySelectorAll("[id^=multiKpiGrid_]").forEach(function(el) {\n'
        '                el.innerHTML = multiHtml;\n'
        '            });\n'
        '            // Update map and tables with aggregated data\n'
        '            updateMapForMulti(selected);\n'
        '            buildMultiTables(selected);\n'
        '            // Bind click handlers to new multi-expandable rows\n'
        '            bindMultiExpandableRows();\n'
        '\n'
        '            var labels = selected.map(function(v) { return VT_LABELS[v] || v; });\n'
        '            document.querySelectorAll(".multi-vessel-banner").forEach(function(b) {\n'
        '                b.style.display = "block";\n'
        '                b.innerHTML = "\\u26A0 Multi-vessel mode: showing <strong>" + labels.join(" + ") + "</strong> — aggregated port call / ship count results (capacity hidden because DWT is not economically comparable across vessel types).";\n'
        '            });\n'
        '            document.querySelectorAll(\'.table-section[data-metric-type="tonnage"]\').forEach(function(s) {\n'
        '                s.style.display = "none";\n'
        '            });\n'
        '        } else {\n'
        '            // SINGLE SELECT: show that vessel type everywhere\n'
        '            document.querySelectorAll(".vessel-panel").forEach(function(p) {\n'
        '                p.classList.toggle("active", p.getAttribute("data-vessel") === displayVessel);\n'
        '            });\n'
        '            if (typeof window.updateMapForVessel === "function") {\n'
        '                window.updateMapForVessel(displayVessel);\n'
        '            }\n'
        '            document.querySelectorAll(".multi-vessel-banner").forEach(function(b) { b.style.display = "none"; });\n'
        '            document.querySelectorAll(\'.table-section[data-metric-type="tonnage"]\').forEach(function(s) {\n'
        '                s.style.display = "";\n'
        '            });\n'
        '        }\n'
        '    }\n'
        '    window._updateVesselView = updateVesselView;\n'
        '\n'
        '    // Recompute dimming when sigma slider changes\n'
        '    window._recomputeDimming = function() {\n'
        '        var t = getSigmaThreshold();\n'
        '        // 1. Server-rendered elements with data-dev/data-sg attributes\n'
        '        document.querySelectorAll(".sig-dimmable[data-dev]").forEach(function(el) {\n'
        '            var d = parseFloat(el.getAttribute("data-dev"));\n'
        '            var sg = parseFloat(el.getAttribute("data-sg"));\n'
        '            var significant = (t === 0) || isNaN(d) || isNaN(sg) || sg < 0.01 || Math.abs(d) >= t * sg;\n'
        '            // For table rows: toggle dev-ns on the row itself\n'
        '            if (el.tagName === "TR") {\n'
        '                el.classList.toggle("dev-ns", !significant);\n'
        '            } else {\n'
        '                // For KPI sub-rows: toggle dev-ns on the inner .kpi-value span\n'
        '                var span = el.querySelector(".kpi-value");\n'
        '                if (span) span.classList.toggle("dev-ns", !significant);\n'
        '            }\n'
        '        });\n'
        '        // 2. Multi-VT mode: rebuild dynamic KPI and tables (they use getSigmaThreshold)\n'
        '        var selected = getSelectedVessels();\n'
        '        if (selected.length > 1) {\n'
        '            var expandedIds = saveExpandedCharts();\n'
        '            var multiHtml = buildMultiKpi(selected);\n'
        '            document.querySelectorAll("[id^=multiKpiGrid_]").forEach(function(el) {\n'
        '                el.innerHTML = multiHtml;\n'
        '            });\n'
        '            buildMultiTables(selected);\n'
        '            bindMultiExpandableRows();\n'
        '            restoreExpandedCharts(expandedIds);\n'
        '            updateMapForMulti(selected);\n'
        '        } else if (selected.length === 1) {\n'
        '            if (typeof window.updateMapForVessel === "function") {\n'
        '                window.updateMapForVessel(selected[0]);\n'
        '            }\n'
        '        }\n'
        '    };\n'
        '\n'
        '    // Update single-VT KPI sub-rows from data-ta/data-tc attributes\n'
        '    function updateSingleKpiSubRows() {\n'
        '        var w = getWeekIdx();\n'
        '        var kpiCount = 0, tableCount = 0;\n'
        '        document.querySelectorAll(".sig-dimmable[data-ta]").forEach(function(el) {\n'
        '            var taStr = el.getAttribute("data-ta");\n'
        '            var tcStr = el.getAttribute("data-tc");\n'
        '            if (!taStr || !tcStr) return;\n'
        '            var ta = taStr.split(",").map(Number);\n'
        '            var tc = tcStr.split(",").map(Number);\n'
        '            if (w >= ta.length) return;\n'
        '            var a = ta[w], cf = tc[w];\n'
        '            var dev = (Math.abs(cf) < 0.0001) ? 0 : (a - cf) / Math.abs(cf) * 100;\n'
        '            var sign = dev > 0 ? "+" : "";\n'
        '            var cls = dev < 0 ? "negative" : "positive";\n'
        '            var span = el.querySelector(".kpi-value");\n'
        '            if (span) {\n'
        '                span.textContent = sign + Math.round(dev * 10) / 10 + "%";\n'
        '                span.className = "kpi-value " + cls;\n'
        '            }\n'
        '            // Update data-dev for sigma dimming\n'
        '            el.setAttribute("data-dev", Math.round(dev * 100) / 100);\n'
        '            if (el.tagName === "TR") tableCount++; else kpiCount++;\n'
        '        });\n'
        '        console.log("[weekSlider] updateSingleKpiSubRows: updated " + kpiCount + " KPI divs, " + tableCount + " table rows (week=" + w + ")");\n'
        '    }\n'
        '\n'
        '    // Update single-VT table rows using data-ta/data-tc attributes\n'
        '    function updateSingleTableRows() {\n'
        '        var w = getWeekIdx();\n'
        '        // Query all active vessel panels (KPI + table panels both get .active)\n'
        '        document.querySelectorAll(".vessel-panel.active tr[data-ta]").forEach(function(row) {\n'
        '            var taStr = row.getAttribute("data-ta");\n'
        '            var tcStr = row.getAttribute("data-tc");\n'
        '            if (!taStr || !tcStr) return;\n'
        '            var ta = taStr.split(",").map(Number);\n'
        '            var tc = tcStr.split(",").map(Number);\n'
        '            if (w >= ta.length) return;\n'
        '            var a = ta[w], cf = tc[w];\n'
        '            var dev = (Math.abs(cf) < 0.0001) ? null : (a - cf) / Math.abs(cf) * 100;\n'
        '            // Update actual cell\n'
        '            var actCell = row.querySelector(".wk-actual");\n'
        '            if (actCell) actCell.textContent = a.toLocaleString(undefined, {maximumFractionDigits: 0});\n'
        '            // Update cf cell\n'
        '            var cfCell = row.querySelector(".wk-cf");\n'
        '            if (cfCell) cfCell.textContent = cf.toLocaleString(undefined, {maximumFractionDigits: 0});\n'
        '            // Update deviation cell\n'
        '            var devCell = row.querySelector(".wk-dev");\n'
        '            if (devCell && dev != null) {\n'
        '                var sign = dev > 0 ? "+" : "";\n'
        '                devCell.textContent = sign + Math.round(dev * 10) / 10 + "%";\n'
        '                devCell.className = devCell.className.replace(/\\b(negative|positive)\\b/g, "").trim();\n'
        '                devCell.classList.add(dev < 0 ? "negative" : "positive");\n'
        '            }\n'
        '            // Update data-dev for sigma dimming\n'
        '            if (dev != null) row.setAttribute("data-dev", Math.round(dev * 100) / 100);\n'
        '        });\n'
        '    }\n'
        '\n'
        '    // Recompute everything when week slider changes\n'
        '    // Helper: save expanded chart row state before table rebuild\n'
        '    function saveExpandedCharts() {\n'
        '        var ids = [];\n'
        '        document.querySelectorAll(".expandable-row.multi-expandable.expanded").forEach(function(row) {\n'
        '            ids.push(row.getAttribute("data-target"));\n'
        '        });\n'
        '        // Destroy chart instances that will be removed by innerHTML replacement\n'
        '        ids.forEach(function(id) {\n'
        '            if (window.chartInstances && window.chartInstances[id]) {\n'
        '                window.chartInstances[id].destroy();\n'
        '                delete window.chartInstances[id];\n'
        '            }\n'
        '        });\n'
        '        return ids;\n'
        '    }\n'
        '    // Helper: restore expanded chart rows after table rebuild\n'
        '    function restoreExpandedCharts(ids) {\n'
        '        ids.forEach(function(targetId) {\n'
        '            var chartRow = document.getElementById(targetId);\n'
        '            var expandRow = document.querySelector(\'.expandable-row[data-target="\' + targetId + \'\"]\');\n'
        '            if (chartRow && expandRow) {\n'
        '                chartRow.style.display = "table-row";\n'
        '                expandRow.classList.add("expanded");\n'
        '                var dataScript = chartRow.querySelector(".chart-data");\n'
        '                if (dataScript) {\n'
        '                    try {\n'
        '                        var cd = JSON.parse(dataScript.textContent);\n'
        '                        createInlineChart(targetId, cd, false);\n'
        '                    } catch(e) {}\n'
        '                }\n'
        '            }\n'
        '        });\n'
        '    }\n'
        '\n'
        '    window._recomputeWeek = function() {\n'
        '        var selected = getSelectedVessels();\n'
        '        console.log("[weekSlider] _recomputeWeek called, weekIdx=" + getWeekIdx() + ", selected=" + selected.length + " VTs: " + selected.join(","));\n'
        '        if (selected.length > 1) {\n'
        '            var expandedIds = saveExpandedCharts();\n'
        '            var multiHtml = buildMultiKpi(selected);\n'
        '            document.querySelectorAll("[id^=multiKpiGrid_]").forEach(function(el) {\n'
        '                el.innerHTML = multiHtml;\n'
        '            });\n'
        '            buildMultiTables(selected);\n'
        '            bindMultiExpandableRows();\n'
        '            restoreExpandedCharts(expandedIds);\n'
        '            updateMapForMulti(selected);\n'
        '        } else if (selected.length === 1) {\n'
        '            console.log("[weekSlider] single-VT branch, updating KPI sub-rows + table rows + map");\n'
        '            updateSingleKpiSubRows();\n'
        '            updateSingleTableRows();\n'
        '            if (typeof window.updateMapForVessel === "function") {\n'
        '                window.updateMapForVessel(selected[0]);\n'
        '            }\n'
        '            // Re-apply sigma dimming after value updates\n'
        '            if (typeof window._recomputeDimming === "function") {\n'
        '                window._recomputeDimming();\n'
        '            }\n'
        '        }\n'
        '    };\n'
        '\n'
        '    document.querySelectorAll(".vessel-btn").forEach(function(btn) {\n'
        '        btn.addEventListener("click", function() {\n'
        '            var vessel = btn.getAttribute("data-vessel");\n'
        '            var selected = getSelectedVessels();\n'
        '\n'
        '            var idx = selected.indexOf(vessel);\n'
        '            if (idx >= 0) {\n'
        '                if (selected.length > 1) {\n'
        '                    selected.splice(idx, 1);\n'
        '                    btn.classList.remove("active");\n'
        '                    if (window._displayVessel === vessel) {\n'
        '                        window._displayVessel = selected[0];\n'
        '                    }\n'
        '                }\n'
        '            } else {\n'
        '                selected.push(vessel);\n'
        '                btn.classList.add("active");\n'
        '                window._displayVessel = vessel;\n'
        '            }\n'
        '            window._selectedVessels = selected;\n'
        '            updateVesselView();\n'
        '        });\n'
        '    });\n'
        '    // Initialize multi-VT view on page load\n'
        '    updateVesselView();\n'
        '    // If map loaded before this IIFE (unlikely), sync now; otherwise queue for when map is ready\n'
        '    var selected = getSelectedVessels();\n'
        '    if (selected.length > 1) {\n'
        '        if (window._mapReady && window._mapObj) {\n'
        '            updateMapForMulti(selected);\n'
        '        } else {\n'
        '            window._pendingMapSync = function() { updateMapForMulti(getSelectedVessels()); };\n'
        '        }\n'
        '    }\n'
        '})();\n'
        '\n// ── Table section toggle (chokepoints / regional / ports) ──\n'
        '(function() {\n'
        '    document.querySelectorAll(".table-section-btn").forEach(function(btn) {\n'
        '        btn.addEventListener("click", function() {\n'
        '            var section = btn.getAttribute("data-table-section");\n'
        '            // Update active button\n'
        '            document.querySelectorAll(".table-section-btn").forEach(function(b) { b.classList.remove("active"); });\n'
        '            btn.classList.add("active");\n'
        '            // Destroy existing inline charts\n'
        '            if (typeof chartInstances !== "undefined") {\n'
        '                Object.keys(chartInstances).forEach(function(k) {\n'
        '                    chartInstances[k].destroy();\n'
        '                    delete chartInstances[k];\n'
        '                });\n'
        '            }\n'
        '            // Toggle table groups\n'
        '            document.querySelectorAll(".table-group").forEach(function(g) {\n'
        '                g.classList.toggle("active", g.classList.contains("table-group-" + section));\n'
        '            });\n'
        '            // Collapse any open chart rows\n'
        '            document.querySelectorAll(".chart-row").forEach(function(r) { r.style.display = "none"; });\n'
        '            document.querySelectorAll(".expandable-row.expanded").forEach(function(r) { r.classList.remove("expanded"); });\n'
        '        });\n'
        '    });\n'
        '})();\n'
        '\n// ── KPI card click → scroll to matching table section ──\n'
        '(function() {\n'
        '    document.addEventListener("click", function(e) {\n'
        '        var card = e.target.closest(".kpi-card[data-kpi-section]");\n'
        '        if (!card) return;\n'
        '        var section = card.getAttribute("data-kpi-section");\n'
        '        if (!section) return;\n'
        '        // Switch the table toggle to the matching section\n'
        '        document.querySelectorAll(".table-section-btn").forEach(function(b) {\n'
        '            b.classList.toggle("active", b.getAttribute("data-table-section") === section);\n'
        '        });\n'
        '        // Show matching table group, hide others\n'
        '        document.querySelectorAll(".table-group").forEach(function(g) {\n'
        '            g.classList.toggle("active", g.classList.contains("table-group-" + section));\n'
        '        });\n'
        '        // Collapse any open chart rows\n'
        '        if (typeof chartInstances !== "undefined") {\n'
        '            Object.keys(chartInstances).forEach(function(k) {\n'
        '                chartInstances[k].destroy();\n'
        '                delete chartInstances[k];\n'
        '            });\n'
        '        }\n'
        '        document.querySelectorAll(".chart-row").forEach(function(r) { r.style.display = "none"; });\n'
        '        document.querySelectorAll(".expandable-row.expanded").forEach(function(r) { r.classList.remove("expanded"); });\n'
        '        // Scroll to the table section toggle bar\n'
        '        var toggle = document.querySelector(".table-section-toggle");\n'
        '        if (toggle) toggle.scrollIntoView({behavior: "smooth", block: "start"});\n'
        '    });\n'
        '})();\n'
    )

    # Also fix the expandable-row chart handler to use a global chartInstances
    # Replace the IIFE-scoped var with a global
    final_html = final_html.replace(
        '    var chartInstances = {};',
        '    window.chartInstances = window.chartInstances || {};  var chartInstances = window.chartInstances;'
    )

    # Build aggregation data for multi-vessel-select mode (single seasonal setting s=13)
    s_data = all_data[default_s]
    agg_data_js = '\n// ── Aggregation data for multi-vessel-select mode ──\n'
    agg_data_js += '(function() {\n'
    agg_data_js += build_aggregation_data_js(s_data)
    agg_data_js += build_table_aggregation_data_js(s_data)
    agg_data_js += '})();\n'

    # Insert seasonal JS + aggregation data before the closing </script>
    last_script_close = final_html.rfind('</script>')
    final_html = final_html[:last_script_close] + agg_data_js + seasonal_js + final_html[last_script_close:]

    with open(output_path, 'w') as f:
        f.write(final_html)


def main():
    """Main entry point."""
    output_dir = '/sessions/admiring-ecstatic-ptolemy/mnt/Forecasting/outputs/nowcast'
    output_path = os.path.join(output_dir, 'hormuz_nowcast_dashboard.html')

    # Load s=13 results (single seasonal setting)
    fp = os.path.join(output_dir, 'nowcast_results_s13.json')
    if not os.path.exists(fp):
        # Fallback to legacy single file
        fp = os.path.join(output_dir, 'nowcast_results.json')
    print(f'Loading data from {fp}...')
    all_data = {13: load_data(fp)}

    print('Building dashboard...')
    build_html_multi(all_data, output_path)

    print(f'Dashboard written to {output_path}')


if __name__ == '__main__':
    main()
