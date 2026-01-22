"""
historical_analytics.py

Role:
 - Compute descriptive statistics and simple trend/danger assessments for
   historical air-quality time series for a single city.

Design and Methods (for Methods section):
 - All calculations are deterministic, rule-based and fully transparent.
 - For each pollutant we compute min, max, mean, median and the fraction
   of measurements above a configurable "safe" threshold.
 - Trend detection compares the first and last terciles (first/last 1/3)
   of the time series and reports "worsening", "improving" or "no_change"
   using clear relative-change rules. This is chosen as a robust,
   interpretable method suitable for applied-science reporting.

Notes on thresholds:
 - Thresholds are provided as defaults but MUST be checked against the
   units of the source data (µg/m3 for PM2.5/PM10 in typical sensors,
   AQI is unitless). Adjust thresholds to match your measurement units.
"""

from pathlib import Path
from typing import Dict, Any, Optional
import math

import pandas as pd

# Default thresholds for 'safe' and 'hazard' zones. These are conservative
#, human-readable choices meant for short-term (hourly/daily) interpretation.
# Units: PM2.5/PM10 in µg/m3; AQI is unitless; other gases should match data units.
DEFAULT_THRESHOLDS = {
    "aqi": {"safe": 50, "hazard": 200},
    "pm2_5": {"safe": 12.0, "hazard": 55.4},
    "pm10": {"safe": 54.0, "hazard": 254.0},
    "o3": {"safe": 70.0, "hazard": 200.0},
    "no2": {"safe": 100.0, "hazard": 400.0},
    "so2": {"safe": 20.0, "hazard": 500.0},
    "co": {"safe": 1000.0, "hazard": 10000.0},
}


def _safe_count(series: pd.Series, safe_threshold: float) -> float:
    if series.dropna().empty:
        return 0.0
    return (series > safe_threshold).sum() / len(series) * 100.0


def _count_hazard_entries(series: pd.Series, hazard_threshold: float) -> int:
    # Count transitions from below to above the hazard threshold
    s = series.fillna(-math.inf)
    above = s > hazard_threshold
    # count rising edges
    edges = (~above.shift(1, fill_value=False)) & (above)
    return int(edges.sum())


def _detect_trend(series: pd.Series) -> str:
    """Detect simple sustained trend: compare means of first and last terciles.

    Rules (transparent):
    - Split the time series into first and last third by order.
    - Compute mean_first, mean_last. If mean_last >= mean_first*(1+0.05)
      and there are at least 10 observations -> 'worsening'.
    - If mean_last <= mean_first*(1-0.05) -> 'improving'.
    - Otherwise -> 'no_change'.

    The 5% relative threshold and minimum-observations rule make the
    decision robust to noise while remaining easy to explain in Methods.
    """
    vals = series.dropna()
    n = len(vals)
    if n < 6:
        return "no_change"
    third = max(1, n // 3)
    first = vals.iloc[:third]
    last = vals.iloc[-third:]
    mean_first = first.mean()
    mean_last = last.mean()
    # If means are nan, return no_change
    if pd.isna(mean_first) or pd.isna(mean_last):
        return "no_change"
    rel_change = 0.0
    if mean_first != 0:
        rel_change = (mean_last - mean_first) / abs(mean_first)
    # require at least 10 observations to call sustained trend
    if n >= 10 and rel_change >= 0.05:
        return "worsening"
    if n >= 10 and rel_change <= -0.05:
        return "improving"
    return "no_change"


def analyze_pollutant(series: pd.Series, name: str,
                      thresholds: Optional[Dict[str, float]] = None) -> Dict[str, Any]:
    """Compute stats, percent above safe, hazard entries and trend for one pollutant."""
    if thresholds is None:
        thresholds = DEFAULT_THRESHOLDS.get(name, {"safe": float('nan'), "hazard": float('nan')})
    safe_th = thresholds.get("safe")
    hazard_th = thresholds.get("hazard")

    s = pd.to_numeric(series, errors="coerce")
    n = int(s.count())
    result: Dict[str, Any] = {
        "n_measurements": n,
        "min": None,
        "max": None,
        "mean": None,
        "median": None,
        "percent_above_safe": None,
        "hazard_entries": None,
        "trend": None,
    }

    if n == 0:
        return result

    result["min"] = float(s.min()) if not pd.isna(s.min()) else None
    result["max"] = float(s.max()) if not pd.isna(s.max()) else None
    result["mean"] = float(s.mean()) if not pd.isna(s.mean()) else None
    result["median"] = float(s.median()) if not pd.isna(s.median()) else None

    if safe_th is not None and not math.isnan(safe_th):
        result["percent_above_safe"] = float(_safe_count(s, safe_th))
    else:
        result["percent_above_safe"] = None

    if hazard_th is not None and not math.isnan(hazard_th):
        result["hazard_entries"] = int(_count_hazard_entries(s, hazard_th))
    else:
        result["hazard_entries"] = None

    result["trend"] = _detect_trend(s)

    return result


def analyze_city_history(df: pd.DataFrame, thresholds: Optional[Dict[str, Dict[str, float]]] = None) -> Dict[str, Any]:
    """Analyze historical DataFrame for a city.

    Expected DataFrame columns: at least `timestamp` (datetime-like) and any
    of the pollutant names: `aqi`, `pm2_5`, `pm10`, `o3`, `no2`, `so2`, `co`.

    Returns a dict with per-pollutant analysis and a small summary.
    """
    if thresholds is None:
        thresholds = DEFAULT_THRESHOLDS

    out: Dict[str, Any] = {"pollutants": {}, "n_rows": int(len(df))}

    pollutants = ["aqi", "pm2_5", "pm10", "o3", "no2", "so2", "co"]
    for p in pollutants:
        if p in df.columns:
            out["pollutants"][p] = analyze_pollutant(df[p], p, thresholds.get(p))

    return out


def analyze_city_file(path: Path, thresholds: Optional[Dict[str, Dict[str, float]]] = None) -> Dict[str, Any]:
    """Read a JSON history file (list of records) and analyze it.

    The JSON file is expected to be a list of objects with keys
    `timestamp`, `city`, `data` (matching the earlier project structure).
    """
    import json

    with path.open("r", encoding="utf-8") as fh:
        obj = json.load(fh)

    # flatten into DataFrame
    rows = []
    for rec in obj:
        ts = rec.get("timestamp")
        city = rec.get("city")
        data = rec.get("data") or {}
        row = {"timestamp": ts, "city": city}
        row.update(data)
        rows.append(row)

    df = pd.DataFrame(rows)
    # normalize timestamp
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    return analyze_city_history(df, thresholds=thresholds)
