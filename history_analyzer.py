"""
history_analyzer.py

Purpose:
 - Read cleaned measurements CSV and produce a structured historical summary
   for a single city: distributions of risk levels, bad-time share, worst
   moments and top bad days. Designed for backend use in the Oxy project.

Inputs:
 - `outputs/all_rows_fixed.csv` (CSV with columns: timestamp, city, aqi, pm2_5, ...)
 - city name (string)

Outputs:
 - dict (JSON-friendly) with summary metrics (returned by function)
from pathlib import Path
import argparse
import json
from typing import Dict, Any, List

import pandas as pd

from decision_engine import interpret, _aqi_to_level, _pm25_to_level


BAD_LEVELS = {"Unhealthy", "Hazardous"}
MODERATE_OR_WORSE_LEVELS = {"Moderate", "Unhealthy", "Hazardous"}


def _apply_interpret_row(row) -> Dict[str, Any]:
    aqi = row.get("aqi")
    pm25 = row.get("pm2_5")
    ts = row.get("timestamp")
    city = row.get("city")
    return interpret(aqi, pm25, timestamp=str(ts) if pd.notna(ts) else None, city=city)


def analyze_city_from_csv(city: str, input_csv: Path = Path("outputs") / "all_rows_fixed.csv", output_dir: Path = Path("outputs")) -> Dict[str, Any]:
    df = pd.read_csv(input_csv)
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    df_city = df[df["city"] == city].copy()

    total_rows_initial = len(df_city)

    mask_missing = df_city["timestamp"].isna() | (df_city["aqi"].isna() & df_city["pm2_5"].isna())
    dropped_rows = int(mask_missing.sum())
    df_city = df_city.loc[~mask_missing].copy()

    total_measurements = len(df_city)

    interpretations: List[Dict[str, Any]] = []
    records = []
    for _, row in df_city.iterrows():
        out = _apply_interpret_row(row)
        interpretations.append(out)
        records.append({"timestamp": row["timestamp"], "interpret": out})

    drivers: List[str] = []
    for idx in range(len(interpretations)):
        r = df_city.iloc[idx]
        aqi = r.get("aqi")
        pm25 = r.get("pm2_5")
        a_level = _aqi_to_level(aqi)
        p_level = _pm25_to_level(pm25)
        if a_level > p_level:
            driver = "AQI"
        elif p_level > a_level:
            driver = "PM2.5"
        else:
            driver = "Both"
        interpretations[idx]["driver"] = driver
        drivers.append(driver)

    from collections import Counter

    level_counts = Counter([i["risk_level"] for i in interpretations])
    distribution = {}
    for lvl, cnt in level_counts.items():
        distribution[lvl] = {"count": int(cnt), "pct": float(cnt / total_measurements * 100) if total_measurements>0 else 0.0}

    unhealthy_count = sum(cnt for lvl, cnt in level_counts.items() if lvl in BAD_LEVELS)
    unhealthy_or_worse_share = float(unhealthy_count / total_measurements * 100) if total_measurements>0 else 0.0

    moderate_count = sum(cnt for lvl, cnt in level_counts.items() if lvl in MODERATE_OR_WORSE_LEVELS)
    moderate_or_worse_share = float(moderate_count / total_measurements * 100) if total_measurements>0 else 0.0

    driver_counts = Counter(drivers)
    drivers_distribution: Dict[str, Any] = {}
    for k in ("AQI", "PM2.5", "Both"):
        c = int(driver_counts.get(k, 0))
        drivers_distribution[k] = {"count": c, "pct": float(c / total_measurements * 100) if total_measurements>0 else 0.0}

    period_start = df_city["timestamp"].min().isoformat() if total_measurements>0 else None
    period_end = df_city["timestamp"].max().isoformat() if total_measurements>0 else None

    worst_entry = None
    if interpretations:
        for idx, interp in enumerate(interpretations):
            interp["__idx"] = idx
        max_score = max(i["score"] for i in interpretations)
        candidates = [i for i in interpretations if i["score"] == max_score]
        chosen = None
        chosen_ts = None
        for c in candidates:
            idx = c.get("__idx")
            ts = records[idx]["timestamp"]
            if chosen is None or ts < chosen_ts:
                chosen = c
                chosen_ts = ts
        if chosen is not None:
            idx = chosen.get("__idx")
            try:
                orig_row = df_city.iloc[idx]
            except Exception:
                orig_row = None

            determined_by = chosen.get("driver", "Both")
            if orig_row is not None:
                aqi_val = orig_row.get("aqi")
                pm25_val = orig_row.get("pm2_5")
                a_level = _aqi_to_level(aqi_val)
                p_level = _pm25_to_level(pm25_val)
                if a_level > p_level:
                    determined_by = "AQI"
                elif p_level > a_level:
                    determined_by = "PM2.5"
                else:
                    determined_by = "Both"

            worst_entry = {
                "timestamp": records[chosen['__idx']]["timestamp"].isoformat() if records[chosen['__idx']]["timestamp"] is not None else None,
                "risk_level": chosen["risk_level"],
                "reasons": chosen.get("reasons", []),
                "determined_by": determined_by,
            }

    top_bad_windows = []
    top_bad_windows_moderate_or_worse = []
    top_bad_windows_unhealthy_or_worse = []
    if total_measurements > 0:
        df_city["date"] = df_city["timestamp"].dt.date
        df_city = df_city.reset_index(drop=True)
        df_city["risk_level"] = [i["risk_level"] for i in interpretations]
        day_groups = df_city.groupby("date")
        day_stats = []
        for d, g in day_groups:
            n = len(g)
            unhealthy_n = g[ g["risk_level"].isin(BAD_LEVELS) ].shape[0]
            unhealthy_share = float(unhealthy_n / n * 100) if n>0 else 0.0
            moderate_n = g[ g["risk_level"].isin(MODERATE_OR_WORSE_LEVELS) ].shape[0]
            moderate_share = float(moderate_n / n * 100) if n>0 else 0.0
            day_idx = g.index.tolist()
            day_scores = [(idx, interpretations[idx]["score"]) for idx in day_idx]
            day_scores.sort(key=lambda x: (-x[1], records[x[0]]["timestamp"]))
            top_idx = day_scores[0][0]
            day_stats.append({
                "date": str(d),
                "moderate_or_worse_share": moderate_share,
                "unhealthy_or_worse_share": unhealthy_share,
                "n": n,
                "representative_worst": interpretations[top_idx],
            })
        day_stats_moderate = sorted(day_stats, key=lambda x: (-x["moderate_or_worse_share"], -x["n"]))
        day_stats_unhealthy = sorted(day_stats, key=lambda x: (-x["unhealthy_or_worse_share"], -x["n"]))
        top_bad_windows_moderate_or_worse = day_stats_moderate[:3]
        top_bad_windows_unhealthy_or_worse = day_stats_unhealthy[:3]
        top_bad_windows = top_bad_windows_moderate_or_worse

    summary = ""
    if total_measurements == 0:
        summary = f"No valid measurements for {city} in the provided file."
    else:
        days_span = (pd.to_datetime(period_end) - pd.to_datetime(period_start)).days + 1 if period_start and period_end else None
        span_text = f"the last {days_span} days" if days_span is not None and days_span>0 else "the analysed period"
        if worst_entry:
            summary = (
                f"Over {span_text}, Moderate or worse occurred {moderate_or_worse_share:.1f}% of the time; "
                f"Unhealthy or worse occurred {unhealthy_or_worse_share:.1f}%. The worst recorded moment was on {worst_entry['timestamp']} ({worst_entry['risk_level']})."
            )
        else:
            summary = (
                f"Over {span_text}, Moderate or worse occurred {moderate_or_worse_share:.1f}% of the time; "
                f"Unhealthy or worse occurred {unhealthy_or_worse_share:.1f}%. No single worst moment identified."
            )
        if total_measurements > 0:
            top_driver = max(drivers_distribution.items(), key=lambda kv: kv[1]["count"])[0]
            top_pct = drivers_distribution[top_driver]["pct"]
            if top_driver == "AQI":
                driver_text = f"AQI category ({top_pct:.1f}%)"
            elif top_driver == "PM2.5":
                driver_text = f"PM2.5 ({top_pct:.1f}%)"
            else:
                driver_text = f"both AQI and PM2.5 equally ({top_pct:.1f}%)"
            summary = summary + f" The risk level was most often driven by {driver_text}."

    out: Dict[str, Any] = {
        "city": city,
        "total_measurements": int(total_measurements),
        "dropped_rows": int(dropped_rows),
        "period_start": period_start,
        "period_end": period_end,
        "distribution": distribution,
        "moderate_or_worse_share_pct": float(moderate_or_worse_share),
        "unhealthy_or_worse_share_pct": float(unhealthy_or_worse_share),
        "drivers_distribution": drivers_distribution,
        "worst_moment": worst_entry,
        "top_bad_windows_moderate_or_worse": top_bad_windows_moderate_or_worse,
        "top_bad_windows_unhealthy_or_worse": top_bad_windows_unhealthy_or_worse,
        "top_bad_windows": top_bad_windows,
        "summary": summary,
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"history_summary_{city}.json"
    with out_path.open("w", encoding="utf-8") as fh:
        json.dump(out, fh, ensure_ascii=False, indent=2, default=str)

    return out


def main():
    p = argparse.ArgumentParser(description="Historical analyzer for city air quality")
    p.add_argument("--city", required=True, help="City name to analyze (exact match in CSV)")
    p.add_argument("--input", default="outputs/all_rows_fixed.csv", help="Input CSV path")
    p.add_argument("--output-dir", default="outputs", help="Directory to save summary JSON")
    args = p.parse_args()

    res = analyze_city_from_csv(args.city, input_csv=Path(args.input), output_dir=Path(args.output_dir))
            # drivers_distribution computed above
