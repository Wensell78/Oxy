"""
city_report_builder.py

Purpose:
 - Build a single JSON report for a city combining the latest measurement,
   a decision summary and historical analysis for UI consumption.

Inputs:
 - city: city name (string)
 - all_rows_path: path to cleaned CSV with timestamp, city, aqi, pm2_5, pm10, o3, no2, so2, co
 - handbook_path: path to indicator handbook JSON

Outputs:
 - Python dict (JSON-serializable) with keys: city, generated_at, current, history, indicators, notes
 - CLI saves the dict to outputs/city_report_<city>.json when run directly

Notes:
 - Uses existing `decision_engine.interpret` for AQI category and PM2.5 only.
 - Reuses `history_analyzer.analyze_city_from_csv` for historical summary.
 - Does not modify other modules.
"""

from pathlib import Path
import json
from typing import Dict, Any, Optional
from datetime import datetime

import pandas as pd

from decision_engine import interpret
from history_analyzer import analyze_city_from_csv


def _safe_get(row: pd.Series, col: str) -> Optional[Any]:
    if col not in row.index:
        return None
    v = row.get(col)
    if pd.isna(v):
        return None
    return v


def build_city_report(city: str,
                      all_rows_path: str = "outputs/all_rows_fixed.csv",
                      handbook_path: str = "indicator_handbook.json",
                      handbook_mode: str = "minimal") -> Dict[str, Any]:
    """Build full city report for UI.

    Returns a JSON-serializable dict.
    """
    all_rows = Path(all_rows_path)
    if not all_rows.exists():
        raise FileNotFoundError(f"Input CSV not found: {all_rows}")

    df = pd.read_csv(all_rows)
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    df_city = df[df["city"] == city].copy()
    if df_city.empty:
        current = {
            "timestamp": None,
            "aqi_category": None,
            "pm2_5": None,
            "pm10": None,
            "o3": None,
            "no2": None,
            "so2": None,
            "co": None,
            "decision": None,
        }
        history = analyze_city_from_csv(city, input_csv=all_rows, output_dir=Path("outputs"))
    else:
        # pick latest by timestamp; if timestamps missing, use last row order
        if "timestamp" in df_city.columns and df_city["timestamp"].notna().any():
            df_city = df_city.sort_values("timestamp")
            last = df_city.iloc[-1]
        else:
            last = df_city.iloc[-1]

        # map CSV columns to indicators
        aqi_val = _safe_get(last, "aqi")
        pm25_val = _safe_get(last, "pm2_5")
        pm10_val = _safe_get(last, "pm10")
        o3_val = _safe_get(last, "o3")
        no2_val = _safe_get(last, "no2")
        so2_val = _safe_get(last, "so2")
        co_val = _safe_get(last, "co")

        # decision: only feed aqi category and pm2_5 to decision engine
        try:
            decision = interpret(aqi_val, pm25_val,
                                 timestamp=(last["timestamp"].isoformat() if pd.notna(last.get("timestamp")) else None),
                                 city=city)
        except Exception:
            decision = None

        current = {
            "timestamp": (last["timestamp"].isoformat() if pd.notna(last.get("timestamp")) else None),
            "aqi_category": int(aqi_val) if aqi_val is not None else None,
            "pm2_5": float(pm25_val) if pm25_val is not None else None,
            "pm10": float(pm10_val) if pm10_val is not None else None,
            "o3": float(o3_val) if o3_val is not None else None,
            "no2": float(no2_val) if no2_val is not None else None,
            "so2": float(so2_val) if so2_val is not None else None,
            "co": float(co_val) if co_val is not None else None,
            "decision": decision,
        }

        history = analyze_city_from_csv(city, input_csv=all_rows, output_dir=Path("outputs"))

    # load handbook and include indicators according to handbook_mode
    indicators: Dict[str, Any] = {}
    try:
        hb = Path(handbook_path)
        if hb.exists():
            with hb.open("r", encoding="utf-8") as fh:
                handbook = json.load(fh)
            hb_inds = handbook.get("indicators", {})

            mapping = {
                "aqi_category": "AQI",
                "pm2_5": "PM2.5",
                "pm10": "PM10",
                "o3": "O3",
                "no2": "NO2",
                "so2": "SO2",
                "co": "CO",
            }

            mode = (handbook_mode or "minimal").lower()
            if mode == "none":
                indicators = {}
            elif mode == "full":
                indicators = hb_inds.copy()
            else:
                # minimal: include AQI and PM2.5 always; include others only if present in current
                for required in ("AQI", "PM2.5"):
                    if required in hb_inds:
                        indicators[required] = hb_inds[required]
                for cur_k, hb_k in mapping.items():
                    if hb_k in ("AQI", "PM2.5"):
                        continue
                    if current.get(cur_k) is not None and hb_k in hb_inds:
                        indicators[hb_k] = hb_inds[hb_k]
        else:
            indicators = {}
    except Exception:
        indicators = {}

    out = {
        "city": city,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "current": current,
        "history": history,
        "indicators": indicators,
        "handbook_mode": handbook_mode,
        "notes": [
            "AQI is OpenWeather category 1..5, not numerical AQI index.",
            "Thresholds may vary by agency and averaging period."
        ]
    }

    return out


def main():
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--city", required=True, help="City name to build report for (exact match)")
    p.add_argument("--input", default="outputs/all_rows_fixed.csv", help="Input cleaned CSV path")
    p.add_argument("--handbook-path", default="indicator_handbook.json", help="Indicator handbook JSON path")
    p.add_argument("--handbook", choices=["minimal", "full", "none"], default="minimal",
                   help="How much of the handbook to embed in the report: minimal|full|none")
    p.add_argument("--output-dir", default="outputs", help="Directory to save report JSON")
    args = p.parse_args()

    res = build_city_report(args.city, all_rows_path=args.input,
                            handbook_path=args.handbook_path, handbook_mode=args.handbook)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"city_report_{args.city}.json"
    with out_path.open("w", encoding="utf-8") as fh:
        json.dump(res, fh, ensure_ascii=False, indent=2, default=str)
    print(f"Saved report to {out_path}")


if __name__ == "__main__":
    main()
