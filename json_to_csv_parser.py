from pathlib import Path
import argparse
import json
import logging
import sys

try:
    import pandas as pd
except Exception:
    print("Пожалуйста, установите pandas: pip install pandas")
    sys.exit(1)


logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def find_json_files(input_dir: Path):
    return sorted([p for p in input_dir.rglob("*.json") if p.is_file()])


def parse_record(rec: dict):
    ts = rec.get("timestamp")
    city = rec.get("city")
    data = rec.get("data") or {}

    aqi = data.get("aqi")
    pm25 = data.get("pm2_5")

    return {"timestamp": ts, "city": city, "aqi": aqi, "pm2_5": pm25}


def read_all(input_dir: Path) -> pd.DataFrame:
    files = find_json_files(input_dir)
    if not files:
        logging.warning(f"No JSON files found in {input_dir}")

    rows = []
    for f in files:
        try:
            with f.open("r", encoding="utf-8") as fh:
                obj = json.load(fh)
        except Exception as e:
            logging.warning(f"Skipping {f} — can't read JSON: {e}")
            continue

        if not isinstance(obj, list):
            logging.warning(f"Skipping {f} — expected list at top-level")
            continue

        for rec in obj:
            parsed = parse_record(rec)
            rows.append(parsed)

    df = pd.DataFrame(rows)

    expected_cols = ["timestamp", "city", "aqi", "pm2_5"]
    for c in expected_cols:
        if c not in df.columns:
            df[c] = pd.NA

    df = df[expected_cols]

    ts_raw = df["timestamp"].astype("string")
    ts_clean = (
        ts_raw
        .str.replace("\ufeff", "", regex=False)
        .str.replace(r"[\x00-\x1f]", "", regex=True)
        .str.strip()
        .str.replace("T", " ", regex=False)
        .str.replace("Z", "", regex=False)
    )
    df["timestamp"] = pd.to_datetime(ts_clean, errors="coerce")
    df["city"] = df["city"].astype("string")
    df["aqi"] = pd.to_numeric(df["aqi"], errors="coerce")
    df["pm2_5"] = pd.to_numeric(df["pm2_5"], errors="coerce")

    return df


def compute_aggregates(df: pd.DataFrame) -> pd.DataFrame:
    ag = df.groupby("city", dropna=True).agg(
        mean_aqi=("aqi", "mean"),
        max_aqi=("aqi", "max"),
        median_pm2_5=("pm2_5", "median"),
        measurements=("aqi", "count"),
    )
    ag = ag.reset_index()
    return ag


def save_outputs(df: pd.DataFrame, ag: pd.DataFrame, output_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)
    full_csv = output_dir / "all_rows.csv"
    ag_csv = output_dir / "city_aggregates.csv"

    df_sorted = df.sort_values(by=["city", "timestamp"]) if not df.empty else df

    df_sorted.to_csv(full_csv, index=False, date_format="%Y-%m-%d %H:%M:%S")
    ag.to_csv(ag_csv, index=False)

    logging.info(f"Saved full rows to {full_csv}")
    logging.info(f"Saved aggregates to {ag_csv}")


def main():
    p = argparse.ArgumentParser(description="Aggregate air quality JSON history into CSVs")
    p.add_argument("--input-dir", type=Path, default=Path.cwd() / "data" / "logs" / "history",
                   help="Directory with JSON files (default: data/logs/history)")
    p.add_argument("--output-dir", type=Path, default=Path.cwd() / "outputs",
                   help="Directory for output CSVs (default: outputs)")
    args = p.parse_args()

    input_dir = args.input_dir
    output_dir = args.output_dir

    logging.info(f"Reading JSON files from: {input_dir}")
    df = read_all(input_dir)

    if df.empty:
        logging.warning("No data parsed — exiting without writing files.")
        return

    logging.info(f"Parsed rows: {len(df)} | Unique cities: {df['city'].nunique(dropna=True)}")

    aggregates = compute_aggregates(df)

    save_outputs(df, aggregates, output_dir)


if __name__ == "__main__":
    main()
