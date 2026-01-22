from pathlib import Path
import argparse
import logging
import sys

try:
    import pandas as pd
except Exception:
    print("Please install pandas: pip install pandas")
    sys.exit(1)


logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def main():
    p = argparse.ArgumentParser(description="Fix and validate timestamp column in CSV")
    p.add_argument("--input", type=Path, default=Path("outputs") / "all_rows.csv",
                   help="Input CSV file (default: outputs/all_rows.csv)")
    p.add_argument("--output", type=Path, default=Path("outputs") / "all_rows_fixed.csv",
                   help="Output cleaned CSV (default: outputs/all_rows_fixed.csv)")
    p.add_argument("--invalid-out", type=Path, default=Path("outputs") / "all_rows_invalid_timestamp.csv",
                   help="CSV where rows with irrecoverable timestamps are stored")
    args = p.parse_args()

    input_path = args.input
    out_path = args.output
    invalid_out = args.invalid_out

    if not input_path.exists():
        logging.error(f"Input file not found: {input_path}")
        sys.exit(2)

    logging.info(f"Reading: {input_path}")
    df = pd.read_csv(input_path, dtype={"city": "string"})

    if "timestamp" not in df.columns:
        logging.error("Input CSV does not contain 'timestamp' column")
        sys.exit(3)

    total_rows = len(df)

    ts_raw = df["timestamp"].astype("string")

    ts_clean = (
        ts_raw
        .str.replace("\ufeff", "", regex=False)
        .str.replace(r"[\x00-\x1f]", "", regex=True)
        .str.strip()
        .str.replace("T", " ", regex=False)
        .str.replace("Z", "", regex=False)
    )

    parsed = pd.to_datetime(ts_clean, errors="coerce")

    n_invalid = int(parsed.isna().sum())
    logging.info(f"Total rows: {total_rows}; parsed successfully: {total_rows - n_invalid}; failed: {n_invalid}")

    invalid_rate = n_invalid / total_rows if total_rows > 0 else 0.0

    invalid_mask = parsed.isna()
    if n_invalid > 0:
        invalid_out.parent.mkdir(parents=True, exist_ok=True)
        df.loc[invalid_mask].to_csv(invalid_out, index=False)

    out_path.parent.mkdir(parents=True, exist_ok=True)

    threshold = 0.05
    if invalid_rate < threshold:
        logging.info(f"Invalid rate {invalid_rate:.2%} < {threshold:.0%}: dropping invalid rows")
        df_clean = df.loc[~invalid_mask].copy()
        df_clean["timestamp"] = parsed.loc[~invalid_mask].dt.strftime("%Y-%m-%d %H:%M:%S")
        df_clean = df_clean.sort_values(by="timestamp").reset_index(drop=True)
        df_clean.to_csv(out_path, index=False)
        logging.info(f"Saved cleaned CSV to {out_path} ({len(df_clean)} rows); invalids saved to {invalid_out}")
    else:
        logging.warning(f"Invalid rate {invalid_rate:.2%} >= {threshold:.0%}: not dropping rows by default")
        df_all = df.copy()
        df_all["timestamp"] = parsed
        df_all = df_all.sort_values(by="timestamp", na_position="last").reset_index(drop=True)
        df_all.to_csv(out_path, index=False)
        logging.info(f"Saved CSV with parsed timestamps (NaT for invalids) to {out_path}; invalids saved to {invalid_out}")


if __name__ == "__main__":
    main()
