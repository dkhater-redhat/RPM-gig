# scripts/peek.py — inspect latest (prefer *_supported.parquet)

import pandas as pd
from pathlib import Path
import os, sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"

def pick_parquet():
    # list all parquet files
    parquets = list(DATA_DIR.glob("*.parquet"))
    if not parquets:
        print(f"No parquet files found in {DATA_DIR}\nRun: python -m scripts.extract_day")
        sys.exit(1)

    # prefer supported files
    supported = [p for p in parquets if p.stem.endswith("_supported")]
    # sort by modified time (newest last)
    supported.sort(key=lambda p: p.stat().st_mtime)
    parquets.sort(key=lambda p: p.stat().st_mtime)

    if supported:
        chosen = supported[-1]
        picked_supported = True
    else:
        chosen = parquets[-1]
        picked_supported = False

    return chosen, picked_supported

def main():
    p, picked_supported = pick_parquet()
    df = pd.read_parquet(p)

    tag = "SUPPORTED" if picked_supported else "RAW"
    print(f"[{tag}] File: {p.name}")
    print("Full path:", p)
    print("Rows:", len(df))
    print("Systems:", df["inventory_id"].nunique() if "inventory_id" in df.columns else "(n/a)")
    print("Packages:", df["name"].nunique() if "name" in df.columns else "(n/a)")
    print("Columns:", list(df.columns))

    # If repo_source exists (from the supported filter), show a quick breakdown
    if "repo_source" in df.columns:
        print("\nrepo_source counts:")
        print(df["repo_source"].value_counts().to_string())

    print("\nPreview:")
    print(df.head(5))

if __name__ == "__main__":
    main()
