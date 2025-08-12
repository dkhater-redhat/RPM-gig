#  peek.py is responsible for inspecting our Parquet file and printing a summary of data

import pandas as pd
from pathlib import Path
import glob, os, sys

# resolve project root
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# candidate data dirs to search
candidates = [
    PROJECT_ROOT / "data",
    Path.cwd() / "data",
]

paths = []
for d in candidates:
    paths.extend(sorted(glob.glob(str(d / "*.parquet"))))

if not paths:
    print("No parquet files found. Looked in:")
    for d in candidates:
        print(" -", d)
    print("\nRun: python -m scripts.extract_day  (to create one)")
    sys.exit(1)

p = paths[-1]
df = pd.read_parquet(p)

print("File:", os.path.basename(p))
print("Full path:", p)
print("Rows:", len(df))
print("Systems:", df["inventory_id"].nunique())
print("Packages:", df["name"].nunique())
print("Columns:", list(df.columns))

print("\nPreview:")
print(df.head(5))
