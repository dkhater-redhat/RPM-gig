# scripts/filter_to_supported_repos.py
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
PKG_DIR = DATA_DIR / "packages"

# Input CSVs you scraped
APPSTREAM_CSV = PKG_DIR / "rhel9_appstream_packages.csv"
BASEOS_CSV    = PKG_DIR / "rhel9_baseos_packages.csv"

# Find latest parquet from your extract
def latest_parquet() -> Path:
    files = sorted(DATA_DIR.glob("rhel10_*.parquet"))
    assert files, f"No rhel10_*.parquet files in {DATA_DIR}"
    return files[-1]

def load_whitelists():
    # minimal columns: 'Package' is the package name column in your CSV
    app = pd.read_csv(APPSTREAM_CSV, usecols=["Package"])
    base = pd.read_csv(BASEOS_CSV, usecols=["Package"])

    # Normalize names to strings, strip whitespace
    app["Package"]  = app["Package"].astype(str).str.strip()
    base["Package"] = base["Package"].astype(str).str.strip()

    app_names  = set(app["Package"].tolist())
    base_names = set(base["Package"].tolist())
    union      = app_names | base_names

    # Make a repo label lookup (for reporting)
    label = {name: "AppStream" for name in app_names}
    label.update({name: "BaseOS" for name in base_names})

    return union, label

def main():
    inp = latest_parquet()
    print(f"Reading: {inp}")
    df = pd.read_parquet(inp)

    # Optional memory tweak
    for col in ("name", "arch", "virt_what_info", "vendor"):
        if col in df.columns:
            df[col] = df[col].astype("category")

    whitelist, label_map = load_whitelists()
    print(f"Whitelist sizes -> AppStream+BaseOS union: {len(whitelist):,}")

    # Filter to supported packages only
    before_rows = len(df)
    before_pkgs = df["name"].nunique()
    before_sys  = df["inventory_id"].nunique()

    df_f = df[df["name"].isin(whitelist)].copy()

    # Add a repo label column from which CSV it came
    df_f["repo_source"] = df_f["name"].map(label_map).astype("category")

    after_rows = len(df_f)
    after_pkgs = df_f["name"].nunique()
    after_sys  = df_f["inventory_id"].nunique()

    print(f"Kept rows: {after_rows:,} / {before_rows:,} "
          f"({after_rows/before_rows:.1%})")
    print(f"Kept unique packages: {after_pkgs:,} / {before_pkgs:,}")
    print(f"Systems represented (any supported pkg present): {after_sys:,} / {before_sys:,}")

    # Save a new parquet alongside the original
    out = inp.with_name(inp.stem + "_supported.parquet")
    df_f.to_parquet(out, index=False)
    print(f"Saved → {out}")

    # Small sanity preview
    print("\nPreview:")
    print(df_f[["inventory_id","name","repo_source","arch"]].head(10))

if __name__ == "__main__":
    main()
