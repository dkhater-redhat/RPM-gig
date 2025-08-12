import hashlib, os
from pathlib import Path
import pandas as pd
from .config import DATA_DIR

def cache_name(sql: str, prefix: str = "slice", ext: str = "parquet") -> Path:
    fid = hashlib.md5(sql.encode()).hexdigest()[:12]
    return DATA_DIR / f"{prefix}_{fid}.{ext}"

def save_parquet(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)

def load_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)
