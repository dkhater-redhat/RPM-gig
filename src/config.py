from dotenv import load_dotenv
import os
from pathlib import Path

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
SQL_DIR = PROJECT_ROOT / "sql"
DATA_DIR.mkdir(exist_ok=True, parents=True)

TRINO_HOST = os.getenv("TRINO_HOST", "prod.sep.starburst.redhat.com")
TRINO_PORT = int(os.getenv("TRINO_PORT", "443"))
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "s3_datahub_insights")
TRINO_SCHEMA  = os.getenv("TRINO_SCHEMA", "insights_wh_extraction_rules")
TRINO_USER = os.getenv("TRINO_USER", "dkhater")
LOCAL_CERT_BUNDLE = os.getenv("LOCAL_CERT_BUNDLE")
