import pandas as pd
import requests
from bs4 import BeautifulSoup

URL = "https://docs.redhat.com/en/documentation/red_hat_enterprise_linux/9/html-single/package_manifest/index"

headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "text/html,application/xhtml+xml"
}

resp = requests.get(URL, headers=headers, timeout=60)
resp.raise_for_status()
soup = BeautifulSoup(resp.text, "html.parser")

def table_after_section(soup, section_id_text):
    # find the section heading by id or text match, then grab the first table after it
    # Red Hat docs use anchors like "BaseOS repository" / "AppStream repository"
    h = soup.find(lambda tag: tag.name in ["h2", "h3"] and section_id_text.lower() in tag.get_text(strip=True).lower())
    if not h:
        return None
    tbl = h.find_next("table")
    if tbl is None:
        return None
    # Let pandas parse the HTML table into a DataFrame
    return pd.read_html(str(tbl))[0]

baseos_df = table_after_section(soup, "The BaseOS repository")
appstream_df = table_after_section(soup, "The AppStream repository")

# Keep just package name + any other useful columns (license, compat level)
def normalize(df):
    if df is None:
        return None
    cols = [c for c in df.columns if isinstance(c, str)]
    # Common col names are often "Package", "License", "Application compatibility level"
    keep = [c for c in cols if c.lower() in {"package","license","application compatibility level"}]
    return df[keep] if keep else df

baseos_df = normalize(baseos_df)
appstream_df = normalize(appstream_df)

# Save to CSV
if baseos_df is not None:
    baseos_df.to_csv("rhel9_baseos_packages.csv", index=False)
if appstream_df is not None:
    appstream_df.to_csv("rhel9_appstream_packages.csv", index=False)

print("BaseOS rows:", 0 if baseos_df is None else len(baseos_df))
print("AppStream rows:", 0 if appstream_df is None else len(appstream_df))
