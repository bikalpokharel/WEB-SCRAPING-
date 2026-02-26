import os
import sys

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)
    
import time
import shutil
from datetime import datetime
from typing import Dict, List, Optional
from scraper_core import categorize_role_taxonomy

import pandas as pd


# =========================
# CONFIG
# =========================
DATA_DIR = "/Users/bikal/Library/CloudStorage/OneDrive-Personal/Nepal_Job_Market_Live_Data/xlsx"
LOCAL_CACHE_DIR = "/Users/bikal/Data_scraping/data_local"
LOCAL_DASH_CSV = os.path.join(LOCAL_CACHE_DIR, "jobs_master_local.csv")

MASTER_XLSX = os.path.join(DATA_DIR, "jobs_master.xlsx")
MASTER_CSV = os.path.join(DATA_DIR, "jobs_master.csv")  # fast for dashboards

FILES = {
    "merojob": os.path.join(DATA_DIR, "merojob_jobs.xlsx"),
    "jobsnepal": os.path.join(DATA_DIR, "jobsnepal_jobs.xlsx"),
    "linkedin": os.path.join(DATA_DIR, "linkedin_jobs.xlsx"),
}

# master schema (you can add more columns anytime)
MASTER_SCHEMA = [
    "global_key",
    "source",
    "job_id",
    "job_url",
    "title",
    "company",
    "company_link",
    "location",
    "country",
    "posted_date",
    "num_applicants",
    "work_mode",
    "employment_type",
    "position",
    "type",
    "compensation",
    "commitment",
    "skills",
    "category_primary",
    "domain_l1",
    "domain_l2",
    "domain_l3",
    "tax_confidence",
    "scraped_at",
]

SORT_BY = "scraped_at"

PLACEHOLDERS = {
    "Non", "non", "", "N/A", "na", "NA", "-", "—", "None", "NONE",
    "<na>", "<NA>", "nan", "NaN", "NULL", "null"
}

TAX_COLS = ["category_primary", "domain_l1", "domain_l2", "domain_l3", "tax_confidence"]

def _ensure_taxonomy(master: pd.DataFrame) -> pd.DataFrame:
    """
    Backfill taxonomy for old rows that don't have it yet.
    Uses existing columns only (no re-scrape).
    """
    master = master.copy()

    # make sure columns exist
    for c in TAX_COLS:
        if c not in master.columns:
            master[c] = pd.NA

    def _missing(x) -> bool:
        if pd.isna(x): 
            return True
        s = str(x).strip().lower()
        return (s == "") or (s in {"non", "none", "na", "n/a", "null", "nan", "<na>", "<na>"})

    # rows that need taxonomy
    mask = master["domain_l1"].apply(_missing) | master["category_primary"].apply(_missing)
    if not mask.any():
        return master

    subset_idx = master.index[mask].tolist()

    for idx in subset_idx:
        row = master.loc[idx]

        tax = categorize_role_taxonomy(
            title=str(row.get("title", "") or ""),
            skills=str(row.get("skills", "") or ""),
            position=str(row.get("position", "") or ""),
            employment_type=str(row.get("employment_type", "") or ""),
            description=str(row.get("description", "") or row.get("full_text", "") or ""),
            industry=str(row.get("industry", "") or ""),
        )

        master.at[idx, "category_primary"] = tax.get("category_primary")
        master.at[idx, "domain_l1"] = tax.get("domain_l1")
        master.at[idx, "domain_l2"] = tax.get("domain_l2")
        master.at[idx, "domain_l3"] = tax.get("domain_l3")
        master.at[idx, "tax_confidence"] = tax.get("tax_confidence")

    return master

# =========================
# HELPERS
# =========================
def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _read_excel_with_retry(path: str, retries: int = 3, pause: float = 1.5) -> pd.DataFrame:
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            return pd.read_excel(path, engine="openpyxl")
        except Exception as e:
            last_err = e
            print(f"[WARN] read_excel failed ({attempt}/{retries}) for: {path}\n  -> {e}")
            time.sleep(pause * attempt)
    raise last_err


def _copy_to_local_cache(src_path: str, cache_dir: str) -> Optional[str]:
    """
    Copy OneDrive file to local cache to avoid timeouts and partial reads.
    """
    try:
        _ensure_dir(cache_dir)
        dst_path = os.path.join(cache_dir, os.path.basename(src_path))
        tmp = dst_path + f".tmp_{int(time.time())}"
        shutil.copy2(src_path, tmp)
        os.replace(tmp, dst_path)  # atomic replace
        return dst_path
    except Exception as e:
        print(f"[WARN] Failed to copy to local cache: {src_path}\n  -> {e}")
        return None


def _atomic_write_excel(df: pd.DataFrame, out_path: str) -> None:
    _ensure_dir(os.path.dirname(out_path))
    tmp_path = out_path + f".tmp_{int(time.time())}.xlsx"
    df.to_excel(tmp_path, index=False, engine="openpyxl")
    os.replace(tmp_path, out_path)


def _atomic_write_csv(df: pd.DataFrame, out_path: str) -> None:
    _ensure_dir(os.path.dirname(out_path))
    tmp_path = out_path + f".tmp_{int(time.time())}.csv"
    df.to_csv(tmp_path, index=False)
    os.replace(tmp_path, out_path)


def _normalize_source(portal_key: str, df: pd.DataFrame) -> pd.DataFrame:
    if "source" not in df.columns:
        df["source"] = portal_key
    df["source"] = df["source"].fillna(portal_key).astype(str).str.strip().str.lower()
    df.loc[df["source"].isin(["<na>", "nan", "none", "null", ""]), "source"] = portal_key
    return df


def _clean_placeholders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace known placeholder strings with pandas NA.
    Compatible with pandas 2.x and 3.x.
    """
    df = df.copy()

    # Replace exact matches
    df = df.replace(list(PLACEHOLDERS), pd.NA)

    # Strip strings + blank -> NA
    for col in df.columns:
        if df[col].dtype == "object" or str(df[col].dtype).startswith("string"):
            df[col] = df[col].astype("string").str.strip()
            df[col] = df[col].replace("", pd.NA)

    return df


def _ensure_columns(df: pd.DataFrame, schema: List[str]) -> pd.DataFrame:
    for c in schema:
        if c not in df.columns:
            df[c] = pd.NA
    return df


def _standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # compatibility mapping
    if "it_non_it" in df.columns and "category_primary" not in df.columns:
        df["category_primary"] = df["it_non_it"]
    return df


def _build_global_key(df: pd.DataFrame) -> pd.DataFrame:
    """
    Priority:
      1) job_url (best)
      2) source:job_id
      3) source:title:company:location fallback
    """

    def _s(x) -> str:
        if pd.isna(x):
            return ""
        return str(x).strip()

    url = df["job_url"] if "job_url" in df.columns else pd.Series([""] * len(df))
    job_id = df["job_id"] if "job_id" in df.columns else pd.Series([""] * len(df))
    source = df["source"] if "source" in df.columns else pd.Series([""] * len(df))
    title = df["title"] if "title" in df.columns else pd.Series([""] * len(df))
    company = df["company"] if "company" in df.columns else pd.Series([""] * len(df))
    location = df["location"] if "location" in df.columns else pd.Series([""] * len(df))

    keys: List[str] = []
    for i in range(len(df)):
        u = _s(url.iloc[i])
        s = _s(source.iloc[i]).lower()
        j = _s(job_id.iloc[i])

        if u:
            keys.append(u)
            continue
        if s and j:
            keys.append(f"{s}:{j}")
            continue

        t = _s(title.iloc[i]).lower()
        c = _s(company.iloc[i]).lower()
        l = _s(location.iloc[i]).lower()
        keys.append(f"{s}:{t}:{c}:{l}".strip(":"))

    df["global_key"] = keys
    return df


def _parse_scraped_at(master: pd.DataFrame) -> pd.DataFrame:
    """
    Parse scraped_at to datetime safely (keeps original column but also provides parsed values).
    """
    if SORT_BY in master.columns:
        master[SORT_BY] = pd.to_datetime(master[SORT_BY], errors="coerce")
    return master


# =========================
# MAIN
# =========================
def main():
    print("\n🧩 BUILDING MASTER DATASET (always from latest portal files)")
    print("=" * 70)

    dfs: List[pd.DataFrame] = []
    counts: Dict[str, int] = {}

    for portal, path in FILES.items():
        if not os.path.exists(path):
            print(f"❌ Missing portal file: {portal} -> {path}")
            continue

        # Always read the latest portal Excel
        try:
            df = _read_excel_with_retry(path, retries=3, pause=1.5)
        except Exception as e:
            print(f"[WARN] Direct read failed for {portal}: {e}")
            cached = _copy_to_local_cache(path, LOCAL_CACHE_DIR)
            if not cached:
                print(f"❌ Could not read {portal} (direct+cache failed). Skipping.")
                continue
            df = _read_excel_with_retry(cached, retries=3, pause=1.0)

        df = _standardize_columns(df)
        df = _normalize_source(portal, df)
        df = _clean_placeholders(df)

        counts[portal] = len(df)
        dfs.append(df)
        print(f"✅ Loaded {portal}: rows={len(df)} cols={df.shape[1]}")

    if not dfs:
        print("\n❌ No portal files loaded. Master build aborted.")
        return

    master = pd.concat(dfs, ignore_index=True)

    # Ensure schema columns exist
    master = _ensure_columns(master, MASTER_SCHEMA)

    master = _ensure_taxonomy(master)

    # Build global key for dedupe
    master = _build_global_key(master)
    master["global_key"] = master["global_key"].astype("string").str.strip()

    # Reorder schema first
    remaining_cols = [c for c in master.columns if c not in MASTER_SCHEMA]
    master = master[MASTER_SCHEMA + remaining_cols]

    # Parse + sort so newest scraped_at is first (newest wins)
    master = _parse_scraped_at(master)
    if SORT_BY in master.columns:
        master = master.sort_values(SORT_BY, ascending=False, na_position="last").reset_index(drop=True)

    # Drop duplicates across all portals (keep newest)
    before = len(master)
    master = master.drop_duplicates(subset=["global_key"], keep="first").reset_index(drop=True)
    after = len(master)

    # Add metadata columns
    master["master_built_at"] = datetime.now().isoformat(timespec="seconds")

    # Save outputs atomically
    # 1) Always write LOCAL dashboard CSV (fast + stable)
    _ensure_dir(LOCAL_CACHE_DIR)
    _atomic_write_csv(master, LOCAL_DASH_CSV)

    # 2) Write to OneDrive outputs
    _atomic_write_excel(master, MASTER_XLSX)
    _atomic_write_csv(master, MASTER_CSV)

    print("\n✅ saved local dashboard csv:", LOCAL_DASH_CSV)
    print("✅ saved:", MASTER_XLSX, "rows:", len(master))
    print("✅ saved:", MASTER_CSV)
    print("   portal_rows_loaded:", counts)
    print(f"   dedupe_removed: {before - after}")
    print("Done.")


if __name__ == "__main__":
    main()