import os
import time
import shutil
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd


# =========================
# CONFIG
# =========================
DATA_DIR = "/Users/bikal/Library/CloudStorage/OneDrive-Personal/Nepal_Job_Market_Live_Data/xlsx"
LOCAL_CACHE_DIR = "/Users/bikal/Data_scraping/data_local"

MASTER_XLSX = os.path.join(DATA_DIR, "jobs_master.xlsx")
MASTER_CSV = os.path.join(DATA_DIR, "jobs_master.csv")  # optional

FILES = {
    "merojob": os.path.join(DATA_DIR, "merojob_jobs.xlsx"),
    "jobsnepal": os.path.join(DATA_DIR, "jobsnepal_jobs.xlsx"),
    "linkedin": os.path.join(DATA_DIR, "linkedin_jobs.xlsx"),
}

# Your current schema is 18 columns. You can grow this toward 100 safely.
# Put the full 100 columns here later; script will create missing ones with NA.
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
    "scraped_at",
]

# Optional: sort output by scraped_at descending if present
SORT_BY = "scraped_at"


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
    try:
        _ensure_dir(cache_dir)
        dst_path = os.path.join(cache_dir, os.path.basename(src_path))
        shutil.copy2(src_path, dst_path)
        return dst_path
    except Exception as e:
        print(f"[WARN] Failed to copy to local cache: {src_path}\n  -> {e}")
        return None


def _normalize_source(portal_key: str, df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure `source` exists and is consistent.
    """
    if "source" not in df.columns:
        df["source"] = portal_key
    df["source"] = df["source"].fillna(portal_key).astype(str).str.strip()
    # normalize case for consistency
    df["source"] = df["source"].str.lower()
    return df


def _clean_placeholders(df: pd.DataFrame) -> pd.DataFrame:
    return df.replace(["Non", "non", ""], pd.NA)


def _ensure_columns(df: pd.DataFrame, schema: List[str]) -> pd.DataFrame:
    for c in schema:
        if c not in df.columns:
            df[c] = pd.NA
    return df


def _build_global_key(df: pd.DataFrame) -> pd.DataFrame:
    """
    Robust cross-portal key:
    - Prefer job_url if present
    - Else source:job_id
    - Else source:title:company:location (fallback)
    """
    # normalize strings
    def _s(x):
        if pd.isna(x):
            return ""
        return str(x).strip()

    url = df["job_url"] if "job_url" in df.columns else pd.Series([""] * len(df))
    job_id = df["job_id"] if "job_id" in df.columns else pd.Series([""] * len(df))
    source = df["source"] if "source" in df.columns else pd.Series([""] * len(df))

    title = df["title"] if "title" in df.columns else pd.Series([""] * len(df))
    company = df["company"] if "company" in df.columns else pd.Series([""] * len(df))
    location = df["location"] if "location" in df.columns else pd.Series([""] * len(df))

    keys = []
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

        # fallback composite key
        t = _s(title.iloc[i]).lower()
        c = _s(company.iloc[i]).lower()
        l = _s(location.iloc[i]).lower()
        keys.append(f"{s}:{t}:{c}:{l}".strip(":"))

    df["global_key"] = keys
    return df


def _standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fix common name inconsistencies across portals.
    Example: LinkedIn used it_non_it earlier; map it into category_primary.
    """
    if "it_non_it" in df.columns and "category_primary" not in df.columns:
        df["category_primary"] = df["it_non_it"]
    return df


def _safe_to_excel(df: pd.DataFrame, out_path: str) -> None:
    # avoid writing to temp/lock file
    base = os.path.basename(out_path)
    if base.startswith("~$"):
        raise ValueError(f"Refusing to write to Excel temp/lock file: {out_path}")
    df.to_excel(out_path, index=False, engine="openpyxl")


# =========================
# MAIN
# =========================
def main():
    print("\n🧩 BUILDING MASTER DATASET")
    print("=" * 60)

    dfs = []
    counts = {}

    for portal, path in FILES.items():
        if not os.path.exists(path):
            print(f"❌ Missing portal file: {portal} -> {path}")
            continue

        # read with retry; fallback to local cache if OneDrive stalls
        try:
            df = _read_excel_with_retry(path, retries=3, pause=1.5)
        except Exception as e:
            print(f"[WARN] Direct read failed for {portal}: {e}")
            cached = _copy_to_local_cache(path, LOCAL_CACHE_DIR)
            if not cached:
                print(f"❌ Could not read {portal} (direct+cache failed). Skipping.")
                continue
            df = _read_excel_with_retry(cached, retries=3, pause=1.0)

        df = _clean_placeholders(df)
        df = _standardize_columns(df)
        df = _normalize_source(portal, df)

        counts[portal] = len(df)
        dfs.append(df)

        print(f"✅ Loaded {portal}: rows={len(df)} cols={df.shape[1]}")

    if not dfs:
        print("\n❌ No portal files loaded. Master build aborted.")
        return

    master = pd.concat(dfs, ignore_index=True)

    # Ensure schema columns exist
    master = _ensure_columns(master, MASTER_SCHEMA)

    # Build global key for dedupe
    master = _build_global_key(master)

    # Reorder to master schema first, then the remaining columns
    remaining_cols = [c for c in master.columns if c not in MASTER_SCHEMA]
    master = master[MASTER_SCHEMA + remaining_cols]

    # Drop duplicates across all portals
    before = len(master)
    master["global_key"] = master["global_key"].astype(str).str.strip()
    master = master.drop_duplicates(subset=["global_key"]).reset_index(drop=True)
    after = len(master)

    # Optional sorting
    if SORT_BY in master.columns:
        try:
            master[SORT_BY] = pd.to_datetime(master[SORT_BY], errors="coerce")
            master = master.sort_values(SORT_BY, ascending=False).reset_index(drop=True)
        except Exception:
            pass

    # Save outputs
    _safe_to_excel(master, MASTER_XLSX)

    # CSV optional (good for pandas / dashboards)
    try:
        tmp_csv = MASTER_CSV + ".tmp"
        master.to_csv(tmp_csv, index=False)
        os.replace(tmp_csv, MASTER_CSV)  # atomic swap

        csv_saved = True
    except Exception:
        csv_saved = False

    print("\n✅ saved:", MASTER_XLSX, "rows:", len(master))
    print("   portal_rows_loaded:", counts)
    print(f"   dedupe_removed: {before - after}")

    if csv_saved:
        print("✅ saved:", MASTER_CSV)

    print("\nDone.")


if __name__ == "__main__":
    main()
