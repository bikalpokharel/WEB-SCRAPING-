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
LOCAL_CACHE_DIR = "/Users/bikal/Data_scraping/data_local"  # local cache to avoid OneDrive read timeouts
REPORT_FILE = os.path.join(DATA_DIR, "portal_quality_report.xlsx")

FILES = {
    "merojob": os.path.join(DATA_DIR, "merojob_jobs.xlsx"),
    "jobsnepal": os.path.join(DATA_DIR, "jobsnepal_jobs.xlsx"),
    "linkedin": os.path.join(DATA_DIR, "linkedin_jobs.xlsx"),
}

# Define which columns are "core" vs "optional" for quality reporting
CORE_COLS = [
    "job_id",
    "title",
    "company",
    "location",
    "posted_date",
    "job_url",
    "source",
    "scraped_at",
]

# Optional columns (everything else will be treated as optional if present)
OPTIONAL_COLS_HINT = [
    "company_link",
    "num_applicants",
    "work_mode",
    "employment_type",
    "position",
    "type",
    "compensation",
    "commitment",
    "skills",
    "category_primary",
]


# =========================
# HELPERS
# =========================
def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _read_excel_with_retry(path: str, retries: int = 3, pause: float = 1.5) -> pd.DataFrame:
    """
    Reads excel robustly (OneDrive sometimes times out).
    Retries and raises last exception if still failing.
    """
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
    Copies file into local cache and returns cached path.
    Returns None if copy fails.
    """
    try:
        _ensure_dir(cache_dir)
        dst_path = os.path.join(cache_dir, os.path.basename(src_path))
        shutil.copy2(src_path, dst_path)
        return dst_path
    except Exception as e:
        print(f"[WARN] Failed to copy to local cache: {src_path}\n  -> {e}")
        return None


def _compute_sparsity(df: pd.DataFrame) -> float:
    rows, cols = df.shape
    if rows == 0 or cols == 0:
        return 0.0
    return (df.isna().sum().sum() / (rows * cols)) * 100.0


def _compute_cols_above_threshold(df: pd.DataFrame, threshold_pct: float = 70.0) -> int:
    if df.shape[1] == 0:
        return 0
    col_missing_pct = df.isna().mean() * 100.0
    return int((col_missing_pct > threshold_pct).sum())


def _subset_sparsity(df: pd.DataFrame, cols: List[str]) -> float:
    present = [c for c in cols if c in df.columns]
    if not present:
        return 0.0
    sub = df[present]
    rows, ncols = sub.shape
    if rows == 0 or ncols == 0:
        return 0.0
    return (sub.isna().sum().sum() / (rows * ncols)) * 100.0


def _missing_by_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns per-column missing counts and %.
    """
    total_rows = len(df)
    miss = df.isna().sum()
    pct = (miss / total_rows) * 100 if total_rows else 0
    out = pd.DataFrame({
        "missing_count": miss,
        "missing_pct": pct,
    }).sort_values("missing_pct", ascending=False)
    out.index.name = "column"
    return out


# =========================
# MAIN
# =========================
def main():
    print("\n📊 PORTAL QUALITY SUMMARY")
    print("=" * 60)

    summary_rows = []
    per_portal_missing_tables: Dict[str, pd.DataFrame] = {}

    for portal, path in FILES.items():
        if not os.path.exists(path):
            print(f"\n❌ Missing file for {portal}: {path}")
            continue

        # 1) Try reading directly (OneDrive can timeout)
        try:
            df = _read_excel_with_retry(path, retries=3, pause=1.5)
        except Exception as e:
            print(f"\n[WARN] Direct read failed for {portal}: {e}")
            # 2) Fallback: copy to local cache then read
            cached = _copy_to_local_cache(path, LOCAL_CACHE_DIR)
            if not cached:
                print(f"❌ Could not read {portal} (direct+cache failed). Skipping.")
                continue
            df = _read_excel_with_retry(cached, retries=3, pause=1.0)

        # Normalize placeholders to NA
        df = df.replace(["Non", "non", ""], pd.NA)

        rows, cols = df.shape
        overall = _compute_sparsity(df)

        # core vs optional
        core = _subset_sparsity(df, CORE_COLS)
        # optional columns = present columns minus core (and keep hint ordering)
        present_optional = [c for c in df.columns if c not in CORE_COLS]
        optional = _subset_sparsity(df, present_optional)

        above_70 = _compute_cols_above_threshold(df, threshold_pct=70.0)

        summary_rows.append({
            "portal": portal,
            "rows": rows,
            "columns": cols,
            "overall_sparsity_%": round(overall, 2),
            "core_sparsity_%": round(core, 2),
            "optional_sparsity_%": round(optional, 2),
            "columns_above_70%_missing": above_70,
        })

        per_portal_missing_tables[portal] = _missing_by_column(df)

    if not summary_rows:
        print("\n❌ No portal files could be analyzed.")
        return

    summary_df = pd.DataFrame(summary_rows).sort_values("overall_sparsity_%")

    print(summary_df.to_string(index=False))

    # Save report with multiple sheets
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    with pd.ExcelWriter(REPORT_FILE, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="summary", index=False)

        # Each portal: missing-by-column
        for portal, miss_df in per_portal_missing_tables.items():
            sheet = f"{portal}_missing"
            # Excel sheet name limit: 31 chars
            sheet = sheet[:31]
            miss_df.to_excel(writer, sheet_name=sheet)

        # metadata sheet
        meta = pd.DataFrame([{
            "generated_at": ts,
            "data_dir": DATA_DIR,
            "local_cache_dir": LOCAL_CACHE_DIR,
            "notes": "If OneDrive times out, script copies files to local cache and retries.",
        }])
        meta.to_excel(writer, sheet_name="meta", index=False)

    print("\n✅ Saved portal quality report to:")
    print(REPORT_FILE)


if __name__ == "__main__":
    main()
