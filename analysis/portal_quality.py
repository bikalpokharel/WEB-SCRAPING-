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
REPORT_FILE = os.path.join(DATA_DIR, "portal_quality_report.xlsx")

FILES = {
    "merojob": os.path.join(DATA_DIR, "merojob_jobs.xlsx"),
    "jobsnepal": os.path.join(DATA_DIR, "jobsnepal_jobs.xlsx"),
    "linkedin": os.path.join(DATA_DIR, "linkedin_jobs.xlsx"),
    "master": os.path.join(DATA_DIR, "jobs_master.xlsx"),
}

PLACEHOLDERS = {
    "Non", "non", "", "N/A", "na", "NA", "-", "—", "None", "NONE",
    "<na>", "<NA>", "nan", "NaN", "NULL", "null"
}

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
        tmp = dst_path + f".tmp_{int(time.time())}"
        shutil.copy2(src_path, tmp)
        os.replace(tmp, dst_path)
        return dst_path
    except Exception as e:
        print(f"[WARN] Failed to copy to local cache: {src_path}\n  -> {e}")
        return None


def _clean_placeholders(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.replace(list(PLACEHOLDERS), pd.NA)

    for col in df.columns:
        if df[col].dtype == "object" or str(df[col].dtype).startswith("string"):
            df[col] = df[col].astype("string").str.strip()
            df[col] = df[col].replace("", pd.NA)

    return df


def _compute_sparsity(df: pd.DataFrame) -> float:
    rows, cols = df.shape
    if rows == 0 or cols == 0:
        return 0.0
    return (df.isna().sum().sum() / (rows * cols)) * 100.0


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
    total_rows = len(df)
    miss = df.isna().sum()
    pct = (miss / total_rows) * 100 if total_rows else 0

    out = pd.DataFrame({
        "missing_count": miss,
        "missing_pct": pct.round(2),
    })
    out = out.sort_values("missing_pct", ascending=False)
    out.index.name = "column"
    return out


def _atomic_write_excel_multisheet(sheets: Dict[str, pd.DataFrame], out_path: str) -> None:
    """
    Atomic write for multi-sheet Excel.
    Prevents corrupted Excel if interrupted.
    """
    _ensure_dir(os.path.dirname(out_path))
    tmp_path = out_path + f".tmp_{int(time.time())}.xlsx"

    with pd.ExcelWriter(tmp_path, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            sheet_clean = sheet_name[:31]  # Excel sheet limit
            df.to_excel(writer, sheet_name=sheet_clean, index=True if df.index.name else False)

    os.replace(tmp_path, out_path)


# =========================
# MAIN
# =========================
def main():
    print("\n📊 PORTAL QUALITY SUMMARY (recalculated every run)")
    print("=" * 70)

    summary_rows = []
    per_portal_missing_tables: Dict[str, pd.DataFrame] = {}

    for portal, path in FILES.items():
        if not os.path.exists(path):
            print(f"\n❌ Missing file for {portal}: {path}")
            continue

        try:
            df = _read_excel_with_retry(path)
        except Exception as e:
            print(f"\n[WARN] Direct read failed for {portal}: {e}")
            cached = _copy_to_local_cache(path, LOCAL_CACHE_DIR)
            if not cached:
                print(f"❌ Could not read {portal}. Skipping.")
                continue
            df = _read_excel_with_retry(cached)

        df = _clean_placeholders(df)

        rows, cols = df.shape
        overall = _compute_sparsity(df)
        core = _subset_sparsity(df, CORE_COLS)

        optional_cols = [c for c in df.columns if c not in CORE_COLS]
        optional = _subset_sparsity(df, optional_cols)

        col_missing_pct = df.isna().mean() * 100 if cols else pd.Series([])
        above_70 = int((col_missing_pct > 70).sum()) if cols else 0

        summary_rows.append({
            "portal": portal,
            "rows": rows,
            "columns": cols,
            "overall_sparsity_%": round(overall, 2),
            "core_sparsity_%": round(core, 2),
            "optional_sparsity_%": round(optional, 2),
            "columns_above_70%_missing": above_70,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
        })

        per_portal_missing_tables[portal] = _missing_by_column(df)

    if not summary_rows:
        print("\n❌ No portal files could be analyzed.")
        return

    summary_df = pd.DataFrame(summary_rows).sort_values("overall_sparsity_%")

    print(summary_df.to_string(index=False))

    # Prepare sheets
    sheets = {
        "summary": summary_df
    }

    for portal, miss_df in per_portal_missing_tables.items():
        sheets[f"{portal}_missing"] = miss_df

    sheets["meta"] = pd.DataFrame([{
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "data_dir": DATA_DIR,
        "local_cache_dir": LOCAL_CACHE_DIR,
        "notes": "Sparsity recalculated every run. Placeholders treated as missing.",
    }])

    # Atomic write
    _atomic_write_excel_multisheet(sheets, REPORT_FILE)

    print("\n✅ Saved portal quality report to:")
    print(REPORT_FILE)


if __name__ == "__main__":
    main()