# analysis/backfill_taxonomy.py
from __future__ import annotations

import os
import sys
import time
from typing import Dict, List, Optional

import pandas as pd


# ============================================================
# Make project imports work when running:
#   python analysis/backfill_taxonomy.py
# ============================================================
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config import CONFIG  # noqa: E402
from scraper_core import categorize_role_taxonomy, clean  # noqa: E402


# ============================================================
# Settings
# ============================================================
PORTAL_FILES = {
    "merojob": os.path.join(CONFIG.data_dir, "merojob_jobs.xlsx"),
    "jobsnepal": os.path.join(CONFIG.data_dir, "jobsnepal_jobs.xlsx"),
    "linkedin": os.path.join(CONFIG.data_dir, "linkedin_jobs.xlsx"),
}

TAX_COLS = ["category_primary", "domain_l1", "domain_l2", "domain_l3", "tax_confidence"]

PLACEHOLDERS = {"", "non", "none", "na", "n/a", "-", "—", "<na>", "nan"}


# ============================================================
# Helpers
# ============================================================
def is_missing(x) -> bool:
    # pd.NA-safe missing check
    if pd.isna(x):
        return True
    s = str(x).strip()
    if not s:
        return True
    return s.lower() in PLACEHOLDERS


def _ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    for c in TAX_COLS:
        if c not in df.columns:
            df[c] = pd.NA
    return df


def _to_string_cols(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype("string")
    return df


def _atomic_write_excel(df: pd.DataFrame, out_path: str) -> None:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    tmp = out_path + f".tmp_{int(time.time())}.xlsx"
    df.to_excel(tmp, index=False, engine="openpyxl")
    os.replace(tmp, out_path)


def _normalize_tax_dict(tax: Dict) -> Dict[str, str]:
    """
    Make taxonomy stable and NEVER return 'Non' for domain_l3.
    (So old data actually gets filled with something visible.)
    """
    def nz(v, default: str) -> str:
        s = "" if v is None else str(v).strip()
        if not s or s.lower() in PLACEHOLDERS:
            return default
        return s

    out = {
        "category_primary": nz(tax.get("category_primary"), "Non-IT"),
        "domain_l1": nz(tax.get("domain_l1"), "Non-IT-Other"),
        "domain_l2": nz(tax.get("domain_l2"), "Other"),
        "domain_l3": nz(tax.get("domain_l3"), "Other"),  # ✅ KEY
        "tax_confidence": nz(tax.get("tax_confidence"), "0.35"),
    }
    return out


# ============================================================
# Backfill core
# ============================================================
def backfill_file(path: str, portal_name: str) -> None:
    if not os.path.exists(path):
        print(f"⚠️ {portal_name}: file not found -> {path}")
        return

    # Read
    df = pd.read_excel(path, engine="openpyxl")
    df = df.loc[:, ~df.columns.duplicated()].copy()  # defensive
    df = _ensure_cols(df)
    df = _to_string_cols(df, TAX_COLS)

    # Count missing before
    before_missing = df["domain_l3"].apply(is_missing).sum()

    changed_rows = 0
    updated_rows = 0

    for idx, row in df.iterrows():
        # Only backfill rows that are missing ANY of the taxonomy fields
        needs = any(is_missing(row.get(c)) for c in TAX_COLS)
        if not needs:
            continue

        title = clean(row.get("title")) or ""
        skills = clean(row.get("skills")) or ""
        position = clean(row.get("position")) or ""
        employment_type = clean(row.get("employment_type")) or ""
        industry = clean(row.get("industry")) or clean(row.get("category_primary")) or ""
        description = clean(row.get("description")) or ""

        # If description isn't present in your portal files, use skills as fallback
        if not description:
            description = skills

        tax = categorize_role_taxonomy(
            title=title,
            skills=skills,
            position=position,
            employment_type=employment_type,
            description=description,
            industry=industry,
        )
        tax = _normalize_tax_dict(tax)

        # Track change
        old_snapshot = {c: ("" if pd.isna(df.at[idx, c]) else str(df.at[idx, c]).strip()) for c in TAX_COLS}
        new_snapshot = {c: str(tax.get(c, "")).strip() for c in TAX_COLS}

        # Apply only where missing (prevents overwriting good values)
        wrote_any = False
        for c in TAX_COLS:
            if is_missing(df.at[idx, c]):
                df.at[idx, c] = new_snapshot[c]
                wrote_any = True

        if wrote_any:
            updated_rows += 1

        # consider "changed" if any field differs (even if it wasn't missing)
        if any(old_snapshot[c] != new_snapshot[c] and not is_missing(new_snapshot[c]) for c in TAX_COLS):
            changed_rows += 1

    after_missing = df["domain_l3"].apply(is_missing).sum()

    _atomic_write_excel(df, path)

    print(f"\n🔄 Backfilling taxonomy for {portal_name}...")
    print(f"✅ {portal_name} updated rows: {updated_rows}")
    print(f"   Rows changed : {changed_rows}")
    print(f"   Missing before (domain_l3): {before_missing}")
    print(f"   Missing after  (domain_l3): {after_missing}")


def main() -> None:
    for portal, path in PORTAL_FILES.items():
        backfill_file(path, portal)


if __name__ == "__main__":
    main()