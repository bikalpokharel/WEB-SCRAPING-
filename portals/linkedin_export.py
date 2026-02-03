"""
portals/linkedin_export.py

Robust LinkedIn integration via *your own exported data* (CSV or XLSX).

Fixes:
- Reads newest export from data/linkedin_exports (csv/xlsx/xls)
- Fuzzy-detects columns (regex) + supports config overrides
- Works even if there's NO URL column by generating linkedin://<hash>
- Prevents "all rows collapse to 1" by including row_index in fingerprint
- Prints detected mapping for transparency/debug
"""

from __future__ import annotations

import glob
import os
import re
import hashlib
from typing import Dict, List, Optional, Any

import pandas as pd

from scraper_core import (
    clean,
    clean_or_non,
    now_iso,
    classify_it_non_it,
    parse_experience_years,
    parse_salary,
)


# -------------------------
# File selection + reading
# -------------------------
def _newest_file(folder: str, patterns: List[str]) -> Optional[str]:
    paths: List[str] = []
    for pat in patterns:
        paths.extend(glob.glob(os.path.join(folder, pat)))
    if not paths:
        return None
    paths.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return paths[0]


def _safe_read(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext in [".xlsx", ".xls"]:
        return pd.read_excel(path, engine="openpyxl")
    try:
        return pd.read_csv(path)
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="latin1")


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower().replace("\ufeff", "") for c in df.columns]
    return df


# -------------------------
# Column detection
# -------------------------
def _pick_col(
    df: pd.DataFrame,
    *,
    explicit: Optional[str],
    exact_candidates: List[str],
    regex_candidates: List[str],
) -> Optional[str]:
    """
    Pick a column using:
      1) explicit config override (case-insensitive)
      2) exact matches
      3) regex match (first hit)
    """
    cols = list(df.columns)

    # 1) explicit override
    if explicit:
        exp = explicit.strip().lower()
        if exp in cols:
            return exp

    # 2) exact candidates
    for c in exact_candidates:
        cc = c.strip().lower()
        if cc in cols:
            return cc

    # 3) regex candidates
    for rgx in regex_candidates:
        pat = re.compile(rgx, re.I)
        for col in cols:
            if pat.search(col):
                return col

    return None


def _get_cell(row, col: Optional[str]) -> Optional[str]:
    if not col:
        return None
    try:
        return clean(row[col])
    except Exception:
        return None


# -------------------------
# Stable synthetic URL
# -------------------------
def _stable_hash(parts: List[Any]) -> str:
    s = " | ".join([str(p).strip().lower() for p in parts if str(p).strip()])
    if not s:
        s = "empty"
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _synthetic_job_url(file_basename: str, row_index: int, parts: List[Any]) -> str:
    # Always include row_index to avoid collisions when dataset fields are blank/repeated
    h = _stable_hash(parts + [file_basename, row_index])
    return f"linkedin://{h}"


# -------------------------
# Main
# -------------------------
def collect_rows(config) -> List[Dict]:
    folder = config.linkedin_exports_dir
    patterns = [config.linkedin_export_pattern, "*.xlsx", "*.xls"]

    export_path = _newest_file(folder, patterns)
    if not export_path:
        print(f"[LinkedInExport] No export file found in: {folder}")
        print(f"[LinkedInExport] Expected one of: {patterns}")
        return []

    df = _safe_read(export_path)
    df = _normalize_columns(df)

    file_basename = os.path.basename(export_path)

    # Column mapping with fuzzy detection + config overrides
    c_title = _pick_col(
        df,
        explicit=getattr(config, "linkedin_col_title", None),
        exact_candidates=["job title", "title", "position", "role", "job", "job_name"],
        regex_candidates=[r"(job[_\s-]*)?title\b", r"\bposition\b", r"\brole\b"],
    )

    c_company = _pick_col(
        df,
        explicit=getattr(config, "linkedin_col_company", None),
        exact_candidates=["company", "company name", "organization", "employer", "company_name"],
        regex_candidates=[r"\bcompany\b", r"org(anization)?\b", r"\bemployer\b", r"firm\b"],
    )

    c_location = _pick_col(
        df,
        explicit=getattr(config, "linkedin_col_location", None),
        exact_candidates=["location", "job location", "city", "job_location"],
        regex_candidates=[r"\blocation\b", r"\bcity\b", r"\bregion\b", r"\baddress\b"],
    )

    c_url = _pick_col(
        df,
        explicit=getattr(config, "linkedin_col_url", None),
        exact_candidates=["job url", "url", "link", "job link", "job_link", "posting url", "posting link"],
        regex_candidates=[r"\burl\b", r"\blink\b", r"\bhref\b"],
    )

    c_date = _pick_col(
        df,
        explicit=getattr(config, "linkedin_col_date", None),
        exact_candidates=["date saved", "saved date", "saved on", "date", "created at", "time", "saved_at"],
        regex_candidates=[r"\bdate\b", r"\btime\b", r"created", r"saved", r"posted"],
    )

    c_desc = _pick_col(
        df,
        explicit=getattr(config, "linkedin_col_description", None),
        exact_candidates=["description", "job description", "details", "summary"],
        regex_candidates=[r"desc(ription)?", r"\bsummary\b", r"\bdetails\b"],
    )

    print("[LinkedInExport] Detected mapping:")
    print(f"  title: {c_title}")
    print(f"  company: {c_company}")
    print(f"  location: {c_location}")
    print(f"  url: {c_url}")
    print(f"  date: {c_date}")
    print(f"  description: {c_desc}")

    rows: List[Dict] = []

    for idx, r in df.iterrows():
        title = _get_cell(r, c_title)
        company = _get_cell(r, c_company)
        location = _get_cell(r, c_location)
        raw_url = _get_cell(r, c_url)
        date_saved = _get_cell(r, c_date)
        desc = _get_cell(r, c_desc)

        if raw_url:
            job_url = raw_url
        else:
            parts = [title, company, location, date_saved]
            if desc:
                parts.append(desc[:120])
            job_url = _synthetic_job_url(file_basename, int(idx), parts)

        category_primary = classify_it_non_it(title or "", "", f"{company or ''} {location or ''} {desc or ''}")

        exp_min, exp_max = parse_experience_years(None)
        sal_min, sal_max, sal_currency, sal_period = parse_salary(None)

        rows.append(
            {
                "source": "LinkedInExport",
                "category_primary": clean_or_non(category_primary, default="Non"),
                "industry": "Non",
                "designation": clean_or_non(title, default="Non"),
                "company": clean_or_non(company, default="Non"),
                "level": "Non",

                "experience_raw": "Non",
                "experience_min_years": exp_min,
                "experience_max_years": exp_max,

                "onsite_hybrid_remote": "Non",

                "salary_raw": "Non",
                "salary_min": sal_min,
                "salary_max": sal_max,
                "salary_currency": clean_or_non(sal_currency, default="Non"),
                "salary_period": clean_or_non(sal_period, default="Non"),

                "location": clean_or_non(location, default="Non"),
                "deadline": "Non",
                "job_type": "Non",
                "date_posted": clean_or_non(date_saved, default="Non"),
                "job_url": clean_or_non(job_url, default="Non"),
                "scraped_at": now_iso(),
            }
        )

    print(f"[LinkedInExport] Loaded {len(rows)} rows from: {export_path}")
    return rows
