"""
config.py
Central configuration for the Nepal job scraping pipeline.

All tunable constants live here to keep the codebase clean,
readable, and easy for other developers to understand.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ScrapeConfig:
    # -------------------------
    # General
    # -------------------------
    data_dir: str = "data"
    headless: bool = False

    # -------------------------
    # Collection
    # -------------------------
    pages: int = 20
    limit: int = 400

    # -------------------------
    # Politeness
    # -------------------------
    sleep_listing_sec: float = 2.0
    sleep_between_pages_sec: float = 0.5

    # -------------------------
    # Watch mode
    # -------------------------
    watch_default_interval_sec: int = 600

    # -------------------------
    # LinkedIn export input (CSV/XLSX)
    # -------------------------
    linkedin_exports_dir: str = "data/linkedin_exports"
    linkedin_export_pattern: str = "*.csv"  # newest file wins (also supports xlsx/xls via linkedin_export.py)

    # Optional explicit mapping overrides (use if auto-detect fails)
    linkedin_col_title: Optional[str] = None
    linkedin_col_company: Optional[str] = None
    linkedin_col_location: Optional[str] = None
    linkedin_col_url: Optional[str] = None
    linkedin_col_date: Optional[str] = None
    linkedin_col_description: Optional[str] = None


# ✅ Single shared config instance (run_pipeline imports this)
CONFIG = ScrapeConfig()
