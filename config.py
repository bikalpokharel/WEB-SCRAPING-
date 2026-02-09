# config.py
from dataclasses import dataclass
from typing import Optional
import os


@dataclass(frozen=True)
class ScrapeConfig:
    # -------------------------
    # General
    # -------------------------
    data_dir: str = "/Users/bikal/Library/CloudStorage/OneDrive-Personal/Nepal_Job_Market_Live_Data/xlsx"
    headless: bool = False

    # -------------------------
    # Collection
    # -------------------------
    pages: int = 50
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
    # LinkedIn (IMPORTANT)
    # -------------------------
    linkedin_use_chrome_profile: bool = True

    chrome_profile_path: str = "data/chrome_profiles/linkedin"
    chrome_profile_dir: str = "Default"

    # Optional: only if you want to attempt username/password login (not used currently)
    linkedin_email: str = os.getenv("LINKEDIN_EMAIL", "")
    linkedin_password: str = os.getenv("LINKEDIN_PASSWORD", "")

    # LinkedIn export input (CSV/XLSX)
    linkedin_exports_dir: str = "data/linkedin_exports"
    linkedin_export_pattern: str = "*.csv"

    linkedin_col_title: Optional[str] = None
    linkedin_col_company: Optional[str] = None
    linkedin_col_location: Optional[str] = None
    linkedin_col_url: Optional[str] = None
    linkedin_col_date: Optional[str] = None
    linkedin_col_description: Optional[str] = None


CONFIG = ScrapeConfig()
