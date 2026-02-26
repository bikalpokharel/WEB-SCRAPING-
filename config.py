# config.py
from dataclasses import dataclass
import os
from typing import Optional


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
    pages: int = 4
    limit: int = 300

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
    # LinkedIn
    # -------------------------
    linkedin_targets: tuple = (
        {"country": "Nepal", "geoId": "104630404"},
        {"country": "India", "geoId": "102713980"},
        {"country": "United States", "geoId": "103644278"},
        {"country": "United Kingdom", "geoId": "101165590"},
        {"country": "Canada", "geoId": "101174742"},
        {"country": "Australia", "geoId": "101452733"},
        {"country": "Germany", "geoId": "101282230"},
        {"country": "France", "geoId": "105015875"},
        {"country": "United Arab Emirates", "geoId": "106204383"},
        {"country": "Saudi Arabia", "geoId": "102890883"},
        {"country": "Qatar", "geoId": "105333783"},
        {"country": "Japan", "geoId": "101355337"},
        {"country": "South Korea", "geoId": "105149562"},
    )

    linkedin_page_size: int = 25

    linkedin_email: str = os.getenv("LINKEDIN_EMAIL", "")
    linkedin_password: str = os.getenv("LINKEDIN_PASSWORD", "")

    linkedin_exports_dir: str = "data/linkedin_exports"
    linkedin_export_pattern: str = "*.csv"

    linkedin_col_title: Optional[str] = None
    linkedin_col_company: Optional[str] = None
    linkedin_col_location: Optional[str] = None
    linkedin_col_url: Optional[str] = None
    linkedin_col_date: Optional[str] = None
    linkedin_col_description: Optional[str] = None


CONFIG = ScrapeConfig()