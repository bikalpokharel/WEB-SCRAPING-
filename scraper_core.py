"""
scraper_core.py
Shared utilities for the Nepal job scraping pipeline.

This module is imported by portal scrapers (merojob.py, jobsnepal.py, etc.).
Keep it stable and backward-compatible so portals don't break.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Optional, Tuple

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


# -----------------------------------------------------------------------------
# Selenium Driver
# -----------------------------------------------------------------------------
def make_fast_driver(headless: bool = True) -> webdriver.Chrome:
    """
    Create a Selenium Chrome driver with mild stealth settings.
    Some sites behave differently if they detect automation; these options help.

    Note:
    - We keep JS enabled.
    - We block only images for speed (less suspicious than blocking CSS/fonts).
    """
    opts = Options()

    if headless:
        opts.add_argument("--headless=new")

    opts.add_argument("--window-size=1400,900")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")

    # Stealth-ish options
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    # Realistic UA (macOS Chrome)
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    )

    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.cookies": 1,
        "profile.managed_default_content_settings.javascript": 1,
    }
    opts.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=opts,
    )

    # Hide webdriver flag
    try:
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {
                "source": """
                    Object.defineProperty(navigator, 'webdriver', {
                      get: () => undefined
                    })
                """
            },
        )
    except Exception:
        pass

    driver.set_page_load_timeout(45)
    return driver


# -----------------------------------------------------------------------------
# Common helpers
# -----------------------------------------------------------------------------
def clean(s: Optional[str]) -> Optional[str]:
    """Normalize whitespace and trim."""
    if s is None:
        return None
    s = re.sub(r"\s+", " ", str(s)).strip()
    return s if s else None


def clean_or_non(s: Optional[str], default: str = "Non") -> str:
    """
    Clean a string; if None/blank return default.
    You requested missing values should be stored as 'Non'.
    """
    v = clean(s)
    return v if v else default


def now_iso() -> str:
    """UTC timestamp for scraped_at."""
    return datetime.utcnow().isoformat()


def infer_work_mode(text: str) -> Optional[str]:
    """Infer Remote/Hybrid/On-site from page text."""
    t = (text or "").lower()
    if any(k in t for k in ["remote", "work from home", "wfh"]):
        return "Remote"
    if "hybrid" in t:
        return "Hybrid"
    if "on-site" in t or "onsite" in t:
        return "On-site"
    return None


# -----------------------------------------------------------------------------
# IT vs Non-IT classifier (shared)
# -----------------------------------------------------------------------------
def classify_it_non_it(designation: str = "", industry: str = "", full_text: str = "") -> str:
    """
    Classify a job into IT or Non-IT using keyword heuristics.

    Returns:
        "IT" or "Non-IT"
    """
    text = f"{designation} {industry} {full_text}".lower()

    # Important: treat "it" carefully (marketing contains "it")
    if re.search(r"\bit\b", text):
        return "IT"

    it_keywords = [
        "information technology", "software", "developer", "engineer",
        "backend", "frontend", "full stack", "fullstack", "devops",
        "cloud", "aws", "azure", "gcp", "kubernetes", "docker",
        "data engineer", "data scientist", "machine learning", "ml", "ai",
        "cyber security", "cybersecurity", "security engineer",
        "qa", "test engineer", "automation", "sdet",
        "network", "system admin", "sysadmin", "database", "dba",
        "python", "java", "javascript", "react", "node", "django", "flask",
        "php", "laravel", "dotnet", ".net", "c#", "c++", "golang", "go",
        "android", "ios", "mobile app", "app developer",
        "ui/ux", "ux", "ui designer", "product designer",
        "technical support", "helpdesk", "support engineer",
    ]

    if any(k in text for k in it_keywords):
        return "IT"

    return "Non-IT"


# -----------------------------------------------------------------------------
# Experience normalization
# -----------------------------------------------------------------------------
def normalize_experience_years(experience_raw: Optional[str]) -> Tuple[Optional[float], Optional[float]]:
    """
    Convert experience text into (min_years, max_years).

    Examples:
        "More than 5 years" -> (5, None)
        "1-3 years" -> (1, 3)
        "2+ years" -> (2, None)
        "Not Required" -> (0, 0)
    """
    if not experience_raw:
        return None, None

    s = experience_raw.lower().strip()

    if "not required" in s or "no experience" in s:
        return 0.0, 0.0

    # Range: 1-3 years
    m = re.search(r"(\d+(\.\d+)?)\s*-\s*(\d+(\.\d+)?)\s*year", s)
    if m:
        return float(m.group(1)), float(m.group(3))

    # 2+ years
    m = re.search(r"(\d+(\.\d+)?)\s*\+\s*year", s)
    if m:
        return float(m.group(1)), None

    # More than 5 years
    m = re.search(r"more than\s*(\d+(\.\d+)?)\s*year", s)
    if m:
        return float(m.group(1)), None

    # Single value: 2 years
    m = re.search(r"(\d+(\.\d+)?)\s*year", s)
    if m:
        v = float(m.group(1))
        return v, v

    return None, None


def parse_experience_years(experience_raw: Optional[str]) -> Tuple[Optional[float], Optional[float]]:
    """Backward-compatible name."""
    return normalize_experience_years(experience_raw)


# -----------------------------------------------------------------------------
# Salary normalization
# -----------------------------------------------------------------------------
def normalize_salary(salary_raw: Optional[str]) -> Tuple[Optional[int], Optional[int], Optional[str], Optional[str]]:
    """
    Parse salary into (min, max, currency, period).

    Handles:
        "Not Disclosed"
        "Based on experience"
        "NPR 40,000 - 60,000 / Month"
        "Rs. 50,000"
    """
    if not salary_raw:
        return None, None, None, None

    s = salary_raw.strip()
    low = s.lower()

    if any(k in low for k in ["not disclosed", "based on experience", "negotiable"]):
        return None, None, None, None

    currency = None
    if any(k in low for k in ["npr", "rs", "रु"]):
        currency = "NPR"

    period = None
    if "month" in low:
        period = "month"
    elif "year" in low or "annum" in low:
        period = "year"
    elif "day" in low:
        period = "day"

    nums = [int(x.replace(",", "")) for x in re.findall(r"(\d[\d,]*)", s)]
    if not nums:
        return None, None, currency, period

    if len(nums) == 1:
        return nums[0], nums[0], currency, period

    return min(nums), max(nums), currency, period


def parse_salary(salary_raw: Optional[str]) -> Tuple[Optional[int], Optional[int], Optional[str], Optional[str]]:
    """Backward-compatible name."""
    return normalize_salary(salary_raw)
