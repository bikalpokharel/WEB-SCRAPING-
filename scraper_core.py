# scraper_core.py
from __future__ import annotations

import os
import re
import tempfile
from datetime import datetime
from typing import Optional, Tuple

from selenium import webdriver
from selenium.common.exceptions import SessionNotCreatedException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


def make_fast_driver(headless: bool = True) -> webdriver.Chrome:
    """
    Default driver for non-LinkedIn portals.
    """
    opts = Options()
    opts.page_load_strategy = "eager"

    if headless:
        opts.add_argument("--headless=new")

    opts.add_argument("--window-size=1400,900")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")

    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    opts.add_argument("--disable-features=TranslateUI")
    opts.add_argument("--disable-background-networking")
    opts.add_argument("--disable-renderer-backgrounding")
    opts.add_argument("--disable-background-timer-throttling")
    opts.add_argument("--disable-backgrounding-occluded-windows")
    opts.add_argument("--disable-ipc-flooding-protection")

    opts.add_argument(
        "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=opts,
    )

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

    driver.set_page_load_timeout(90)
    return driver


def _build_chrome_options(headless: bool) -> Options:
    opts = Options()
    opts.page_load_strategy = "eager"

    if headless:
        opts.add_argument("--headless=new")

    opts.add_argument("--window-size=1400,900")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--disable-features=TranslateUI")
    opts.add_argument("--remote-debugging-port=0")  # helps some mac builds

    # Optional: user can set CHROME_BINARY if needed
    chrome_bin = os.getenv("CHROME_BINARY", "").strip()
    if chrome_bin:
        opts.binary_location = chrome_bin

    return opts


def make_linkedin_driver(headless: bool, profile_path: str, profile_dir: str) -> webdriver.Chrome:
    """
    ✅ Use a dedicated Selenium Chrome profile folder.
    This avoids profile locks/crashes from your real Chrome.
    """
    opts = Options()
    opts.page_load_strategy = "eager"

    if headless:
        opts.add_argument("--headless=new")

    opts.add_argument("--window-size=1400,900")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-blink-features=AutomationControlled")

    # ✅ Dedicated Selenium profile
    os.makedirs(profile_path, exist_ok=True)
    opts.add_argument(f"--user-data-dir={profile_path}")
    opts.add_argument(f"--profile-directory={profile_dir}")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=opts,
    )
    driver.set_page_load_timeout(90)
    return driver

def clean(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = re.sub(r"\s+", " ", str(s)).strip()
    return s if s else None


def clean_or_non(s: Optional[str], default: str = "Non") -> str:
    v = clean(s)
    return v if v else default


def now_iso() -> str:
    return datetime.utcnow().isoformat()


# --- (rest of your functions remain unchanged) ---
def infer_work_mode(text: str) -> Optional[str]:
    t = (text or "").lower()
    if any(k in t for k in ["remote", "work from home", "wfh"]):
        return "Remote"
    if "hybrid" in t:
        return "Hybrid"
    if "on-site" in t or "onsite" in t:
        return "On-site"
    return None


def classify_it_non_it(designation: str = "", industry: str = "", full_text: str = "") -> str:
    text = f"{designation} {industry} {full_text}".lower()
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


def normalize_experience_years(experience_raw: Optional[str]) -> Tuple[Optional[float], Optional[float]]:
    if not experience_raw:
        return None, None

    s = experience_raw.lower().strip()

    if "not required" in s or "no experience" in s:
        return 0.0, 0.0

    m = re.search(r"(\d+(\.\d+)?)\s*-\s*(\d+(\.\d+)?)\s*year", s)
    if m:
        return float(m.group(1)), float(m.group(3))

    m = re.search(r"(\d+(\.\d+)?)\s*\+\s*year", s)
    if m:
        return float(m.group(1)), None

    m = re.search(r"more than\s*(\d+(\.\d+)?)\s*year", s)
    if m:
        return float(m.group(1)), None

    m = re.search(r"(\d+(\.\d+)?)\s*year", s)
    if m:
        v = float(m.group(1))
        return v, v

    return None, None


def parse_experience_years(experience_raw: Optional[str]) -> Tuple[Optional[float], Optional[float]]:
    return normalize_experience_years(experience_raw)


def normalize_salary(salary_raw: Optional[str]) -> Tuple[Optional[int], Optional[int], Optional[str], Optional[str]]:
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
    return normalize_salary(salary_raw)
