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
    LinkedIn driver:
    - Try dedicated profile first (persistent login)
    - If it fails (profile lock), fallback to temp profile (still works, but may need login)
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

    # Try dedicated profile first
    os.makedirs(profile_path, exist_ok=True)
    opts.add_argument(f"--user-data-dir={profile_path}")
    opts.add_argument(f"--profile-directory={profile_dir}")

    try:
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=opts,
        )
        driver.set_page_load_timeout(90)
        return driver
    except Exception as e:
        print("[WARN] Chrome session failed with dedicated profile (likely locked).")
        print("[WARN] Falling back to a temporary Chrome profile. Details:", e)

        # Fallback: temp profile
        tmp_profile = tempfile.mkdtemp(prefix="linkedin_tmp_profile_")
        opts2 = Options()
        opts2.page_load_strategy = "eager"
        if headless:
            opts2.add_argument("--headless=new")
        opts2.add_argument("--window-size=1400,900")
        opts2.add_argument("--disable-gpu")
        opts2.add_argument("--no-sandbox")
        opts2.add_argument("--disable-dev-shm-usage")
        opts2.add_argument("--disable-blink-features=AutomationControlled")
        opts2.add_argument(f"--user-data-dir={tmp_profile}")
        opts2.add_argument("--profile-directory=Default")

        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=opts2,
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

    if "hybrid" in t:
        return "Hybrid"
    if any(k in t for k in ["remote", "work from home", "wfh"]):
        return "Remote"
    if any(k in t for k in ["on-site", "onsite", "on site"]):
        return "On-site"
    return None

def infer_country(location_text: str | None, default: str = "Nepal") -> str:
    """
    Very simple country inference.
    For MeroJob/JobsNepal, defaulting to Nepal is usually correct.
    You can expand this later with more rules.
    """
    t = (location_text or "").lower().strip()

    # obvious Nepal matches
    if any(k in t for k in ["nepal", "kathmandu", "lalitpur", "bhaktapur", "pokhara", "butwal", "biratnagar", "dharan"]):
        return "Nepal"

    # if location looks empty/unknown, still default Nepal
    if not t or t in {"non", "none", "na", "n/a"}:
        return default

    # fallback default
    return default


def classify_it_non_it(designation: str = "", industry: str = "", full_text: str = "") -> str:
    text = f"{designation} {industry} {full_text}".lower()

    # ✅ longer phrases are safe as substring matches
    phrase_keywords = [
        "information technology",
        "software development", "software engineer",
        "full stack", "fullstack",
        "data engineer", "data scientist",
        "machine learning", "cybersecurity", "cyber security",
        "system administrator", "technical support", "help desk", "helpdesk",
        "cloud computing", "devops",
        "rest api", "api development",
    ]

    # ✅ single words must be WHOLE WORD matches (avoid false positives)
    word_keywords = [
        "developer", "programmer", "engineer",
        "python", "java", "javascript", "react", "node", "django", "flask",
        "php", "laravel",
        "docker", "kubernetes",
        "aws", "azure", "gcp",
        "network", "database", "sql",
        "ai", "ml", "qa", "sdet",
    ]

    special_patterns = [
        r"\.net\b",
        r"\bc\+\+\b",
        r"\bc#\b",
        r"\bgolang\b",
        r"\bgo\b",
    ]

    for k in phrase_keywords:
        if k in text:
            return "IT"

    for w in word_keywords:
        if re.search(rf"\b{re.escape(w)}\b", text):
            return "IT"

    for pat in special_patterns:
        if re.search(pat, text):
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
