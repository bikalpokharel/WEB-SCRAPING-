"""
portals/jobsnepal.py

Scraper for JobsNepal (https://www.jobsnepal.com)

Key approach:
- Collect URLs safely using JS extraction (avoids stale element errors)
- Only keep real job-detail pages:
    https://jobsnepal.com/<slug>-<job_id>
    https://www.jobsnepal.com/<slug>-<job_id>
"""

from __future__ import annotations

import re
import time
from typing import Dict, List, Optional
from urllib.parse import urlparse

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from scraper_core import (
    clean,
    clean_or_non,
    now_iso,
    infer_work_mode,
    classify_it_non_it,
    parse_experience_years,
    parse_salary,
)

BASE = "https://www.jobsnepal.com"
LISTING_URL = "https://www.jobsnepal.com/jobs?page={page}"

# Real JobsNepal job pages look like:
#   https://jobsnepal.com/seo-specialist-140871
#   https://www.jobsnepal.com/seo-specialist-140871
JOB_DETAIL_RE = re.compile(r"^https://(www\.)?jobsnepal\.com/[a-z0-9\-]+-\d+$", re.IGNORECASE)


def _js_collect_links(driver) -> List[str]:
    """
    Collect all hrefs with JS in one shot to avoid StaleElementReferenceException.
    """
    hrefs = driver.execute_script(
        """
        return Array.from(document.querySelectorAll('a[href]'))
          .map(a => a.href)
          .filter(Boolean);
        """
    )
    out = []
    for h in hrefs:
        h = (h or "").split("#")[0].strip()
        if h:
            out.append(h)
    return out


def collect_job_urls(driver, pages: int = 10, limit: int = 200) -> List[str]:
    """
    Collect job detail URLs from JobsNepal listing pages.
    """
    urls: List[str] = []

    for p in range(1, pages + 1):
        listing_url = LISTING_URL.format(page=p)
        driver.get(listing_url)

        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        time.sleep(1.0)

        print(f"[Listing] {listing_url}  (current={driver.current_url})")

        hrefs = _js_collect_links(driver)

        for href in hrefs:
            if not JOB_DETAIL_RE.match(href):
                continue

            host = urlparse(href).netloc.lower()
            if "jobsnepal.com" not in host:
                continue

            urls.append(href)

        if len(urls) >= limit:
            break

        time.sleep(0.4)

    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)

    return out[:limit]


def parse_job_detail(driver, url: str) -> Optional[Dict]:
    """
    Parse a JobsNepal job page.

    We parse from whole-page text with regex because markup can vary.
    """
    driver.get(url)
    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.TAG_NAME, "body"))
    )

    text = driver.find_element(By.TAG_NAME, "body").text or ""
    if not text:
        return None

    if "Job posted on" not in text and "Apply before" not in text and "Posted Date" not in text:
        return None

    def rex(pattern: str) -> Optional[str]:
        m = re.search(pattern, text, flags=re.IGNORECASE)
        return clean(m.group(1)) if m else None

    # Title
    designation = None
    try:
        designation = clean(driver.find_element(By.TAG_NAME, "h1").text)
    except Exception:
        designation = None

    # Dates
    date_posted = rex(r"Job posted on\s*([0-9]{1,2}\s+[A-Za-z]{3,},\s+[0-9]{4})")
    deadline = rex(r"Apply before[:\s]*([0-9]{1,2}\s+[A-Za-z]{3,},\s+[0-9]{4})")

    # Industry/Category
    industry = rex(r"Category\s+([^\n]+)")

    # Experience
    experience_raw = rex(r"Experience\s+([^\n]+)")
    exp_min, exp_max = parse_experience_years(experience_raw)

    # Salary
    salary_raw = rex(r"Salary\s+([^\n]+)")
    sal_min, sal_max, sal_currency, sal_period = parse_salary(salary_raw)

    # Type/Level
    job_type = rex(r"Position Type\s+([^\n]+)")
    level = rex(r"Position Level\s+([^\n]+)")

    # Location
    location = rex(r"City\s+([^\n]+)")

    work_mode = infer_work_mode(text)
    category_primary = classify_it_non_it(designation or "", industry or "", text)

    return {
        "source": "JobsNepal",
        "category_primary": clean_or_non(category_primary, default="Non"),
        "industry": clean_or_non(industry, default="Non"),
        "designation": clean_or_non(designation, default="Non"),
        "level": clean_or_non(level, default="Non"),

        "experience_raw": clean_or_non(experience_raw, default="Non"),
        "experience_min_years": exp_min,
        "experience_max_years": exp_max,

        "onsite_hybrid_remote": clean_or_non(work_mode, default="Non"),

        "salary_raw": clean_or_non(salary_raw, default="Non"),
        "salary_min": sal_min,
        "salary_max": sal_max,
        "salary_currency": clean_or_non(sal_currency, default="Non"),
        "salary_period": clean_or_non(sal_period, default="Non"),

        "location": clean_or_non(location, default="Non"),
        "deadline": clean_or_non(deadline, default="Non"),
        "job_type": clean_or_non(job_type, default="Non"),
        "date_posted": clean_or_non(date_posted, default="Non"),
        "job_url": url,
        "scraped_at": now_iso(),
    }
