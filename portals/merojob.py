import time
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from config import CONFIG
from scraper_core import (
    clean, clean_or_non,
    infer_work_mode, now_iso, classify_it_non_it,
    parse_experience_years, parse_salary
)



BASE = "https://merojob.com"
LISTING_URL = "https://merojob.com/search?page={page}"


def collect_job_urls(driver, pages: int = 3, limit: int = 60) -> List[str]:
    """
    Collect job detail URLs from MeroJob listing pages.

    Strategy:
      1) Prefer job-card links (more coverage + less noise)
      2) Apply strict filtering to keep only real job detail slugs
      3) Deduplicate while preserving order
      4) Fallback to all anchors if selectors change
    """
    urls: List[str] = []
    seen = set()

    blocked_slugs = {
        "company", "companies",
        "designation", "designations",
        "job-level", "job-levels",
        "category", "categories",
        "location", "locations",
        "search", "blog", "training", "events",
        "about", "contact", "faq",
        "login", "register",
        "employer", "jobseeker",
        "services", "skill",
        "employer-zone",
        "terms-and-conditions", "privacy-policy",
    }

    # These selectors try to target job title links in listing cards
    job_link_selectors = [
        # common job-title anchor patterns
        "h1 a[href]", "h2 a[href]", "h3 a[href]",
        "a.job-title[href]",
        # anchors inside possible listing containers
        ".search-list a[href]",
        ".job-card a[href]",
        ".card a[href]",
        "main a[href]",
    ]

    def normalize_job_url(href: str) -> Optional[str]:
        """Return standardized job detail URL or None if not valid."""
        if not href:
            return None

        href = urljoin(BASE, href).split("#")[0].strip()

        if not href.startswith(BASE):
            return None

        path = urlparse(href).path.strip("/")
        if not path:
            return None

        first_segment = path.split("/")[0].strip()

        # rule 1: remove known routes
        if first_segment.lower() in blocked_slugs:
            return None

        # rule 2: job slugs almost always contain "-" (e.g., architect-525)
        if "-" not in first_segment:
            return None

        # rule 3: avoid tiny junk
        if len(first_segment) < 8:
            return None

        return f"{BASE}/{first_segment}"

    for p in range(1, pages + 1):
        listing_url = LISTING_URL.format(page=p)
        driver.get(listing_url)
        time.sleep(CONFIG.sleep_listing_sec)

        print(f"[Listing] {listing_url}  (current={driver.current_url})")

        anchors = []
        for sel in job_link_selectors:
            found = driver.find_elements(By.CSS_SELECTOR, sel)
            if found:
                anchors.extend(found)

        # fallback if site changes markup
        if not anchors:
            anchors = driver.find_elements(By.CSS_SELECTOR, "a[href]")

        for a in anchors:
            raw_href = (a.get_attribute("href") or "").strip()
            job_url = normalize_job_url(raw_href)
            if not job_url:
                continue

            if job_url not in seen:
                seen.add(job_url)
                urls.append(job_url)

            if len(urls) >= limit:
                return urls[:limit]

        time.sleep(CONFIG.sleep_between_pages_sec)

    return urls[:limit]


def parse_job_detail(driver, url: str) -> Optional[Dict]:
    """
    Parse a job detail page and return a record dict.
    Returns None if page does not look like a job posting.
    """
    import re

    driver.get(url)

    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.TAG_NAME, "body"))
    )

    text = driver.find_element(By.TAG_NAME, "body").text or ""

    # Validate it's a real job page
    if "Published on:" not in text and "Apply Before:" not in text:
        return None

    def rex(pattern: str) -> Optional[str]:
        m = re.search(pattern, text, flags=re.IGNORECASE)
        return clean(m.group(1)) if m else None

    # Title
    designation = None
    try:
        designation = clean(driver.find_element(By.TAG_NAME, "h1").text)
    except Exception:
        designation = rex(r"\n([A-Za-z0-9][^\n]{3,120})\n\s*Views:\s*\d+")

    date_posted = rex(r"Published on:\s*([A-Za-z]{3,}\s+\d{1,2},\s+\d{4})")
    deadline = rex(r"Apply Before:\s*([A-Za-z]{3,}\s+\d{1,2},\s+\d{4})")

    level = rex(r"\|\s*(Entry Level|Mid Level|Senior Level|Top Level)\s*\|")
    job_type = rex(r"\|\s*(Full Time|Part Time|Contract|Temporary|Freelance|Internship|Traineeship|Volunteer)\s*\|")

    location = rex(r"Vacancy:\s*\d+\s*\|\s*([^\|]+)\|\s*Experience:")
    experience = rex(r"Experience:\s*([^\|]+)\|")
    salary = rex(r"Offered Salary:\s*([^\n]+)")
    exp_min, exp_max = parse_experience_years(experience or "")
    sal_min, sal_max, sal_currency, sal_period = parse_salary(salary or "")


    industry = rex(
        r"\n([A-Za-z][A-Za-z0-9\s\/,&\-\.\+]+)\n\s*\+\d+\s*more\s*\n\s*\|\s*\n\s*(Entry Level|Mid Level|Senior Level|Top Level)\b"
    )

    if not industry:
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        pub_idx = next((i for i, ln in enumerate(lines) if "Published on:" in ln), None)
        if pub_idx is not None and pub_idx + 1 < len(lines):
            possible = lines[pub_idx + 1]
            if "/" in possible:
                industry = clean(possible)

    work_mode = infer_work_mode(text)

    category_primary = classify_it_non_it(designation or "", industry or "", text or "")

    return {
    # -------------------------
    # Source & classification
    # -------------------------
    "source": "MeroJob",
    "category_primary": clean_or_non(category_primary),

    # -------------------------
    # Core job metadata
    # -------------------------
    "industry": clean_or_non(industry),
    "designation": clean_or_non(designation),
    "level": clean_or_non(level),

    # -------------------------
    # Experience (raw + normalized)
    # -------------------------
    "experience_raw": clean(experience),
    "experience_min_years": exp_min,
    "experience_max_years": exp_max,

    # -------------------------
    # Work mode
    # -------------------------
    "onsite_hybrid_remote": clean_or_non(work_mode),

    # -------------------------
    # Salary (raw + normalized)
    # -------------------------
    "salary_raw": clean(salary),
    "salary_min": sal_min,
    "salary_max": sal_max,
    "salary_currency": sal_currency,
    "salary_period": sal_period,

    # -------------------------
    # Other details
    # -------------------------
    "location": clean_or_non(location),
    "deadline": clean_or_non(deadline),
    "job_type": clean_or_non(job_type),
    "date_posted": clean_or_non(date_posted),

    # -------------------------
    # Tracking
    # -------------------------
    "job_url": url,
    "scraped_at": now_iso(),
}
