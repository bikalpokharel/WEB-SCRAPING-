from __future__ import annotations

import re
import time
from typing import Dict, List, Optional

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    WebDriverException,
    InvalidSessionIdException,
)

from scraper_core import clean, now_iso, infer_work_mode, classify_it_non_it, infer_country

BASE = "https://merojob.com"
SEARCH = f"{BASE}/search"


# -------------------------
# Driver health + safe navigation
# -------------------------
def _driver_alive(driver) -> bool:
    try:
        _ = driver.current_url
        return True
    except Exception:
        return False


def _safe_get(driver, url: str, timeout: int = 30) -> None:
    if not _driver_alive(driver):
        raise RuntimeError("DRIVER_DIED")

    try:
        driver.set_page_load_timeout(timeout)
        driver.get(url)

        # quick sanity: body exists
        try:
            WebDriverWait(driver, 8).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
        except Exception:
            pass

        # detect common block pages
        title = (driver.title or "").lower()
        body_text = ""
        try:
            body_text = (driver.find_element(By.TAG_NAME, "body").text or "").lower()
        except Exception:
            body_text = ""

        if ("attention required" in title) or ("cloudflare" in title) or ("verify you are human" in body_text):
            raise RuntimeError("BLOCKED_OR_CHALLENGE")

    except (InvalidSessionIdException, WebDriverException) as e:
        msg = str(e).lower()
        if ("invalid session id" in msg) or ("disconnected" in msg) or ("not connected to devtools" in msg):
            raise RuntimeError("DRIVER_DIED") from e
        raise


def _abs_url(href: str) -> str:
    href = (href or "").strip()
    if not href:
        return ""
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return BASE + href
    return BASE + "/" + href


def _text_after_label(full_text: str, label: str) -> Optional[str]:
    if not full_text:
        return None
    m = re.search(rf"{re.escape(label)}\s*([A-Za-z]{{3,}}\s+\d{{1,2}},\s+\d{{4}})", full_text)
    return clean(m.group(1)) if m else None


def _pick_text(driver, css_list: List[str]) -> Optional[str]:
    for sel in css_list:
        try:
            el = driver.find_element(By.CSS_SELECTOR, sel)
            t = clean(el.text)
            if t:
                return t
        except Exception:
            continue
    return None


def _pick_attr(driver, css_list: List[str], attr: str) -> Optional[str]:
    for sel in css_list:
        try:
            el = driver.find_element(By.CSS_SELECTOR, sel)
            v = clean(el.get_attribute(attr))
            if v:
                return v
        except Exception:
            continue
    return None


# -------------------------
# 1) COLLECT URLS (List page)
# -------------------------
def collect_job_urls(
    driver,
    pages: int = 1,
    limit: int = 200,
    per_page: int = 6,
    sleep_sec: float = 0.5,
) -> List[str]:
    pages = max(1, int(pages or 1))
    limit = int(limit or 200)
    per_page = int(per_page or 6)
    sleep_sec = float(sleep_sec or 0.0)

    urls: List[str] = []
    seen = set()

    for page in range(1, pages + 1):
        page_url = f"{SEARCH}?limit={per_page}&offset={page}"
        print(f"[MEROJOB] Collecting page {page}/{pages}: {page_url}")

        _safe_get(driver, page_url)

        try:
            WebDriverWait(driver, 25).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'h3 a[href^="/"]'))
            )
        except TimeoutException:
            print("[MEROJOB] No job links found (timeout). Skipping page.")
            if sleep_sec:
                time.sleep(sleep_sec)
            continue

        a_tags = driver.find_elements(By.CSS_SELECTOR, 'h3 a[href^="/"]')

        for a in a_tags:
            href = (a.get_attribute("href") or "").strip()
            if not href:
                continue
            if "/employer/" in href:
                continue
            if "/search" in href:
                continue

            u = _abs_url(href.replace(BASE, "")) if href.startswith(BASE) else _abs_url(href)
            if u and u not in seen:
                seen.add(u)
                urls.append(u)

            if len(urls) >= limit:
                break

        if len(urls) >= limit:
            break

        if sleep_sec:
            time.sleep(sleep_sec)

    return urls


# -------------------------
# 2) PARSE JOB DETAIL PAGE
# -------------------------
def parse_job_detail(driver, job_url: str) -> Optional[Dict]:
    job_url = (job_url or "").strip()
    if not job_url:
        return None

    print(f"[MEROJOB] Parsing: {job_url}")
    _safe_get(driver, job_url)

    try:
        WebDriverWait(driver, 25).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "h1"))
        )
    except TimeoutException:
        print("[MEROJOB] h1 not found (timeout). Skipping.")
        return None

    try:
        page_text = driver.find_element(By.TAG_NAME, "body").text or ""
    except Exception:
        page_text = ""

    title = _pick_text(driver, ["h1"])

    posted_date = _text_after_label(page_text, "Published on:")

    company_link = _pick_attr(driver, ['a[href*="/employer/"]'], "href")
    if company_link and company_link.startswith("/"):
        company_link = BASE + company_link

    company = _pick_text(driver, ['a[href*="/employer/"]'])
    if company:
        company = clean(company.split("\n")[0])

    location = _pick_text(driver, [
        '[data-sentry-component="JobHeader"] span',
        "span.text-muted",
    ])
    if not location:
        for pat in [
            r"\bJob Location\s*:\s*([^\n]+)",
            r"\bLocation\s*:\s*([^\n]+)",
            r"\bJob Location\s*([^\n]+)",
        ]:
            m = re.search(pat, page_text, flags=re.I)
            if m:
                location = clean(m.group(1))
                break

    # ✅ FIX 1: Always safe country (never empty)
    country = infer_country(location or "", default="Nepal") or "Nepal"

    employment_type = None
    for k in ["Full Time", "Part Time", "Contract", "Internship", "Freelance"]:
        if re.search(rf"\b{re.escape(k)}\b", page_text, flags=re.I):
            employment_type = k
            break

    position = None
    for k in ["Entry Level", "Mid Level", "Senior Level", "Top Level"]:
        if re.search(rf"\b{re.escape(k)}\b", page_text, flags=re.I):
            position = k
            break

    commitment = None
    m_exp = re.search(r"Experience\s*:\s*([^\n]+)", page_text, flags=re.I)
    if m_exp:
        commitment = clean(m_exp.group(1))

    compensation = None
    m_sal = re.search(r"Offered Salary\s*:\s*([^\n]+)", page_text, flags=re.I)
    if m_sal:
        compensation = clean(m_sal.group(1))

    skills = None
    if page_text:
        idx = page_text.lower().find("skills required")
        if idx != -1:
            chunk = page_text[idx: idx + 800]
            candidates = re.findall(r"\n([A-Za-z0-9\+\#\.\-\/ ]{2,40})\n", chunk)
            cleaned = []
            seen = set()
            for c in candidates:
                c2 = clean(c)
                if not c2:
                    continue
                if c2.lower() in {"skills required", "apply now", "job description"}:
                    continue
                if c2 not in seen:
                    seen.add(c2)
                    cleaned.append(c2)
            if cleaned:
                skills = ", ".join(cleaned[:30])

    work_mode = infer_work_mode(page_text)

    job_id = job_url.rstrip("/").split("/")[-1] if "/" in job_url else None

    category_primary = classify_it_non_it(
        designation=title or "",
        industry="",
        full_text=page_text or "",
    )

    row = {
        "job_id": job_id,
        "title": title,
        "company": company,
        "company_link": company_link,
        "location": location,
        "country": country,  # ✅ NEW field kept
        "posted_date": posted_date,
        "num_applicants": None,
        "work_mode": work_mode,
        "employment_type": employment_type,
        "position": position,
        "type": None,
        "compensation": compensation,
        "commitment": commitment,
        "skills": skills,
        "category_primary": category_primary,
        "job_url": job_url,
        "source": "merojob",
        "scraped_at": now_iso(),
    }

    # ✅ FIX 2: include "country" in ordered_keys so it doesn’t get dropped
    ordered_keys = [
        "job_id", "title", "company", "company_link",
        "location", "country",
        "posted_date", "num_applicants", "work_mode",
        "employment_type", "position", "type",
        "compensation", "commitment", "skills",
        "category_primary", "job_url", "source", "scraped_at"
    ]
    return {k: row.get(k) for k in ordered_keys}
