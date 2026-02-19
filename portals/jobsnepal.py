# portals/jobsnepal.py
from __future__ import annotations

import re
import time
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, urljoin

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from scraper_core import clean, clean_or_non, now_iso, infer_work_mode, infer_country

BASE = "https://www.jobsnepal.com"
LISTING_URL = "https://www.jobsnepal.com/jobs?page={page}"

JOB_DETAIL_RE = re.compile(r"^https://(www\.)?jobsnepal\.com/.+-(\d+)$", re.I)


# -------------------------
# URL COLLECTION
# -------------------------
def _js_collect_links(driver) -> List[str]:
    hrefs = driver.execute_script(
        """
        return Array.from(document.querySelectorAll('a[href]'))
          .map(a => a.href)
          .filter(Boolean);
        """
    )
    out: List[str] = []
    for h in hrefs:
        h = (h or "").split("#")[0].strip()
        if h:
            out.append(h)
    return out


def collect_job_urls(
    driver,
    pages: int = 10,
    limit: int = 200,
    per_page: int = 30,      # accepted for unified pipeline signature (not used)
    sleep_sec: float = 0.3,  # used for pacing
) -> List[str]:
    urls: List[str] = []

    pages = max(1, int(pages or 1))
    limit = int(limit or 200)
    sleep_sec = float(sleep_sec or 0.3)

    for p in range(1, pages + 1):
        driver.get(LISTING_URL.format(page=p))
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(sleep_sec)

        hrefs = _js_collect_links(driver)
        for href in hrefs:
            if not JOB_DETAIL_RE.match(href):
                continue
            if "jobsnepal.com" not in urlparse(href).netloc.lower():
                continue
            urls.append(href)

        if len(urls) >= limit:
            break

        time.sleep(sleep_sec)

    # dedupe preserve order
    seen = set()
    out: List[str] = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)

    return out[:limit]


# -------------------------
# HELPERS
# -------------------------
def _extract_job_id_from_url(url: str) -> Optional[str]:
    path = urlparse(url).path.strip("/")
    m = re.search(r"-(\d+)$", path)
    return m.group(1) if m else None


def _get_text(driver, css: str) -> Optional[str]:
    try:
        return clean(driver.find_element(By.CSS_SELECTOR, css).text)
    except Exception:
        return None


def _get_meta_content(driver, css: str) -> Optional[str]:
    try:
        el = driver.find_element(By.CSS_SELECTOR, css)
        return clean(el.get_attribute("content"))
    except Exception:
        return None


def _get_anchor_text_href(driver, css: str) -> Tuple[Optional[str], Optional[str]]:
    try:
        a = driver.find_element(By.CSS_SELECTOR, css)
        t = clean(a.text)
        href = (a.get_attribute("href") or "").strip()
        if href:
            href = urljoin(BASE, href)
        return t, (href or None)
    except Exception:
        return None, None


def _norm_label(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def _parse_overview_table(driver) -> Dict[str, Optional[str]]:
    out: Dict[str, Optional[str]] = {}

    try:
        rows = driver.find_elements(By.CSS_SELECTOR, "div.job-overview-inner table tr")
    except Exception:
        rows = []

    for tr in rows:
        try:
            tds = tr.find_elements(By.CSS_SELECTOR, "td")
            if len(tds) < 2:
                continue

            label = clean(tds[0].text) or ""
            value_td = tds[1]
            value_text = clean(value_td.text)
            lab = _norm_label(label)
            if not lab:
                continue

            out[lab] = value_text

            if lab == "category":
                spans = value_td.find_elements(By.CSS_SELECTOR, "span.font-weight-semibold")
                cats = [clean(s.text) for s in spans if clean(s.text)]
                out["categories"] = ", ".join(cats) if cats else value_text

            elif lab == "position type":
                spans = value_td.find_elements(By.CSS_SELECTOR, "span.font-weight-semibold")
                types = [clean(s.text) for s in spans if clean(s.text)]
                out["employment_type"] = ", ".join(types) if types else value_text

            elif lab == "position level":
                out["position"] = value_text

            elif lab == "salary":
                out["salary_raw"] = value_text

            elif lab == "posted date":
                out["posted_date"] = value_text

            elif lab == "city":
                city = None
                try:
                    city = clean(value_td.find_element(By.CSS_SELECTOR, "[itemprop='addressLocality']").text)
                except Exception:
                    city = None
                out["city"] = city or value_text

            elif lab == "education":
                spans = value_td.find_elements(By.CSS_SELECTOR, "span.font-weight-semibold")
                edu = [clean(s.text) for s in spans if clean(s.text)]
                out["education"] = ", ".join(edu) if edu else value_text

        except Exception:
            continue

    return out


def _get_job_description_text(driver) -> str:
    candidates = [
        "#div-job-details",
        "div#div-job-details span[itemprop='description']",
        "div.job-details-by-emloyer",
        "span[itemprop='description']",
    ]

    for css in candidates:
        try:
            el = driver.find_element(By.CSS_SELECTOR, css)
            txt = clean(el.text)
            if txt and len(txt) > 30:
                return txt
        except Exception:
            pass

    return ""


def _find_num_applicants(desc_text: str) -> Optional[str]:
    if not desc_text:
        return None
    t = re.sub(r"\s+", " ", desc_text)
    m = re.search(r"\b(\d{1,6})\s+applicants?\b", t, re.I)
    if m:
        return f"{m.group(1)} applicants"
    return None


def _extract_commitment(desc_text: str, ov: Dict[str, Optional[str]]) -> Optional[str]:
    for key in ["contract duration", "duration", "working hours", "work hours", "shift"]:
        v = clean(ov.get(key))
        if v:
            return v

    if not desc_text:
        return None

    m = re.search(r"\b(duration|contract\s+duration)\s*[:\-]\s*([^\n]+)", desc_text, re.I)
    if m:
        return clean(m.group(2))

    m = re.search(r"\bfor\s+(\d+)\s*(months?|month|years?|year)\b", desc_text, re.I)
    if m:
        return clean(f"{m.group(1)} {m.group(2)}")

    m = re.search(r"\b(working\s+hours?|work\s+hours?)\s*[:\-]\s*([^\n]+)", desc_text, re.I)
    if m:
        return clean(m.group(2))

    return None


def _extract_skills(desc_text: str, ov: Dict[str, Optional[str]]) -> Optional[str]:
    if desc_text:
        patterns = [
            r"(?:required\s+skills?|key\s+skills?|skills?|desired\s+skills?|competencies)\s*[:\-]\s*(.+)",
            r"(?:required\s+skills?|key\s+skills?|skills?|desired\s+skills?|competencies)\s*\n\s*(.+)",
        ]

        for pat in patterns:
            m = re.search(pat, desc_text, flags=re.I)
            if m:
                tail = m.group(1)

                stop_words = [
                    "\nhow to apply",
                    "\napplication",
                    "\napply",
                    "\nclick here",
                    "\nresponsibilities",
                    "\njob responsibilities",
                    "\nqualification",
                    "\nrequirements",
                    "\noverview",
                ]
                lower_tail = tail.lower()
                cut = len(tail)
                for sw in stop_words:
                    idx = lower_tail.find(sw)
                    if idx != -1:
                        cut = min(cut, idx)
                tail = tail[:cut].strip()

                tail = tail.replace("•", "\n").replace("–", "-")
                lines = [clean(x) for x in tail.splitlines()]
                lines = [x for x in lines if x and len(x) > 2]
                if lines:
                    return ", ".join(lines[:20])

        lines = [clean(x) for x in desc_text.splitlines()]
        bullet_lines = [
            x for x in lines
            if 3 <= len(x) <= 90 and any(k in x.lower() for k in ["skill", "ms office", "excel", "python", "seo", "communication", "presentation"])
        ]
        if bullet_lines:
            seen = set()
            uniq = []
            for b in bullet_lines:
                if b.lower() not in seen:
                    seen.add(b.lower())
                    uniq.append(b)
            return ", ".join(uniq[:15])

    edu = clean(ov.get("education"))
    cats = clean(ov.get("categories"))
    fallback_parts = [p for p in [edu, cats] if p]
    if fallback_parts:
        return ", ".join(fallback_parts)

    return None


def _category_primary(title: str, categories: str) -> str:
    t = (title or "").lower()
    c = (categories or "").lower()

    if "information technology" in c or "it jobs" in c:
        return "IT"

    it_terms = [
        "software", "developer", "engineer", "data", "ml", "ai",
        "devops", "security", "cyber", "network", "system admin",
        "frontend", "backend", "full stack", "mern", "django",
        "php", "python", "java", "react", "seo", "qa", "tester",
        "ui/ux", "ux", "ui", "cloud", "aws", "azure", "database",
    ]
    if any(term in t for term in it_terms):
        return "IT"

    return "Non-IT"


# -------------------------
# PARSER
# -------------------------
def parse_job_detail(driver, url: str) -> Optional[Dict]:
    driver.get(url)
    WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    time.sleep(0.2)

    desc_text = _get_job_description_text(driver)
    ov = _parse_overview_table(driver)

    job_id = _extract_job_id_from_url(url)

    title = (
        _get_text(driver, "div.job-details h1.job-title")
        or _get_text(driver, "h1.job-title")
        or _get_text(driver, "h1")
    )

    company = _get_text(driver, ".company-info .company-title")
    company_link = None

    if not company:
        company, company_link = _get_anchor_text_href(driver, "h3.job-company a[href]")

    if not company_link:
        _, company_link = _get_anchor_text_href(driver, "a[href^='employer/'], a[href*='/employer/']")

    categories = ov.get("categories")
    location = ov.get("city")

    # ✅ Country: JobsNepal is Nepal-focused. Always default Nepal.
    # Only override if infer_country confidently returns something else.
    inferred = infer_country(location or "", default="Nepal")
    country = inferred or "Nepal"

    employment_type = ov.get("employment_type")
    position = ov.get("position")
    salary_raw = ov.get("salary_raw")

    posted_date = _get_meta_content(driver, "meta[itemprop='datePosted']") or ov.get("posted_date")

    combined_for_mode = " ".join([desc_text or "", location or ""]).strip()
    work_mode = infer_work_mode(combined_for_mode)

    num_applicants = _find_num_applicants(desc_text)
    commitment = _extract_commitment(desc_text, ov)
    skills = _extract_skills(desc_text, ov)

    category_primary = _category_primary(title or "", categories or "")
    typ = employment_type

    return {
        "job_id": clean_or_non(job_id, default="Non"),
        "title": clean_or_non(title, default="Non"),
        "company": clean_or_non(company, default="Non"),
        "company_link": clean_or_non(company_link, default="Non"),

        "location": clean_or_non(location, default="Non"),
        "country": clean_or_non(country, default="Nepal"),  # ✅ will never be empty now
        "posted_date": clean_or_non(posted_date, default="Non"),
        "num_applicants": clean_or_non(num_applicants, default="Non"),

        "work_mode": clean_or_non(work_mode, default="Non"),
        "employment_type": clean_or_non(employment_type, default="Non"),

        "position": clean_or_non(position, default="Non"),
        "type": clean_or_non(typ, default="Non"),

        "compensation": clean_or_non(salary_raw, default="Non"),
        "commitment": clean_or_non(commitment, default="Non"),
        "skills": clean_or_non(skills, default="Non"),

        "category_primary": clean_or_non(category_primary, default="Non-IT"),

        "job_url": url,
        "source": "jobsnepal",
        "scraped_at": now_iso(),
    }
