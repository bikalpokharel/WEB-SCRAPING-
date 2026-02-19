# /Users/bikal/Data_scraping/portals/linkedin.py
from __future__ import annotations

import time
import re
import logging
from typing import List, Dict, Optional, Tuple
from urllib.parse import quote_plus

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import (
    TimeoutException,
    StaleElementReferenceException,
    InvalidSessionIdException,
    WebDriverException,
)

from scraper_core import make_linkedin_driver, clean, now_iso, classify_it_non_it

logger = logging.getLogger("linkedin")

WAIT_TIMEOUT = 25

# -------------------------
# IMPORTANT: Persistent Chrome profile (keeps you logged in)
# -------------------------
PROFILE_PATH = "/Users/bikal/Data_scraping/_chrome_profiles/linkedin"
PROFILE_DIR = "Default"  # change to "Profile 1" if your LinkedIn is logged in there

# -------------------------
# Selectors (LEFT list variants)
# -------------------------
JOB_CARD_SELECTORS = [
    "div.job-card-container[data-job-id]",
    "li.jobs-search-results__list-item",
    "div.job-card-container",
]

JOB_LINK_SELECTORS = [
    "a.job-card-container__link",
    "a.job-card-list__title--link",
    "a[href*='/jobs/view/']",
]

# -------------------------
# Selectors (RIGHT detail variants)
# -------------------------
DETAIL_READY_SELECTORS = [
    "div.jobs-search__job-details--container",
    "div.job-details-jobs-unified-top-card__container--two-pane",
    "div.jobs-details__main-content",
    "div.job-view-layout.jobs-details",
]

TITLE_SELECTORS = [
    "div.job-details-jobs-unified-top-card__job-title h1 a",
    "h1 a[href*='/jobs/view/']",
]

COMPANY_SELECTORS = [
    "div.job-details-jobs-unified-top-card__company-name a",
    "a[data-test-app-aware-link][href*='/company/']",
]

TERTIARY_SELECTORS = [
    "div.job-details-jobs-unified-top-card__tertiary-description-container",
]

PREF_STRONG_SELECTOR = "div.job-details-fit-level-preferences button strong"

ABOUT_JOB_MT4_SELECTORS = [
    "div.jobs-description__content div.mt4",
    "div.jobs-description-content__text--stretch div.mt4",
]

SKILLS_HEADER_SELECTOR = "h3.js-skills-header"


# ============================================================
# NEW: Multi-country listing URL builder (country geoId-based)
# ============================================================
def build_listing_url(country: str, geo_id: str, start: int = 0) -> str:
    """
    geoId is LinkedIn's location identifier.
    sortBy=R => most recent.
    start = 0,25,50...
    """
    loc = quote_plus(country or "")
    return f"https://www.linkedin.com/jobs/search/?geoId={geo_id}&location={loc}&sortBy=R&start={start}"


# -------------------------
# Popup close
# -------------------------
def _close_popups(driver) -> None:
    selectors = [
        "button.modal__dismiss",
        "button.artdeco-modal__dismiss",
        "button[aria-label='Dismiss']",
        "button[aria-label='Close']",
        "button[aria-label='Close dialog']",
    ]
    for sel in selectors:
        try:
            for b in driver.find_elements(By.CSS_SELECTOR, sel)[:3]:
                try:
                    if b.is_displayed() and b.is_enabled():
                        b.click()
                        time.sleep(0.15)
                except Exception:
                    pass
        except Exception:
            pass


# -------------------------
# Auth detection
# -------------------------
def _is_on_authwall(driver) -> bool:
    """
    Returns True if LinkedIn authwall/login/checkpoint is detected.
    IMPORTANT: Must be safe when Selenium session dies.
    """
    try:
        u = (driver.current_url or "").lower()
    except (InvalidSessionIdException, WebDriverException):
        # session is dead; caller should recreate driver
        return False
    except Exception:
        return False

    if ("authwall" in u) or ("/login" in u) or ("checkpoint" in u):
        return True

    login_selectors = [
        "input#username",
        "input#password",
        "form.login__form",
        "button[type='submit']",
    ]
    try:
        for sel in login_selectors:
            if driver.find_elements(By.CSS_SELECTOR, sel):
                return True
    except (InvalidSessionIdException, WebDriverException):
        return False
    except Exception:
        pass

    return False



def _maybe_prompt_login_if_needed(driver) -> None:
    if _is_on_authwall(driver):
        print("\n🔐 LinkedIn login detected.")
        print("✅ Please log in in the opened Chrome window.")
        print("⚠️ IMPORTANT: Do NOT close Chrome. Just log in.")
        input("Press ENTER here after login is complete...\n")


# -------------------------
# Helpers
# -------------------------
def _safe_click(driver, el) -> bool:
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        time.sleep(0.15)
        el.click()
        return True
    except Exception:
        try:
            driver.execute_script("arguments[0].click();", el)
            return True
        except Exception:
            return False


def _wait_for_any(driver, selectors: List[str], timeout: int = WAIT_TIMEOUT) -> Optional[str]:
    end = time.time() + timeout
    while time.time() < end:
        for sel in selectors:
            try:
                if driver.find_elements(By.CSS_SELECTOR, sel):
                    return sel
            except Exception:
                pass
        time.sleep(0.2)
    raise TimeoutException(f"Timeout waiting for any of: {selectors}")


def _find_first(driver, selectors: List[str]):
    for sel in selectors:
        for _ in range(3):
            try:
                return driver.find_element(By.CSS_SELECTOR, sel)
            except StaleElementReferenceException:
                time.sleep(0.2)
                continue
            except Exception:
                break
    return None


def _find_all_first_match(driver, selectors: List[str]):
    for sel in selectors:
        try:
            els = driver.find_elements(By.CSS_SELECTOR, sel)
            if els:
                return els
        except Exception:
            continue
    return []


def _get_text_from_any(driver, selectors: List[str]) -> Optional[str]:
    for _ in range(3):
        try:
            el = _find_first(driver, selectors)
            return clean(el.text) if el else None
        except StaleElementReferenceException:
            time.sleep(0.2)
            continue
        except Exception:
            return None
    return None


def _get_anchor_text_href_from_any(driver, selectors: List[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Stale-safe: LinkedIn rerenders the top card, so element handles go stale.
    Retry by re-finding and re-reading.
    """
    for _ in range(4):
        try:
            el = _find_first(driver, selectors)
            if not el:
                return None, None
            txt = clean(el.text)
            href = el.get_attribute("href") or None
            return txt, href
        except StaleElementReferenceException:
            time.sleep(0.25)
            continue
        except Exception:
            return None, None
    return None, None


def _get_left_scroll_container(driver):
    selectors = [
        "div.scaffold-layout__list",
        "div.scaffold-layout__list-container",
        "div.jobs-search-results-list",
        "div.jobs-search-results-list__container",
    ]
    for sel in selectors:
        try:
            return driver.find_element(By.CSS_SELECTOR, sel)
        except Exception:
            pass
    return None


def _extract_job_id_from_card(card) -> Optional[str]:
    try:
        jid = clean(card.get_attribute("data-job-id"))
        if jid and jid.isdigit():
            return jid
    except Exception:
        pass

    for sel in JOB_LINK_SELECTORS:
        try:
            a = card.find_element(By.CSS_SELECTOR, sel)
            href = a.get_attribute("href") or ""
            m = re.search(r"/jobs/view/(\d+)", href)
            if m:
                return m.group(1)
        except Exception:
            pass

    return None


def _scroll_left_results_until_loaded(driver, target=25, timeout=55):
    """
    Scroll LEFT results pane until target unique job ids are mounted in DOM
    (LinkedIn virtualizes job cards).
    """
    end = time.time() + timeout
    container = _get_left_scroll_container(driver)

    seen = set()
    last_count = 0
    stable_loops = 0

    while time.time() < end:
        _close_popups(driver)

        cards = _find_all_first_match(driver, JOB_CARD_SELECTORS)

        for c in cards:
            jid = _extract_job_id_from_card(c)
            if jid:
                seen.add(jid)

        if len(seen) >= int(target * 0.95):
            return cards

        if len(seen) == last_count:
            stable_loops += 1
        else:
            stable_loops = 0
            last_count = len(seen)

        if stable_loops >= 12:
            return cards

        try:
            if container:
                driver.execute_script("arguments[0].scrollTop += 900;", container)
            else:
                driver.execute_script("window.scrollBy(0, 900);")
        except Exception:
            pass

        time.sleep(0.9)

    return _find_all_first_match(driver, JOB_CARD_SELECTORS)


def _parse_tertiary(tertiary_text: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    t = (tertiary_text or "").replace("\n", " ")
    t = re.sub(r"\s+", " ", t).strip()
    if not t:
        return None, None, None

    parts = [p.strip() for p in t.split("·") if p.strip()]
    location = parts[0] if len(parts) >= 1 else None
    posted_date = None
    num_applicants = None

    for p in parts[1:]:
        low = p.lower()
        if "ago" in low or "today" in low or "yesterday" in low:
            posted_date = p
        if "applicant" in low:
            num_applicants = p

    return clean(location), clean(posted_date), clean(num_applicants)


def _parse_prefs(pref_texts: List[str]) -> Tuple[Optional[str], Optional[str]]:
    work_mode = None
    employment_type = None

    for p in pref_texts or []:
        if not p:
            continue
        low = p.lower()

        if "$" in p or "₹" in p or "rs" in low or "npr" in low or "/hr" in low or "per hour" in low:
            continue

        if low in {"remote", "hybrid", "on-site", "onsite"}:
            work_mode = "On-site" if low in {"on-site", "onsite"} else p.title()

        if low in {"full-time", "part-time", "contract", "internship", "temporary"}:
            employment_type = "Full-time" if low == "full-time" else p.title()

    return clean(work_mode), clean(employment_type)


def _parse_optional_kv(text_block: str) -> Dict[str, Optional[str]]:
    t = (text_block or "")
    t = re.sub(r"\s+", " ", t).strip()
    kv = {"position": None, "type": None, "compensation": None, "commitment": None}

    def grab(label: str) -> Optional[str]:
        m = re.search(
            rf"{label}\s*:\s*(.+?)(?=(Position|Type|Compensation|Location|Commitment)\s*:|$)",
            t,
            re.I,
        )
        return clean(m.group(1)) if m else None

    kv["position"] = grab("Position")
    kv["type"] = grab("Type")
    kv["compensation"] = grab("Compensation")
    kv["commitment"] = grab("Commitment")
    return kv


def _extract_skills(driver) -> Optional[str]:
    try:
        header = driver.find_element(By.CSS_SELECTOR, SKILLS_HEADER_SELECTOR)
        p = header.find_element(By.XPATH, "following::p[1]")
        return clean(p.text)
    except Exception:
        return None


def _driver_alive(driver) -> bool:
    try:
        _ = driver.current_url
        return True
    except Exception:
        return False


def _open(driver, url: str, attempts: int = 3):
    """
    Returns (ok, driver). If driver session dies, recreates it.
    """
    for i in range(1, attempts + 1):
        try:
            logger.info(f"Opening: {url} (attempt {i}/{attempts})")
            driver.get(url)
            time.sleep(3.0)
            _close_popups(driver)
            return True, driver

        except (InvalidSessionIdException, WebDriverException) as e:
            logger.warning(f"Driver session died while opening URL: {e}")

            try:
                driver.quit()
            except Exception:
                pass

            driver = make_linkedin_driver(
                headless=False,
                profile_path=PROFILE_PATH,
                profile_dir=PROFILE_DIR,
            )
            time.sleep(2.0)
            continue

        except Exception as e:
            logger.warning(f"Open failed attempt {i}/{attempts}: {e}")
            time.sleep(2.0)

    return False, driver


# ============================================================
# MAIN ENTRY (CALLED BY run_pipeline.py rows mode)
# ============================================================
def linkedin_parse(config) -> List[Dict]:
    """
    Multi-country LinkedIn scraper.
    - Loops over config.linkedin_targets
    - Adds `country` column for every row
    """
    rows: List[Dict] = []
    seen_ids = set()

    pages = int(getattr(config, "pages", 1) or 1)
    limit = int(getattr(config, "limit", 60) or 60)
    page_size = int(getattr(config, "linkedin_page_size", 25) or 25)

    # NEW: multi-country targets (list of dicts with country + geoId)
    targets = list(getattr(config, "linkedin_targets", []) or [])
    if not targets:
        # fallback if you forget to set config
        targets = [{"country": "Nepal", "geoId": "104630404"}]

    driver = make_linkedin_driver(
        headless=False,
        profile_path=PROFILE_PATH,
        profile_dir=PROFILE_DIR,
    )

    try:
        # Ensure logged in
        ok, driver = _open(driver, "https://www.linkedin.com/jobs/", attempts=3)
        if not ok:
            return rows
        _maybe_prompt_login_if_needed(driver)

        for t in targets:
            if limit and len(rows) >= limit:
                break

            country = str(t.get("country", "")).strip() or "Unknown"
            geo_id = str(t.get("geoId", "")).strip()
            if not geo_id:
                logger.warning(f"[SKIP] Missing geoId for target: {t}")
                continue

            logger.info(f"\n🌍 TARGET: {country} | geoId={geo_id}")

            for page_index in range(1, pages + 1):
                if limit and len(rows) >= limit:
                    break

                start = (page_index - 1) * page_size
                listing_url = build_listing_url(country=country, geo_id=geo_id, start=start)

                ok, driver = _open(driver, listing_url, attempts=3)
                if not ok:
                    break

                _maybe_prompt_login_if_needed(driver)

                try:
                    _wait_for_any(driver, JOB_CARD_SELECTORS, timeout=WAIT_TIMEOUT)
                except TimeoutException:
                    logger.warning(f"[{country}] No job cards found.")
                    break

                logger.info(f"[{country}] Page {page_index}/{pages} start={start}")

                cards = _scroll_left_results_until_loaded(driver, target=page_size, timeout=55)
                logger.info(f"[{country}] DOM-mounted cards: {len(cards)} (target={page_size})")
                if not cards:
                    break

                for card in cards:
                    if limit and len(rows) >= limit:
                        break

                    _close_popups(driver)

                    job_id = _extract_job_id_from_card(card)
                    if not job_id:
                        continue
                    if job_id in seen_ids:
                        continue
                    seen_ids.add(job_id)

                    link = None
                    for sel in JOB_LINK_SELECTORS:
                        try:
                            link = card.find_element(By.CSS_SELECTOR, sel)
                            break
                        except Exception:
                            continue
                    if not link:
                        continue

                    if not _safe_click(driver, link):
                        continue

                    time.sleep(1.2)
                    _close_popups(driver)

                    if _is_on_authwall(driver):
                        logger.warning(f"[{country}] authwall appeared after click")
                        _maybe_prompt_login_if_needed(driver)
                        continue

                    try:
                        _wait_for_any(driver, DETAIL_READY_SELECTORS, timeout=WAIT_TIMEOUT)
                    except TimeoutException:
                        continue

                    time.sleep(0.6)

                    title = _get_text_from_any(driver, TITLE_SELECTORS)
                    company, company_link = _get_anchor_text_href_from_any(driver, COMPANY_SELECTORS)

                    tertiary_text = _get_text_from_any(driver, TERTIARY_SELECTORS) or ""
                    location, posted_date, num_applicants = _parse_tertiary(tertiary_text)

                    pref_texts: List[str] = []
                    try:
                        strongs = driver.find_elements(By.CSS_SELECTOR, PREF_STRONG_SELECTOR)
                        pref_texts = [clean(s.text) for s in strongs if clean(s.text)]
                    except Exception:
                        pass
                    work_mode, employment_type = _parse_prefs(pref_texts)

                    position = typ = compensation = commitment = None
                    mt4 = _find_first(driver, ABOUT_JOB_MT4_SELECTORS)
                    if mt4:
                        kv = _parse_optional_kv(mt4.text)
                        position = kv.get("position")
                        typ = kv.get("type")
                        compensation = kv.get("compensation")
                        commitment = kv.get("commitment")

                    skills = _extract_skills(driver)

                    category_primary = classify_it_non_it(
                        designation=title or "",
                        industry="",
                        full_text=f"{skills or ''} {position or ''} {typ or ''}",
                    )

                    job_url = f"https://www.linkedin.com/jobs/view/{job_id}/"

                    rows.append({
                        "job_id": job_id,
                        "title": title,
                        "company": company,
                        "company_link": company_link,
                        "location": location,
                        "country": country,  # ✅ NEW COLUMN
                        "posted_date": posted_date,
                        "num_applicants": num_applicants,
                        "work_mode": work_mode,
                        "employment_type": employment_type,
                        "position": position,
                        "type": typ,
                        "compensation": compensation,
                        "commitment": commitment,
                        "skills": skills,
                        "category_primary": category_primary,
                        "job_url": job_url,
                        "source": "linkedin",
                        "scraped_at": now_iso(),
                    })

                    logger.info(f"[{country}] appended job_id={job_id} rows={len(rows)}")

                # small pacing between pages/countries reduces blocks
                time.sleep(1.0)

        return rows

    finally:
        try:
            driver.quit()
        except Exception:
            pass
