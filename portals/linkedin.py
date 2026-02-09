from __future__ import annotations

import re
import time
from pathlib import Path
from typing import Dict, List, Optional

from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from config import CONFIG
from scraper_core import clean, clean_or_non, now_iso, make_linkedin_driver

LISTING_URL = "https://www.linkedin.com/jobs/search?location=Nepal"
JOB_ID_10 = re.compile(r"(\d{10})")


# -------------------------
# Debug helpers
# -------------------------
def _dump_debug(driver, prefix: str) -> None:
    try:
        Path("debug").mkdir(exist_ok=True)
        stamp = str(int(time.time()))
        html_path = f"debug/{prefix}_{stamp}.html"
        png_path = f"debug/{prefix}_{stamp}.png"
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(driver.page_source or "")
        try:
            driver.save_screenshot(png_path)
        except Exception:
            pass
        print(f"[DEBUG] Dumped: {html_path} and {png_path}")
        print(f"[DEBUG] URL: {driver.current_url}")
        print(f"[DEBUG] Title: {driver.title}")
    except Exception:
        pass


def _wait_body(driver, timeout: int = 25) -> None:
    WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    time.sleep(0.6)


def _close_modal_if_present(driver) -> bool:
    selectors = [
        "button.modal__dismiss",
        "button[aria-label='Dismiss']",
        "button[aria-label='Close']",
        "button.artdeco-modal__dismiss",
        "button.artdeco-toast-item__dismiss",
    ]
    for sel in selectors:
        try:
            btns = driver.find_elements(By.CSS_SELECTOR, sel)
            for b in btns[:3]:
                try:
                    if b.is_displayed() and b.is_enabled():
                        driver.execute_script("arguments[0].click();", b)
                        time.sleep(0.2)
                        return True
                except Exception:
                    continue
        except Exception:
            continue
    return False


def _wait_listing_ul(driver, timeout: int = 25) -> bool:
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "ul.job-search__result-list, ul.jobs-search__results-list")
            )
        )
        return True
    except Exception:
        return False


def _get_cards(driver):
    return driver.find_elements(
        By.CSS_SELECTOR,
        "ul.job-search__result-list > li, ul.jobs-search__results-list > li",
    )


def _show_more_button(driver):
    try:
        return driver.find_element(By.CSS_SELECTOR, "button.infinite-scroller__show-more-button")
    except Exception:
        return None


def _scroll_page(driver):
    try:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    except Exception:
        pass


def _ensure_loaded_enough(driver, target_count: int, hard_limit_clicks: int = 25) -> None:
    clicks = 0
    while clicks < hard_limit_clicks:
        _close_modal_if_present(driver)

        cards = _get_cards(driver)
        if len(cards) >= target_count:
            return

        btn = _show_more_button(driver)
        if not btn:
            return

        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            time.sleep(0.3)
            if btn.is_displayed() and btn.is_enabled():
                driver.execute_script("arguments[0].click();", btn)
                clicks += 1
                time.sleep(1.6)
            else:
                return
        except Exception:
            return


def _job_id_from_href(href: str) -> Optional[str]:
    if not href:
        return None
    m = re.search(r"/jobs/view/(\d+)", href)
    if m:
        return m.group(1)
    m2 = re.search(r"currentJobId=(\d+)", href)
    if m2:
        return m2.group(1)
    m3 = JOB_ID_10.search(href)
    if m3:
        return m3.group(1)
    return None


def _click_card_by_index(driver, idx: int) -> Optional[Dict[str, str]]:
    """
    Re-fetch cards each time (LinkedIn re-renders list).
    """
    try:
        cards = _get_cards(driver)
        if idx < 0 or idx >= len(cards):
            return None

        link = cards[idx].find_element(By.CSS_SELECTOR, "a.base-card__full-link")
        href = clean(link.get_attribute("href")) or ""
        jid = _job_id_from_href(href) or "Non"

        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", link)
        time.sleep(0.2)
        driver.execute_script("arguments[0].click();", link)

        return {"job_id": jid, "href": href or "Non"}

    except StaleElementReferenceException:
        # Retry once
        try:
            cards = _get_cards(driver)
            if idx < 0 or idx >= len(cards):
                return None
            link = cards[idx].find_element(By.CSS_SELECTOR, "a.base-card__full-link")
            href = clean(link.get_attribute("href")) or ""
            jid = _job_id_from_href(href) or "Non"
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", link)
            time.sleep(0.2)
            driver.execute_script("arguments[0].click();", link)
            return {"job_id": jid, "href": href or "Non"}
        except Exception:
            return None
    except Exception:
        return None


def _wait_detail_panel(driver) -> bool:
    try:
        WebDriverWait(driver, 25).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "section.top-card-layout, .top-card-layout"))
        )
        return True
    except Exception:
        return False


def _text_first(driver, selectors: List[str]) -> Optional[str]:
    for sel in selectors:
        try:
            el = driver.find_element(By.CSS_SELECTOR, sel)
            t = clean(el.text)
            if t:
                return t
        except Exception:
            continue
    return None


def _attr_first(driver, selectors: List[str], attr: str) -> Optional[str]:
    for sel in selectors:
        try:
            el = driver.find_element(By.CSS_SELECTOR, sel)
            v = clean(el.get_attribute(attr))
            if v:
                return v
        except Exception:
            continue
    return None


def _is_logged_in(driver) -> bool:
    """
    ✅ Reliable login check for LinkedIn:
    When logged in, the global nav and 'Me' menu trigger exist.
    """
    try:
        if driver.find_elements(By.CSS_SELECTOR, "a.global-nav__primary-link-me-menu-trigger"):
            return True
    except Exception:
        pass
    return False


def _wait_until_logged_in_and_list_ready(driver, timeout_sec: int = 180) -> bool:
    """
    What to do:
    - Wait until user logs in AND job list is visible.
    How:
    - repeatedly check logged-in selector + job list selector.
    """
    print("[ACTION] Login to LinkedIn in the opened Chrome window NOW.")
    print("[ACTION] After login, come back—scraper will auto-continue.")

    start = time.time()
    while time.time() - start < timeout_sec:
        _close_modal_if_present(driver)

        logged = _is_logged_in(driver)
        list_ok = _wait_listing_ul(driver, timeout=3)

        if logged and list_ok:
            print("[INFO] Login confirmed + job list visible. Starting scrape loop...")
            return True

        time.sleep(2)

    print("[ERROR] Login not detected within timeout.")
    _dump_debug(driver, "linkedin_login_not_detected")
    return False


def _parse_detail_from_current_view(driver, card_job_id: str = "Non", card_href: str = "Non") -> Optional[Dict]:
    _close_modal_if_present(driver)

    if not _wait_detail_panel(driver):
        _dump_debug(driver, "linkedin_detail_not_loaded")
        return None

    _close_modal_if_present(driver)

    title = _text_first(driver, [
        "h1.top-card-layout__title",
        "h1.jobs-unified-top-card__job-title",
        "h1",
    ])

    posted_date = _text_first(driver, [
        "span.posted-time-ago__text",
        ".posted-time-ago__text",
    ])

    num_applicants = _text_first(driver, [
        "span.num-applicants__caption",
        ".num-applicants__caption",
    ])

    company_profile_url = _attr_first(driver, [
        "a.topcard__org-name-link",
        ".top-card-layout__company-name a",
        ".jobs-unified-top-card__company-name a",
    ], "href")

    company_name = _text_first(driver, [
        "a.topcard__org-name-link",
        ".top-card-layout__company-name a",
        ".jobs-unified-top-card__company-name a",
        ".top-card-layout__company-name",
        ".jobs-unified-top-card__company-name",
    ])

    location = _text_first(driver, [
        "span.topcard__flavor--bullet",
        "span.topcard__flavor",
        ".top-card-layout__first-subline .top-card-layout__first-subline-item",
        ".jobs-unified-top-card__bullet",
    ])

    seniority_level = None
    employment_type = None
    job_function = None
    industries = None

    items = []
    try:
        items = driver.find_elements(By.CSS_SELECTOR, "ul.description-job-criteria-list li")
    except Exception:
        items = []

    if not items:
        try:
            items = driver.find_elements(By.CSS_SELECTOR, "div.decorate-job-posting__details ul li")
        except Exception:
            items = []

    for li in items:
        try:
            key = clean(li.find_element(By.CSS_SELECTOR, "h3").text) or ""
            val = clean(li.find_element(By.CSS_SELECTOR, "span").text) or ""
        except Exception:
            continue

        k = key.lower()
        if "seniority" in k:
            seniority_level = val
        elif "employment" in k:
            employment_type = val
        elif "job function" in k:
            job_function = val
        elif "industr" in k:
            industries = val

    return {
        "job_id": clean_or_non(card_job_id, default="Non"),
        "title": clean_or_non(title, default="Non"),
        "posted_date": clean_or_non(posted_date, default="Non"),
        "num_applicants": clean_or_non(num_applicants, default="Non"),
        "company_name": clean_or_non(company_name, default="Non"),
        "company_profile_url": clean_or_non(company_profile_url, default="Non"),
        "location": clean_or_non(location, default="Non"),
        "seniority_level": clean_or_non(seniority_level, default="Non"),
        "employment_type": clean_or_non(employment_type, default="Non"),
        "job_function": clean_or_non(job_function, default="Non"),
        "industries": clean_or_non(industries, default="Non"),
        "job_url": clean_or_non(card_href, default=clean_or_non(driver.current_url, default="Non")),
        "scraped_at": now_iso(),
    }


def collect_rows(config=CONFIG) -> List[Dict]:
    driver = make_linkedin_driver(
        headless=False,
        profile_path=config.chrome_profile_path,
        profile_dir=config.chrome_profile_dir,
    )

    rows: List[Dict] = []
    seen_job_ids = set()

    try:
        driver.get(LISTING_URL)
        _wait_body(driver)
        _close_modal_if_present(driver)

        print("[INFO] Opened:", driver.current_url)

        # ✅ DO NOT CONTINUE UNTIL LOGIN CONFIRMED + LIST READY
        ok = _wait_until_logged_in_and_list_ready(driver, timeout_sec=180)
        if not ok:
            return []

        target = int(config.limit or 200)

        _ensure_loaded_enough(driver, target_count=min(target, 60))

        idx = 0
        consecutive_fail_clicks = 0

        while len(rows) < target:
            _close_modal_if_present(driver)

            cards = _get_cards(driver)

            # Load more cards if needed
            if idx >= len(cards):
                prev_len = len(cards)
                _scroll_page(driver)
                _ensure_loaded_enough(driver, target_count=min(target, prev_len + 30))
                time.sleep(0.6)
                cards = _get_cards(driver)

                if idx >= len(cards):
                    break

            click_meta = _click_card_by_index(driver, idx)
            if not click_meta:
                consecutive_fail_clicks += 1
                idx += 1
                if consecutive_fail_clicks >= 10:
                    _dump_debug(driver, "linkedin_many_click_failures")
                    break
                continue

            consecutive_fail_clicks = 0
            idx += 1

            card_jid = click_meta.get("job_id", "Non")
            card_href = click_meta.get("href", "Non")

            if card_jid != "Non" and card_jid in seen_job_ids:
                continue

            row = _parse_detail_from_current_view(driver, card_job_id=card_jid, card_href=card_href)
            if not row:
                continue

            jid = row.get("job_id", "Non")
            if not jid or jid == "Non":
                continue

            if jid in seen_job_ids:
                continue

            seen_job_ids.add(jid)
            rows.append(row)

            print(f"[INFO] Collected {len(rows)} | {row.get('title')} @ {row.get('company_name')} | {jid}")

            time.sleep(0.7)

            # Preload more when near end
            if idx >= len(_get_cards(driver)) - 5:
                _ensure_loaded_enough(driver, target_count=min(target, len(_get_cards(driver)) + 30))

        return rows

    finally:
        try:
            driver.quit()
        except Exception:
            pass


# compatibility stubs
def collect_job_urls(driver, pages: int = 1, limit: int = 200) -> List[str]:
    return []


def parse_job_detail(driver, url: str) -> Optional[Dict]:
    return None
