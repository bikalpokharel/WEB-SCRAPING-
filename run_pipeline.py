# run_pipeline.py
import os
import time
import shutil
import argparse
import logging
from logging.handlers import RotatingFileHandler
from typing import Callable, Dict, Optional, Set, List

import pandas as pd

from config import CONFIG
from scraper_core import make_fast_driver

# Portal modules
# Portal modules
from portals.merojob import collect_job_urls as mero_collect, parse_job_detail as mero_parse
from portals.jobsnepal import collect_job_urls as jobs_collect, parse_job_detail as jobs_parse
from portals.linkedin import linkedin_parse

PORTALS = {
    "merojob": {
        "mode": "selenium",
        "collect": mero_collect,
        "parse": mero_parse,
        "pages": CONFIG.pages,
        "limit": CONFIG.limit,
        "per_page": 30,
        "dedupe_key": "job_url",
    },
    "jobsnepal": {
        "mode": "selenium",
        "collect": jobs_collect,
        "parse": jobs_parse,
        "pages": CONFIG.pages,
        "limit": CONFIG.limit,
        "dedupe_key": "job_url",
    },
    "linkedin": {
        "mode": "rows",
        "collect_rows": linkedin_parse,
        "dedupe_key": "job_id",
         "autosave_every": 5, 
    },
}

def setup_logger(name: str) -> logging.Logger:
    os.makedirs("logs", exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    log_path = os.path.join("logs", f"{name}.log")
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=2_000_000,
        backupCount=5,
        encoding="utf-8",
    )
    console_handler = logging.StreamHandler()

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(fmt)
    console_handler.setFormatter(fmt)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger


def get_output_paths(portal_name: str) -> Dict[str, str]:
    data_dir = CONFIG.data_dir
    os.makedirs(data_dir, exist_ok=True)

    internal_dir = os.path.join(data_dir, "_internal")
    os.makedirs(internal_dir, exist_ok=True)

    return {
        "xlsx": os.path.join(data_dir, f"{portal_name}_jobs.xlsx"),
        "urls": os.path.join(internal_dir, f"{portal_name}_urls_latest.txt"),
    }


def _is_excel_temp_file(path: str) -> bool:
    base = os.path.basename(path or "")
    return base.startswith("~$")


def load_existing_values(xlsx_path: str, key: str) -> Set[str]:
    if _is_excel_temp_file(xlsx_path):
        return set()

    if not os.path.exists(xlsx_path):
        return set()

    try:
        df_old = pd.read_excel(xlsx_path, engine="openpyxl")
        if key not in df_old.columns:
            return set()
        return set(df_old[key].dropna().astype(str).str.strip().tolist())
    except Exception:
        return set()


def save_latest_urls(urls: List[str], path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for u in urls:
            f.write(u + "\n")


LOCAL_CACHE_DIR = "/Users/bikal/Data_scraping/data_local" 

def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def _is_excel_temp_file(path: str) -> bool:
    base = os.path.basename(path or "")
    return base.startswith("~$")

def _copy_to_local_cache(src_path: str, cache_dir: str) -> Optional[str]:
    """
    Copy OneDrive file to local cache to avoid timeouts and partial reads.
    """
    try:
        _ensure_dir(cache_dir)
        dst_path = os.path.join(cache_dir, os.path.basename(src_path))
        shutil.copy2(src_path, dst_path)
        return dst_path
    except Exception as e:
        print(f"[WARN] Failed to copy to local cache: {src_path}\n  -> {e}")
        return None

def _atomic_write_excel(df: pd.DataFrame, out_path: str) -> None:
    """
    Write to temp file then atomic replace => avoids corrupted xlsx.
    """
    _ensure_dir(os.path.dirname(out_path))
    tmp_path = out_path + f".tmp_{int(time.time())}.xlsx"
    df.to_excel(tmp_path, index=False, engine="openpyxl")
    os.replace(tmp_path, out_path)  # atomic on same filesystem

def _read_excel_with_retry(path: str, retries: int = 3, pause: float = 1.5) -> pd.DataFrame:
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            return pd.read_excel(path, engine="openpyxl")
        except Exception as e:
            last_err = e
            print(f"[WARN] read_excel failed ({attempt}/{retries}) for: {path}\n  -> {e}")
            time.sleep(pause * attempt)
    raise last_err

def upsert_rows_to_excel(xlsx_path: str, new_rows: List[Dict], dedupe_key: str) -> int:
    """
    SAFE UPSERT:
    - never reads/writes directly on OneDrive
    - uses local cache file
    - atomic write
    - copies final back to OneDrive
    """
    if not new_rows:
        return 0

    if _is_excel_temp_file(xlsx_path):
        raise ValueError(f"Refusing to write to Excel temp/lock file: {xlsx_path}")

    REQUIRED_COLS = [
        "job_id",
        "title",
        "company",
        "company_link",
        "location",
        "country",          # ✅ keep your new column here
        "posted_date",
        "num_applicants",
        "work_mode",
        "employment_type",
        "position",
        "type",
        "compensation",
        "commitment",
        "skills",
        "category_primary",
        "job_url",
        "source",
        "scraped_at",
    ]

    # 1) Convert new rows to df + enforce schema
    new_df = pd.DataFrame(new_rows)
    for c in REQUIRED_COLS:
        if c not in new_df.columns:
            new_df[c] = None
    new_df = new_df[REQUIRED_COLS]

    # 2) Work on local cached file (or create new local file)
    _ensure_dir(LOCAL_CACHE_DIR)
    local_path = os.path.join(LOCAL_CACHE_DIR, os.path.basename(xlsx_path))

    old_df = None
    old_count = 0

    # If OneDrive file exists, copy it locally first
    if os.path.exists(xlsx_path):
        cached = _copy_to_local_cache(xlsx_path, LOCAL_CACHE_DIR)
        if cached:
            local_path = cached

    # 3) Read local file (if exists); if corrupted, rename and rebuild
    if os.path.exists(local_path):
        try:
            old_df = _read_excel_with_retry(local_path, retries=3, pause=1.0)

            # If file has duplicated columns, fix
            if old_df.columns.duplicated().any():
                old_df = old_df.loc[:, ~old_df.columns.duplicated()].copy()

            for c in REQUIRED_COLS:
                if c not in old_df.columns:
                    old_df[c] = None
            old_df = old_df[REQUIRED_COLS]
            old_count = len(old_df)

        except Exception as e:
            bad_path = local_path + f".corrupted_{int(time.time())}"
            try:
                os.rename(local_path, bad_path)
                print(f"[WARN] Local Excel corrupted. Moved to: {bad_path}")
            except Exception:
                print(f"[WARN] Local Excel corrupted and could not be renamed: {local_path}")
            old_df = None
            old_count = 0

    # 4) Combine + UPSERT (prefer new values when old is missing)
    if old_df is None:
        merged = new_df.copy()
        added = len(new_df)
    else:
        merged = pd.concat([old_df, new_df], ignore_index=True)

        if dedupe_key in merged.columns:
            merged[dedupe_key] = merged[dedupe_key].astype(str).str.strip()

            # Sort so newest scraped_at wins when there are duplicates
            if "scraped_at" in merged.columns:
                merged["scraped_at"] = pd.to_datetime(merged["scraped_at"], errors="coerce")
                merged = merged.sort_values("scraped_at", ascending=False)

            # Keep first occurrence per key (newest first)
            merged = merged.drop_duplicates(subset=[dedupe_key], keep="first").reset_index(drop=True)
            added = max(0, len(merged) - old_count)
        else:
            added = len(new_df)

        # Optional: fill missing values in old rows using new rows
        # (Since we kept newest-first, this is already handled by keep="first")

    # 5) Write locally atomically
    _atomic_write_excel(merged, local_path)

    # 6) Copy local back to OneDrive path atomically (copy2 then replace)
    try:
        tmp_remote = xlsx_path + f".tmp_{int(time.time())}"
        shutil.copy2(local_path, tmp_remote)
        os.replace(tmp_remote, xlsx_path)
    except Exception as e:
        print(f"[WARN] Could not copy updated Excel back to OneDrive yet: {e}")
        print(f"[WARN] Local updated file is here: {local_path}")

    return added

def _run_merojob_rows(_cfg) -> List[Dict]:
    """
    Runs Merojob row collector using Selenium driver internally.
    Retries if Chrome session crashes (InvalidSessionId / DevTools disconnect).
    """
    from selenium.common.exceptions import InvalidSessionIdException, WebDriverException

    max_tries = 3
    last_err = None

    for attempt in range(1, max_tries + 1):
        driver = make_fast_driver(headless=CONFIG.headless)
        try:
            rows = merojob_collect_rows(
                driver,
                limit_total=int(CONFIG.limit or 200),
                per_page=24,
                start_offset=1,
                sleep_s=0.25,
                logger=None,
            ) or []
            return rows

        except (InvalidSessionIdException, WebDriverException) as e:
            last_err = e
            # Typical crash strings: "invalid session id", "disconnected: not connected to DevTools"
            print(f"[WARN] Merojob driver crashed (attempt {attempt}/{max_tries}). Recreating driver... {e}")

        finally:
            try:
                driver.quit()
            except Exception:
                pass

        time.sleep(2.0)

    # If all attempts fail, raise the last error so it shows in logs.
    if last_err:
        raise last_err
    return []


def run_portal_once(
    portal_name: str,
    cfg: Dict,
    logger: logging.Logger,
) -> int:
    """
    Supports two modes:
      - selenium: collect URLs -> parse each URL
      - rows: collect rows directly (LinkedIn / Merojob)
    """
    paths = get_output_paths(portal_name)
    out_xlsx = paths["xlsx"]
    out_urls = paths["urls"]

    dedupe_key = cfg.get("dedupe_key", "job_url")

    logger.info(f"Excel output path: {out_xlsx}")
    logger.info(f"URL audit path: {out_urls}")

    existing_keys = load_existing_values(out_xlsx, dedupe_key)
    logger.info(f"Existing {dedupe_key} already saved: {len(existing_keys)}")

    mode = (cfg.get("mode") or "selenium").lower().strip()

    # -------------------------
    # ROWS MODE (LinkedIn / Merojob)
    # -------------------------
    if mode == "rows":
        collect_rows_fn = cfg.get("collect_rows")
        if not collect_rows_fn:
            logger.error("Rows mode is missing collect_rows function.")
            return 0

        AUTOSAVE_EVERY = int(cfg.get("autosave_every", 5) or 5)
        appended_total = 0
        buffer_rows: List[Dict] = []

        try:
            rows = collect_rows_fn(CONFIG) or []
            logger.info(f"Collected rows: {len(rows)}")

            if not rows:
                return 0

            # audit file stores dedupe_key values (URLs for Merojob, job_id for LinkedIn)
            ids = [
                str(r.get(dedupe_key, "")).strip()
                for r in rows
                if str(r.get(dedupe_key, "")).strip()
            ]
            save_latest_urls(ids, out_urls)

            for r in rows:
                k = str(r.get(dedupe_key, "")).strip()
                if not k or k in existing_keys:
                    continue

                existing_keys.add(k)
                buffer_rows.append(r)

                if len(buffer_rows) >= AUTOSAVE_EVERY:
                    appended = upsert_rows_to_excel(out_xlsx, buffer_rows, dedupe_key=dedupe_key)
                    appended_total += appended
                    logger.info(f"[Autosave] Appended {appended} rows.")
                    buffer_rows = []

            if buffer_rows:
                appended = upsert_rows_to_excel(out_xlsx, buffer_rows, dedupe_key=dedupe_key)
                appended_total += appended
                logger.info(f"[Final Save] Appended {appended} rows.")
                buffer_rows = []

            logger.info(f"Added {appended_total} NEW rows total.")
            return appended_total

        except KeyboardInterrupt:
            logger.warning("Interrupted by user (Ctrl+C). Saving buffered rows...")

            if buffer_rows:
                appended = upsert_rows_to_excel(out_xlsx, buffer_rows, dedupe_key=dedupe_key)
                appended_total += appended
                logger.info(f"[Interrupt Save] Appended {appended} rows.")

            return appended_total

        except Exception:
            logger.exception("Rows-mode portal cycle failed with an unexpected error.")
            try:
                if buffer_rows:
                    appended = upsert_rows_to_excel(out_xlsx, buffer_rows, dedupe_key=dedupe_key)
                    appended_total += appended
                    logger.info(f"[Error Save] Appended {appended} rows.")
            except Exception:
                logger.exception("Failed saving buffered rows after error.")
            return appended_total

    # -------------------------
    # SELENIUM URL MODE
    # -------------------------
    collect_fn: Optional[Callable] = cfg.get("collect")
    parse_fn: Optional[Callable] = cfg.get("parse")
    pages = int(cfg.get("pages", 1) or 1)
    limit = int(cfg.get("limit", 200) or 200)

    if not collect_fn or not parse_fn:
        logger.error("Selenium mode missing collect/parse functions.")
        return 0

    AUTOSAVE_EVERY = 5
    MAX_CONSEC_FAILS = 3

    driver = make_fast_driver(headless=CONFIG.headless)

    buffer_rows: List[Dict] = []
    total_appended = 0
    consecutive_fails = 0

    try:
        per_page = int(cfg.get("per_page", 30) or 30)
        urls = collect_fn(
            driver,
            pages=pages,
            limit=limit,
            per_page=per_page,
            sleep_sec=CONFIG.sleep_between_pages_sec,
        ) or []
        save_latest_urls(urls, out_urls)

        logger.info(f"Total collected URLs: {len(urls)}")
        if urls:
            logger.info(f"First 10 URLs: {urls[:10]}")

        for i, u in enumerate(urls, 1):
            logger.info(f"[{portal_name.upper()}] {i}/{len(urls)} {u}")

            try:
                row = parse_fn(driver, u)
                if not row:
                    consecutive_fails += 1
                    continue

                row_key = str(row.get(dedupe_key, "")).strip()
                if not row_key or row_key in existing_keys:
                    continue

                buffer_rows.append(row)
                existing_keys.add(row_key)
                consecutive_fails = 0

            except Exception as e:
                consecutive_fails += 1

                msg = str(e)

                if "BLOCKED_OR_CHALLENGE" in msg:
                    logger.warning("MeroJob challenge page detected. Restarting driver + slowing down...")
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    time.sleep(6.0)  # cooldown
                    driver = make_fast_driver(headless=CONFIG.headless)
                    consecutive_fails = 0
                    continue

                if "DRIVER_DIED" in msg:
                    logger.warning("Driver died detected. Will trigger restart logic.")
                    # let the normal MAX_CONSEC_FAILS restart happen quickly
                    # (and we already lowered it to 2)
                    logger.exception(f"Failed to parse URL (skipping): {u}")
                    continue

                logger.exception(f"Failed to parse URL (skipping): {u}")


            if len(buffer_rows) >= AUTOSAVE_EVERY:
                try:
                    appended_now = upsert_rows_to_excel(out_xlsx, buffer_rows, dedupe_key=dedupe_key)
                    total_appended += appended_now
                    logger.info(f"[Autosave] Appended {appended_now} rows to {out_xlsx}")
                    buffer_rows = []
                except Exception:
                    logger.exception("[Autosave] Failed while saving to Excel (continuing).")

            if consecutive_fails >= MAX_CONSEC_FAILS:
                logger.warning(f"Too many consecutive failures ({consecutive_fails}). Restarting driver...")
                try:
                    driver.quit()
                except Exception:
                    pass
                driver = make_fast_driver(headless=CONFIG.headless)
                consecutive_fails = 0
                time.sleep(2.0)

    except Exception:
        logger.exception("Portal cycle failed with an unexpected error.")
        return total_appended

    finally:
        try:
            driver.quit()
        except Exception:
            pass

    if buffer_rows:
        try:
            appended_now = upsert_rows_to_excel(out_xlsx, buffer_rows, dedupe_key=dedupe_key)
            total_appended += appended_now
            logger.info(f"[Final Save] Appended {appended_now} rows to {out_xlsx}")
        except Exception:
            logger.exception("Failed while saving final rows to Excel.")

    if total_appended == 0:
        logger.info("No NEW data collected. Excel remains unchanged.")
        return 0

    logger.info(f"Added {total_appended} NEW rows total.")
    logger.info(f"Saved to: {out_xlsx}")
    logger.info(f"Latest URL list saved to: {out_urls}")
    return total_appended


def parse_args():
    parser = argparse.ArgumentParser(description="Nepal job scraping pipeline (multi-portal).")
    parser.add_argument("--watch", action="store_true", help="Run continuously.")
    parser.add_argument("--interval", type=int, default=CONFIG.watch_default_interval_sec)
    parser.add_argument("--portal", type=str, default="all")
    return parser.parse_args()


def main():
    args = parse_args()
    interval = max(30, int(args.interval))
    portal_choice = (args.portal or "all").lower().strip()

    if portal_choice == "all":
        selected = PORTALS
    else:
        if portal_choice not in PORTALS:
            raise ValueError(f"Invalid portal name: {portal_choice}")
        selected = {portal_choice: PORTALS[portal_choice]}

    loggers = {name: setup_logger(name) for name in selected.keys()}

    def run_all_once():
        for name, cfg in selected.items():
            logger = loggers[name]
            logger.info("----- START CYCLE -----")
            run_portal_once(portal_name=name, cfg=cfg, logger=logger)
            logger.info("------ END CYCLE ------\n")

    if not args.watch:
        run_all_once()
        return

    for name in selected.keys():
        loggers[name].info(f"Watch mode enabled. Interval: {interval} seconds.")
        loggers[name].info("Press Ctrl + C to stop.")

    try:
        while True:
            run_all_once()
            time.sleep(interval)
    except KeyboardInterrupt:
        for name in selected.keys():
            loggers[name].info("Stopped watch mode (Ctrl + C).")


if __name__ == "__main__":
    main()
