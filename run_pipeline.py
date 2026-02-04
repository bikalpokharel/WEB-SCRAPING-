# run_pipeline.py
import os
import time
import argparse
import logging
from logging.handlers import RotatingFileHandler
from typing import Callable, Dict, Optional, Set, List

import pandas as pd

from config import CONFIG
from scraper_core import make_fast_driver

# Portal modules
from portals.merojob import collect_job_urls as mero_collect, parse_job_detail as mero_parse
from portals.jobsnepal import collect_job_urls as jobs_collect, parse_job_detail as jobs_parse
from portals.linkedin_export import collect_rows as linkedin_collect_rows


# -------------------------
# Portal registry
# -------------------------
PORTALS = {
    "merojob": {
        "mode": "selenium",
        "collect": mero_collect,
        "parse": mero_parse,
        "pages": CONFIG.pages,
        "limit": CONFIG.limit,
    },
    "jobsnepal": {
        "mode": "selenium",
        "collect": jobs_collect,
        "parse": jobs_parse,
        "pages": CONFIG.pages,
        "limit": CONFIG.limit,
    },
    "linkedin": {
        "mode": "rows",
        "collect_rows": linkedin_collect_rows,
        "pages": 0,
        "limit": 0,
    },
}


# -------------------------
# Logging
# -------------------------
def setup_logger(name: str) -> logging.Logger:
    """
    Logger per portal:
      - console output
      - rotating file logs/<name>.log
    """
    os.makedirs("logs", exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Avoid duplicate handlers in watch loops / re-runs
    if logger.handlers:
        return logger

    log_path = os.path.join("logs", f"{name}.log")

    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=2_000_000,   # 2MB
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


# -------------------------
# Helpers
# -------------------------
def get_output_paths(portal_name: str) -> Dict[str, str]:
    data_dir = CONFIG.data_dir
    os.makedirs(data_dir, exist_ok=True)

    # Internal (non-shared) audit files
    internal_dir = os.path.join(data_dir, "_internal")
    os.makedirs(internal_dir, exist_ok=True)

    return {
        "xlsx": os.path.join(data_dir, f"{portal_name}_jobs.xlsx"),
        "urls": os.path.join(internal_dir, f"{portal_name}_urls_latest.txt"),
    }


def load_existing_urls(xlsx_path: str) -> Set[str]:
    """Load existing job URLs from Excel to prevent duplicates across runs."""
    if not os.path.exists(xlsx_path):
        return set()

    try:
        df_old = pd.read_excel(xlsx_path, engine="openpyxl")
        if "job_url" not in df_old.columns:
            return set()
        return set(df_old["job_url"].dropna().astype(str).str.strip().tolist())
    except Exception:
        return set()


def save_latest_urls(urls: List[str], path: str) -> None:
    """Save collected URLs to a file for audit/debugging."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for u in urls:
            f.write(u + "\n")


def append_rows_to_excel(xlsx_path: str, new_rows: list[dict]) -> int:
    """
    Append new rows to an Excel file, dedupe by job_url, save back.
    Returns number of rows actually appended (unique vs existing file).
    """
    if not new_rows:
        return 0

    new_df = pd.DataFrame(new_rows)

    if os.path.exists(xlsx_path):
        old_df = pd.read_excel(xlsx_path, engine="openpyxl")
        old_count = len(old_df)
        df = pd.concat([old_df, new_df], ignore_index=True)
    else:
        old_count = 0
        df = new_df.copy()

    if "job_url" in df.columns:
        df["job_url"] = df["job_url"].astype(str).str.strip()
        df = df.drop_duplicates(subset=["job_url"]).reset_index(drop=True)
        added = max(0, len(df) - old_count)
    else:
        # If no job_url, we cannot dedupe reliably
        added = len(new_df)

    df.to_excel(xlsx_path, index=False, engine="openpyxl")
    return added


# -------------------------
# Core runner (per portal)
# -------------------------
def run_portal_once(
    portal_name: str,
    collect_fn: Optional[Callable],            # selenium listing collector
    parse_fn: Optional[Callable],              # selenium detail parser
    pages: int,
    limit: int,
    logger: logging.Logger,
    collect_rows_fn: Optional[Callable] = None # rows-mode collector
) -> int:
    """
    Run one incremental cycle for a single portal.
    Returns number of NEW rows added.

    Supports two portal modes:
      1) Selenium mode: collect_fn + parse_fn
      2) Rows mode: collect_rows_fn (returns list[dict])
    """
    paths = get_output_paths(portal_name)
    out_xlsx = paths["xlsx"]
    out_urls = paths["urls"]

    existing_urls = load_existing_urls(out_xlsx)
    logger.info(f"Existing URLs already saved: {len(existing_urls)}")

    # ------------------------------------------------------------
    # MODE 2: ROWS MODE (NO SELENIUM) ✅ LinkedIn Export / APIs
    # ------------------------------------------------------------
    if collect_rows_fn is not None:
        try:
            rows: List[Dict] = collect_rows_fn(CONFIG) or []

            urls = [
                str(r.get("job_url", "")).strip()
                for r in rows
                if str(r.get("job_url", "")).strip()
            ]
            save_latest_urls(urls, out_urls)

            logger.info(f"Total collected rows: {len(rows)}")
            if urls:
                logger.info(f"First 10 URLs: {urls[:10]}")

            # Filter new rows by job_url (dedupe vs existing excel)
            new_rows = []
            for r in rows:
                u = str(r.get("job_url", "")).strip()
                if not u:
                    continue
                if u not in existing_urls:
                    new_rows.append(r)

            logger.info(f"New rows to append (not in Excel yet): {len(new_rows)}")

            if not new_rows:
                logger.info("No NEW data collected. Excel remains unchanged.")
                return 0

            appended = append_rows_to_excel(out_xlsx, new_rows)
            logger.info(f"Added {appended} NEW rows.")
            logger.info(f"Saved to: {out_xlsx}")
            logger.info(f"Latest URL list saved to: {out_urls}")
            return appended

        except Exception:
            logger.exception("Portal cycle failed with an unexpected error.")
            return 0

    # ------------------------------------------------------------
    # MODE 1: SELENIUM MODE ✅ MeroJob / JobsNepal
    # ------------------------------------------------------------
    if collect_fn is None or parse_fn is None:
        logger.error("Selenium portal requires collect_fn and parse_fn.")
        return 0

    driver = make_fast_driver(headless=CONFIG.headless)
    rows = []

    try:
        urls = collect_fn(driver, pages=pages, limit=limit)
        save_latest_urls(urls, out_urls)

        logger.info(f"Total collected URLs: {len(urls)}")
        if urls:
            logger.info(f"First 10 URLs: {urls[:10]}")

        new_urls = [u for u in urls if u not in existing_urls]
        logger.info(f"New URLs to scrape (not in Excel yet): {len(new_urls)}")

        for i, u in enumerate(new_urls, 1):
            logger.info(f"[{portal_name.upper()} NEW] {i}/{len(new_urls)} {u}")
            row = parse_fn(driver, u)
            if row:
                rows.append(row)

    except Exception:
        logger.exception("Portal cycle failed with an unexpected error.")
        return 0

    finally:
        driver.quit()

    if not rows:
        logger.info("No NEW data collected. Excel remains unchanged.")
        return 0

    appended = append_rows_to_excel(out_xlsx, rows)
    logger.info(f"Added {appended} NEW rows.")
    logger.info(f"Saved to: {out_xlsx}")
    logger.info(f"Latest URL list saved to: {out_urls}")
    return appended


# -------------------------
# CLI + Watch mode
# -------------------------
def parse_args():
    parser = argparse.ArgumentParser(
        description="Nepal job scraping pipeline (multi-portal, incremental Excel updates)."
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Run continuously and update Excel every interval.",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=CONFIG.watch_default_interval_sec,
        help="Watch mode interval in seconds (default from config).",
    )
    parser.add_argument(
        "--portal",
        type=str,
        default="all",
        help="Run a single portal: merojob | jobsnepal | linkedin | all",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    interval = max(30, int(args.interval))
    portal_choice = (args.portal or "all").lower().strip()

    # Decide which portals to run
    if portal_choice == "all":
        selected = PORTALS
    else:
        if portal_choice not in PORTALS:
            raise ValueError(f"Unknown portal '{portal_choice}'. Use: merojob | jobsnepal | linkedin | all")
        selected = {portal_choice: PORTALS[portal_choice]}

    # Create loggers per portal
    loggers = {name: setup_logger(name) for name in selected.keys()}

    def run_all_once():
        for name, cfg in selected.items():
            logger = loggers[name]
            logger.info("----- START CYCLE -----")

            mode = (cfg.get("mode") or "selenium").lower()

            if mode == "rows":
                run_portal_once(
                    portal_name=name,
                    collect_fn=None,
                    parse_fn=None,
                    pages=0,
                    limit=0,
                    logger=logger,
                    collect_rows_fn=cfg.get("collect_rows"),
                )
            else:
                run_portal_once(
                    portal_name=name,
                    collect_fn=cfg.get("collect"),
                    parse_fn=cfg.get("parse"),
                    pages=cfg.get("pages", CONFIG.pages),
                    limit=cfg.get("limit", CONFIG.limit),
                    logger=logger,
                )

            logger.info("------ END CYCLE ------\n")

    if not args.watch:
        run_all_once()
        return

    # Watch mode
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
