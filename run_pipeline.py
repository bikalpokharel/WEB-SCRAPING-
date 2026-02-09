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
from portals.linkedin import collect_rows as linkedin_collect_rows


PORTALS = {
    "merojob": {
        "mode": "selenium",
        "collect": mero_collect,
        "parse": mero_parse,
        "pages": CONFIG.pages,
        "limit": CONFIG.limit,
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
            "collect_rows": linkedin_collect_rows,
            "limit": CONFIG.limit,
            "dedupe_key": "job_id",

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


def append_rows_to_excel(xlsx_path: str, new_rows: List[Dict], dedupe_key: str) -> int:
    if not new_rows:
        return 0

    if _is_excel_temp_file(xlsx_path):
        raise ValueError(f"Refusing to write to Excel temp/lock file: {xlsx_path}")

    new_df = pd.DataFrame(new_rows)

    old_df = None
    old_count = 0

    if os.path.exists(xlsx_path):
        try:
            old_df = pd.read_excel(xlsx_path, engine="openpyxl")
            old_count = len(old_df)
        except Exception:
            bad_path = xlsx_path + f".corrupted_{int(time.time())}"
            try:
                os.rename(xlsx_path, bad_path)
                print(f"[WARN] Existing Excel corrupted. Moved to: {bad_path}")
            except Exception:
                print(f"[WARN] Existing Excel corrupted and could not be renamed: {xlsx_path}")
            old_df = None
            old_count = 0

    df = pd.concat([old_df, new_df], ignore_index=True) if old_df is not None else new_df.copy()

    if dedupe_key in df.columns:
        df[dedupe_key] = df[dedupe_key].astype(str).str.strip()
        df = df.drop_duplicates(subset=[dedupe_key]).reset_index(drop=True)
        added = max(0, len(df) - old_count)
    else:
        added = len(new_df)

    df.to_excel(xlsx_path, index=False, engine="openpyxl")
    return added


def run_portal_once(
    portal_name: str,
    cfg: Dict,
    logger: logging.Logger,
) -> int:
    """
    Supports two modes:
      - selenium: collect URLs -> parse each URL
      - rows: collect rows directly (LinkedIn)
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
    # ROWS MODE (LinkedIn)
    # -------------------------
    if mode == "rows":
        collect_rows_fn = cfg.get("collect_rows")
        if not collect_rows_fn:
            logger.error("Rows mode is missing collect_rows function.")
            return 0

        try:
            rows = collect_rows_fn(CONFIG) or []
            logger.info(f"Collected rows: {len(rows)}")

            # write URL audit file (job_url list)
            urls = [str(r.get("job_url", "")).strip() for r in rows if str(r.get("job_url", "")).strip()]
            save_latest_urls(urls, out_urls)

            # filter already-existing
            new_rows = []
            for r in rows:
                k = str(r.get(dedupe_key, "")).strip()
                if not k or k in existing_keys:
                    continue
                existing_keys.add(k)
                new_rows.append(r)

            appended = append_rows_to_excel(out_xlsx, new_rows, dedupe_key=dedupe_key)
            logger.info(f"Added {appended} NEW rows total.")
            return appended

        except Exception:
            logger.exception("Rows-mode portal cycle failed with an unexpected error.")
            return 0

    # -------------------------
    # SELENIUM URL MODE (others)
    # -------------------------
    collect_fn: Optional[Callable] = cfg.get("collect")
    parse_fn: Optional[Callable] = cfg.get("parse")
    pages = int(cfg.get("pages", 1) or 1)
    limit = int(cfg.get("limit", 200) or 200)

    if not collect_fn or not parse_fn:
        logger.error("Selenium mode missing collect/parse functions.")
        return 0

    AUTOSAVE_EVERY = 5
    MAX_CONSEC_FAILS = 6

    driver = make_fast_driver(headless=CONFIG.headless)

    buffer_rows: List[Dict] = []
    total_appended = 0
    consecutive_fails = 0

    try:
        urls = collect_fn(driver, pages=pages, limit=limit) or []
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

            except Exception:
                consecutive_fails += 1
                logger.exception(f"Failed to parse URL (skipping): {u}")

            if len(buffer_rows) >= AUTOSAVE_EVERY:
                try:
                    appended_now = append_rows_to_excel(out_xlsx, buffer_rows, dedupe_key=dedupe_key)
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
            appended_now = append_rows_to_excel(out_xlsx, buffer_rows, dedupe_key=dedupe_key)
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

    selected = PORTALS if portal_choice == "all" else {portal_choice: PORTALS[portal_choice]}
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
