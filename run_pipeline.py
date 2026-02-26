# run_pipeline.py
from __future__ import annotations

import os
import time
import shutil
import argparse
import logging
import subprocess
import sys
from logging.handlers import RotatingFileHandler
from typing import Callable, Dict, Optional, Set, List

import pandas as pd

from config import CONFIG
from scraper_core import make_fast_driver

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
        "autosave_every": 5,          
    },
    "jobsnepal": {
        "mode": "selenium",
        "collect": jobs_collect,
        "parse": jobs_parse,
        "pages": CONFIG.pages,
        "limit": CONFIG.limit,
        "dedupe_key": "job_url",
        "autosave_every": 5,         
    },
    "linkedin": {
        "mode": "rows",
        "collect_rows": linkedin_parse,
        "dedupe_key": "job_id",
        "autosave_every": 5,         
    },
}

# =========================
# Logging
# =========================
def setup_logger(name: str) -> logging.Logger:
    os.makedirs("logs", exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # avoid duplicate handlers in watch mode / repeated runs
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


# =========================
# Post-cycle tasks
# =========================
def run_post_cycle_tasks(logger: logging.Logger) -> None:
    """
    After scraping + UPSERT:
    1) Build master dataset (usually jobs_master.csv/xlsx)
    2) Recompute quality report
    Uses the current venv python (sys.executable).
    """
    project_root = os.path.dirname(os.path.abspath(__file__))

    build_master_path = os.path.join(project_root, "analysis", "build_master.py")
    portal_quality_path = os.path.join(project_root, "analysis", "portal_quality.py")

    # 1) Build master
    if os.path.exists(build_master_path):
        logger.info("📦 Post-task: Building master dataset (jobs_master.*)...")
        try:
            subprocess.run(
                [sys.executable, build_master_path],
                check=True,
                cwd=project_root,
            )
            logger.info("✅ Master build complete.")
        except subprocess.CalledProcessError as e:
            logger.exception(f"❌ build_master.py failed: {e}")
    else:
        logger.warning(f"⚠ build_master.py not found at: {build_master_path}")
     
    # 1.5) Backfill taxonomy
    backfill_tax_path = os.path.join(project_root, "analysis", "backfill_taxonomy.py")
    if os.path.exists(backfill_tax_path):
        logger.info("🏷️ Post-task: Backfilling taxonomy for old rows (domain_l1/l2/l3)...")
        try:
            subprocess.run([sys.executable, backfill_tax_path], check=True, cwd=project_root)
            logger.info("✅ Taxonomy backfill complete.")
        except subprocess.CalledProcessError as e:
            logger.exception(f"❌ backfill_taxonomy.py failed: {e}")
            
    # 2) Quality report
    if os.path.exists(portal_quality_path):
        logger.info("📊 Post-task: Recomputing portal quality report (portal_quality_report.xlsx)...")
        try:
            subprocess.run(
                [sys.executable, portal_quality_path],
                check=True,
                cwd=project_root,
            )
            logger.info("✅ Portal quality report updated.")
        except subprocess.CalledProcessError as e:
            logger.exception(f"❌ portal_quality.py failed: {e}")
    else:
        logger.warning(f"⚠ portal_quality.py not found at: {portal_quality_path}")


# =========================
# Paths + helpers
# =========================
def get_output_paths(portal_name: str) -> Dict[str, str]:
    data_dir = CONFIG.data_dir
    os.makedirs(data_dir, exist_ok=True)

    internal_dir = os.path.join(data_dir, "_internal")
    os.makedirs(internal_dir, exist_ok=True)

    return {
        "xlsx": os.path.join(data_dir, f"{portal_name}_jobs.xlsx"),
        "urls": os.path.join(internal_dir, f"{portal_name}_urls_latest.txt"),
    }


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _is_excel_temp_file(path: str) -> bool:
    base = os.path.basename(path or "")
    return base.startswith("~$")


def load_existing_values(xlsx_path: str, key: str) -> Set[str]:
    """
    Used only for quick 'how many exist already' logging.
    NOTE: UPSERT logic does not depend on this set.
    """
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
    _ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        for u in urls:
            f.write(u + "\n")


# =========================
# Local cache for OneDrive safety
# =========================
LOCAL_CACHE_DIR = "/Users/bikal/Data_scraping/data_local"

TAX_COLS = [
    "category_primary",
    "domain_l1",
    "domain_l2",
    "domain_l3",
    "tax_confidence",
]


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
    Write to temp then atomic replace => avoids corrupted xlsx.
    """
    _ensure_dir(os.path.dirname(out_path))
    tmp_path = out_path + f".tmp_{int(time.time())}.xlsx"
    df.to_excel(tmp_path, index=False, engine="openpyxl")
    os.replace(tmp_path, out_path)


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


def upsert_rows_to_excel(
    xlsx_path: str,
    new_rows: List[Dict],
    dedupe_key: str,
    update_cols: Optional[List[str]] = None,
    overwrite_existing: bool = False,
) -> int:
    """
    TRUE UPSERT:
    - Keeps ALL columns (dynamic union old + new)
    - For existing keys: updates only update_cols
        * overwrite_existing=False: only fills blanks/NA/placeholder-like
        * overwrite_existing=True: always overwrites those cols
    - For new keys: inserts full row
    - Still uses local cache + atomic write for OneDrive safety

    Returns number of NEW keys inserted.
    """
    if not new_rows:
        return 0

    if _is_excel_temp_file(xlsx_path):
        raise ValueError(f"Refusing to write to Excel temp/lock file: {xlsx_path}")

    # ---- placeholders ----
    PLACEHOLDERS = {"Non", "non", "", "N/A", "na", "NA", "-", "—", "None", "NONE", "<NA>", "nan"}

    def _is_missingish(v) -> bool:
        if v is None:
            return True
        s = str(v).strip()
        if s in PLACEHOLDERS:
            return True
        return False

    def _normalize_df(df: pd.DataFrame, all_cols: List[str]) -> pd.DataFrame:
        # add missing columns
        for c in all_cols:
            if c not in df.columns:
                df[c] = pd.NA

        # remove dup columns if any
        if df.columns.duplicated().any():
            df = df.loc[:, ~df.columns.duplicated()].copy()

        # reorder
        df = df[all_cols].copy()

        # cast all to string for stability
        for c in all_cols:
            df[c] = df[c].astype("string")

        # normalize placeholders to NA
        df = df.replace(list(PLACEHOLDERS), pd.NA)

        # strip key
        if dedupe_key in df.columns:
            df[dedupe_key] = df[dedupe_key].astype("string").str.strip()

        return df

    new_df = pd.DataFrame(new_rows)

    # local cache
    _ensure_dir(LOCAL_CACHE_DIR)
    local_path = os.path.join(LOCAL_CACHE_DIR, os.path.basename(xlsx_path))

    if os.path.exists(xlsx_path):
        cached = _copy_to_local_cache(xlsx_path, LOCAL_CACHE_DIR)
        if cached:
            local_path = cached

    old_df = None
    old_count = 0

    if os.path.exists(local_path):
        try:
            old_df = _read_excel_with_retry(local_path, retries=3, pause=1.0)
            old_count = len(old_df)
        except Exception:
            bad_path = local_path + f".corrupted_{int(time.time())}"
            try:
                os.rename(local_path, bad_path)
                print(f"[WARN] Local Excel corrupted. Moved to: {bad_path}")
            except Exception:
                pass
            old_df = None
            old_count = 0

    if old_df is None or old_df.empty:
        # just save new (keep all cols)
        all_cols = list(new_df.columns)
        if dedupe_key not in all_cols:
            raise ValueError(f"New rows missing dedupe_key '{dedupe_key}'")

        new_df = _normalize_df(new_df, all_cols)

        _atomic_write_excel(new_df, local_path)
        try:
            tmp_remote = xlsx_path + f".tmp_{int(time.time())}"
            shutil.copy2(local_path, tmp_remote)
            os.replace(tmp_remote, xlsx_path)
        except Exception as e:
            print(f"[WARN] Could not copy updated Excel back to OneDrive yet: {e}")
            print(f"[WARN] Local updated file is here: {local_path}")

        return len(new_df)

    # ---- dynamic union of columns ----
    all_cols = list(dict.fromkeys(list(old_df.columns) + list(new_df.columns)))

    # make sure key exists
    if dedupe_key not in all_cols:
        raise ValueError(f"Excel and new rows missing dedupe_key '{dedupe_key}'")

    old_df = _normalize_df(old_df, all_cols)
    new_df = _normalize_df(new_df, all_cols)

    # update_cols default: only taxonomy cols if you pass them; else update everything except key
    if update_cols is None:
        update_cols = [c for c in all_cols if c != dedupe_key]
    else:
        update_cols = [c for c in update_cols if c in all_cols and c != dedupe_key]

    # set index for fast updates
    old_idx = old_df.set_index(dedupe_key)
    new_idx = new_df.set_index(dedupe_key)

    # NEW keys => insert
    new_ids = new_idx.index.difference(old_idx.index)
    inserted = len(new_ids)
    if inserted:
        old_idx = pd.concat([old_idx, new_idx.loc[new_ids]], axis=0)

    # Existing keys => fill/update selected columns
    common_ids = new_idx.index.intersection(old_idx.index)
    for jid in common_ids:
        for col in update_cols:
            new_val = new_idx.at[jid, col]
            if _is_missingish(new_val):
                continue

            if overwrite_existing:
                old_idx.at[jid, col] = new_val
            else:
                old_val = old_idx.at[jid, col]
                if _is_missingish(old_val):
                    old_idx.at[jid, col] = new_val

        # Keep newest scraped_at if present
        if "scraped_at" in all_cols:
            o = old_idx.at[jid, "scraped_at"]
            n = new_idx.at[jid, "scraped_at"]
            # If either missing, skip
            if not _is_missingish(o) and not _is_missingish(n):
                try:
                    if pd.to_datetime(n, errors="coerce") > pd.to_datetime(o, errors="coerce"):
                        old_idx.at[jid, "scraped_at"] = n
                except Exception:
                    pass

    merged = old_idx.reset_index()

    # Optional: sort by scraped_at desc
    if "scraped_at" in merged.columns:
        merged["_scraped_at_dt"] = pd.to_datetime(merged["scraped_at"], errors="coerce")
        merged = merged.sort_values("_scraped_at_dt", ascending=False).drop(columns=["_scraped_at_dt"]).reset_index(drop=True)

    # write
    _atomic_write_excel(merged, local_path)

    try:
        tmp_remote = xlsx_path + f".tmp_{int(time.time())}"
        shutil.copy2(local_path, tmp_remote)
        os.replace(tmp_remote, xlsx_path)
    except Exception as e:
        print(f"[WARN] Could not copy updated Excel back to OneDrive yet: {e}")
        print(f"[WARN] Local updated file is here: {local_path}")

    return inserted

# =========================
# Portal runner
# =========================
def run_portal_once(portal_name: str, cfg: Dict, logger: logging.Logger) -> int:
    """
    Supports:
      - selenium: collect URLs -> parse each URL
      - rows: collect rows directly (LinkedIn / future rows portals)
    Returns number of NEW keys inserted (UPSERT may still update existing rows).
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

        autosave_every = int(cfg.get("autosave_every", 5) or 5)
        inserted_total = 0
        buffer_rows: List[Dict] = []

        try:
            rows = collect_rows_fn(CONFIG) or []
            logger.info(f"Collected rows: {len(rows)}")
            if not rows:
                return 0

            ids = [
                str(r.get(dedupe_key, "")).strip()
                for r in rows
                if str(r.get(dedupe_key, "")).strip()
            ]
            save_latest_urls(ids, out_urls)

            for r in rows:
                k = str(r.get(dedupe_key, "")).strip()
                if not k:
                    continue

                buffer_rows.append(r)
                existing_keys.add(k)

                if len(buffer_rows) >= autosave_every:
                    inserted = upsert_rows_to_excel(out_xlsx, buffer_rows, dedupe_key=dedupe_key)
                    inserted_total += inserted

                    logger.info(
                        f"[Autosave] ✅ Saved batch_rows={len(buffer_rows)} -> {out_xlsx} | "
                        f"NEW keys this batch={inserted} | NEW keys cycle_total={inserted_total}"
                    )

                    buffer_rows = []

            if buffer_rows:
                inserted = upsert_rows_to_excel(out_xlsx, buffer_rows, dedupe_key=dedupe_key)
                inserted_total += inserted

                logger.info(
                    f"[Final Save] ✅ Saved batch_rows={len(buffer_rows)} -> {out_xlsx} | "
                    f"NEW keys this batch={inserted} | NEW keys cycle_total={inserted_total}"
                )
            return inserted_total

        except KeyboardInterrupt:
            logger.warning("Interrupted by user (Ctrl+C). Saving buffered rows...")
            if buffer_rows:
                inserted = upsert_rows_to_excel(out_xlsx, buffer_rows, dedupe_key=dedupe_key, update_cols=TAX_COLS)
                inserted_total += inserted
                logger.info(f"[Interrupt Save] Inserted {inserted} NEW keys (UPSERT applied).")
            return inserted_total

        except Exception:
            logger.exception("Rows-mode portal cycle failed with an unexpected error.")
            try:
                if buffer_rows:
                    inserted = upsert_rows_to_excel(out_xlsx, buffer_rows, dedupe_key=dedupe_key, update_cols=TAX_COLS)
                    inserted_total += inserted
                    logger.info(f"[Error Save] Inserted {inserted} NEW keys (UPSERT applied).")
            except Exception:
                logger.exception("Failed saving buffered rows after error.")
            return inserted_total

    # -------------------------
    # SELENIUM MODE
    # -------------------------
    collect_fn: Optional[Callable] = cfg.get("collect")
    parse_fn: Optional[Callable] = cfg.get("parse")

    pages = int(cfg.get("pages", 1) or 1)
    limit = int(cfg.get("limit", 200) or 200)
    per_page = int(cfg.get("per_page", 30) or 30)

    if not collect_fn or not parse_fn:
        logger.error("Selenium mode missing collect/parse functions.")
        return 0

    autosave_every = int(cfg.get("autosave_every", 5) or 5)
    max_consec_fails = int(cfg.get("max_consec_fails", 3) or 3)

    driver = make_fast_driver(headless=CONFIG.headless)

    buffer_rows: List[Dict] = []
    inserted_total = 0
    consecutive_fails = 0

    try:
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
                if not row_key:
                    continue

                buffer_rows.append(row)
                existing_keys.add(row_key)
                consecutive_fails = 0

            except Exception as e:
                consecutive_fails += 1
                msg = str(e)

                if "BLOCKED_OR_CHALLENGE" in msg:
                    logger.warning("Challenge page detected. Restarting driver + cooldown...")
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    time.sleep(6.0)
                    driver = make_fast_driver(headless=CONFIG.headless)
                    consecutive_fails = 0
                    continue

                logger.exception(f"Failed to parse URL (skipping): {u}")

            if len(buffer_rows) >= autosave_every:
                try:
                    inserted = upsert_rows_to_excel(out_xlsx, buffer_rows, dedupe_key=dedupe_key, update_cols=TAX_COLS)
                    inserted_total += inserted
                    logger.info(f"[Autosave] ✅ Saved {len(buffer_rows)} rows -> {out_xlsx} | NEW keys: {inserted} | Total saved this cycle: {inserted_total + inserted}")
                    buffer_rows = []
                except Exception:
                    logger.exception("[Autosave] Failed while saving to Excel (continuing).")

            if consecutive_fails >= max_consec_fails:
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
        return inserted_total

    finally:
        try:
            driver.quit()
        except Exception:
            pass

    if buffer_rows:
        try:
            inserted = upsert_rows_to_excel(out_xlsx, buffer_rows, dedupe_key=dedupe_key, update_cols=TAX_COLS)
            inserted_total += inserted
            logger.info(f"[Final Save] ✅ Saved {len(buffer_rows)} rows -> {out_xlsx} | NEW keys: {inserted} | Total saved this cycle: {inserted_total}")
        except Exception:
            logger.exception("Failed while saving final rows to Excel.")

    logger.info(f"Inserted {inserted_total} NEW keys total (UPSERT applied).")
    logger.info(f"Saved to: {out_xlsx}")
    logger.info(f"Latest URL list saved to: {out_urls}")
    return inserted_total


# =========================
# CLI
# =========================
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

        # Post-cycle tasks AFTER all portals
        try:
            any_logger = next(iter(loggers.values()))
            run_post_cycle_tasks(any_logger)
        except Exception:
            pass

    for name in selected.keys():
        loggers[name].info(f"Watch mode: {args.watch} | Interval: {interval} seconds.")
        loggers[name].info("Press Ctrl + C to stop (watch mode).")

    if not args.watch:
        run_all_once()
        return

    try:
        while True:
            run_all_once()
            time.sleep(interval)
    except KeyboardInterrupt:
        for name in selected.keys():
            loggers[name].info("Stopped watch mode (Ctrl + C).")


if __name__ == "__main__":
    main()