"""" 
# portals/dummy.py
from __future__ import annotations
from typing import Dict, List
from scraper_core import now_iso

def collect_job_urls(driver=None, pages: int = 1, limit: int = 10) -> List[str]:
    # Not used, but required by selenium-mode interface
    return []

def parse_job_detail(driver=None, url: str = "") -> Dict:
    # Not used
    return {}

def collect_rows(config) -> List[Dict]:
    #Rows-mode collector: returns list[dict] with job_url (dedupe key).
    #Each run returns the SAME first 2 rows + a NEW 3rd row after a few seconds
    #(so you can test incremental append).
    
    # Always present rows
    rows = [
        {
            "source": "DUMMY",
            "designation": "Dummy Job 1",
            "company": "Dummy Co",
            "location": "Kathmandu",
            "job_url": "https://dummy/jobs/1",
            "scraped_at": now_iso(),
        },
        {
            "source": "DUMMY",
            "designation": "Dummy Job 2",
            "company": "Dummy Co",
            "location": "Pokhara",
            "job_url": "https://dummy/jobs/2",
            "scraped_at": now_iso(),
        },
    ]

    # Add a “new” row that changes each time by using timestamp in URL
    # (this guarantees 1 new row gets appended each run)
    import time
    t = int(time.time())
    rows.append({
        "source": "DUMMY",
        "designation": f"Dummy Job NEW {t}",
        "company": "Dummy Co",
        "location": "Nepal",
        "job_url": f"https://dummy/jobs/new-{t}",
        "scraped_at": now_iso(),
    })

    return rows 
"""