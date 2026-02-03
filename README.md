# Nepal Job Market Scraping Pipeline (Multi-Portal, Incremental, Production-Grade)

A modular, production-style data pipeline to collect job postings from Nepal job portals using **Python + Selenium + pandas**, with **incremental (idempotent) Excel updates**, **multi-portal support**, and **watch mode** for continuous monitoring.

---

## ✅ Features

- **Multi-portal architecture** (add portals easily)
- **Two portal modes**
  - **Selenium mode** (scrape listing pages + job detail pages)
  - **Rows mode** (CSV/API/export → normalized records, no browser)
- **Incremental updates (idempotent)**
  - Excel files are **never overwritten**
  - Only **new jobs** are appended (dedupe by `job_url`)
- **Normalized schema**
  - salary, experience, work-mode inference, IT vs Non-IT classification
- **Watch mode**
  - auto re-scrapes portals every N seconds
- **Per-portal logging**
  - rotating logs under `logs/`

---

## 📁 Project Structure

Data_scraping/
├── config.py
├── scraper_core.py
├── run_pipeline.py
├── portals/
│ ├── merojob.py
│ ├── jobsnepal.py
│ └── linkedin_export.py
├── data/
│ ├── merojob_jobs.xlsx
│ ├── jobsnepal_jobs.xlsx
│ ├── linkedin_jobs.xlsx
│ └── *_urls_latest.txt
├── logs/
│ └── *.log
└── requirements.txt


---

## ⚙️ Requirements

- Python **3.10+** recommended
- Google Chrome installed
- macOS / Linux / Windows supported

---

## 🚀 Setup (Terminal)

From inside your project folder:

```bash
cd /path/to/Data_scraping
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
▶️ Run the Pipeline
Run one portal:

python run_pipeline.py --portal merojob
python run_pipeline.py --portal jobsnepal
python run_pipeline.py --portal linkedin
Run all portals:

python run_pipeline.py --portal all
🔁 Watch Mode (Continuous Monitoring)
Re-run portals every 10 minutes (600 seconds):

python run_pipeline.py --watch --interval 600 --portal all
Stop with:

Ctrl + C
📦 Output Files
All outputs go to data/.

Each portal produces:

<portal>_jobs.xlsx → normalized job dataset

<portal>_urls_latest.txt → latest scraped URLs for auditing/debugging

Example:

data/jobsnepal_jobs.xlsx
data/jobsnepal_urls_latest.txt
🧠 Normalized Schema (Columns)
Every saved job row is normalized into a consistent schema:

source

category_primary (IT / Non-IT)

industry

designation

company (some portals)

level

experience_raw

experience_min_years

experience_max_years

salary_raw

salary_min

salary_max

salary_currency

salary_period

onsite_hybrid_remote

location

deadline

job_type

date_posted

job_url

scraped_at

Missing values are stored as "Non" by design.

🧩 Portal Modes Explained
1) Selenium Mode
Used for portals that require browsing pages (MeroJob, JobsNepal).

Each Selenium portal must implement:

collect_job_urls(driver, pages, limit) -> List[str]

parse_job_detail(driver, url) -> Dict | None

2) Rows Mode
Used for portals that can be imported from CSV/Excel/API (LinkedIn export).

Rows portal must implement:

collect_rows(CONFIG) -> List[Dict]

🔗 LinkedIn Export Setup
This pipeline does NOT scrape LinkedIn directly.
Instead, you export your saved jobs/applications and place the file into:

data/linkedin_exports/
Steps (Terminal)
Ensure folder exists:

mkdir -p data/linkedin_exports
Copy your export into it (example from Downloads):

cp ~/Downloads/*.csv data/linkedin_exports/
Run:

python run_pipeline.py --portal linkedin
Notes
If the export lacks job URLs, the pipeline generates a stable synthetic ID like:
linkedin://<hash>

If your export columns don’t match expected names, use the mapping override fields in config.py.

🛠 Configuration (config.py)
All tuning is in config.py via a shared CONFIG object.

Key settings:

pages, limit

headless (true/false)

watch_default_interval_sec

LinkedIn export folder + pattern

LinkedIn column mapping overrides (optional)

Example (override LinkedIn mapping):

linkedin_col_title = "job_title"
linkedin_col_company = "company_name"
linkedin_col_location = "city"
🧾 Logging
Logs are stored in:

logs/<portal>.log
Examples:

tail -n 50 logs/jobsnepal.log
tail -n 50 logs/linkedin.log
🧰 Troubleshooting
1) ImportError: cannot import name 'CONFIG' from 'config'
Fix: ensure config.py ends with:

CONFIG = ScrapeConfig()
2) LinkedIn: “No export file found”
Make sure a CSV/XLSX exists in:

ls -lah data/linkedin_exports
Then run again.

3) Selenium issues / ChromeDriver problems
Update dependencies:

pip install -U selenium webdriver-manager
If headless mode causes portal issues, disable headless in config:

headless = False
🧱 Adding a New Portal
Create a new portal module:

portals/<newportal>.py
Implement either Selenium mode functions or rows mode.

Register it in run_pipeline.py inside PORTALS:

PORTALS["newportal"] = {
  "mode": "selenium",
  "collect": new_collect,
  "parse": new_parse,
  "pages": CONFIG.pages,
  "limit": CONFIG.limit,
}
