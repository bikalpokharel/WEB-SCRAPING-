Job Market Scraping Pipeline

Multi-Portal • Incremental UPSERT • Watch Mode • Master Builder • Quality Audit • Live Dashboard

A modular, production-style scraping and analytics pipeline built with:

Python

Selenium

pandas

Excel incremental UPSERT storage

Master dataset builder

Portal quality auditor

Dash live dashboard

Designed for continuous scraping, safe OneDrive usage, and real-time analytics.

✅ Key Features
🔹 Multi-Portal Architecture

Each portal lives independently inside:

portals/

Currently supported:

merojob (Selenium)

jobsnepal (Selenium)

linkedin (Rows mode / structured return)

Adding a new portal requires:

Creating portals/<newportal>.py

Implementing required functions

Registering inside run_pipeline.py

🔹 Two Portal Modes
1️⃣ Selenium Mode

Used for:

MeroJob

JobsNepal

Required functions:

collect_job_urls(driver, pages, limit, ...)
parse_job_detail(driver, url)

Flow:

Collect listing URLs

Visit each URL

Parse job details

2️⃣ Rows Mode

Used for:

LinkedIn

Required function:

collect_rows(CONFIG) -> List[Dict]

Flow:

Returns normalized rows directly

UPSERT handled centrally

🔹 Incremental UPSERT Storage

Excel files are never overwritten.

If:

Same key appears again → row is updated

New key appears → row is inserted

Newest scraped_at always wins.

Safe for:

Watch mode

Repeated scraping

Continuous refresh

🔹 OneDrive-Safe Atomic Writes

To prevent corrupted Excel files:

Files are written to temp files

Then atomically replaced

Local cache used before read/write

Prevents:

BadZipFile

Partial write corruption

Network timeouts

🔹 Watch Mode

Re-runs scraping automatically:

python run_pipeline.py --watch --interval 600 --portal all

Runs every 600 seconds.

Stop with:

Ctrl + C
🔹 Master Dataset Builder

analysis/build_master.py

Creates:

jobs_master.xlsx

jobs_master.csv

jobs_master_local.csv (for dashboards)

Features:

Cross-portal deduplication

Global key generation

Newest record wins

Placeholder normalization

Atomic writes

🔹 Portal Quality Audit

analysis/portal_quality.py

Generates:

portal_quality_report.xlsx

Includes:

Overall sparsity

Core field sparsity

Optional field sparsity

Missing-by-column breakdown

Per-portal sheets

Master dataset audit

🔹 Live Dashboard (Dash)

Located in:

dashboard/

Features:

Time series of job counts

Multi-filter system

Multi-line comparison

Auto-refresh

Manual refresh

Responsive design

Reads:

jobs_master.csv
or
jobs_master_local.csv

Run:

python dashboard/app.py
📁 Project Structure
Data_scraping/
├── config.py
├── scraper_core.py
├── run_pipeline.py
├── analysis/
│   ├── build_master.py
│   └── portal_quality.py
├── dashboard/
│   └── app.py
├── portals/
│   ├── merojob.py
│   ├── jobsnepal.py
│   └── linkedin.py
├── data/
│   ├── merojob_jobs.xlsx
│   ├── jobsnepal_jobs.xlsx
│   ├── linkedin_jobs.xlsx
│   └── _internal/
├── logs/
│   └── *.log
├── data_local/
│   └── jobs_master_local.csv
├── requirements.txt
└── README.md
⚙️ Requirements

Python 3.10+

Google Chrome

ChromeDriver (webdriver-manager recommended)

Install:

python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
▶️ Running the Pipeline
Run single portal
python run_pipeline.py --portal merojob
python run_pipeline.py --portal jobsnepal
python run_pipeline.py --portal linkedin
Run all portals
python run_pipeline.py --portal all
🔁 Watch Mode
python run_pipeline.py --watch --interval 600 --portal all
📦 Output Files

Per-portal:

data/<portal>_jobs.xlsx
data/_internal/<portal>_urls_latest.txt

Master:

jobs_master.xlsx
jobs_master.csv
jobs_master_local.csv

Quality:

portal_quality_report.xlsx
🧠 Normalized Schema

All portal rows are normalized into:

job_id

title

company

company_link

location

country

posted_date

num_applicants

work_mode

employment_type

position

type

compensation

commitment

skills

category_primary

job_url

source

scraped_at

Master adds:

global_key

master_built_at

🔐 Security Notes

Avoid committing Excel data publicly

Do not hardcode credentials

LinkedIn automation may trigger rate limits

Prefer logged-in Chrome profile

Use environment variables for secrets

🛠 Troubleshooting
1️⃣ Excel corrupted (BadZipFile)

Delete corrupted file and re-run pipeline.

2️⃣ Pandas frequency error

Use:

dt.floor("h")

(not "H")

3️⃣ Dash run_server obsolete

Use:

app.run(...)
4️⃣ Selenium driver crash

Driver restart logic already implemented.

🚀 Advanced Capabilities (Optional Extensions)

Trend tracking of sparsity over time

Anomaly detection (row drop alerts)

JSON export for APIs

Database backend (PostgreSQL)

Docker containerization

Scheduled cron deployment

Production logging to structured logs

CI pipeline

🏗 System Architecture Overview

Scraper → UPSERT → Master Builder → Quality Audit → Dashboard

Portal Excel Files
        ↓
build_master.py
        ↓
jobs_master.csv
        ↓
Dash Dashboard
🏁 Status

This pipeline is:

✔ Production-stable
✔ OneDrive-safe
✔ Watch-mode safe
✔ Incremental
✔ Dashboard-ready
✔ Extendable