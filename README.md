Nepal Job Market Scraping Pipeline (Multi-Portal, Incremental UPSERT, Watch Mode)

A modular, production-style pipeline to collect job postings from multiple portals using Python + Selenium + pandas, with incremental UPSERT updates to Excel, multi-portal architecture, and watch mode for continuous refresh. Includes optional Dash dashboard for live charts.

✅ Key Features
Multi-portal architecture

Add new portals easily (each portal lives in portals/)

Two portal styles (supported)

Selenium portal (listing → detail scraping)

collect_job_urls()

parse_job_detail()

Rows portal (already-structured data, CSV/API)

collect_rows() (optional mode)

Incremental storage (UPSERT = update + insert)

Excel files are never overwritten

Existing jobs are updated when the same ID appears again

New jobs are inserted automatically

Works well in watch mode (re-scrape repeatedly without duplicates)

Normalized schema

Work mode inference (remote/hybrid/onsite)

IT vs Non-IT classification

Country inference added for MeroJob, JobsNepal, LinkedIn

Saved consistently to Excel

Watch mode

Re-runs portals every N seconds

Supports autosave batching (LinkedIn can autosave every X rows)

Logging

Per-portal logs under logs/ (helps debug each portal independently)

📁 Project Structure
Data_scraping/
├── config.py
├── scraper_core.py
├── run_pipeline.py
├── portals/
│   ├── merojob.py
│   ├── jobsnepal.py
│   └── linkedin.py                # ✅ Selenium LinkedIn multi-country
├── dashboard/
│   └── app.py                     # ✅ Dash dashboard
├── data/
│   ├── merojob_jobs.xlsx
│   ├── jobsnepal_jobs.xlsx
│   ├── linkedin_jobs.xlsx
│   └── *_urls_latest.txt
├── logs/
│   └── *.log
└── requirements.txt

⚙️ Requirements

Python 3.10+

Google Chrome

Selenium + ChromeDriver (webdriver-manager recommended)

🚀 Setup
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

All outputs go to your configured Excel directory (from config.py).

Each portal produces:

data/<portal>_jobs.xlsx → normalized dataset

data/<portal>_urls_latest.txt → URL audit/debug

Examples:

data/jobsnepal_jobs.xlsx

data/jobsnepal_urls_latest.txt

🧠 Normalized Schema (Columns)

Your saved Excel rows are normalized into a consistent set of columns (example):

job_id

title

company

company_link

location

country ✅ (new)

posted_date

num_applicants

work_mode

employment_type

position

type

compensation

commitment

skills

category_primary (IT / Non-IT)

job_url

source

scraped_at

Missing values are stored as "Non" by design (as you implemented).

🌍 LinkedIn: Multi-Country Selenium Scraping (Current System)

✅ Your pipeline now supports scraping LinkedIn across multiple countries using geoId.

Configure targets in config.py
linkedin_targets = (
  {"country": "Nepal", "geoId": "104630404"},
  {"country": "United States", "geoId": "103644278"},
  ...
)

What LinkedIn scraper does

Loops through each target country

Builds listing URLs using geoId

Collects job IDs and opens job details

Adds country into every output row

Saves incrementally via UPSERT (optionally autosave batches)

🧩 Portal Modes Explained
1) Selenium Mode (MeroJob, JobsNepal, LinkedIn)

Each portal implements:

collect_job_urls(driver, pages, limit, ...) -> List[str]

parse_job_detail(driver, url) -> Dict | None

2) Rows Mode (Optional / future)

For CSV/API sources:

collect_rows(CONFIG) -> List[Dict]

📊 Dashboard (Dash)

Your dashboard is under:

dashboard/app.py


Your component IDs (confirmed via grep):

status

interval

jobs_per_portal

it_nonit_by_portal

top_locations

employment_type

scrape_trend

Run dashboard:

python dashboard/app.py


Important fixes you already applied / must keep:

Dash: app.run(...) not app.run_server(...)

Pandas frequency: use "h" not "H" for .dt.floor("h") (new pandas behavior)

🧾 Logging

Logs are stored in:

logs/<portal>.log


Examples:

tail -n 50 logs/jobsnepal.log
tail -n 50 logs/linkedin.log

🛠 Troubleshooting
1) Dash error: app.run_server obsolete

Fix in dashboard/app.py:

app.run(debug=True, host="127.0.0.1", port=8050)

2) Pandas error: Invalid frequency "H"

Use:

dt.floor("h")

3) Excel error: BadZipFile: File is not a zip file

This happens when the .xlsx file is corrupted (often due to interruption while saving).
Fix:

Delete the corrupted Excel file (or rename it as backup)

Re-run the pipeline so it regenerates a clean .xlsx

4) Selenium error: InvalidSessionIdException

Driver got killed/crashed or session expired.
Fix:

Recreate the driver for each run (recommended)

Avoid long idle sessions

Ensure driver.quit() runs in finally

🔐 Security & Long-Run Risks (What you should know)

LinkedIn is sensitive to automation → higher risk of rate-limit, authwall, temporary blocks.

Store credentials using environment variables, not hardcoded.

Prefer using a logged-in Chrome profile (less password handling).

Don’t upload raw datasets publicly (may contain tracked URLs / identifiable patterns).

Keep your OneDrive output path safe (avoid exposing links publicly).

🧱 Adding a New Portal

Create:

portals/<newportal>.py


Implement selenium or rows mode functions

Register in run_pipeline.py under PORTALS
