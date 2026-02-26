# scraper_core.py
from __future__ import annotations

import os
import re
import tempfile
from datetime import datetime
from typing import Dict, Optional, Tuple

from selenium import webdriver
from selenium.common.exceptions import SessionNotCreatedException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


def make_fast_driver(headless: bool = True) -> webdriver.Chrome:
    """
    Default driver for non-LinkedIn portals.
    """
    opts = Options()
    opts.page_load_strategy = "eager"

    if headless:
        opts.add_argument("--headless=new")

    opts.add_argument("--window-size=1400,900")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")

    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    opts.add_argument("--disable-features=TranslateUI")
    opts.add_argument("--disable-background-networking")
    opts.add_argument("--disable-renderer-backgrounding")
    opts.add_argument("--disable-background-timer-throttling")
    opts.add_argument("--disable-backgrounding-occluded-windows")
    opts.add_argument("--disable-ipc-flooding-protection")

    opts.add_argument(
        "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=opts,
    )

    try:
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {
                "source": """
                    Object.defineProperty(navigator, 'webdriver', {
                      get: () => undefined
                    })
                """
            },
        )
    except Exception:
        pass

    driver.set_page_load_timeout(90)
    return driver


def _build_chrome_options(headless: bool) -> Options:
    opts = Options()
    opts.page_load_strategy = "eager"

    if headless:
        opts.add_argument("--headless=new")

    opts.add_argument("--window-size=1400,900")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--disable-features=TranslateUI")
    opts.add_argument("--remote-debugging-port=0")  # helps some mac builds

    # Optional: user can set CHROME_BINARY if needed
    chrome_bin = os.getenv("CHROME_BINARY", "").strip()
    if chrome_bin:
        opts.binary_location = chrome_bin

    return opts


def make_linkedin_driver(headless: bool, profile_path: str, profile_dir: str) -> webdriver.Chrome:
    """
    LinkedIn driver:
    - Try dedicated profile first (persistent login)
    - If it fails (profile lock), fallback to temp profile (still works, but may need login)
    """
    opts = Options()
    opts.page_load_strategy = "eager"

    if headless:
        opts.add_argument("--headless=new")

    opts.add_argument("--window-size=1400,900")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-blink-features=AutomationControlled")

    # Try dedicated profile first
    os.makedirs(profile_path, exist_ok=True)
    opts.add_argument(f"--user-data-dir={profile_path}")
    opts.add_argument(f"--profile-directory={profile_dir}")

    try:
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=opts,
        )
        driver.set_page_load_timeout(90)
        return driver
    except Exception as e:
        print("[WARN] Chrome session failed with dedicated profile (likely locked).")
        print("[WARN] Falling back to a temporary Chrome profile. Details:", e)

        # Fallback: temp profile
        tmp_profile = tempfile.mkdtemp(prefix="linkedin_tmp_profile_")
        opts2 = Options()
        opts2.page_load_strategy = "eager"
        if headless:
            opts2.add_argument("--headless=new")
        opts2.add_argument("--window-size=1400,900")
        opts2.add_argument("--disable-gpu")
        opts2.add_argument("--no-sandbox")
        opts2.add_argument("--disable-dev-shm-usage")
        opts2.add_argument("--disable-blink-features=AutomationControlled")
        opts2.add_argument(f"--user-data-dir={tmp_profile}")
        opts2.add_argument("--profile-directory=Default")

        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=opts2,
        )
        driver.set_page_load_timeout(90)
        return driver

def clean(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = re.sub(r"\s+", " ", str(s)).strip()
    return s if s else None


def clean_or_non(s: Optional[str], default: str = "Non") -> str:
    v = clean(s)
    return v if v else default


def now_iso() -> str:
    return datetime.utcnow().isoformat()


# --- (rest of your functions remain unchanged) ---
def infer_work_mode(text: str) -> Optional[str]:
    t = (text or "").lower()

    if "hybrid" in t:
        return "Hybrid"
    if any(k in t for k in ["remote", "work from home", "wfh"]):
        return "Remote"
    if any(k in t for k in ["on-site", "onsite", "on site"]):
        return "On-site"
    return None

def infer_country(location_text: str | None, default: str = "Nepal") -> str:
    """
    Very simple country inference.
    For MeroJob/JobsNepal, defaulting to Nepal is usually correct.
    You can expand this later with more rules.
    """
    t = (location_text or "").lower().strip()

    # obvious Nepal matches
    if any(k in t for k in ["nepal", "kathmandu", "lalitpur", "bhaktapur", "pokhara", "butwal", "biratnagar", "dharan"]):
        return "Nepal"

    # if location looks empty/unknown, still default Nepal
    if not t or t in {"non", "none", "na", "n/a"}:
        return default

    # fallback default
    return default


def classify_it_non_it(designation: str = "", industry: str = "", full_text: str = "") -> str:
    text = f"{designation} {industry} {full_text}".lower()

    # ✅ longer phrases are safe as substring matches
    phrase_keywords = [
        "information technology",
        "software development", "software engineer",
        "full stack", "fullstack",
        "data engineer", "data scientist",
        "machine learning", "cybersecurity", "cyber security",
        "system administrator", "technical support", "help desk", "helpdesk",
        "cloud computing", "devops",
        "rest api", "api development",
    ]

    # ✅ single words must be WHOLE WORD matches (avoid false positives)
    word_keywords = [
        "developer", "programmer", "engineer",
        "python", "java", "javascript", "react", "node", "django", "flask",
        "php", "laravel",
        "docker", "kubernetes",
        "aws", "azure", "gcp",
        "network", "database", "sql",
        "ai", "ml", "qa", "sdet",
    ]

    special_patterns = [
        r"\.net\b",
        r"\bc\+\+\b",
        r"\bc#\b",
        r"\bgolang\b",
        r"\bgo\b",
    ]

    for k in phrase_keywords:
        if k in text:
            return "IT"

    for w in word_keywords:
        if re.search(rf"\b{re.escape(w)}\b", text):
            return "IT"

    for pat in special_patterns:
        if re.search(pat, text):
            return "IT"

    return "Non-IT"

def categorize_role_taxonomy(
    title: str = "",
    skills: str = "",
    position: str = "",
    employment_type: str = "",
    description: str = "",
    industry: str = "",
) -> Dict[str, Optional[str]]:
    """
    Returns unified taxonomy for BOTH IT and Non-IT:
      category_primary: IT | Non-IT
      domain_l1: e.g. Frontend | Sales | Finance | Healthcare ...
      domain_l2: e.g. React | B2B Sales | Accounting ...
      domain_l3: e.g. Next.js | SEO | Payroll ...
      tax_confidence: float 0..1

    Rule-based and safe. You can expand keywords anytime.
    """

    def _norm(x: str) -> str:
        x = (x or "").lower()
        x = re.sub(r"\s+", " ", x).strip()
        return x

    text = _norm(f"{title} {skills} {position} {employment_type} {industry} {description}")

    # -----------------------------
    # 1) IT detection (reuse your robust signals)
    # -----------------------------
    it_phrase_keywords = [
        "information technology",
        "software development", "software engineer",
        "full stack", "fullstack",
        "data engineer", "data scientist",
        "machine learning", "cybersecurity", "cyber security",
        "system administrator", "technical support", "help desk", "helpdesk",
        "cloud computing", "devops",
        "rest api", "api development",
    ]
    it_word_keywords = [
        "developer", "programmer", "engineer",
        "python", "java", "javascript", "react", "node", "django", "flask",
        "php", "laravel",
        "docker", "kubernetes",
        "aws", "azure", "gcp",
        "network", "database", "sql",
        "ai", "ml", "qa", "sdet",
    ]
    it_special_patterns = [
        r"\.net\b",
        r"\bc\+\+\b",
        r"\bc#\b",
        r"\bgolang\b",
        r"\bgo\b",
    ]

    it_hits = 0
    for k in it_phrase_keywords:
        if k in text:
            it_hits += 2
    for w in it_word_keywords:
        if re.search(rf"\b{re.escape(w)}\b", text):
            it_hits += 1
    for pat in it_special_patterns:
        if re.search(pat, text):
            it_hits += 2

    is_it = it_hits >= 2

    # -----------------------------
    # 2) IT taxonomy (L1/L2/L3)
    # -----------------------------
    if is_it:
        # L1 buckets
        it_l1_rules = {
            "Security": [r"\bsoc\b", "siem", "pentest", "penetration", "vulnerability", "blue team", "red team", "grc", "iso 27001", "owasp", "incident response"],
            "DevOps": ["devops", "sre", "kubernetes", "docker", "ci/cd", "jenkins", "github actions", "terraform", "ansible", "helm", "prometheus", "grafana"],
            "Data": ["data engineer", "etl", "data warehouse", "power bi", "tableau", "dbt", "spark", "hadoop", "analytics", "bi developer"],
            "AI": ["machine learning", "deep learning", "nlp", "computer vision", "llm", "pytorch", "tensorflow", "genai", "generative ai"],
            "Frontend": ["frontend", "front end", "react", "vue", "angular", "javascript", "typescript", "next.js", "nuxt", "tailwind", "html", "css"],
            "Backend": ["backend", "back end", "api", "django", "fastapi", "flask", "spring", "node", "express", ".net", "asp.net", "laravel", "rails"],
            "Mobile": ["android", "ios", "swift", "kotlin", "flutter", "react native", "xamarin"],
            "QA": ["qa", "quality assurance", "sdet", "automation testing", "selenium", "cypress", "playwright", "jmeter"],
            "IT-Other": ["system admin", "sysadmin", "it support", "helpdesk", "network engineer", "ccna", "linux admin", "windows server"],
        }

        def _score_bucket(rules: list) -> int:
            s = 0
            for r in rules:
                if r.startswith(r"\b"):
                    if re.search(r, text):
                        s += 2
                else:
                    if r in text:
                        s += 1
            return s

        l1_scores = {k: _score_bucket(v) for k, v in it_l1_rules.items()}
        domain_l1 = max(l1_scores, key=lambda k: l1_scores[k]) if l1_scores else "IT-Other"
        if l1_scores.get(domain_l1, 0) == 0:
            domain_l1 = "IT-Other"

        # L2 / L3
        domain_l2 = None
        domain_l3 = None

        if domain_l1 == "Frontend":
            if "react" in text:
                domain_l2 = "React"
                if "next" in text:
                    domain_l3 = "Next.js"
            elif "vue" in text:
                domain_l2 = "Vue"
                if "nuxt" in text:
                    domain_l3 = "Nuxt"
            elif "angular" in text:
                domain_l2 = "Angular"
            else:
                domain_l2 = "Vanilla"

        elif domain_l1 == "Backend":
            if "python" in text:
                domain_l2 = "Python"
                if "django" in text:
                    domain_l3 = "Django"
                elif "fastapi" in text:
                    domain_l3 = "FastAPI"
                elif "flask" in text:
                    domain_l3 = "Flask"
            elif ".net" in text or "asp.net" in text or "c#" in text:
                domain_l2 = ".NET"
                if "asp.net" in text:
                    domain_l3 = "ASP.NET"
            elif "java" in text:
                domain_l2 = "Java"
                if "spring" in text:
                    domain_l3 = "Spring"
            elif "node" in text or "express" in text:
                domain_l2 = "Node"
                if "express" in text:
                    domain_l3 = "Express"
            elif "php" in text:
                domain_l2 = "PHP"
                if "laravel" in text:
                    domain_l3 = "Laravel"

        elif domain_l1 == "AI":
            if "nlp" in text or "language model" in text or "llm" in text:
                domain_l2 = "NLP"
            elif "computer vision" in text or "opencv" in text:
                domain_l2 = "CV"
            else:
                domain_l2 = "ML"

            if "pytorch" in text:
                domain_l3 = "PyTorch"
            elif "tensorflow" in text:
                domain_l3 = "TensorFlow"

        elif domain_l1 == "Data":
            if "data engineer" in text or "etl" in text:
                domain_l2 = "Engineering"
            elif "power bi" in text or "tableau" in text or "analytics" in text:
                domain_l2 = "Analytics"
            else:
                domain_l2 = "Science"

        elif domain_l1 == "Security":
            if "soc" in text or "siem" in text:
                domain_l2 = "SOC"
            elif "pentest" in text or "penetration" in text:
                domain_l2 = "Offensive"
            elif "grc" in text or "compliance" in text:
                domain_l2 = "GRC"
            else:
                domain_l2 = "Defensive"

        elif domain_l1 == "DevOps":
            if "kubernetes" in text or "helm" in text:
                domain_l2 = "Kubernetes"
            elif "terraform" in text or "iac" in text:
                domain_l2 = "IaC"
            elif "ci/cd" in text or "jenkins" in text or "github actions" in text:
                domain_l2 = "CI/CD"
            else:
                domain_l2 = "Cloud"

        elif domain_l1 == "QA":
            if "automation" in text or "selenium" in text or "cypress" in text or "playwright" in text:
                domain_l2 = "Automation"
            else:
                domain_l2 = "Manual"

        elif domain_l1 == "Mobile":
            if "flutter" in text:
                domain_l2 = "Cross-platform"
                domain_l3 = "Flutter"
            elif "react native" in text:
                domain_l2 = "Cross-platform"
                domain_l3 = "React Native"
            elif "android" in text or "kotlin" in text:
                domain_l2 = "Android"
            elif "ios" in text or "swift" in text:
                domain_l2 = "iOS"
            else:
                domain_l2 = "Cross-platform"

        # confidence: based on it_hits + l1 score
        base = min(1.0, 0.35 + (it_hits * 0.08) + (l1_scores.get(domain_l1, 0) * 0.05))

        return {
            "category_primary": "IT",
            "domain_l1": domain_l1,
            "domain_l2": domain_l2,
            "domain_l3": domain_l3,
            "tax_confidence": round(float(base), 3),
        }

    # -----------------------------
    # 3) NON-IT taxonomy (L1/L2/L3)
    # -----------------------------
    non_it_rules = {
        "Sales": ["sales", "business development", "b2b", "b2c", "account executive", "lead generation", "cold call", "crm", "pipeline", "inside sales", "field sales"],
        "Marketing": ["marketing", "seo", "sem", "google ads", "facebook ads", "content", "copywriting", "brand", "social media", "digital marketing", "growth"],
        "Finance": ["finance", "account", "accounting", "audit", "tax", "vat", "payroll", "budget", "cfo", "controller", "banking", "treasury"],
        "HR": ["human resource", "hr", "recruit", "talent", "onboarding", "payroll", "performance", "training", "compensation", "benefits"],
        "Operations": ["operations", "admin", "administration", "office", "procurement", "supply chain", "inventory", "warehouse", "compliance", "process"],
        "Customer Support": ["customer support", "customer service", "call center", "support", "helpdesk", "complaint", "ticket", "csr"],
        "Education": ["teacher", "teaching", "instructor", "lecturer", "school", "college", "curriculum", "tutor", "training"],
        "Healthcare": ["nurse", "doctor", "medical", "clinic", "hospital", "pharmacy", "lab", "health", "dentist", "radiology"],
        "Engineering": ["civil engineer", "mechanical engineer", "electrical engineer", "architect", "construction", "site engineer", "autocad", "quantity surveyor"],
        "Legal": ["legal", "lawyer", "advocate", "paralegal", "contract", "litigation", "compliance officer"],
        "Hospitality": ["hotel", "restaurant", "chef", "cook", "barista", "waiter", "front desk", "housekeeping", "hospitality"],
        "Logistics": ["logistics", "delivery", "driver", "fleet", "transport", "shipment", "dispatch", "courier", "import", "export", "customs"],
        "Design": ["graphic designer", "designer", "photoshop", "illustrator", "indesign", "video editor", "motion graphics", "premiere", "after effects"],
        "Management": ["manager", "project manager", "product manager", "team lead", "director", "head of", "supervisor", "coordinator"],
        "Non-IT-Other": [],
    }

    def _score_list(lst) -> int:
        s = 0
        for k in lst:
            if k in text:
                # phrases get higher weight
                s += 2 if " " in k else 1
        return s

    scores = {k: _score_list(v) for k, v in non_it_rules.items()}
    domain_l1 = max(scores, key=lambda k: scores[k]) if scores else "Non-IT-Other"
    if scores.get(domain_l1, 0) == 0:
        domain_l1 = "Non-IT-Other"

    domain_l2 = None
    domain_l3 = None

    # L2/L3 specialization examples
    if domain_l1 == "Sales":
        if "b2b" in text:
            domain_l2 = "B2B"
        elif "b2c" in text:
            domain_l2 = "B2C"
        else:
            domain_l2 = "General"
        if "crm" in text or "salesforce" in text:
            domain_l3 = "CRM"

    elif domain_l1 == "Marketing":
        if "seo" in text:
            domain_l2 = "SEO"
        elif "sem" in text or "google ads" in text:
            domain_l2 = "Performance"
            domain_l3 = "Google Ads" if "google ads" in text else None
        elif "social media" in text:
            domain_l2 = "Social"
        else:
            domain_l2 = "General"

    elif domain_l1 == "Finance":
        if "audit" in text:
            domain_l2 = "Audit"
        elif "tax" in text or "vat" in text:
            domain_l2 = "Tax"
        elif "payroll" in text:
            domain_l2 = "Payroll"
        else:
            domain_l2 = "Accounting"

    elif domain_l1 == "HR":
        if "recruit" in text or "talent" in text:
            domain_l2 = "Recruitment"
        elif "training" in text or "l&d" in text:
            domain_l2 = "L&D"
        elif "payroll" in text:
            domain_l2 = "Payroll"
        else:
            domain_l2 = "General"

    elif domain_l1 == "Operations":
        if "procurement" in text:
            domain_l2 = "Procurement"
        elif "supply chain" in text or "inventory" in text:
            domain_l2 = "Supply Chain"
        elif "admin" in text or "administration" in text or "office" in text:
            domain_l2 = "Admin"
        else:
            domain_l2 = "General"

    elif domain_l1 == "Customer Support":
        if "call center" in text:
            domain_l2 = "Call Center"
        elif "chat" in text:
            domain_l2 = "Chat Support"
        else:
            domain_l2 = "General"

    elif domain_l1 == "Engineering":
        if "civil" in text:
            domain_l2 = "Civil"
        elif "mechanical" in text:
            domain_l2 = "Mechanical"
        elif "electrical" in text:
            domain_l2 = "Electrical"
        elif "architect" in text:
            domain_l2 = "Architecture"
        else:
            domain_l2 = "General"

    elif domain_l1 == "Healthcare":
        if "nurse" in text:
            domain_l2 = "Nursing"
        elif "pharmacy" in text:
            domain_l2 = "Pharmacy"
        elif "lab" in text:
            domain_l2 = "Lab"
        else:
            domain_l2 = "General"

    elif domain_l1 == "Design":
        if "video" in text or "premiere" in text or "after effects" in text:
            domain_l2 = "Video"
        else:
            domain_l2 = "Graphic"

    elif domain_l1 == "Management":
        if "project manager" in text:
            domain_l2 = "Project"
        elif "product manager" in text:
            domain_l2 = "Product"
        else:
            domain_l2 = "General"

    # confidence: based on best bucket score
    best = scores.get(domain_l1, 0)
    conf = min(1.0, 0.30 + (best * 0.12))
    if domain_l1 == "Non-IT-Other":
        conf = 0.35

    return {
        "category_primary": "Non-IT",
        "domain_l1": domain_l1,
        "domain_l2": domain_l2,
        "domain_l3": domain_l3,
        "tax_confidence": round(float(conf), 3),
    }


def normalize_experience_years(experience_raw: Optional[str]) -> Tuple[Optional[float], Optional[float]]:
    if not experience_raw:
        return None, None

    s = experience_raw.lower().strip()

    if "not required" in s or "no experience" in s:
        return 0.0, 0.0

    m = re.search(r"(\d+(\.\d+)?)\s*-\s*(\d+(\.\d+)?)\s*year", s)
    if m:
        return float(m.group(1)), float(m.group(3))

    m = re.search(r"(\d+(\.\d+)?)\s*\+\s*year", s)
    if m:
        return float(m.group(1)), None

    m = re.search(r"more than\s*(\d+(\.\d+)?)\s*year", s)
    if m:
        return float(m.group(1)), None

    m = re.search(r"(\d+(\.\d+)?)\s*year", s)
    if m:
        v = float(m.group(1))
        return v, v

    return None, None


def parse_experience_years(experience_raw: Optional[str]) -> Tuple[Optional[float], Optional[float]]:
    return normalize_experience_years(experience_raw)


def normalize_salary(salary_raw: Optional[str]) -> Tuple[Optional[int], Optional[int], Optional[str], Optional[str]]:
    if not salary_raw:
        return None, None, None, None

    s = salary_raw.strip()
    low = s.lower()

    if any(k in low for k in ["not disclosed", "based on experience", "negotiable"]):
        return None, None, None, None

    currency = None
    if any(k in low for k in ["npr", "rs", "रु"]):
        currency = "NPR"

    period = None
    if "month" in low:
        period = "month"
    elif "year" in low or "annum" in low:
        period = "year"
    elif "day" in low:
        period = "day"

    nums = [int(x.replace(",", "")) for x in re.findall(r"(\d[\d,]*)", s)]
    if not nums:
        return None, None, currency, period

    if len(nums) == 1:
        return nums[0], nums[0], currency, period

    return min(nums), max(nums), currency, period


def parse_salary(salary_raw: Optional[str]) -> Tuple[Optional[int], Optional[int], Optional[str], Optional[str]]:
    return normalize_salary(salary_raw)
