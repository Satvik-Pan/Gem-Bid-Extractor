from __future__ import annotations

import csv
import os
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(BASE_DIR / ".env")
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
LOG_DIR = BASE_DIR / "logs"

DATA_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)
PDF_CACHE_DIR = DATA_DIR / "pdf_cache"
PDF_CACHE_DIR.mkdir(parents=True, exist_ok=True)

EXCEL_FILE = OUTPUT_DIR / "Extracted_bids.xlsx"
DOUBTFUL_FILE = OUTPUT_DIR / "doubtful_bids.xlsx"
PROCESSED_FILE = DATA_DIR / "processed_bids.json"
LOG_FILE = LOG_DIR / "scraper.log"
RUN_STATUS_FILE = DATA_DIR / "last_run_status.json"
SYNC_QUEUE_FILE = DATA_DIR / "db_sync_queue.jsonl"
DNS_CACHE_FILE = DATA_DIR / "dns_cache.json"

KEYWORDS_FILE = BASE_DIR / "src" / "gem_bid_extractor" / "keywords.csv"

# ---------------------------------------------------------------------------
# Pipeline 1: Keyword-based search with date filtering
# ---------------------------------------------------------------------------
# How many days back from today to include bids by Start Date.
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "3"))
# Safety cap: stop paginating after this many pages per keyword.
MAX_PAGES_PER_KEYWORD = int(os.environ.get("MAX_PAGES_PER_KEYWORD", "80"))

# ---------------------------------------------------------------------------
# Inclusion keywords — searched one-by-one on GEM portal
# ---------------------------------------------------------------------------
DEFAULT_INCLUSION_KEYWORDS = [
    "Router",
    "NGFW",
    "FIREWALL",
    "FIRE WALL",
    "VPN",
    "UTM",
    "Next Generation Firewall",
    "UNIFIED Threat Manager",
    "Network Security",
    "Web Application Firewall",
    "WAF",
]

# ---------------------------------------------------------------------------
# Exclusion keywords — checked in bid document PDFs
# ---------------------------------------------------------------------------
DEFAULT_EXCLUSION_KEYWORDS = [
    "IPS", "Load Balancer", "NextGen", "Next", "UNIFIED", "SDWAN", "SD WAN",
    "Software Define WAN", "DNS", "Intrusion", "LLB", "SLB", "Web Security",
    "Threat", "Internet", "Gateway", "Perimeter", "Endpoint", "EPS", "Malware",
    "Ransomware", "IPSec", "Edge", "Cyber Security", "Virus", "AAA", "Firepower",
    "ASA", "Bandwidth", "Renewal", "Authentication", "LAN", "Armynet", "Domain",
    "Anti-APT", "Anti-ATP", "Sophos", "Gajshield", "Checkpoint", "Anexgate",
    "Tacitine", "Fortinet", "Fortigate", "PaloAlto", "Quickheal", "Forcepoint",
    "CISCO", "Juniper", "Sonicwall", "TrendMicro", "Mcafee", "Radware",
    "Array Networks", "Haltdos", "DDoS", "Trellix", "Data Loss Prevention",
    "Scada/ Scada Firewall",
]


def _normalize_term(term: str) -> str:
    return " ".join(term.strip().split())


def _load_keyword_sets() -> tuple[list[str], list[str]]:
    if not KEYWORDS_FILE.exists():
        return list(DEFAULT_INCLUSION_KEYWORDS), list(DEFAULT_EXCLUSION_KEYWORDS)

    inclusion: list[str] = []
    exclusion: list[str] = []
    try:
        with KEYWORDS_FILE.open("r", encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                inc = _normalize_term(str(row.get("Inclusion", "")))
                exc = _normalize_term(str(row.get("Exclusion", "")))
                if inc:
                    inclusion.append(inc)
                if exc:
                    exclusion.append(exc)
    except OSError:
        return list(DEFAULT_INCLUSION_KEYWORDS), list(DEFAULT_EXCLUSION_KEYWORDS)

    inclusion = inclusion or list(DEFAULT_INCLUSION_KEYWORDS)
    exclusion = exclusion or list(DEFAULT_EXCLUSION_KEYWORDS)

    # Dedupe preserving order
    inc_seen: set[str] = set()
    inclusion_deduped: list[str] = []
    for t in inclusion:
        k = t.lower()
        if k not in inc_seen:
            inc_seen.add(k)
            inclusion_deduped.append(t)

    exc_seen: set[str] = set()
    exclusion_deduped: list[str] = []
    for t in exclusion:
        k = t.lower()
        if k not in exc_seen:
            exc_seen.add(k)
            exclusion_deduped.append(t)

    return inclusion_deduped or list(DEFAULT_INCLUSION_KEYWORDS), exclusion_deduped or list(DEFAULT_EXCLUSION_KEYWORDS)


INCLUSION_KEYWORDS, EXCLUSION_KEYWORDS = _load_keyword_sets()

COLUMNS = [
    "Category", "Reference No.", "Date", "Name", "Start Date", "Model - Yr", "Quantity",
    "Unit Amount", "Description", "Contact", "EMAIL", "Department",
    "Search Keyword", "Pipeline Source", "Inclusion Hits", "Exclusion Hits",
]

GEM_PAGE_URL = "https://bidplus.gem.gov.in/all-bids"
GEM_API_URL = "https://bidplus.gem.gov.in/all-bids-data"
SORT_ORDER = "Bid-Start-Date-Latest"
REQUEST_DELAY = (1.0, 2.2)
MAX_RETRIES = 3
SESSION_REFRESH_EVERY = 100
PDF_FETCH_TIMEOUT_SECONDS = int(os.environ.get("PDF_FETCH_TIMEOUT_SECONDS", "25"))
PDF_FETCH_RETRIES = int(os.environ.get("PDF_FETCH_RETRIES", "2"))
SELENIUM_HEADLESS = os.environ.get("SELENIUM_HEADLESS", "1").strip().lower() not in {"0", "false", "no"}

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-6")
ANTHROPIC_BASE_URL = os.environ.get("ANTHROPIC_BASE_URL", "https://api.anthropic.com")
ANTHROPIC_DNS_CACHE_TTL_SECONDS = int(os.environ.get("ANTHROPIC_DNS_CACHE_TTL_SECONDS", "21600"))

DB_DSN = os.environ.get("SUPABASE_DB_DSN", "")
DB_HOST = os.environ.get("SUPABASE_DB_HOST", "")
DB_PORT = int(os.environ.get("SUPABASE_DB_PORT", "5432"))
DB_NAME = os.environ.get("SUPABASE_DB_NAME", "postgres")
DB_USER = os.environ.get("SUPABASE_DB_USER", "postgres")
DB_PASSWORD = os.environ.get("SUPABASE_DB_PASSWORD", "")
DB_SSLMODE = os.environ.get("SUPABASE_DB_SSLMODE", "require")
