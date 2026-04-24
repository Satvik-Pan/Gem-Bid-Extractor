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

EXCEL_FILE = OUTPUT_DIR / "Extracted_bids.xlsx"
DOUBTFUL_FILE = OUTPUT_DIR / "doubtful_bids.xlsx"
PROCESSED_FILE = DATA_DIR / "processed_bids.json"
FEEDBACK_FILE = DATA_DIR / "feedback_logs.json"
THRESHOLDS_FILE = DATA_DIR / "thresholds.json"
WATCHLIST_FILE = DATA_DIR / "false_negative_watchlist.json"
LOG_FILE = LOG_DIR / "scraper.log"
SYNC_QUEUE_FILE = DATA_DIR / "db_sync_queue.jsonl"
DNS_CACHE_FILE = DATA_DIR / "dns_cache.json"

KEYWORDS_FILE = BASE_DIR / "src" / "gem_bid_extractor" / "keywords.csv"
MAX_PAGES_PER_PIPELINE = 5

DEFAULT_INCLUSION_KEYWORDS = [
    "router",
    "ngfw",
    "firewall",
    "fire wall",
    "vpn",
    "utm",
    "next generation firewall",
    "unified threat manager",
    "network security",
    "web application firewall",
    "waf",
]

DEFAULT_EXCLUSION_KEYWORDS = [
    "ips", "load balancer", "nextgen", "next", "unified", "sdwan", "sd wan", "software define wan",
    "dns", "intrusion", "llb", "slb", "web security", "threat", "internet", "gateway", "perimeter",
    "endpoint", "eps", "malware", "ransomware", "ipsec", "edge", "cyber security", "virus", "aaa",
    "firepower", "asa", "bandwidth", "renewal", "authentication", "lan", "armynet", "domain",
    "anti-apt", "anti-atp", "sophos", "gajshield", "checkpoint", "anexgate", "tacitine", "fortinet",
    "fortigate", "paloalto", "quickheal", "forcepoint", "cisco", "juniper", "sonicwall", "trendmicro",
    "mcafee", "radware", "array networks", "haltdos", "ddos", "trellix", "data loss prevention",
    "scada/ scada firewall",
]


def _normalize_term(term: str) -> str:
    return " ".join(term.strip().lower().split())


def _load_keyword_sets() -> tuple[list[str], list[str]]:
    if not KEYWORDS_FILE.exists():
        return DEFAULT_INCLUSION_KEYWORDS, DEFAULT_EXCLUSION_KEYWORDS

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
        return DEFAULT_INCLUSION_KEYWORDS, DEFAULT_EXCLUSION_KEYWORDS

    inclusion = inclusion or DEFAULT_INCLUSION_KEYWORDS
    exclusion = exclusion or DEFAULT_EXCLUSION_KEYWORDS
    return sorted(set(inclusion)), sorted(set(exclusion))


INCLUSION_KEYWORDS, EXCLUSION_KEYWORDS = _load_keyword_sets()
KEYWORDS = INCLUSION_KEYWORDS

COLUMNS = [
    "Category", "Reference No.", "Date", "Name", "Start Date", "Model - Yr", "Quantity",
    "Unit Amount", "Description", "Contact", "EMAIL", "Department",
    "Pipeline Source", "LLM Confidence", "LLM Reason",
]

GEM_PAGE_URL = "https://bidplus.gem.gov.in/all-bids"
GEM_API_URL = "https://bidplus.gem.gov.in/all-bids-data"
SORT_ORDER = "Bid-Start-Date-Latest"
LOOKBACK_DAYS = 3
REQUEST_DELAY = (1.0, 2.2)
MAX_RETRIES = 3
SESSION_REFRESH_EVERY = 100

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-6")
ANTHROPIC_BASE_URL = os.environ.get("ANTHROPIC_BASE_URL", "https://api.anthropic.com")
ANTHROPIC_DNS_CACHE_TTL_SECONDS = int(os.environ.get("ANTHROPIC_DNS_CACHE_TTL_SECONDS", "21600"))
LLM_BATCH_SIZE = 35

DB_DSN = os.environ.get("SUPABASE_DB_DSN", "")
DB_HOST = os.environ.get("SUPABASE_DB_HOST", "")
DB_PORT = int(os.environ.get("SUPABASE_DB_PORT", "5432"))
DB_NAME = os.environ.get("SUPABASE_DB_NAME", "postgres")
DB_USER = os.environ.get("SUPABASE_DB_USER", "postgres")
DB_PASSWORD = os.environ.get("SUPABASE_DB_PASSWORD", "")
DB_SSLMODE = os.environ.get("SUPABASE_DB_SSLMODE", "require")

EMBEDDING_MODEL = "all-MiniLM-L6-v2"
EMBEDDING_REF_TEXTS = [
    "cybersecurity procurement firewall management network security",
    "soc managed security services threat monitoring",
    "endpoint security edr antivirus malware defense",
    "vpn secure remote access zero trust",
    "siem log monitoring incident response",
    "penetration testing vulnerability assessment security audit",
]

EXCLUDE_DOMAINS = {
    "construction", "civil", "furniture", "food", "agriculture", "medical", "surgical",
    "vehicle", "automotive", "textile", "stationery", "hospital equipment",
}

RELEVANT_THRESHOLD_DEFAULT = 55.0
DOUBTFUL_THRESHOLD_DEFAULT = 35.0
WATCHLIST_SCORE_MIN = 45.0
WATCHLIST_EMBEDDING_MIN = 0.55
