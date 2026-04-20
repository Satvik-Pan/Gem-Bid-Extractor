from __future__ import annotations

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

KEYWORDS = [
    "router", "ips", "ngfw", "firewall", "vpn", "nextgen", "utm", "sdwan", "waf",
    "web security", "network security", "endpoint", "cyber security", "sophos", "checkpoint",
    "fortinet", "fortigate", "palo alto", "quickheal", "forcepoint", "cisco", "juniper",
    "sonicwall", "trendmicro", "mcafee", "radware", "ddos", "data loss prevention", "scada",
    "edr", "xdr", "soc", "siem", "iam", "ssl", "tls", "ipsec", "intrusion detection",
    "intrusion prevention", "malware", "ransomware", "threat intelligence", "penetration testing",
    "vulnerability assessment", "security audit", "nac", "pam", "mfa", "zero trust",
    "cloud security", "container security", "api security", "encryption", "data protection",
    "disaster recovery", "iso 27001",
]

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
