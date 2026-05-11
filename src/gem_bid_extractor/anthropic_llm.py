from __future__ import annotations

import json
import logging
import re
import time
from urllib.parse import urlparse

import requests

from .dns_cache import CachedDnsResolver
from .settings import (
    ANTHROPIC_API_KEY,
    ANTHROPIC_BASE_URL,
    ANTHROPIC_DNS_CACHE_TTL_SECONDS,
    ANTHROPIC_MODEL,
    DNS_CACHE_FILE,
)

# ---------------------------------------------------------------------------
# Pipeline 2 prompt: strict inclusion/exclusion and confidence classification
# ---------------------------------------------------------------------------
_PIPELINE2_CLASSIFIER_PROMPT = """You are a strict cybersecurity bid analyst.

You must deeply review the FULL bid PDF text and return strict JSON only.

INCLUSION KEYWORDS (exact phrase intent, case-insensitive):
Router
NGFW
FIREWALL
FIRE WALL
VPN
UTM
Next Generation Firewall
UNIFIED Threat Manager
Network Security
Web Application Firewall
WAF

EXCLUSION KEYWORDS (exact phrase intent, case-insensitive):
IPS
Load Balancer
NextGen
Next
UNIFIED
SDWAN
SD WAN
Software Define WAN
DNS
Intrusion
LLB
SLB
Web Security
Threat
Internet
Gateway
Perimeter
Endpoint
EPS
Malware
Ransomware
IPSec
Edge
Cyber Security
Virus
AAA
Firepower
ASA
Bandwidth
Renewal
Authentication
LAN
Armynet
Domain
Anti-APT
Anti-ATP
Sophos
Gajshield
Checkpoint
Anexgate
Tacitine
Fortinet
Fortigate
PaloAlto
Quickheal
Forcepoint
CISCO
Juniper
Sonicwall
TrendMicro
Mcafee
Radware
Array Networks
Haltdos
DDoS
Trellix
Data Loss Prevention
Scada/ Scada Firewall

Rules:
1) Assess the FULL text for exact phrase intent matches. Avoid false positives like "fire ball" or "fire extinguisher" for FIRE WALL/FIREWALL.
2) Always return valid JSON with all fields below.

Return format:
{
  "ref": "<Reference No.>",
  "inclusion_hits": ["..."],
  "exclusion_hits": ["..."],
  "selected_inclusion_keyword": "<exact keyword from list or empty>",
  "reason": "short decision rationale"
}
"""

_FIRE_WALL_PREFILTER_PROMPT = """You are filtering noisy GeM search results for the inclusion keyword 'FIRE WALL'.

Keep the bid only when it is relevant to cybersecurity/network security procurement.
Reject fire-fighting, extinguishers, industrial fire-safety, civil/mechanical, or unrelated domains.

Return strict JSON:
{"ref":"<Reference No.>","keep":true|false,"reason":"short reason"}
"""

_DOUBTFUL_EXCLUSION_REVIEW_PROMPT = """You are doing a second-pass review for DOUBTFUL cybersecurity bids.

Goal: aggressively reduce doubtful rows when exclusion phrases are strongly present.
Give HIGHER WEIGHTAGE to exclusion phrases than inclusion phrases in this pass.

Decision rule for this pass:
- drop=true only when exclusion signal is strong/material for procurement intent
  (brand lock-in, renewal/support-only, endpoint/lan/internet/domain scope mismatch, non-target security stack, etc.)
- drop=false when exclusion mention appears weak/incidental/contextual and bid may still be relevant.

Return strict JSON only:
{"ref":"<Reference No.>","drop":true|false,"strong_exclusion_hits":["..."],"reason":"short reason"}
"""

logger = logging.getLogger(__name__)


class AnthropicClaudeClassifier:
    def __init__(self):
        self.api_key = ANTHROPIC_API_KEY
        self.model = ANTHROPIC_MODEL
        self.base_url = ANTHROPIC_BASE_URL.rstrip("/")
        self.api_host = urlparse(self.base_url).hostname or "api.anthropic.com"
        self.resolver = CachedDnsResolver(DNS_CACHE_FILE, ANTHROPIC_DNS_CACHE_TTL_SECONDS)
        self.enabled = bool(self.api_key and self.model and self.base_url)

    @staticmethod
    def _is_dns_error(exc: requests.RequestException) -> bool:
        text = str(exc).lower()
        return (
            "name resolution" in text
            or "failed to resolve" in text
            or "getaddrinfo" in text
            or "nameresolutionerror" in text
        )

    def _post_messages(self, payload: dict) -> requests.Response:
        url = f"{self.base_url}/v1/messages"
        headers = {
            "x-api-key": self.api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }
        timeout = (15, 180)  # Increased timeout for deep PDF analysis

        try:
            return requests.post(url, headers=headers, json=payload, timeout=timeout)
        except requests.RequestException as exc:
            if not self._is_dns_error(exc):
                raise

            fallback_ip = self.resolver.get_or_resolve_ip(self.api_host)
            if not fallback_ip:
                raise

            logger.warning(
                "Anthropic DNS fallback engaged for %s using cached/resolved IP %s",
                self.api_host,
                fallback_ip,
            )
            with self.resolver.route_host_to_ip(self.api_host, fallback_ip):
                return requests.post(url, headers=headers, json=payload, timeout=timeout)

    @staticmethod
    def _safe_snippet(text: object, max_len: int) -> str:
        s = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", " ", str(text or "")).strip()
        if len(s) <= max_len:
            return s
        return s[: max_len - 3] + "..."

    @staticmethod
    def _extract_json(text: str) -> dict:
        if not isinstance(text, str):
            return {}
        text = text.strip()
        if not text:
            return {}
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            match = re.search(r"\{[\s\S]*\}", text)
            if match:
                try:
                    return json.loads(match.group(0))
                except json.JSONDecodeError:
                    return {}
        return {}

    @staticmethod
    def _read_text(data_json: dict) -> str:
        blocks = data_json.get("content", [])
        if not isinstance(blocks, list):
            return ""
        parts: list[str] = []
        for block in blocks:
            if isinstance(block, dict) and block.get("type") == "text":
                txt = block.get("text", "")
                if isinstance(txt, str):
                    parts.append(txt)
        return "".join(parts)

    def _call_api(
        self,
        system_prompt: str,
        user_content: str,
        max_tokens: int = 1200,
    ) -> dict:
        """Make a single Anthropic API call and return parsed JSON response."""
        if not self.enabled:
            raise RuntimeError("Anthropic classifier is not configured. Set ANTHROPIC_API_KEY, ANTHROPIC_MODEL, and ANTHROPIC_BASE_URL.")

        if len(user_content) > 190_000:
            user_content = user_content[:190_000] + "\n\n[truncated]"

        payload = {
            "model": self.model,
            "max_tokens": max_tokens,
            "temperature": 0,
            "system": system_prompt,
            "messages": [{"role": "user", "content": user_content}],
        }

        last_exc: Exception | None = None
        for attempt in range(1, 11):
            try:
                resp = self._post_messages(payload)
                if resp.status_code == 429:
                    retry_after = resp.headers.get("retry-after")
                    if retry_after and retry_after.isdigit():
                        wait_s = int(retry_after)
                    else:
                        wait_s = min(80, 4 * (2 ** (attempt - 1)))
                    logger.warning("Anthropic rate limit on attempt %d/10; waiting %ss", attempt, wait_s)
                    if attempt < 10:
                        time.sleep(wait_s)
                        continue
                resp.raise_for_status()
                data_json = resp.json()
                text = self._read_text(data_json)
                return self._extract_json(text)
            except requests.ReadTimeout as exc:
                last_exc = exc
                wait_s = min(45, attempt * 4)
                logger.warning("Anthropic timeout on attempt %d/10; waiting %ss", attempt, wait_s)
                if attempt < 10:
                    time.sleep(wait_s)
                    continue
            except requests.RequestException as exc:
                last_exc = exc
                status = exc.response.status_code if isinstance(exc, requests.HTTPError) and exc.response is not None else None
                if self._is_dns_error(exc) and attempt < 10:
                    wait_s = min(20, 2 * attempt)
                    logger.warning("Anthropic DNS resolution failure on attempt %d/10; waiting %ss", attempt, wait_s)
                    time.sleep(wait_s)
                    continue
                if status == 400 and isinstance(exc, requests.HTTPError) and exc.response is not None:
                    try:
                        detail = exc.response.text[:800]
                    except Exception:
                        detail = ""
                    logger.warning("Anthropic HTTP 400 body (truncated): %s", detail)
                if status and status >= 500 and attempt < 10:
                    wait_s = min(45, attempt * 4)
                    logger.warning("Anthropic HTTP %s on attempt %d/10; waiting %ss", status, attempt, wait_s)
                    time.sleep(wait_s)
                    continue
                break

        if last_exc:
            raise RuntimeError(f"Anthropic API call failed after retries: {last_exc}") from last_exc
        raise RuntimeError("Anthropic API call failed after retries")

    def classify_bid(self, bid: dict) -> dict:
        """Deeply classify one bid PDF using strict keyword + relevance analysis."""
        ref = self._safe_snippet(bid.get("Reference No.", ""), 120)
        pdf_text = self._safe_snippet(bid.get("PDF Text", ""), 110000)
        name = self._safe_snippet(bid.get("Name", ""), 500)
        category = self._safe_snippet(bid.get("Category", ""), 200)
        department = self._safe_snippet(bid.get("Department", ""), 300)
        search_kw = self._safe_snippet(bid.get("Search Keyword", ""), 100)

        user_content = (
            f"Reference No.: {ref}\n"
            f"Title: {name}\n"
            f"Category: {category}\n"
            f"Department: {department}\n\n"
            f"Pipeline-1 Search Keyword: {search_kw}\n\n"
            f"=== FULL BID DOCUMENT TEXT ===\n{pdf_text}"
        )

        result = self._call_api(_PIPELINE2_CLASSIFIER_PROMPT, user_content, max_tokens=1200)
        inclusion_hits_raw = result.get("inclusion_hits", [])
        exclusion_hits_raw = result.get("exclusion_hits", [])
        inclusion_hits = [str(x).strip() for x in inclusion_hits_raw if str(x).strip()] if isinstance(inclusion_hits_raw, list) else []
        exclusion_hits = [str(x).strip() for x in exclusion_hits_raw if str(x).strip()] if isinstance(exclusion_hits_raw, list) else []
        selected = str(result.get("selected_inclusion_keyword", "")).strip()
        return {
            "inclusion_hits": inclusion_hits,
            "exclusion_hits": exclusion_hits,
            "selected_inclusion_keyword": selected,
            "reason": str(result.get("reason", ""))[:500],
        }

    def keep_fire_wall_result(self, bid: dict) -> dict:
        ref = self._safe_snippet(bid.get("Reference No.", ""), 120)
        name = self._safe_snippet(bid.get("Name", ""), 400)
        category = self._safe_snippet(bid.get("Category", ""), 200)
        department = self._safe_snippet(bid.get("Department", ""), 300)
        desc = self._safe_snippet(bid.get("Description", ""), 600)
        user_content = (
            f"Reference No.: {ref}\n"
            f"Title: {name}\n"
            f"Category: {category}\n"
            f"Department: {department}\n"
            f"Description: {desc}\n"
        )
        result = self._call_api(_FIRE_WALL_PREFILTER_PROMPT, user_content, max_tokens=300)
        return {
            "keep": bool(result.get("keep", False)),
            "reason": str(result.get("reason", ""))[:300],
        }

    def review_doubtful_exclusion_strength(self, bid: dict) -> dict:
        ref = self._safe_snippet(bid.get("Reference No.", ""), 120)
        name = self._safe_snippet(bid.get("Name", ""), 500)
        category = self._safe_snippet(bid.get("Category", ""), 200)
        department = self._safe_snippet(bid.get("Department", ""), 300)
        search_kw = self._safe_snippet(bid.get("Search Keyword", ""), 120)
        inclusion_hits = self._safe_snippet(bid.get("Inclusion Hits", ""), 500)
        exclusion_hits = self._safe_snippet(bid.get("Exclusion Hits", ""), 500)
        prev_reason = self._safe_snippet(bid.get("LLM Reason", ""), 700)
        pdf_text = self._safe_snippet(bid.get("PDF Text", ""), 90000)

        user_content = (
            f"Reference No.: {ref}\n"
            f"Title: {name}\n"
            f"Category: {category}\n"
            f"Department: {department}\n"
            f"Pipeline-1 Search Keyword: {search_kw}\n"
            f"Inclusion Hits (first pass): {inclusion_hits}\n"
            f"Exclusion Hits (first pass): {exclusion_hits}\n"
            f"First-pass reason: {prev_reason}\n\n"
            f"=== BID DOCUMENT TEXT ===\n{pdf_text}"
        )

        result = self._call_api(_DOUBTFUL_EXCLUSION_REVIEW_PROMPT, user_content, max_tokens=450)
        strong_hits_raw = result.get("strong_exclusion_hits", [])
        strong_hits = (
            [str(x).strip() for x in strong_hits_raw if str(x).strip()]
            if isinstance(strong_hits_raw, list)
            else []
        )
        return {
            "drop": bool(result.get("drop", False)),
            "strong_exclusion_hits": strong_hits,
            "reason": str(result.get("reason", ""))[:400],
        }
