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
2) Provide cybersecurity relevance confidence from 0.0 to 1.0.
3) Confidence should be very low for non-cyber tenders (fire safety hardware, civil, furniture, etc).
4) Always return valid JSON with all fields below.

Return format:
{
  "ref": "<Reference No.>",
  "inclusion_hits": ["..."],
  "exclusion_hits": ["..."],
  "selected_inclusion_keyword": "<exact keyword from list or empty>",
  "confidence": 0.0,
  "is_cybersecurity_relevant": true,
  "reason": "short decision rationale"
}
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
        try:
            confidence = float(result.get("confidence", 0.0))
        except (TypeError, ValueError):
            confidence = 0.0
        confidence = max(0.0, min(1.0, confidence))
        return {
            "inclusion_hits": inclusion_hits,
            "exclusion_hits": exclusion_hits,
            "selected_inclusion_keyword": selected,
            "confidence": round(confidence, 3),
            "is_cybersecurity_relevant": bool(result.get("is_cybersecurity_relevant", confidence >= 0.5)),
            "reason": str(result.get("reason", ""))[:500],
        }
