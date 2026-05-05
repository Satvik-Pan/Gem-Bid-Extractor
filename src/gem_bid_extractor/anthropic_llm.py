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
# Pipeline 2 prompt: Deep PDF analysis for INCLUSION keyword detection
# ---------------------------------------------------------------------------
_PIPELINE2_INCLUSION_PROMPT = """You are a cybersecurity bid analyst. You are given the FULL text extracted from a government bid document PDF.

Your task: Deeply analyze the ENTIRE document and determine if ANY of the following INCLUSION keywords appear in the bid document, either explicitly or as part of the bid's technical requirements/scope.

INCLUSION KEYWORDS:
- Router
- NGFW
- FIREWALL
- FIRE WALL
- VPN
- UTM
- Next Generation Firewall
- UNIFIED Threat Manager
- Network Security
- Web Application Firewall
- WAF

RULES:
1. Search the ENTIRE document text thoroughly — headers, scope, BOQ, technical specs, everything.
2. If you find ANY of these keywords (even one), the bid is RELEVANT.
3. Match case-insensitively. "firewall", "Firewall", "FIREWALL" all count.
4. "Next Generation Firewall" and "NGFW" are the same concept — either counts.
5. Return strict JSON only. No explanations outside the JSON.

Return:
{"ref": "<Reference No.>", "found": true|false, "matched_keywords": ["keyword1", "keyword2"], "reason": "brief explanation"}

- found=true if ANY inclusion keyword was found in the document.
- found=false if NONE of the inclusion keywords were found.
- matched_keywords: list of specific keywords found (empty if none).
"""

# ---------------------------------------------------------------------------
# Pipeline 3 prompt: Deep PDF analysis for EXCLUSION keyword detection
# (This is done via regex in the pipeline, but we keep a prompt ready
#  in case LLM-based exclusion is needed in the future.)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Pipeline 4 prompt: Final confidence scoring for cybersecurity relevance
# ---------------------------------------------------------------------------
_PIPELINE4_CONFIDENCE_PROMPT = """You are a cybersecurity bid relevance analyst for a network security company (WiJungle) that sells products like firewalls, NGFW, UTM, VPN appliances, WAF, and routers.

You are given the FULL text extracted from a government bid document PDF. This bid has already passed initial keyword screening — it did NOT contain direct product keywords but also did NOT contain any disqualifying exclusion keywords.

Your task: Deeply analyze the ENTIRE document and determine if this bid is potentially relevant to a cybersecurity/network security company.

SCORING GUIDE:
- 0.80-1.00: Clearly relevant to cybersecurity/network security even without direct keywords (e.g., security infrastructure, IT security procurement, network appliance procurement)
- 0.50-0.79: Moderately relevant — has IT/networking components that could involve security products
- 0.25-0.49: Weakly relevant — tangential connection to IT/networking, could possibly need security products
- 0.00-0.24: Not relevant — clearly about non-cyber topics (construction, furniture, food, medical, vehicles, stationery, plumbing, cleaning, etc.)

RULES:
1. Read the ENTIRE document carefully.
2. Consider the procurement scope, technical specifications, and department context.
3. Be STRICT: if the bid is about physical goods, construction, or services clearly unrelated to cybersecurity, give a very low score (under 0.20).
4. Return strict JSON only.

Return:
{"ref": "<Reference No.>", "confidence": 0.00, "relevant": true|false, "reason": "brief explanation of why this score was given"}

- relevant=true if confidence >= 0.25 (these go to DOUBTFUL column)
- relevant=false if confidence < 0.25 (these are REJECTED)
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

    # ------------------------------------------------------------------
    # Pipeline 2: Check ONE bid's PDF for inclusion keywords via LLM
    # ------------------------------------------------------------------
    def check_inclusion_keywords(self, bid: dict) -> dict:
        """Analyze a single bid's PDF text for inclusion keywords.

        Returns:
            {"found": bool, "matched_keywords": [...], "reason": "..."}
        """
        ref = self._safe_snippet(bid.get("Reference No.", ""), 120)
        pdf_text = self._safe_snippet(bid.get("PDF Text", ""), 50000)  # Send substantial PDF content
        name = self._safe_snippet(bid.get("Name", ""), 500)
        category = self._safe_snippet(bid.get("Category", ""), 200)
        department = self._safe_snippet(bid.get("Department", ""), 300)

        user_content = (
            f"Reference No.: {ref}\n"
            f"Title: {name}\n"
            f"Category: {category}\n"
            f"Department: {department}\n\n"
            f"=== FULL BID DOCUMENT TEXT ===\n{pdf_text}"
        )

        result = self._call_api(_PIPELINE2_INCLUSION_PROMPT, user_content, max_tokens=800)

        return {
            "found": bool(result.get("found", False)),
            "matched_keywords": result.get("matched_keywords", []),
            "reason": str(result.get("reason", ""))[:500],
        }

    # ------------------------------------------------------------------
    # Pipeline 4: Score ONE bid's PDF for cybersecurity relevance
    # ------------------------------------------------------------------
    def score_relevance(self, bid: dict) -> dict:
        """Analyze a single bid's PDF text and score its cybersecurity relevance.

        Returns:
            {"confidence": float, "relevant": bool, "reason": "..."}
        """
        ref = self._safe_snippet(bid.get("Reference No.", ""), 120)
        pdf_text = self._safe_snippet(bid.get("PDF Text", ""), 50000)  # Send substantial PDF content
        name = self._safe_snippet(bid.get("Name", ""), 500)
        category = self._safe_snippet(bid.get("Category", ""), 200)
        department = self._safe_snippet(bid.get("Department", ""), 300)
        desc = self._safe_snippet(bid.get("Description", ""), 300)

        user_content = (
            f"Reference No.: {ref}\n"
            f"Title: {name}\n"
            f"Category: {category}\n"
            f"Department: {department}\n"
            f"Description: {desc}\n\n"
            f"=== FULL BID DOCUMENT TEXT ===\n{pdf_text}"
        )

        result = self._call_api(_PIPELINE4_CONFIDENCE_PROMPT, user_content, max_tokens=800)

        try:
            conf = float(result.get("confidence", 0.0))
        except (TypeError, ValueError):
            conf = 0.0
        conf = max(0.0, min(1.0, conf))

        return {
            "confidence": round(conf, 3),
            "relevant": bool(result.get("relevant", conf >= 0.25)),
            "reason": str(result.get("reason", ""))[:500],
        }
