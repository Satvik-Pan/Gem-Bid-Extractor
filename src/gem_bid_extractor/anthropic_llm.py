from __future__ import annotations

import json
import logging
import re
import time
from typing import Literal
from urllib.parse import urlparse

import requests

from .dns_cache import CachedDnsResolver
from .settings import (
    ANTHROPIC_API_KEY,
    ANTHROPIC_BASE_URL,
    ANTHROPIC_DNS_CACHE_TTL_SECONDS,
    ANTHROPIC_MODEL,
    DNS_CACHE_FILE,
    LLM_BATCH_SIZE,
)

_PREFILTER_PROMPT = """You are doing broad cybersecurity pre-filtering for Indian government bids.
Return strict JSON only:
{"results":[{"ref":"<Reference No.>","decision":"YES"|"NO","confidence":0.0}]}

Rules:
- YES for clearly cybersecurity-related or potentially cybersecurity-related bids.
- Be recall-oriented: if uncertain but possible cybersecurity, prefer YES with medium confidence.
- NO only when clearly non-cyber domain.
- Return exactly one result per input ref.
"""

_FINAL_CLASS_PROMPT = """You are the final classifier for Indian government bids.
Return strict JSON only:
{"results":[{"ref":"<Reference No.>","category":"EXTRACTED"|"DOUBTFUL"|"REJECTED","confidence":0.0,"reason":"short reason"}]}

Rules:
- EXTRACTED: strong cybersecurity relevance.
- DOUBTFUL: partial/ambiguous relevance, needs review.
- REJECTED: not cybersecurity-related.
- Return exactly one result per input ref.
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
        timeout = (15, 140)

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
    def _bid_summary(bid: dict) -> str:
        desc = str(bid.get("Description", ""))
        if len(desc) > 140:
            desc = desc[:140]
        return (
            f"Ref: {bid.get('Reference No.', '')}\n"
            f"Category: {bid.get('Category', '')}\n"
            f"Title: {bid.get('Name', '')}\n"
            f"Description: {desc}\n"
            f"Department: {bid.get('Department', '')}"
        )

    @staticmethod
    def _extract_json(text: str) -> dict:
        if not isinstance(text, str):
            return {"results": []}
        text = text.strip()
        if not text:
            return {"results": []}
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            match = re.search(r"\{[\s\S]*\}", text)
            if match:
                try:
                    return json.loads(match.group(0))
                except json.JSONDecodeError:
                    return {"results": []}
        return {"results": []}

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

    def _call_messages_api(self, bids: list[dict], system_prompt: str, max_tokens: int = 900) -> dict[str, dict]:
        if not self.enabled:
            raise RuntimeError("Anthropic classifier is not configured. Set ANTHROPIC_API_KEY, ANTHROPIC_MODEL, and ANTHROPIC_BASE_URL.")
        if not bids:
            return {}

        user_content = "\n\n".join(self._bid_summary(b) for b in bids)
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
                if status and status >= 500 and attempt < 10:
                    wait_s = min(45, attempt * 4)
                    logger.warning("Anthropic HTTP %s on attempt %d/10; waiting %ss", status, attempt, wait_s)
                    time.sleep(wait_s)
                    continue
                break

        if last_exc:
            raise RuntimeError(f"Anthropic API call failed after retries: {last_exc}") from last_exc
        raise RuntimeError("Anthropic API call failed after retries")

    @staticmethod
    def _index_by_ref(bids: list[dict]) -> dict[str, dict]:
        out: dict[str, dict] = {}
        for bid in bids:
            ref = str(bid.get("Reference No.", "")).strip()
            if ref:
                out[ref] = bid
        return out

    def prefilter_batch(self, bids: list[dict]) -> dict[str, dict]:
        results: dict[str, dict] = {}
        pending_map = self._index_by_ref(bids)

        for attempt in range(1, 4):
            if not pending_map:
                break
            payload_bids = list(pending_map.values())
            max_tokens = min(2500, max(700, len(payload_bids) * 40))
            data = self._call_messages_api(payload_bids, _PREFILTER_PROMPT, max_tokens=max_tokens)
            for item in data.get("results", []):
                ref = item.get("ref", "")
                decision = str(item.get("decision", "")).upper()
                conf = item.get("confidence")
                if ref and decision in {"YES", "NO"}:
                    try:
                        c_val = float(conf)
                    except (TypeError, ValueError):
                        c_val = 0.5
                    results[ref] = {"ref": ref, "decision": decision, "confidence": max(0.0, min(1.0, c_val))}
                    pending_map.pop(ref, None)

            if pending_map and attempt < 3:
                logger.warning("Prefilter missing refs after attempt %d: %d", attempt, len(pending_map))
                time.sleep(min(10, attempt * 2))

        if pending_map:
            missing_refs = sorted(pending_map.keys())
            raise RuntimeError(f"Anthropic response missing refs: {', '.join(missing_refs[:5])}")

        return results

    def final_classify_batch(self, bids: list[dict]) -> dict[str, dict]:
        results: dict[str, dict] = {}
        pending_map = self._index_by_ref(bids)

        for attempt in range(1, 4):
            if not pending_map:
                break
            payload_bids = list(pending_map.values())
            max_tokens = min(5000, max(1400, len(payload_bids) * 140))
            data = self._call_messages_api(payload_bids, _FINAL_CLASS_PROMPT, max_tokens=max_tokens)
            for item in data.get("results", []):
                ref = item.get("ref", "")
                category = str(item.get("category", "")).upper()
                conf = item.get("confidence")
                reason = str(item.get("reason", "")).strip()
                if ref and category in {"EXTRACTED", "DOUBTFUL", "REJECTED"}:
                    try:
                        c_val = float(conf)
                    except (TypeError, ValueError):
                        c_val = 0.5
                    results[ref] = {
                        "ref": ref,
                        "category": category,
                        "confidence": max(0.0, min(1.0, c_val)),
                        "reason": reason[:300],
                    }
                    pending_map.pop(ref, None)

            if pending_map and attempt < 3:
                logger.warning("Final classify missing refs after attempt %d: %d", attempt, len(pending_map))
                time.sleep(min(10, attempt * 2))

        if pending_map:
            missing_refs = sorted(pending_map.keys())
            raise RuntimeError(f"Anthropic response missing refs: {', '.join(missing_refs[:5])}")

        return results

    def _run_in_batches(self, bids: list[dict], mode: Literal["prefilter", "final"]) -> dict[str, dict]:
        if not self.enabled:
            raise RuntimeError("Anthropic classifier is not configured. Set ANTHROPIC_API_KEY, ANTHROPIC_MODEL, and ANTHROPIC_BASE_URL.")

        results: dict[str, dict] = {}
        total = len(bids)
        batch_size = LLM_BATCH_SIZE if mode == "prefilter" else max(10, LLM_BATCH_SIZE // 3)
        total_batches = (total + batch_size - 1) // batch_size if total else 0

        for i in range(0, total, batch_size):
            chunk = bids[i : i + batch_size]
            if mode == "prefilter":
                logger.info("LLM prefilter batch %d/%d", (i // batch_size) + 1, total_batches)
                batch_results = self.prefilter_batch(chunk)
            else:
                logger.info("LLM final classify batch %d/%d", (i // batch_size) + 1, total_batches)
                batch_results = self.final_classify_batch(chunk)
            results.update(batch_results)
            # Queue-like pacing to reduce API burst traffic and 429 rates.
            if i + batch_size < total:
                time.sleep(3.0)

        return results

    def prefilter(self, bids: list[dict]) -> dict[str, dict]:
        return self._run_in_batches(bids, "prefilter")

    def final_classify(self, bids: list[dict]) -> dict[str, dict]:
        return self._run_in_batches(bids, "final")
