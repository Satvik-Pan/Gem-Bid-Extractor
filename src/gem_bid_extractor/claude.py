from __future__ import annotations

import json
import re
import requests

from .settings import CLAUDE_API_KEY, CLAUDE_BATCH_SIZE, CLAUDE_MODEL


_SYSTEM_PROMPT = """You classify Indian government bids for a cybersecurity company.
Return only JSON:
{"results":[{"ref":"<Reference No.>","v":"y"|"n","c":0.0-1.0}]}

Relevant examples:
- firewall, ngfw, ids/ips, vpn, sd-wan, waf, soc, siem, edr/xdr, vulnerability assessment, penetration testing, security audit, iam/mfa, dlp.
Not relevant examples:
- construction/civil works, furniture, food, medical equipment, vehicles, textiles, fire extinguishers.
"""


class ClaudeSonnetClassifier:
    def __init__(self):
        self.api_key = CLAUDE_API_KEY
        self.model = CLAUDE_MODEL
        self.enabled = bool(self.api_key)

    @staticmethod
    def _bid_summary(bid: dict) -> str:
        return (
            f"Ref: {bid.get('Reference No.', '')}\n"
            f"Category: {bid.get('Category', '')}\n"
            f"Title: {bid.get('Name', '')}\n"
            f"Description: {bid.get('Description', '')}\n"
            f"Department: {bid.get('Department', '')}"
        )

    @staticmethod
    def _extract_json(text: str) -> dict:
        text = text.strip()
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            match = re.search(r"\{[\s\S]*\}", text)
            if match:
                return json.loads(match.group(0))
        return {"results": []}

    def classify_batch(self, bids: list[dict]) -> dict[str, dict]:
        if not self.enabled or not bids:
            return {}

        payload = {
            "model": self.model,
            "max_tokens": 1200,
            "temperature": 0,
            "system": _SYSTEM_PROMPT,
            "messages": [
                {
                    "role": "user",
                    "content": "\n\n".join(self._bid_summary(b) for b in bids),
                }
            ],
        }

        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": self.api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json=payload,
            timeout=120,
        )
        resp.raise_for_status()
        content = resp.json().get("content", [])
        text = "".join(part.get("text", "") for part in content if part.get("type") == "text")
        data = self._extract_json(text)
        return {item.get("ref", ""): item for item in data.get("results", []) if item.get("ref")}

    def classify(self, bids: list[dict]) -> dict[str, dict]:
        results: dict[str, dict] = {}
        for i in range(0, len(bids), CLAUDE_BATCH_SIZE):
            chunk = bids[i : i + CLAUDE_BATCH_SIZE]
            results.update(self.classify_batch(chunk))
        return results
