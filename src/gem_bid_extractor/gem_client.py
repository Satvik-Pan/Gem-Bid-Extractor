from __future__ import annotations

import json
import logging
import random
import re
import time
from datetime import datetime, timezone
from typing import Optional

import requests

from .settings import (
    GEM_API_URL,
    GEM_PAGE_URL,
    MAX_RETRIES,
    REQUEST_DELAY,
    SESSION_REFRESH_EVERY,
    SORT_ORDER,
)

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": GEM_PAGE_URL,
}


def _val(field):
    if isinstance(field, list):
        return field[0] if field else ""
    return field if field is not None else ""


def _parse_iso(iso_str: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    except Exception:
        return None


def _fmt_date(iso_str: str) -> str:
    dt = _parse_iso(iso_str)
    return dt.strftime("%d-%m-%Y %I:%M %p") if dt else iso_str


class GemScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(_HEADERS)
        self.csrf_token: Optional[str] = None
        self._call_count = 0

    def init_session(self):
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self.session.get(GEM_PAGE_URL, timeout=30)
                resp.raise_for_status()
                self.csrf_token = self.session.cookies.get("csrf_gem_cookie")
                if not self.csrf_token:
                    m = re.search(r"csrf_bd_gem_nk['\"]?\s*[:=]\s*['\"]([a-f0-9]+)", resp.text)
                    if m:
                        self.csrf_token = m.group(1)
                if not self.csrf_token:
                    raise RuntimeError("No CSRF token")
                self._call_count = 0
                return
            except Exception as exc:
                logger.warning("Session init attempt %d failed: %s", attempt, exc)
                if attempt < MAX_RETRIES:
                    time.sleep(4 * attempt)
                else:
                    raise

    def _maybe_refresh(self):
        self._call_count += 1
        if self._call_count >= SESSION_REFRESH_EVERY:
            self.init_session()

    def _search_page(self, keyword: str, page: int) -> dict:
        payload = json.dumps(
            {
                "page": page,
                "param": {"searchBid": keyword, "searchType": "fullText"},
                "filter": {
                    "bidStatusType": "ongoing_bids",
                    "byType": "all",
                    "highBidValue": "",
                    "byEndDate": {"from": "", "to": ""},
                    "sort": SORT_ORDER,
                },
            }
        )
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self._maybe_refresh()
                resp = self.session.post(
                    GEM_API_URL,
                    data={"payload": payload, "csrf_bd_gem_nk": self.csrf_token},
                    timeout=30,
                )
                if resp.status_code == 403:
                    self.init_session()
                    continue
                if resp.status_code == 404:
                    return {}
                resp.raise_for_status()
                data = resp.json()
                return data if data.get("code") == 200 else {}
            except requests.RequestException as exc:
                logger.warning("Request attempt %d failed for %s: %s", attempt, keyword, exc)
                if attempt < MAX_RETRIES:
                    time.sleep(2 * attempt)
        return {}

    @staticmethod
    def _parse_bid(doc: dict, keyword: str) -> dict:
        category = _val(doc.get("b_category_name", ""))
        dept = _val(doc.get("ba_official_details_deptName", ""))
        ministry = _val(doc.get("ba_official_details_minName", ""))
        department = f"{ministry} / {dept}" if ministry and dept and ministry != dept else (dept or ministry)
        start_raw = _val(doc.get("final_start_date_sort", ""))
        end_raw = _val(doc.get("final_end_date_sort", ""))

        return {
            "Category": category.split(",")[0].strip() if category else "",
            "Reference No.": _val(doc.get("b_bid_number", "")),
            "Date": _fmt_date(end_raw) if end_raw else "",
            "Name": _val(doc.get("bbt_title", "")) or category,
            "Start Date": _fmt_date(start_raw) if start_raw else "",
            "Model - Yr": "",
            "Quantity": str(_val(doc.get("b_total_quantity", ""))),
            "Unit Amount": "",
            "Description": _val(doc.get("bd_category_name", "")) or category,
            "Contact": "",
            "EMAIL": _val(doc.get("b.b_created_by", "")),
            "Department": department,
            "_keyword": keyword,
            "_start_dt": start_raw,
        }

    def search_keyword(self, keyword: str, cutoff: datetime, seen_ids: set[str]) -> list[dict]:
        bids: list[dict] = []
        page = 1
        while True:
            data = self._search_page(keyword, page)
            docs = data.get("response", {}).get("response", {}).get("docs", []) if data else []
            if not docs:
                break

            stop = False
            for doc in docs:
                bid_id = str(_val(doc.get("b_id", "")))
                if not bid_id or bid_id in seen_ids:
                    continue
                start_raw = _val(doc.get("final_start_date_sort", ""))
                start_dt = _parse_iso(start_raw) if start_raw else None
                if start_dt and start_dt < cutoff:
                    stop = True
                    break
                seen_ids.add(bid_id)
                bids.append(self._parse_bid(doc, keyword))

            if stop or page >= 20:
                break
            page += 1
            time.sleep(random.uniform(*REQUEST_DELAY))
        return bids

    def search_all(self, keywords: list[str], cutoff: datetime) -> list[dict]:
        all_bids: list[dict] = []
        seen_ids: set[str] = set()
        for i, kw in enumerate(keywords, 1):
            logger.info("[%d/%d] %s", i, len(keywords), kw)
            all_bids.extend(self.search_keyword(kw, cutoff, seen_ids))
            if i < len(keywords):
                time.sleep(random.uniform(*REQUEST_DELAY))
        logger.info("Fetched %d unique bids", len(all_bids))
        return all_bids

    def close(self):
        self.session.close()
