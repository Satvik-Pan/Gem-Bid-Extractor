from __future__ import annotations

import json
import logging
import random
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import requests
import urllib3
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

from .pdf_reader import extract_pdf_text
from .settings import (
    GEM_API_URL,
    GEM_PAGE_URL,
    MAX_PAGES_PER_PIPELINE,
    MAX_RETRIES,
    PDF_CACHE_DIR,
    PDF_FETCH_RETRIES,
    PDF_FETCH_TIMEOUT_SECONDS,
    REQUEST_DELAY,
    SELENIUM_HEADLESS,
    SESSION_REFRESH_EVERY,
    SORT_ORDER,
)

logger = logging.getLogger(__name__)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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

        bid_no = _val(doc.get("b_bid_number_parent", ""))
        ra_no = _val(doc.get("b_bid_number", ""))
        parent_bid_id = str(_val(doc.get("b_id_parent", ""))).strip()
        ref_no = bid_no or ra_no
        return {
            "Category": category.split(",")[0].strip() if category else "",
            "Reference No.": ref_no,
            "Bid No.": bid_no,
            "RA No.": ra_no,
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
            "Source URL": f"https://bidplus.gem.gov.in/bidlists/{bid_no}" if bid_no else GEM_PAGE_URL,
            "Bid Doc URL": f"https://bidplus.gem.gov.in/showbidDocument/{parent_bid_id}" if parent_bid_id else "",
            "Bid ID": str(_val(doc.get("b_id", ""))),
            "_keyword": keyword,
            "_start_dt": start_raw,
            "_end_dt": end_raw,
            "_is_ra_listing": bool(ra_no),
        }

    @staticmethod
    def _is_actionable_bid(doc: dict) -> bool:
        bid_no = str(_val(doc.get("b_bid_number_parent", ""))).strip()
        return bid_no.startswith("GEM/") and "/B/" in bid_no

    @staticmethod
    def _sanitize_filename(name: str) -> str:
        return re.sub(r"[^a-zA-Z0-9._-]+", "_", name).strip("_")

    def _create_driver(self) -> webdriver.Chrome:
        options = Options()
        if SELENIUM_HEADLESS:
            options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1400,1600")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--log-level=3")
        return webdriver.Chrome(options=options)

    def _extract_pdf_link(self, driver: webdriver.Chrome, bid_url: str) -> str:
        driver.get(bid_url)
        anchors = driver.find_elements(By.TAG_NAME, "a")
        candidates: list[str] = []
        for anchor in anchors:
            href = (anchor.get_attribute("href") or "").strip()
            if ".pdf" in href.lower():
                candidates.append(href)
        for href in candidates:
            low = href.lower()
            if "handbook" in low or "assets-bg.gem.gov.in/resources/pdf" in low:
                continue
            return href
        html = driver.page_source
        for match in re.finditer(r"https?://[^\"']+\.pdf(?:\?[^\"']*)?", html, flags=re.IGNORECASE):
            link = match.group(0)
            low = link.lower()
            if "handbook" in low or "assets-bg.gem.gov.in/resources/pdf" in low:
                continue
            return link
        for match in re.finditer(r"(/[^\"']+\.pdf(?:\?[^\"']*)?)", html, flags=re.IGNORECASE):
            link = urljoin(bid_url, match.group(1))
            low = link.lower()
            if "handbook" in low or "assets-bg.gem.gov.in/resources/pdf" in low:
                continue
            return link
        return ""

    def _download_pdf(self, pdf_url: str, bid_no: str) -> Path | None:
        if not pdf_url:
            return None
        PDF_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        safe_name = self._sanitize_filename(bid_no) or f"bid_{int(time.time())}"
        out_path = PDF_CACHE_DIR / f"{safe_name}.pdf"
        try:
            resp = self.session.get(pdf_url, timeout=PDF_FETCH_TIMEOUT_SECONDS, verify=False)
            resp.raise_for_status()
            out_path.write_bytes(resp.content)
            return out_path if out_path.exists() and out_path.stat().st_size > 0 else None
        except requests.RequestException as exc:
            logger.warning("PDF download failed for %s (%s): %s", bid_no, pdf_url, exc)
            return None

    def enrich_with_pdf_text(self, bids: list[dict]) -> dict[str, int]:
        if not bids:
            return {"downloaded": 0, "failed": 0, "skipped": 0}

        downloaded = 0
        failed = 0
        skipped = 0
        try:
            driver = self._create_driver()
        except WebDriverException as exc:
            logger.warning("Selenium unavailable; skipping PDF enrichment: %s", exc)
            for bid in bids:
                bid["PDF Text"] = ""
            return {"downloaded": 0, "failed": len(bids), "skipped": 0}

        try:
            for bid in bids:
                bid_no = str(bid.get("Bid No.", "")).strip()
                bid_url = str(bid.get("Source URL", "")).strip()
                bid_doc_url = str(bid.get("Bid Doc URL", "")).strip()
                if not bid_no or not bid_url:
                    skipped += 1
                    bid["PDF Text"] = ""
                    bid["PDF Path"] = ""
                    continue

                if bid_doc_url:
                    pdf_path = self._download_pdf(bid_doc_url, bid_no)
                    if pdf_path:
                        pdf_text = extract_pdf_text(pdf_path)
                        if pdf_text:
                            downloaded += 1
                            bid["PDF Path"] = str(pdf_path)
                            bid["PDF Text"] = pdf_text
                            bid["Description"] = pdf_text[:1500]
                            continue

                pdf_link = ""
                for _ in range(PDF_FETCH_RETRIES + 1):
                    try:
                        pdf_link = self._extract_pdf_link(driver, bid_url)
                    except TimeoutException:
                        pdf_link = ""
                    if pdf_link:
                        break
                    time.sleep(1.0)

                if not pdf_link:
                    failed += 1
                    bid["PDF Text"] = ""
                    bid["PDF Path"] = ""
                    continue

                pdf_path = self._download_pdf(pdf_link, bid_no)
                if not pdf_path:
                    failed += 1
                    bid["PDF Text"] = ""
                    bid["PDF Path"] = ""
                    continue

                pdf_text = extract_pdf_text(pdf_path)
                if not pdf_text:
                    failed += 1
                    bid["PDF Text"] = ""
                    bid["PDF Path"] = str(pdf_path)
                    continue

                downloaded += 1
                bid["PDF Path"] = str(pdf_path)
                bid["PDF Text"] = pdf_text
                bid["Description"] = pdf_text[:1500]
        finally:
            driver.quit()

        return {"downloaded": downloaded, "failed": failed, "skipped": skipped}

    @staticmethod
    def _within_window(end_dt: Optional[datetime], min_date, max_date) -> bool:
        if end_dt is None:
            return True
        end_date = end_dt.astimezone(timezone.utc).date()
        return min_date <= end_date <= max_date

    def search_full(self, max_pages: int = MAX_PAGES_PER_PIPELINE) -> list[dict]:
        bids: list[dict] = []
        seen_ids: set[str] = set()
        ra_excluded = 0
        page = 1
        while page <= max_pages:
            data = self._search_page("", page)
            docs = data.get("response", {}).get("response", {}).get("docs", []) if data else []
            if not docs:
                break

            for doc in docs:
                bid_id = str(_val(doc.get("b_id", "")))
                if not bid_id or bid_id in seen_ids:
                    continue
                seen_ids.add(bid_id)
                if not self._is_actionable_bid(doc):
                    ra_excluded += 1
                    continue
                bid = self._parse_bid(doc, "")
                bid["_pipeline"] = "full"
                bids.append(bid)

            page += 1
            time.sleep(random.uniform(*REQUEST_DELAY))

        if page > max_pages:
            logger.warning("Pipeline full feed hit page cap (%d). Consider increasing MAX_PAGES_PER_PIPELINE.", max_pages)
        logger.info("Excluded %d RA/non-actionable rows from full feed", ra_excluded)
        logger.info("Fetched %d unique bids from full feed", len(bids))
        return bids

    def search_keyword(self, keyword: str, cutoff: datetime, seen_ids: set[str]) -> list[dict]:
        bids: list[dict] = []
        cutoff_date = cutoff.astimezone(timezone.utc).date()
        today_date = datetime.now(timezone.utc).date()
        page = 1
        while page <= MAX_PAGES_PER_PIPELINE:
            data = self._search_page(keyword, page)
            docs = data.get("response", {}).get("response", {}).get("docs", []) if data else []
            if not docs:
                break

            stop = False
            for doc in docs:
                bid_id = str(_val(doc.get("b_id", "")))
                if not bid_id or bid_id in seen_ids:
                    continue
                end_raw = _val(doc.get("final_end_date_sort", ""))
                end_dt = _parse_iso(end_raw) if end_raw else None
                if end_dt and end_dt < cutoff:
                    stop = True
                    break
                if not self._within_window(end_dt, cutoff_date, today_date):
                    continue
                seen_ids.add(bid_id)
                bid = self._parse_bid(doc, keyword)
                bid["_pipeline"] = "keyword"
                bids.append(bid)

            if stop:
                break
            page += 1
            time.sleep(random.uniform(*REQUEST_DELAY))
        if page > MAX_PAGES_PER_PIPELINE:
            logger.warning(
                "Keyword search '%s' hit page cap (%d). Consider increasing MAX_PAGES_PER_PIPELINE.",
                keyword,
                MAX_PAGES_PER_PIPELINE,
            )
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
