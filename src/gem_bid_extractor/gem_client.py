from __future__ import annotations

import json
import logging
import random
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import requests
import urllib3
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select

from .pdf_reader import extract_pdf_text
from .settings import (
    GEM_API_URL,
    GEM_PAGE_URL,
    LOOKBACK_DAYS,
    MAX_PAGES_PER_KEYWORD,
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

    # ------------------------------------------------------------------
    # Session management
    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    # GEM API search
    # ------------------------------------------------------------------
    def _search_page(self, keyword: str, page: int) -> dict:
        """Search GEM portal for a keyword on a specific page.

        Uses 'Contains' search type (fullText), sorted by Bid Start Date Latest.
        """
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
                logger.warning("Request attempt %d failed for '%s' p%d: %s", attempt, keyword, page, exc)
                if attempt < MAX_RETRIES:
                    time.sleep(2 * attempt)
        return {}

    # ------------------------------------------------------------------
    # Bid parsing
    # ------------------------------------------------------------------
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
            "Search Keyword": keyword,
            "_start_dt": start_raw,
            "_end_dt": end_raw,
        }

    # ------------------------------------------------------------------
    # Selenium / PDF helpers
    # ------------------------------------------------------------------
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

    def _sync_session_cookies_from_driver(self, driver: webdriver.Chrome) -> None:
        """Copy browser cookies into the requests session."""
        for c in driver.get_cookies():
            name = c.get("name")
            if not name:
                continue
            try:
                self.session.cookies.set(
                    name, c.get("value", ""),
                    domain=c.get("domain"), path=c.get("path") or "/",
                )
            except Exception:
                self.session.cookies.set(name, c.get("value", ""))

    def _try_set_sort_bid_start_latest(self, driver: webdriver.Chrome) -> None:
        """Best-effort: match portal sort to Bid Start Date latest."""
        try:
            for sel_el in driver.find_elements(By.TAG_NAME, "select"):
                try:
                    sel = Select(sel_el)
                except Exception:
                    continue
                for opt in sel.options:
                    val = (opt.get_attribute("value") or "").strip()
                    if val == SORT_ORDER or "Bid-Start-Date-Latest" in val:
                        sel.select_by_value(val)
                        time.sleep(0.4)
                        return
        except Exception as exc:
            logger.debug("Could not set GEM sort control: %s", exc)

    def _download_pdf(self, pdf_url: str, bid_no: str) -> Path | None:
        if not pdf_url:
            return None
        PDF_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        safe_name = self._sanitize_filename(bid_no) or f"bid_{int(time.time())}"
        out_path = PDF_CACHE_DIR / f"{safe_name}.pdf"
        # Skip if already cached
        if out_path.exists() and out_path.stat().st_size > 80:
            return out_path
        try:
            resp = self.session.get(pdf_url, timeout=PDF_FETCH_TIMEOUT_SECONDS, verify=False)
            resp.raise_for_status()
            out_path.write_bytes(resp.content)
            return out_path if out_path.exists() and out_path.stat().st_size > 0 else None
        except requests.RequestException as exc:
            logger.warning("PDF download failed for %s (%s): %s", bid_no, pdf_url, exc)
            return None

    # ------------------------------------------------------------------
    # Pipeline 1: Search each inclusion keyword, filter by start date
    # ------------------------------------------------------------------
    def search_keyword_with_date_filter(
        self,
        keyword: str,
        lookback_days: int = LOOKBACK_DAYS,
        seen_refs: set[str] | None = None,
    ) -> list[dict]:
        """Search GEM for a single keyword, return bids with start date >= cutoff.

        Paginates until start dates fall before cutoff or safety cap hit.
        Skips refs already in seen_refs (cross-keyword dedup).
        """
        if seen_refs is None:
            seen_refs = set()

        cutoff_date = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).date()
        bids: list[dict] = []
        page = 1
        stop_paging = False

        while page <= MAX_PAGES_PER_KEYWORD and not stop_paging:
            data = self._search_page(keyword, page)
            docs = data.get("response", {}).get("response", {}).get("docs", []) if data else []
            if not docs:
                break

            for doc in docs:
                bid = self._parse_bid(doc, keyword)
                ref = str(bid.get("Reference No.", "")).strip()
                if not ref or ref in seen_refs:
                    continue

                # Check start date against cutoff
                start_raw = bid.get("_start_dt", "")
                start_dt = _parse_iso(start_raw) if start_raw else None

                if start_dt:
                    start_date = start_dt.astimezone(timezone.utc).date()
                    if start_date < cutoff_date:
                        # Sorted latest-first: all remaining will be older
                        stop_paging = True
                        break

                seen_refs.add(ref)
                bids.append(bid)

            if stop_paging:
                break
            page += 1
            time.sleep(random.uniform(*REQUEST_DELAY))

        if page > MAX_PAGES_PER_KEYWORD:
            logger.warning("Keyword '%s' hit page cap (%d pages).", keyword, MAX_PAGES_PER_KEYWORD)

        return bids

    def search_all_inclusion_keywords(
        self,
        keywords: list[str],
        lookback_days: int = LOOKBACK_DAYS,
    ) -> list[dict]:
        """Search each inclusion keyword one by one, dedup across keywords.

        Returns all bids with Start Date >= (today - lookback_days).
        """
        cutoff_date = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).date()
        logger.info(
            "Pipeline 1: Searching %d inclusion keywords, cutoff date: %s",
            len(keywords), cutoff_date.isoformat(),
        )

        all_bids: list[dict] = []
        seen_refs: set[str] = set()

        for i, kw in enumerate(keywords, 1):
            logger.info("Pipeline 1: [%d/%d] Searching keyword: '%s'", i, len(keywords), kw)
            kw_bids = self.search_keyword_with_date_filter(kw, lookback_days, seen_refs)
            all_bids.extend(kw_bids)
            logger.info("Pipeline 1: [%d/%d] '%s' -> %d new bids (total so far: %d)",
                         i, len(keywords), kw, len(kw_bids), len(all_bids))

            if i < len(keywords):
                time.sleep(random.uniform(*REQUEST_DELAY))

        logger.info("Pipeline 1: Total %d unique bids from %d keywords", len(all_bids), len(keywords))
        return all_bids

    # ------------------------------------------------------------------
    # PDF enrichment: download PDFs and extract text
    # ------------------------------------------------------------------
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
            # Warm up session
            try:
                driver.get(GEM_PAGE_URL)
                time.sleep(1.2)
                self._try_set_sort_bid_start_latest(driver)
                self._sync_session_cookies_from_driver(driver)
            except Exception as exc:
                logger.warning("GEM portal warm-up failed (continuing): %s", exc)

            for idx, bid in enumerate(bids, 1):
                bid_no = str(bid.get("Bid No.", "")).strip() or str(bid.get("Reference No.", "")).strip()
                bid_url = str(bid.get("Source URL", "")).strip()
                bid_doc_url = str(bid.get("Bid Doc URL", "")).strip()

                if idx % 25 == 0:
                    logger.info("PDF download progress: %d/%d", idx, len(bids))

                if not bid_no and not bid_doc_url:
                    skipped += 1
                    bid["PDF Text"] = ""
                    bid["PDF Path"] = ""
                    continue

                pdf_path: Path | None = None
                pdf_text = ""

                # Try Bid Doc URL first
                if bid_doc_url:
                    for attempt in range(PDF_FETCH_RETRIES + 2):
                        try:
                            driver.get(bid_doc_url)
                            time.sleep(0.35 + 0.15 * attempt)
                            self._sync_session_cookies_from_driver(driver)
                            pdf_path = self._download_pdf(bid_doc_url, bid_no)
                            if pdf_path and pdf_path.stat().st_size > 80:
                                pdf_text = extract_pdf_text(pdf_path) or ""
                                if pdf_text:
                                    break
                        except (TimeoutException, WebDriverException) as exc:
                            logger.debug("Bid doc attempt %s for %s: %s", attempt, bid_no, exc)
                        pdf_path = None
                        pdf_text = ""

                if pdf_path and pdf_text:
                    downloaded += 1
                    bid["PDF Path"] = str(pdf_path)
                    bid["PDF Text"] = pdf_text
                    bid["Description"] = pdf_text[:1500]
                    continue

                # Fallback: extract PDF link from bid page
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

    def close(self):
        self.session.close()
