from __future__ import annotations

import hashlib
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

from .pdf_reader import extract_pdf_text_with_ocr
from .settings import (
    DOWNLOADS_DIR,
    GEM_API_URL,
    GEM_PAGE_URL,
    LOOKBACK_DAYS,
    MAX_PAGES_PER_KEYWORD,
    MAX_RETRIES,
    PDF_CACHE_DIR,
    PDF_INDEX_FILE,
    PDF_FETCH_RETRIES,
    PDF_FETCH_TIMEOUT_SECONDS,
    REQUEST_DELAY,
    SELENIUM_HEADLESS,
    SESSION_REFRESH_EVERY,
    SORT_ORDER,
    UNRESOLVED_BIDS_FILE,
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
        bid_id = str(_val(doc.get("b_id", ""))).strip()
        ref_no = bid_no or ra_no
        bid_no_for_url = bid_no or ra_no
        bid_doc_id = parent_bid_id or bid_id
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
            "Source URL": f"https://bidplus.gem.gov.in/bidlists/{bid_no_for_url}" if bid_no_for_url else GEM_PAGE_URL,
            "Bid Doc URL": f"https://bidplus.gem.gov.in/showbidDocument/{bid_doc_id}" if bid_doc_id else "",
            "Bid ID": bid_id,
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
        # Keep browser-managed downloads inside project cache folder.
        PDF_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        prefs = {
            "download.default_directory": str(PDF_CACHE_DIR.resolve()),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True,
        }
        options.add_experimental_option("prefs", prefs)
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1400,1600")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--log-level=3")
        options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
        return webdriver.Chrome(options=options)

    @staticmethod
    def _load_pdf_index() -> dict[str, dict]:
        if not PDF_INDEX_FILE.exists():
            return {}
        try:
            data = json.loads(PDF_INDEX_FILE.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        return data if isinstance(data, dict) else {}

    @staticmethod
    def _save_pdf_index(index: dict[str, dict]) -> None:
        PDF_INDEX_FILE.parent.mkdir(parents=True, exist_ok=True)
        PDF_INDEX_FILE.write_text(json.dumps(index, ensure_ascii=True, indent=2), encoding="utf-8")

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

    @staticmethod
    def _extract_pdf_link_from_html(html: str, base_url: str) -> str:
        if not html:
            return ""
        patterns = [
            r"https?://[^\"']+\.pdf(?:\?[^\"']*)?",
            r"/[^\"']+\.pdf(?:\?[^\"']*)?",
            r"https?://[^\"']*download[^\"']*",
            r"/[^\"']*download[^\"']*",
        ]
        for pattern in patterns:
            for match in re.finditer(pattern, html, flags=re.IGNORECASE):
                link = match.group(0).strip()
                if link.startswith("/"):
                    link = urljoin(base_url, link)
                low = link.lower()
                if "handbook" in low or "assets-bg.gem.gov.in/resources/pdf" in low:
                    continue
                return link
        return ""

    @staticmethod
    def _snapshot_pdf_files() -> set[str]:
        PDF_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        return {p.name for p in PDF_CACHE_DIR.glob("*.pdf")}

    @staticmethod
    def _await_new_download(before: set[str], timeout_seconds: int = 10) -> Path | None:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            current = sorted(PDF_CACHE_DIR.glob("*.pdf"), key=lambda p: p.stat().st_mtime, reverse=True)
            for path in current:
                if path.name in before:
                    continue
                if path.stat().st_size <= 80:
                    continue
                # Wait for file size to stabilize.
                size1 = path.stat().st_size
                time.sleep(0.35)
                size2 = path.stat().st_size
                if size2 >= size1 and size2 > 80:
                    return path
            time.sleep(0.35)
        return None

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

    def _sync_driver_cookies_from_session(self, driver: webdriver.Chrome) -> None:
        """Copy requests session cookies into browser context."""
        for c in self.session.cookies:
            cookie = {"name": c.name, "value": c.value, "path": c.path or "/"}
            if c.domain and not c.domain.startswith("."):
                cookie["domain"] = c.domain
            elif c.domain:
                cookie["domain"] = c.domain.lstrip(".")
            try:
                driver.add_cookie(cookie)
            except Exception:
                continue

    @staticmethod
    def _extract_urls_from_onclick(onclick: str, base_url: str) -> list[str]:
        if not onclick:
            return []
        urls: list[str] = []
        for m in re.finditer(r"https?://[^\"'\s)]+", onclick, flags=re.IGNORECASE):
            urls.append(m.group(0))
        for m in re.finditer(r"(/[^\"'\s)]+)", onclick):
            urls.append(urljoin(base_url, m.group(1)))
        deduped: list[str] = []
        seen: set[str] = set()
        for u in urls:
            if u in seen:
                continue
            seen.add(u)
            deduped.append(u)
        return deduped

    def _network_doc_candidates(self, driver: webdriver.Chrome) -> list[str]:
        candidates: list[str] = []
        try:
            logs = driver.get_log("performance")
        except Exception:
            return []
        for row in logs:
            try:
                message = json.loads(row.get("message", "{}")).get("message", {})
            except json.JSONDecodeError:
                continue
            method = message.get("method", "")
            params = message.get("params", {})
            url = ""
            if method == "Network.responseReceived":
                response = params.get("response", {})
                url = str(response.get("url", "")).strip()
                ctype = str(response.get("mimeType", "")).lower()
                if "pdf" in ctype and url:
                    candidates.append(url)
                    continue
            if method == "Network.requestWillBeSent":
                request = params.get("request", {})
                url = str(request.get("url", "")).strip()
            low = url.lower()
            if not url:
                continue
            if ".pdf" in low or "download" in low or "showbiddocument" in low:
                candidates.append(url)
        # preserve order
        out: list[str] = []
        seen: set[str] = set()
        for c in candidates:
            if c in seen:
                continue
            seen.add(c)
            out.append(c)
        return out

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
        digest = hashlib.sha1(pdf_url.encode("utf-8")).hexdigest()[:12]
        safe_name = self._sanitize_filename(f"{bid_no}_{digest}") or f"bid_{int(time.time())}"
        out_path = PDF_CACHE_DIR / f"{safe_name}.pdf"
        # Skip if already cached
        if out_path.exists() and out_path.stat().st_size > 80:
            return out_path
        try:
            resp = self.session.get(pdf_url, timeout=PDF_FETCH_TIMEOUT_SECONDS, verify=False)
            resp.raise_for_status()
            content = resp.content or b""
            ctype = (resp.headers.get("Content-Type") or "").lower()
            head = content[:2048].lstrip().lower()
            looks_like_html = head.startswith(b"<!doctype") or head.startswith(b"<html") or b"<body" in head
            likely_pdf = (
                b"%pdf" in content[:2048].lower()
                or "application/pdf" in ctype
                or "application/octet-stream" in ctype
                or pdf_url.lower().endswith(".pdf")
            )
            if looks_like_html and not likely_pdf:
                return None
            out_path.write_bytes(content)
            return out_path if out_path.exists() and out_path.stat().st_size > 80 else None
        except requests.RequestException as exc:
            logger.warning("PDF download failed for %s (%s): %s", bid_no, pdf_url, exc)
            return None

    def _normalize_downloaded_pdf(self, source_path: Path, bid_no: str, bid_key: str) -> Path:
        PDF_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        key = bid_key or bid_no or str(int(time.time()))
        digest = hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]
        safe_name = self._sanitize_filename(f"{bid_no}_{digest}") or f"bid_{digest}"
        target = PDF_CACHE_DIR / f"{safe_name}.pdf"
        if source_path.resolve() == target.resolve():
            return target
        if target.exists() and target.stat().st_size > 80:
            return target
        try:
            source_path.replace(target)
        except OSError:
            # Cross-device or locked file fallback.
            target.write_bytes(source_path.read_bytes())
        return target

    @staticmethod
    def _has_pdf_magic(path: Path) -> bool:
        try:
            with path.open("rb") as fh:
                head = fh.read(8)
            return head.startswith(b"%PDF")
        except OSError:
            return False

    def _download_via_selenium_click(
        self,
        driver: webdriver.Chrome,
        bid_no: str,
        bid_key: str,
        bid_doc_url: str,
        bid_url: str,
    ) -> dict:
        click_xpath = (
            "//*[(self::a or self::button or @role='button' or @onclick) and ("
            "contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download') or "
            "contains(translate(normalize-space(.),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'document') or "
            "contains(translate(@href,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'.pdf') or "
            "contains(translate(@href,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download') or "
            "contains(translate(@onclick,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'download') or "
            "contains(translate(@onclick,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'document')"
            ")]"
        )
        result = {
            "path": None,
            "reason": "unknown",
            "page_url": "",
            "page_title": "",
            "dom_action_count": 0,
            "network_candidate_count": 0,
        }
        try:
            before = self._snapshot_pdf_files()
            if bid_doc_url:
                driver.get(bid_doc_url)
            else:
                driver.get(bid_url)
            time.sleep(0.8)
            self._sync_driver_cookies_from_session(driver)
            self._sync_session_cookies_from_driver(driver)
            result["page_url"] = driver.current_url
            result["page_title"] = driver.title

            # 1) Prefer explicit clickable download/doc links.
            elements = driver.find_elements(By.XPATH, click_xpath)
            result["dom_action_count"] = len(elements)
            for el in elements[:8]:
                try:
                    href = (el.get_attribute("href") or "").strip()
                    if href:
                        maybe = self._download_pdf(href, bid_no)
                        if maybe and self._has_pdf_magic(maybe):
                            result["path"] = maybe
                            result["reason"] = "download_ok"
                            return result
                    onclick = (el.get_attribute("onclick") or "").strip()
                    for oc_url in self._extract_urls_from_onclick(onclick, driver.current_url):
                        maybe = self._download_pdf(oc_url, bid_no)
                        if maybe and self._has_pdf_magic(maybe):
                            result["path"] = maybe
                            result["reason"] = "download_ok"
                            return result
                    driver.execute_script("arguments[0].click();", el)
                    downloaded = self._await_new_download(before, timeout_seconds=6)
                    if downloaded:
                        norm = self._normalize_downloaded_pdf(downloaded, bid_no, bid_key)
                        if self._has_pdf_magic(norm):
                            result["path"] = norm
                            result["reason"] = "download_ok"
                            return result
                        norm.unlink(missing_ok=True)
                except Exception:
                    continue

            # 2) Fallback: parse page html for direct link.
            html_link = self._extract_pdf_link_from_html(driver.page_source, driver.current_url)
            if html_link:
                maybe = self._download_pdf(html_link, bid_no)
                if maybe and self._has_pdf_magic(maybe):
                    result["path"] = maybe
                    result["reason"] = "download_ok"
                    return result

            # 3) Last fallback: legacy link extractor from bid page.
            if bid_url:
                link = self._extract_pdf_link(driver, bid_url)
                if link:
                    maybe = self._download_pdf(link, bid_no)
                    if maybe and self._has_pdf_magic(maybe):
                        result["path"] = maybe
                        result["reason"] = "download_ok"
                        return result
            # 4) Network-event fallback for JS-only document endpoints.
            candidates = self._network_doc_candidates(driver)
            result["network_candidate_count"] = len(candidates)
            for net_url in candidates[:12]:
                maybe = self._download_pdf(net_url, bid_no)
                if maybe and self._has_pdf_magic(maybe):
                    result["path"] = maybe
                    result["reason"] = "download_ok"
                    return result
            if result["dom_action_count"] == 0:
                result["reason"] = "no_dom_action"
            elif result["network_candidate_count"] == 0:
                result["reason"] = "network_no_doc_response"
            else:
                result["reason"] = "js_action_no_file"
        except Exception as exc:
            logger.debug("Selenium click download failed for %s: %s", bid_no, exc)
            result["reason"] = "exception_in_click_path"
        return result

    # ------------------------------------------------------------------
    # Pipeline 1: Search each inclusion keyword, filter by start date
    # ------------------------------------------------------------------
    def search_keyword_with_date_filter(
        self,
        keyword: str,
        lookback_days: int = LOOKBACK_DAYS,
        seen_keys: set[str] | None = None,
    ) -> list[dict]:
        """Search GEM for a single keyword, return bids with start date >= cutoff.

        Paginates until start dates fall before cutoff or safety cap hit.
        Skips bids already in seen_keys (cross-keyword dedup).
        """
        if seen_keys is None:
            seen_keys = set()

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
                bid_id = str(bid.get("Bid ID", "")).strip()
                bid_doc_url = str(bid.get("Bid Doc URL", "")).strip()
                dedupe_key = bid_id or bid_doc_url or ref

                if not ref or not dedupe_key or dedupe_key in seen_keys:
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

                seen_keys.add(dedupe_key)
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
        seen_keys: set[str] = set()

        for i, kw in enumerate(keywords, 1):
            logger.info("Pipeline 1: [%d/%d] Searching keyword: '%s'", i, len(keywords), kw)
            kw_bids = self.search_keyword_with_date_filter(kw, lookback_days, seen_keys)
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
            return {
                "downloaded": 0,
                "failed": 0,
                "skipped": 0,
                "pdf_file_saved": 0,
                "pdf_text_extracted": 0,
                "parse_ok": 0,
                "ocr_ok": 0,
                "ocr_failed": 0,
                "download_ok": 0,
                "download_missing_link": 0,
                "download_invalid_payload": 0,
                "pdf_extraction_empty": 0,
                "pdf_link_not_found": 0,
                "pdf_not_pdf_payload": 0,
                "pdf_reused": 0,
            }

        downloaded = 0
        failed = 0
        skipped = 0
        pdf_file_saved = 0
        pdf_text_extracted = 0
        parse_ok = 0
        ocr_ok = 0
        ocr_failed = 0
        download_ok = 0
        download_missing_link = 0
        download_invalid_payload = 0
        pdf_extraction_empty = 0
        pdf_link_not_found = 0
        pdf_not_pdf_payload = 0
        pdf_reused = 0
        unresolved_rows: list[dict] = []

        pdf_result_cache: dict[str, tuple[str, str]] = {}
        pdf_index = self._load_pdf_index()
        try:
            driver = self._create_driver()
        except WebDriverException as exc:
            logger.warning("Selenium unavailable; skipping PDF enrichment: %s", exc)
            for bid in bids:
                bid["PDF Text"] = ""
                bid["PDF Path"] = ""
                bid["PDF Status"] = "download_missing_link"
            return {
                "downloaded": 0,
                "failed": len(bids),
                "skipped": 0,
                "pdf_file_saved": 0,
                "pdf_text_extracted": 0,
                "parse_ok": 0,
                "ocr_ok": 0,
                "ocr_failed": len(bids),
                "download_ok": 0,
                "download_missing_link": len(bids),
                "download_invalid_payload": 0,
                "pdf_extraction_empty": len(bids),
                "pdf_link_not_found": len(bids),
                "pdf_not_pdf_payload": 0,
                "pdf_reused": 0,
            }

        try:
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
                bid_id = str(bid.get("Bid ID", "")).strip()
                ref = str(bid.get("Reference No.", "")).strip()
                bid_key = bid_id or bid_doc_url or ref

                if idx % 25 == 0:
                    logger.info("PDF download progress: %d/%d", idx, len(bids))

                if not bid_no and not bid_doc_url:
                    skipped += 1
                    bid["PDF Text"] = ""
                    bid["PDF Path"] = ""
                    bid["PDF Status"] = "download_missing_link"
                    continue

                cached = pdf_result_cache.get(bid_key) if bid_key else None
                if cached:
                    bid["PDF Path"], bid["PDF Text"] = cached
                    bid["Description"] = bid["PDF Text"][:1500] if bid["PDF Text"] else ""
                    bid["PDF Status"] = "cache_reused"
                    skipped += 1
                    pdf_reused += 1
                    continue

                if bid_key and isinstance(pdf_index.get(bid_key), dict):
                    prev_path = str(pdf_index[bid_key].get("pdf_path", "")).strip()
                    if prev_path:
                        prev_file = Path(prev_path)
                        if prev_file.exists() and prev_file.stat().st_size > 80:
                            prev_text, prev_source = extract_pdf_text_with_ocr(prev_file)
                            if prev_text:
                                bid["PDF Path"] = str(prev_file)
                                bid["PDF Text"] = prev_text
                                bid["PDF Text Source"] = prev_source
                                bid["PDF Status"] = "parse_ok" if prev_source == "native" else "ocr_ok"
                                bid["Description"] = prev_text[:1500]
                                pdf_result_cache[bid_key] = (str(prev_file), prev_text)
                                skipped += 1
                                pdf_reused += 1
                                continue

                pdf_path: Path | None = None
                pdf_text = ""
                text_source = "none"
                link_found = False
                invalid_payload = False
                click_result: dict = {}

                if bid_doc_url:
                    for attempt in range(PDF_FETCH_RETRIES + 2):
                        try:
                            click_result = self._download_via_selenium_click(
                                driver=driver,
                                bid_no=bid_no,
                                bid_key=bid_key,
                                bid_doc_url=bid_doc_url,
                                bid_url=bid_url,
                            )
                            pdf_path = click_result.get("path")
                            link_found = True if pdf_path else link_found
                            if pdf_path and pdf_path.stat().st_size > 80:
                                if not self._has_pdf_magic(pdf_path):
                                    invalid_payload = True
                                    download_invalid_payload += 1
                                    pdf_not_pdf_payload += 1
                                    pdf_path = None
                                    break
                                pdf_file_saved += 1
                                download_ok += 1
                                pdf_text, text_source = extract_pdf_text_with_ocr(pdf_path)
                                break
                        except (TimeoutException, WebDriverException) as exc:
                            logger.debug("Bid doc attempt %s for %s: %s", attempt, bid_no, exc)
                        pdf_path = None
                        pdf_text = ""
                        text_source = "none"

                if not pdf_path:
                    pdf_link = ""
                    for _ in range(PDF_FETCH_RETRIES + 1):
                        try:
                            pdf_link = self._extract_pdf_link(driver, bid_url)
                        except TimeoutException:
                            pdf_link = ""
                        if pdf_link:
                            link_found = True
                            break
                        time.sleep(1.0)

                    if not pdf_link:
                        failed += 1
                        download_missing_link += 1
                        pdf_link_not_found += 1
                        bid["PDF Text"] = ""
                        bid["PDF Path"] = ""
                        fail_reason = (
                            click_result.get("reason", "download_missing_link")
                            if "click_result" in locals()
                            else "download_missing_link"
                        )
                        bid["PDF Status"] = fail_reason
                        unresolved_rows.append(
                            {
                                "ref": ref,
                                "bid_no": bid_no,
                                "bid_id": bid_id,
                                "bid_doc_url": bid_doc_url,
                                "bid_url": bid_url,
                                "reason": fail_reason,
                                "page_url": click_result.get("page_url", "") if "click_result" in locals() else "",
                                "page_title": click_result.get("page_title", "") if "click_result" in locals() else "",
                                "dom_action_count": click_result.get("dom_action_count", 0) if "click_result" in locals() else 0,
                                "network_candidate_count": click_result.get("network_candidate_count", 0) if "click_result" in locals() else 0,
                            }
                        )
                        if bid_key:
                            pdf_result_cache[bid_key] = ("", "")
                            pdf_index[bid_key] = {"pdf_path": "", "status": fail_reason}
                        continue

                    pdf_path = self._download_pdf(pdf_link, bid_no)
                    if not pdf_path:
                        failed += 1
                        if link_found and not invalid_payload:
                            pdf_not_pdf_payload += 1
                        download_invalid_payload += 1
                        bid["PDF Text"] = ""
                        bid["PDF Path"] = ""
                        bid["PDF Status"] = "download_invalid_payload"
                        if bid_key:
                            pdf_result_cache[bid_key] = ("", "")
                            pdf_index[bid_key] = {"pdf_path": "", "status": "download_invalid_payload"}
                        continue

                    pdf_file_saved += 1
                    download_ok += 1
                    pdf_text, text_source = extract_pdf_text_with_ocr(pdf_path)

                if not pdf_text:
                    failed += 1
                    ocr_failed += 1
                    pdf_extraction_empty += 1
                    bid["PDF Text"] = ""
                    bid["PDF Path"] = str(pdf_path) if pdf_path else ""
                    bid["PDF Text Source"] = "none"
                    bid["PDF Status"] = "ocr_failed"
                    if bid_key:
                        pdf_result_cache[bid_key] = (str(pdf_path) if pdf_path else "", "")
                        pdf_index[bid_key] = {"pdf_path": str(pdf_path) if pdf_path else "", "status": "ocr_failed"}
                    continue

                downloaded += 1
                pdf_text_extracted += 1
                if text_source == "native":
                    parse_ok += 1
                elif text_source == "ocr":
                    ocr_ok += 1
                bid["PDF Path"] = str(pdf_path)
                bid["PDF Text"] = pdf_text
                bid["PDF Text Source"] = text_source
                bid["PDF Status"] = "parse_ok" if text_source == "native" else "ocr_ok"
                bid["Description"] = pdf_text[:1500]
                try:
                    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
                    digest = hashlib.sha1((bid_key or bid_no).encode("utf-8")).hexdigest()[:10]
                    fname = f"{self._sanitize_filename(bid_no)}_{digest}.pdf"
                    copy_path = DOWNLOADS_DIR / fname
                    if not copy_path.exists():
                        copy_path.write_bytes(pdf_path.read_bytes())
                except OSError as exc:
                    logger.debug("Downloads copy failed for %s: %s", bid_no, exc)
                if bid_key:
                    pdf_result_cache[bid_key] = (str(pdf_path), pdf_text)
                    pdf_index[bid_key] = {"pdf_path": str(pdf_path), "status": f"{text_source}_ok"}
        finally:
            driver.quit()
            self._save_pdf_index(pdf_index)
            UNRESOLVED_BIDS_FILE.parent.mkdir(parents=True, exist_ok=True)
            UNRESOLVED_BIDS_FILE.write_text(
                json.dumps(unresolved_rows, ensure_ascii=True, indent=2),
                encoding="utf-8",
            )

        return {
            "downloaded": downloaded,
            "failed": failed,
            "skipped": skipped,
            "pdf_file_saved": pdf_file_saved,
            "pdf_text_extracted": pdf_text_extracted,
            "parse_ok": parse_ok,
            "ocr_ok": ocr_ok,
            "ocr_failed": ocr_failed,
            "download_ok": download_ok,
            "download_missing_link": download_missing_link,
            "download_invalid_payload": download_invalid_payload,
            "pdf_extraction_empty": pdf_extraction_empty,
            "pdf_link_not_found": pdf_link_not_found,
            "pdf_not_pdf_payload": pdf_not_pdf_payload,
            "pdf_reused": pdf_reused,
            "unresolved_count": len(unresolved_rows),
        }

    def close(self):
        self.session.close()
