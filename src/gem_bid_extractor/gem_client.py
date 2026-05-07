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

from .pdf_reader import extract_pdf_text
from .settings import (
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
                "pdf_extraction_empty": 0,
                "pdf_link_not_found": 0,
                "pdf_not_pdf_payload": 0,
                "pdf_reused": 0,
            }

        downloaded = 0  # Legacy: same as pdf_text_extracted
        failed = 0
        skipped = 0
        pdf_file_saved = 0
        pdf_text_extracted = 0
        pdf_extraction_empty = 0
        pdf_link_not_found = 0
        pdf_not_pdf_payload = 0
        pdf_reused = 0

        # Reuse already-fetched document text across bids in the same run.
        pdf_result_cache: dict[str, tuple[str, str]] = {}
        pdf_index = self._load_pdf_index()
        try:
            driver = self._create_driver()
        except WebDriverException as exc:
            logger.warning("Selenium unavailable; skipping PDF enrichment: %s", exc)
            for bid in bids:
                bid["PDF Text"] = ""
            return {
                "downloaded": 0,
                "failed": len(bids),
                "skipped": 0,
                "pdf_file_saved": 0,
                "pdf_text_extracted": 0,
                "pdf_extraction_empty": 0,
                "pdf_link_not_found": len(bids),
                "pdf_not_pdf_payload": 0,
                "pdf_reused": 0,
            }

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
                bid_id = str(bid.get("Bid ID", "")).strip()
                ref = str(bid.get("Reference No.", "")).strip()
                bid_key = bid_id or bid_doc_url or ref

                if idx % 25 == 0:
                    logger.info("PDF download progress: %d/%d", idx, len(bids))

                if not bid_no and not bid_doc_url:
                    skipped += 1
                    bid["PDF Text"] = ""
                    bid["PDF Path"] = ""
                    continue

                cached = pdf_result_cache.get(bid_key) if bid_key else None
                if cached:
                    bid["PDF Path"], bid["PDF Text"] = cached
                    bid["Description"] = bid["PDF Text"][:1500] if bid["PDF Text"] else ""
                    skipped += 1
                    pdf_reused += 1
                    continue

                # Reuse deterministic artifact from previous runs by bid key.
                if bid_key and isinstance(pdf_index.get(bid_key), dict):
                    prev_path = str(pdf_index[bid_key].get("pdf_path", "")).strip()
                    if prev_path:
                        prev_file = Path(prev_path)
                        if prev_file.exists() and prev_file.stat().st_size > 80:
                            prev_text = extract_pdf_text(prev_file) or ""
                            if prev_text:
                                bid["PDF Path"] = str(prev_file)
                                bid["PDF Text"] = prev_text
                                bid["Description"] = prev_text[:1500]
                                pdf_result_cache[bid_key] = (str(prev_file), prev_text)
                                skipped += 1
                                pdf_reused += 1
                                continue

                pdf_path: Path | None = None
                pdf_text = ""
                link_found = False
                invalid_payload = False

                # Try Bid Doc URL first
                if bid_doc_url:
                    for attempt in range(PDF_FETCH_RETRIES + 2):
                        try:
                            before_download = self._snapshot_pdf_files()
                            driver.get(bid_doc_url)
                            time.sleep(0.8 + 0.25 * attempt)
                            self._sync_session_cookies_from_driver(driver)
                            browser_pdf = self._await_new_download(before_download, timeout_seconds=5)
                            if browser_pdf:
                                link_found = True
                                pdf_path = self._normalize_downloaded_pdf(browser_pdf, bid_no, bid_key)

                            doc_html = driver.page_source
                            direct_link = self._extract_pdf_link_from_html(doc_html, bid_doc_url) or bid_doc_url
                            if direct_link and not pdf_path:
                                link_found = True
                            pdf_path = self._download_pdf(direct_link, bid_no)
                            if not pdf_path:
                                alt_link = self._extract_pdf_link(driver, bid_url)
                                if alt_link:
                                    link_found = True
                                    pdf_path = self._download_pdf(alt_link, bid_no)
                                elif direct_link:
                                    pdf_not_pdf_payload += 1
                            if pdf_path and pdf_path.stat().st_size > 80:
                                if not self._has_pdf_magic(pdf_path):
                                    invalid_payload = True
                                    pdf_not_pdf_payload += 1
                                    pdf_path = None
                                    pdf_text = ""
                                    break
                                pdf_file_saved += 1
                                pdf_text = extract_pdf_text(pdf_path) or ""
                                if pdf_text:
                                    break
                        except (TimeoutException, WebDriverException) as exc:
                            logger.debug("Bid doc attempt %s for %s: %s", attempt, bid_no, exc)
                        pdf_path = None
                        pdf_text = ""

                if pdf_path and pdf_text:
                    downloaded += 1
                    pdf_text_extracted += 1
                    bid["PDF Path"] = str(pdf_path)
                    bid["PDF Text"] = pdf_text
                    bid["Description"] = pdf_text[:1500]
                    if bid_key:
                        pdf_result_cache[bid_key] = (str(pdf_path), pdf_text)
                        pdf_index[bid_key] = {"pdf_path": str(pdf_path), "status": "ok"}
                    continue

                # Fallback: extract PDF link from bid page
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
                    pdf_link_not_found += 1
                    bid["PDF Text"] = ""
                    bid["PDF Path"] = ""
                    if bid_key:
                        pdf_result_cache[bid_key] = ("", "")
                        pdf_index[bid_key] = {"pdf_path": "", "status": "link_not_found"}
                    continue

                pdf_path = self._download_pdf(pdf_link, bid_no)
                if not pdf_path:
                    failed += 1
                    if link_found and not invalid_payload:
                        pdf_not_pdf_payload += 1
                    bid["PDF Text"] = ""
                    bid["PDF Path"] = ""
                    if bid_key:
                        pdf_result_cache[bid_key] = ("", "")
                        pdf_index[bid_key] = {"pdf_path": "", "status": "download_failed"}
                    continue

                pdf_file_saved += 1
                pdf_text = extract_pdf_text(pdf_path)
                if not pdf_text:
                    failed += 1
                    pdf_extraction_empty += 1
                    bid["PDF Text"] = ""
                    bid["PDF Path"] = str(pdf_path)
                    if bid_key:
                        pdf_result_cache[bid_key] = (str(pdf_path), "")
                        pdf_index[bid_key] = {"pdf_path": str(pdf_path), "status": "text_empty"}
                    continue

                downloaded += 1
                pdf_text_extracted += 1
                bid["PDF Path"] = str(pdf_path)
                bid["PDF Text"] = pdf_text
                bid["Description"] = pdf_text[:1500]
                if bid_key:
                    pdf_result_cache[bid_key] = (str(pdf_path), pdf_text)
                    pdf_index[bid_key] = {"pdf_path": str(pdf_path), "status": "ok"}
        finally:
            driver.quit()
            self._save_pdf_index(pdf_index)

        return {
            "downloaded": downloaded,
            "failed": failed,
            "skipped": skipped,
            "pdf_file_saved": pdf_file_saved,
            "pdf_text_extracted": pdf_text_extracted,
            "pdf_extraction_empty": pdf_extraction_empty,
            "pdf_link_not_found": pdf_link_not_found,
            "pdf_not_pdf_payload": pdf_not_pdf_payload,
            "pdf_reused": pdf_reused,
        }

    def close(self):
        self.session.close()
