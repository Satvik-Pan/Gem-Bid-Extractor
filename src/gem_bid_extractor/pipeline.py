from __future__ import annotations

import logging
import re
import unicodedata

from .excel_writer import ExcelWriter
from .gem_client import GemScraper
from .settings import (
    DOUBTFUL_FILE,
    EXCLUSION_KEYWORDS,
    EXCEL_FILE,
    INCLUSION_KEYWORDS,
)
from .storage import BidTracker
from .supabase_store import SupabaseStore

logger = logging.getLogger(__name__)
_ILLEGAL_EXCEL_CHARS = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")


# ---------------------------------------------------------------------------
# Keyword matching helpers (regex-based, fast and accurate)
# ---------------------------------------------------------------------------

def _tokenize_keyword_phrase(term: str) -> list[str]:
    return [p for p in re.split(r"[^a-z0-9]+", term.strip().lower()) if p]


def _single_token_pattern(token: str) -> re.Pattern[str]:
    t = re.escape(token)
    return re.compile(rf"(?<![a-z0-9]){t}(?![a-z0-9])", re.IGNORECASE)


def _flexible_phrase_pattern(term: str) -> re.Pattern[str]:
    parts = _tokenize_keyword_phrase(term)
    if not parts:
        return re.compile(r"$^")
    if len(parts) == 1:
        return _single_token_pattern(parts[0])
    sep = r"[^\w]+"
    body = sep.join(re.escape(p) for p in parts)
    return re.compile(rf"(?<![a-z0-9]){body}(?![a-z0-9])", re.IGNORECASE)


def _compile_keyword_sets(terms: list[str]) -> list[tuple[str, re.Pattern[str]]]:
    seen: set[str] = set()
    out: list[tuple[str, re.Pattern[str]]] = []
    for raw in terms:
        label = " ".join(raw.strip().split())
        if not label:
            continue
        key = label.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append((label, _flexible_phrase_pattern(label)))
    return out


def _unicode_normalize_text(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.replace("\u00ad", "").replace("\u200b", "").replace("\u200c", "")


def _build_haystack(bid: dict) -> str:
    """Build normalized text from bid's PDF text + metadata for keyword matching."""
    parts = [
        str(bid.get("Name", "")),
        str(bid.get("Description", "")),
        str(bid.get("Category", "")),
        str(bid.get("Department", "")),
        str(bid.get("PDF Text", "")),
    ]
    raw = " | ".join(parts)
    s = _unicode_normalize_text(raw)
    s = s.casefold()
    s = re.sub(r"[\u200b\xa0]+", " ", s)
    s = re.sub(r"[\s\r\n\t]+", " ", s)
    s = re.sub(r"[_/\\|:;,.]+", " ", s)
    s = re.sub(r"-+", " ", s)
    s = re.sub(r" +", " ", s).strip()
    return f" {s} "


def _find_keyword_hits(
    pattern_set: list[tuple[str, re.Pattern[str]]], haystack: str
) -> list[str]:
    return [label for label, pat in pattern_set if pat.search(haystack)]


def _sanitize_for_excel(value: object) -> object:
    if not isinstance(value, str):
        return value
    return _ILLEGAL_EXCEL_CHARS.sub("", value)


def _sanitize_bid(bid: dict) -> dict:
    for key, val in list(bid.items()):
        bid[key] = _sanitize_for_excel(val)
    return bid


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run() -> dict:
    # Compile keyword patterns
    inclusion_patterns = _compile_keyword_sets(INCLUSION_KEYWORDS)
    exclusion_patterns = _compile_keyword_sets(EXCLUSION_KEYWORDS)

    scraper = GemScraper()
    tracker = BidTracker()
    db = SupabaseStore()
    writer_main = ExcelWriter(EXCEL_FILE)
    writer_doubtful = ExcelWriter(DOUBTFUL_FILE)
    writer_main.save([])
    writer_doubtful.save([])
    db.ensure_schema()

    # ==================================================================
    # PIPELINE 1: Search each inclusion keyword on GEM portal
    #   - Put each keyword in search box (Contains mode)
    #   - Sort by Bid Start Date Latest First
    #   - Extract bids with Start Date >= (today - 3 days)
    #   - Dedupe across all keywords
    # ==================================================================
    logger.info("=" * 70)
    logger.info("PIPELINE 1: Keyword-based search on GEM portal")
    logger.info("=" * 70)

    scraper.init_session()
    try:
        pipeline1_bids = scraper.search_all_inclusion_keywords(INCLUSION_KEYWORDS)
    finally:
        scraper.close()

    # Skip already-processed bids
    pipeline1_new = [
        b for b in pipeline1_bids
        if not tracker.is_processed(str(b.get("Reference No.", "")).strip())
    ]
    logger.info(
        "Pipeline 1 complete: %d total bids, %d new (not previously processed)",
        len(pipeline1_bids), len(pipeline1_new),
    )

    if not pipeline1_new:
        logger.info("No new bids to process. Exiting.")
        return {
            "pipeline1_total": len(pipeline1_bids),
            "pipeline1_new": 0,
            "pipeline2_extracted": 0,
            "pipeline2_doubtful": 0,
            "pipeline2_rejected": 0,
            "saved_extracted": 0,
            "saved_doubtful": 0,
            "supabase_sync": True,
        }

    # Download PDFs for all new bids
    logger.info("Pipeline 1: Downloading PDFs for %d new bids...", len(pipeline1_new))
    pdf_stats = scraper.enrich_with_pdf_text(pipeline1_new)
    pipeline1_with_pdf = [b for b in pipeline1_new if str(b.get("PDF Text", "")).strip()]
    pipeline1_no_pdf = [b for b in pipeline1_new if not str(b.get("PDF Text", "")).strip()]
    logger.info(
        "Pipeline 1 PDFs: downloaded=%d, failed=%d, skipped=%d, with_text=%d, no_text=%d",
        pdf_stats["downloaded"], pdf_stats["failed"], pdf_stats["skipped"],
        len(pipeline1_with_pdf), len(pipeline1_no_pdf),
    )

    # ==================================================================
    # PIPELINE 2: Analyze bid document PDFs for exclusion keywords
    #   - Check each PDF for exclusion keywords
    #   - No exclusion keywords found -> EXTRACTED
    #   - Exclusion + inclusion keywords both found -> DOUBTFUL
    #   - Only exclusion keywords (no inclusion in PDF) -> REJECT
    # ==================================================================
    logger.info("=" * 70)
    logger.info("PIPELINE 2: PDF analysis for exclusion keywords")
    logger.info("=" * 70)

    extracted: list[dict] = []
    doubtful: list[dict] = []
    rejected: list[dict] = []

    for i, bid in enumerate(pipeline1_with_pdf, 1):
        ref = str(bid.get("Reference No.", "")).strip()
        haystack = _build_haystack(bid)

        exclusion_hits = _find_keyword_hits(exclusion_patterns, haystack)
        inclusion_hits = _find_keyword_hits(inclusion_patterns, haystack)

        if not exclusion_hits:
            # No exclusion keywords -> EXTRACTED
            bid["Final Category"] = "EXTRACTED"
            bid["Pipeline Source"] = "pipeline2_extracted"
            bid["Inclusion Hits"] = ", ".join(inclusion_hits[:8])
            bid["Exclusion Hits"] = ""
            extracted.append(_sanitize_bid(bid))
            logger.info(
                "Pipeline 2: [%d/%d] EXTRACTED - %s (inclusion: %s)",
                i, len(pipeline1_with_pdf), ref,
                ", ".join(inclusion_hits[:3]) or "search-matched",
            )
        elif exclusion_hits and inclusion_hits:
            # Both exclusion AND inclusion -> DOUBTFUL
            bid["Final Category"] = "DOUBTFUL"
            bid["Pipeline Source"] = "pipeline2_doubtful_both"
            bid["Inclusion Hits"] = ", ".join(inclusion_hits[:8])
            bid["Exclusion Hits"] = ", ".join(exclusion_hits[:8])
            doubtful.append(_sanitize_bid(bid))
            logger.info(
                "Pipeline 2: [%d/%d] DOUBTFUL - %s (has both: incl=%s, excl=%s)",
                i, len(pipeline1_with_pdf), ref,
                ", ".join(inclusion_hits[:3]),
                ", ".join(exclusion_hits[:3]),
            )
        else:
            # Only exclusion keywords, no inclusion -> REJECT
            rejected.append(bid)
            logger.info(
                "Pipeline 2: [%d/%d] REJECTED - %s (exclusion only: %s)",
                i, len(pipeline1_with_pdf), ref,
                ", ".join(exclusion_hits[:3]),
            )

    logger.info(
        "Pipeline 2 complete: EXTRACTED=%d, DOUBTFUL=%d, REJECTED=%d",
        len(extracted), len(doubtful), len(rejected),
    )

    # ==================================================================
    # SAVE: Write results to Excel, Supabase, and tracker
    # ==================================================================
    logger.info("=" * 70)
    logger.info("SAVING RESULTS")
    logger.info("=" * 70)

    saved_extracted = writer_main.save(extracted)
    saved_doubtful = writer_doubtful.save(doubtful)

    db_sync_ok = db.sync_with_retry([*extracted, *doubtful])
    if db_sync_ok:
        logger.info("Supabase sync: OK")
    else:
        logger.warning("Supabase sync: failed/queued (%s)", db.last_error or "unknown")

    # Mark everything in tracker
    for bid in extracted:
        tracker.mark(bid.get("Reference No.", ""), "extracted", 100, 1.0)
    for bid in doubtful:
        tracker.mark(bid.get("Reference No.", ""), "doubtful", 60, 0.5)
    for bid in rejected:
        tracker.mark(bid.get("Reference No.", ""), "rejected_exclusion", 0, 0)
    for bid in pipeline1_no_pdf:
        tracker.mark(bid.get("Reference No.", ""), "no_pdf", 0, 0)
    tracker.save()

    # Final summary
    logger.info("=" * 70)
    logger.info("RUN COMPLETE")
    logger.info("=" * 70)
    logger.info("Pipeline 1: %d bids fetched, %d new", len(pipeline1_bids), len(pipeline1_new))
    logger.info("Pipeline 1: %d with PDF, %d without PDF", len(pipeline1_with_pdf), len(pipeline1_no_pdf))
    logger.info("Pipeline 2: %d EXTRACTED, %d DOUBTFUL, %d REJECTED", len(extracted), len(doubtful), len(rejected))
    logger.info("Saved: Extracted=%d, Doubtful=%d", saved_extracted, saved_doubtful)

    return {
        "pipeline1_total": len(pipeline1_bids),
        "pipeline1_new": len(pipeline1_new),
        "pipeline1_with_pdf": len(pipeline1_with_pdf),
        "pipeline1_no_pdf": len(pipeline1_no_pdf),
        "pdf_downloaded": pdf_stats["downloaded"],
        "pdf_failed": pdf_stats["failed"],
        "pipeline2_extracted": len(extracted),
        "pipeline2_doubtful": len(doubtful),
        "pipeline2_rejected": len(rejected),
        "saved_extracted": saved_extracted,
        "saved_doubtful": saved_doubtful,
        "supabase_sync": db_sync_ok,
    }
