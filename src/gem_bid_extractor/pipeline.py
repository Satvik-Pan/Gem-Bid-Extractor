from __future__ import annotations

import logging
import re
import unicodedata

from .anthropic_llm import AnthropicClaudeClassifier
from .excel_writer import ExcelWriter
from .gem_client import GemScraper
from .settings import (
    DOUBTFUL_REVIEW_MIN_ROWS,
    DOUBTFUL_FILE,
    EXCEL_FILE,
    INCLUSION_KEYWORDS,
    UNRESOLVED_BIDS_FILE,
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


def _looks_obviously_non_cyber_firewall(bid: dict) -> bool:
    text = " ".join(
        [
            str(bid.get("Name", "")),
            str(bid.get("Description", "")),
            str(bid.get("Category", "")),
            str(bid.get("Department", "")),
        ]
    ).lower()
    noisy_terms = [
        "extinguisher", "hydrant", "sprinkler", "fire alarm", "foam", "ladder",
        "insurance", "vehicle", "furniture", "civil", "paint", "cleaning", "store",
    ]
    return any(t in text for t in noisy_terms) and not any(
        k in text for k in ["network", "security", "router", "vpn", "utm", "waf", "firewall"]
    )


def _looks_cyber_candidate_firewall(bid: dict) -> bool:
    text = " ".join(
        [
            str(bid.get("Name", "")),
            str(bid.get("Description", "")),
            str(bid.get("Category", "")),
        ]
    ).lower()
    return any(k in text for k in ["firewall", "network security", "waf", "utm", "vpn", "router", "ngfw"])


def _dedupe_bids_by_identity(bids: list[dict]) -> list[dict]:
    deduped: dict[str, dict] = {}
    for bid in bids:
        bid_id = str(bid.get("Bid ID", "")).strip()
        ref = str(bid.get("Reference No.", "")).strip()
        doc_url = str(bid.get("Bid Doc URL", "")).strip()
        key = bid_id or ref or doc_url
        if not key:
            continue
        deduped[key] = bid
    return list(deduped.values())


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run() -> dict:
    scraper = GemScraper()
    llm = AnthropicClaudeClassifier()
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
    # Special prefilter only for FIRE WALL noisy search results.
    fire_wall = []
    others = []
    for bid in pipeline1_new:
        if str(bid.get("Search Keyword", "")).strip().upper() == "FIRE WALL":
            fire_wall.append(bid)
        else:
            others.append(bid)
    if fire_wall:
        filtered_fire_wall: list[dict] = []
        for bid in fire_wall:
            if _looks_obviously_non_cyber_firewall(bid):
                logger.info(
                    "Pipeline 1 FIRE WALL prefilter dropped %s: obvious non-cyber context",
                    str(bid.get("Reference No.", "")).strip(),
                )
                continue
            if not _looks_cyber_candidate_firewall(bid):
                logger.info(
                    "Pipeline 1 FIRE WALL prefilter dropped %s: no cyber signal in metadata",
                    str(bid.get("Reference No.", "")).strip(),
                )
                continue
            verdict = llm.keep_fire_wall_result(bid)
            if verdict["keep"]:
                filtered_fire_wall.append(bid)
            else:
                logger.info(
                    "Pipeline 1 FIRE WALL prefilter dropped %s: %s",
                    str(bid.get("Reference No.", "")).strip(),
                    verdict["reason"] or "not cyber-relevant",
                )
        pipeline1_new = [*others, *filtered_fire_wall]
    else:
        pipeline1_new = others

    pipeline1_new = _dedupe_bids_by_identity(pipeline1_new)
    logger.info(
        "Pipeline 1 complete: %d total bids, %d new (not previously processed)",
        len(pipeline1_bids), len(pipeline1_new),
    )

    if not pipeline1_new:
        logger.info("No new bids to process. Exiting.")
        return {
            "pipeline1_total": len(pipeline1_bids),
            "pipeline1_new": 0,
            "docs_parsed_native": 0,
            "docs_parsed_ocr": 0,
            "docs_unresolved": 0,
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
    logger.info(
        "Pipeline 1 PDF details: file_saved=%d, parse_ok=%d, ocr_ok=%d, ocr_failed=%d, missing_link=%d, "
        "invalid_payload=%d, reused=%d, unresolved_count=%d",
        pdf_stats.get("pdf_file_saved", 0),
        pdf_stats.get("parse_ok", 0),
        pdf_stats.get("ocr_ok", 0),
        pdf_stats.get("ocr_failed", 0),
        pdf_stats.get("download_missing_link", 0),
        pdf_stats.get("download_invalid_payload", 0),
        pdf_stats.get("pdf_reused", 0),
        pdf_stats.get("unresolved_count", 0),
    )

    if pipeline1_no_pdf:
        refs = ", ".join(str(b.get("Reference No.", "")).strip() for b in pipeline1_no_pdf[:10])
        logger.warning(
            "Pipeline 1 PDF retrieval incomplete for %d bids (sample refs: %s)",
            len(pipeline1_no_pdf), refs,
        )
        if UNRESOLVED_BIDS_FILE.exists():
            logger.warning("Detailed unresolved bid diagnostics written to: %s", UNRESOLVED_BIDS_FILE)

    # ==================================================================
    # PIPELINE 2: strict routing using keyword hits from each bid document
    # ==================================================================
    logger.info("=" * 70)
    logger.info("PIPELINE 2: strict inclusion/exclusion routing")
    logger.info("=" * 70)

    extracted: list[dict] = []
    doubtful: list[dict] = []
    rejected: list[dict] = []

    if not llm.enabled:
        raise RuntimeError("Anthropic classifier not configured; Pipeline 2 requires Sonnet.")

    for i, bid in enumerate(pipeline1_with_pdf, 1):
        ref = str(bid.get("Reference No.", "")).strip()
        analysis = llm.classify_bid(bid)
        inclusion_hits = analysis["inclusion_hits"]
        exclusion_hits = analysis["exclusion_hits"]
        selected_keyword = str(analysis["selected_inclusion_keyword"]).strip()
        if selected_keyword and selected_keyword not in inclusion_hits:
            inclusion_hits = [selected_keyword, *inclusion_hits]
        if not selected_keyword:
            selected_keyword = (
                inclusion_hits[0]
                if inclusion_hits
                else str(bid.get("Search Keyword", "")).strip()
            )

        bid["LLM Confidence"] = ""
        bid["Inclusion Hits"] = ", ".join(inclusion_hits[:8])
        bid["Exclusion Hits"] = ", ".join(exclusion_hits[:8])

        if not inclusion_hits:
            bid["Pipeline Source"] = "pipeline2_reject_no_inclusion"
            bid["LLM Reason"] = f"Rejected: exact inclusion phrase not found in PDF. {analysis['reason']}".strip()
            rejected.append(_sanitize_bid(bid))
            logger.info(
                "Pipeline 2: [%d/%d] REJECTED - %s (no exact inclusion phrase)",
                i, len(pipeline1_with_pdf), ref,
            )
            continue

        if exclusion_hits:
            # inclusion+exclusion => doubtful
            bid["Final Category"] = "DOUBTFUL"
            bid["Pipeline Source"] = "pipeline2_doubtful_inclusion_exclusion"
            bid["LLM Reason"] = (
                f"Selected by keyword: {selected_keyword}; "
                f"Exclusion phrase(s) also found: {', '.join(exclusion_hits[:3])}. {analysis['reason']}"
            ).strip()
            doubtful.append(_sanitize_bid(bid))
            logger.info(
                "Pipeline 2: [%d/%d] DOUBTFUL - %s (incl=%s, excl=%s)",
                i, len(pipeline1_with_pdf), ref,
                ", ".join(inclusion_hits[:2]),
                ", ".join(exclusion_hits[:2]),
            )
            continue

        bid["Final Category"] = "EXTRACTED"
        bid["Pipeline Source"] = "pipeline2_extracted"
        bid["LLM Reason"] = f"Selected by keyword: {selected_keyword}. {analysis['reason']}".strip()
        extracted.append(_sanitize_bid(bid))
        logger.info(
            "Pipeline 2: [%d/%d] EXTRACTED - %s (inclusion=%s)",
            i, len(pipeline1_with_pdf), ref,
            selected_keyword or "search-matched",
        )

    # Do not drop unresolved documents; keep for ops follow-up.
    for bid in pipeline1_no_pdf:
        bid["Final Category"] = "DOUBTFUL"
        bid["Pipeline Source"] = "pipeline2_doubtful_unresolved_document"
        bid["Inclusion Hits"] = str(bid.get("Search Keyword", "")).strip()
        bid["Exclusion Hits"] = ""
        bid["LLM Confidence"] = ""
        bid["LLM Reason"] = "Bid document text unresolved after download/ocr attempts; kept for review."
        doubtful.append(_sanitize_bid(bid))

    if len(doubtful) > DOUBTFUL_REVIEW_MIN_ROWS:
        logger.info(
            "Pipeline 2 doubtful refinement: %d rows > threshold %d, running exclusion-weighted Sonnet recheck",
            len(doubtful),
            DOUBTFUL_REVIEW_MIN_ROWS,
        )
        refined_doubtful: list[dict] = []
        dropped_from_doubtful = 0
        for bid in doubtful:
            # Keep unresolved-document cases in doubtful for ops follow-up.
            if str(bid.get("Pipeline Source", "")).strip() == "pipeline2_doubtful_unresolved_document":
                refined_doubtful.append(bid)
                continue
            ref = str(bid.get("Reference No.", "")).strip()
            verdict = llm.review_doubtful_exclusion_strength(bid)
            if verdict["drop"]:
                strong_hits = ", ".join(verdict.get("strong_exclusion_hits", [])[:4])
                note = f"Refined reject (strong exclusion weightage): {verdict.get('reason', '')}".strip()
                if strong_hits:
                    note = f"{note} | strong exclusions: {strong_hits}"
                bid["Final Category"] = "REJECTED"
                bid["Pipeline Source"] = "pipeline2_reject_refined_doubtful"
                bid["LLM Reason"] = note
                rejected.append(_sanitize_bid(bid))
                dropped_from_doubtful += 1
                logger.info("Pipeline 2 doubtful refinement: REJECTED %s", ref)
            else:
                refined_doubtful.append(bid)
        doubtful = refined_doubtful
        logger.info(
            "Pipeline 2 doubtful refinement complete: moved_to_rejected=%d, remaining_doubtful=%d",
            dropped_from_doubtful,
            len(doubtful),
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
        tracker.mark(bid.get("Reference No.", ""), "doubtful", 50, 0.5)
    for bid in rejected:
        tracker.mark(bid.get("Reference No.", ""), "rejected", 0, 0)
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
        "pdf_file_saved": pdf_stats.get("pdf_file_saved", 0),
        "docs_parsed_native": pdf_stats.get("parse_ok", 0),
        "docs_parsed_ocr": pdf_stats.get("ocr_ok", 0),
        "docs_unresolved": pdf_stats.get("ocr_failed", 0),
        "download_missing_link": pdf_stats.get("download_missing_link", 0),
        "download_invalid_payload": pdf_stats.get("download_invalid_payload", 0),
        "pdf_reused": pdf_stats.get("pdf_reused", 0),
        "unresolved_count": pdf_stats.get("unresolved_count", 0),
        "pipeline2_extracted": len(extracted),
        "pipeline2_doubtful": len(doubtful),
        "pipeline2_rejected": len(rejected),
        "saved_extracted": saved_extracted,
        "saved_doubtful": saved_doubtful,
        "supabase_sync": db_sync_ok,
    }
