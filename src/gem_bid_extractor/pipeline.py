from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone

from .anthropic_llm import AnthropicClaudeClassifier
from .excel_writer import ExcelWriter
from .gem_client import GemScraper
from .settings import (
    DOUBTFUL_FILE,
    EXCLUSION_KEYWORDS,
    EXCEL_FILE,
    INCLUSION_KEYWORDS,
    KEYWORDS,
    LOOKBACK_DAYS,
)
from .storage import BidTracker
from .supabase_store import SupabaseStore

logger = logging.getLogger(__name__)
_NON_WORD_BOUNDARY = r"[^a-z0-9]+"
_WEAK_EXCLUSION_TERMS = {"next", "threat", "internet", "domain", "edge", "gateway"}


def _compile_keyword_pattern(term: str) -> re.Pattern[str]:
    chunks = [re.escape(chunk) for chunk in re.split(_NON_WORD_BOUNDARY, term.lower()) if chunk]
    if not chunks:
        return re.compile(r"$^")
    pattern = r"\b" + r"\s+".join(chunks) + r"\b"
    return re.compile(pattern, re.IGNORECASE)


_INCLUSION_PATTERNS = [(term, _compile_keyword_pattern(term)) for term in INCLUSION_KEYWORDS]
_EXCLUSION_PATTERNS = [(term, _compile_keyword_pattern(term)) for term in EXCLUSION_KEYWORDS]


def _dedupe_by_ref(bids: list[dict]) -> list[dict]:
    seen: set[str] = set()
    out: list[dict] = []
    for bid in bids:
        ref = str(bid.get("Reference No.", "")).strip()
        if not ref or ref in seen:
            continue
        seen.add(ref)
        out.append(bid)
    return out


def _merge_candidates(full_prefiltered: list[dict], keyword_bids: list[dict]) -> list[dict]:
    merged: dict[str, dict] = {}

    for bid in full_prefiltered:
        ref = str(bid.get("Reference No.", "")).strip()
        if not ref:
            continue
        bid["Pipeline Source"] = "pipeline1_full_llm"
        merged[ref] = bid

    for bid in keyword_bids:
        ref = str(bid.get("Reference No.", "")).strip()
        if not ref:
            continue
        if ref in merged:
            src = str(merged[ref].get("Pipeline Source", "pipeline1_full_llm"))
            merged[ref]["Pipeline Source"] = "pipeline1+pipeline2" if "pipeline2_keyword" not in src else src
            continue
        bid["Pipeline Source"] = "pipeline2_keyword"
        merged[ref] = bid

    return list(merged.values())


def _keyword_flags(bid: dict) -> tuple[bool, bool, list[str], list[str]]:
    text_parts = [
        str(bid.get("Name", "")),
        str(bid.get("Description", "")),
        str(bid.get("Category", "")),
    ]
    haystack = " ".join(text_parts).lower().strip()
    inclusion_hits = [term for term, pattern in _INCLUSION_PATTERNS if pattern.search(haystack)]
    exclusion_hits = [term for term, pattern in _EXCLUSION_PATTERNS if pattern.search(haystack)]
    strong_exclusion_hits = [term for term in exclusion_hits if term not in _WEAK_EXCLUSION_TERMS]
    weak_exclusion_hits = [term for term in exclusion_hits if term in _WEAK_EXCLUSION_TERMS]
    has_inclusion = bool(inclusion_hits)
    has_exclusion = bool(strong_exclusion_hits) or len(weak_exclusion_hits) >= 2
    return has_inclusion, has_exclusion, inclusion_hits, exclusion_hits


def run() -> dict:
    scraper = GemScraper()
    llm = AnthropicClaudeClassifier()
    tracker = BidTracker()
    db = SupabaseStore()

    writer_main = ExcelWriter(EXCEL_FILE)
    writer_doubtful = ExcelWriter(DOUBTFUL_FILE)
    # Ensure output workbooks exist and we append only new rows.
    writer_main.save([])
    writer_doubtful.save([])
    db.ensure_schema()

    if not llm.enabled:
        raise RuntimeError("Anthropic classifier is disabled. Configure ANTHROPIC_API_KEY/ANTHROPIC_MODEL/ANTHROPIC_BASE_URL in .env")

    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(days=LOOKBACK_DAYS - 1)

    scraper.init_session()
    try:
        # Pipeline 1: full feed (no keyword filter), then broad LLM prefilter.
        full_bids = scraper.search_full(cutoff)

        # Pipeline 2: keyword based extraction.
        keyword_bids = scraper.search_all(KEYWORDS, cutoff)
    finally:
        scraper.close()

    full_new = [b for b in _dedupe_by_ref(full_bids) if not tracker.is_processed(str(b.get("Reference No.", "")).strip())]
    keyword_new = [b for b in _dedupe_by_ref(keyword_bids) if not tracker.is_processed(str(b.get("Reference No.", "")).strip())]

    logger.info("Pipeline1 full new bids: %d", len(full_new))
    logger.info("Pipeline2 keyword new bids: %d", len(keyword_new))

    if not full_new and not keyword_new:
        writer_main.save([])
        writer_doubtful.save([])
        return {
            "new": 0,
            "relevant": 0,
            "doubtful": 0,
            "rejected": 0,
            "prefilter_kept": 0,
            "merged_candidates": 0,
            "llm_prefilter_coverage": 1.0,
            "llm_final_coverage": 1.0,
        }

    logger.info("Pipeline1 prefilter candidates: %d", len(full_new))
    prefilter_map = llm.prefilter(full_new) if full_new else {}
    prefilter_coverage = (len(prefilter_map) / len(full_new)) if full_new else 1.0

    prefiltered_full: list[dict] = []
    for bid in full_new:
        ref = str(bid.get("Reference No.", "")).strip()
        decision = prefilter_map.get(ref)
        if decision is None:
            logger.warning("Missing LLM prefilter decision for %s; defaulting to keep", ref)
            decision = {"decision": "YES", "confidence": 0.5}
        keep = decision.get("decision") == "YES" or float(decision.get("confidence", 0.0)) >= 0.4
        if keep:
            bid["Prefilter Confidence"] = round(float(decision.get("confidence", 0.0)), 3)
            prefiltered_full.append(bid)

    logger.info("Pipeline1 kept after broad prefilter: %d", len(prefiltered_full))

    combined_candidates = _merge_candidates(prefiltered_full, keyword_new)
    combined_candidates = [b for b in _dedupe_by_ref(combined_candidates) if not tracker.is_processed(str(b.get("Reference No.", "")).strip())]

    llm_candidates: list[dict] = []
    excluded_dropped: list[dict] = []
    for bid in combined_candidates:
        _, has_exclusion, _, exclusion_hits = _keyword_flags(bid)
        if has_exclusion:
            bid["Exclusion Hits"] = ", ".join(exclusion_hits[:6])
            excluded_dropped.append(bid)
            continue
        llm_candidates.append(bid)

    logger.info("Merged candidates for final classification: %d", len(combined_candidates))
    logger.info("Excluded bids dropped before final LLM: %d", len(excluded_dropped))

    final_map = llm.final_classify(llm_candidates) if llm_candidates else {}
    final_coverage = (len(final_map) / len(llm_candidates)) if llm_candidates else 1.0

    relevant: list[dict] = []
    doubtful: list[dict] = []
    rejected: list[dict] = []
    final_fallback_count = 0

    for bid in llm_candidates:
        ref = str(bid.get("Reference No.", "")).strip()
        final_vote = final_map.get(ref)
        if final_vote is None:
            final_fallback_count += 1
            logger.warning("Missing final LLM classification for %s; defaulting to DOUBTFUL", ref)
            final_vote = {
                "category": "DOUBTFUL",
                "confidence": 0.35,
                "reason": "Fallback category: missing final LLM response",
            }

        category = str(final_vote.get("category", "REJECTED")).upper()
        confidence = round(float(final_vote.get("confidence", 0.0)), 3)
        reason = str(final_vote.get("reason", ""))
        has_inclusion, has_exclusion, inclusion_hits, exclusion_hits = _keyword_flags(bid)

        if has_inclusion and not has_exclusion and category != "EXTRACTED":
            category = "EXTRACTED"
            reason = f"{reason} | Inclusion keyword detected; promoted to EXTRACTED.".strip(" |")

        bid["Final Category"] = category
        bid["LLM Confidence"] = confidence
        bid["LLM Reason"] = reason
        bid["Inclusion Match"] = has_inclusion
        bid["Exclusion Match"] = has_exclusion
        bid["Inclusion Hits"] = ", ".join(inclusion_hits[:6])
        bid["Exclusion Hits"] = ", ".join(exclusion_hits[:6])

        if category == "EXTRACTED":
            relevant.append(bid)
        elif category == "DOUBTFUL":
            doubtful.append(bid)
        else:
            rejected.append(bid)

    added_main = writer_main.save(relevant)
    added_doubtful = writer_doubtful.save(doubtful)

    db_sync_ok = db.sync_with_retry([*relevant, *doubtful, *rejected])
    if db_sync_ok:
        logger.info("Supabase sync: enabled")
    else:
        logger.warning("Supabase sync: queued for retry (%s)", db.last_error or "connectivity/config issue")

    for bid in relevant:
        tracker.mark(bid.get("Reference No.", ""), "extracted", 100, bid.get("LLM Confidence", 0))
    for bid in doubtful:
        tracker.mark(bid.get("Reference No.", ""), "doubtful", 60, bid.get("LLM Confidence", 0))
    for bid in rejected:
        tracker.mark(bid.get("Reference No.", ""), "rejected", 0, bid.get("LLM Confidence", 0))
    for bid in excluded_dropped:
        tracker.mark(bid.get("Reference No.", ""), "excluded", 0, 0)
    tracker.save()

    logger.info("LLM prefilter coverage: %.2f", prefilter_coverage)
    logger.info("LLM final coverage: %.2f", final_coverage)
    logger.info("LLM final fallback count: %d", final_fallback_count)
    logger.info("Results -> relevant: %d, doubtful: %d, rejected: %d", len(relevant), len(doubtful), len(rejected))
    logger.info("Saved -> main: %d, doubtful: %d", added_main, added_doubtful)

    return {
        "new": len(combined_candidates),
        "llm_candidates": len(llm_candidates),
        "excluded_dropped": len(excluded_dropped),
        "relevant": len(relevant),
        "doubtful": len(doubtful),
        "rejected": len(rejected),
        "saved_main": added_main,
        "saved_doubtful": added_doubtful,
        "prefilter_kept": len(prefiltered_full),
        "merged_candidates": len(combined_candidates),
        "llm_prefilter_coverage": round(prefilter_coverage, 4),
        "llm_final_coverage": round(final_coverage, 4),
        "llm_final_fallback_count": final_fallback_count,
        "supabase_sync": db_sync_ok,
    }
