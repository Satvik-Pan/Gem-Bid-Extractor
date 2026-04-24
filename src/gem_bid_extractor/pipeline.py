from __future__ import annotations

import logging
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


def _keyword_flags(bid: dict) -> tuple[bool, bool]:
    text_parts = [
        str(bid.get("Name", "")),
        str(bid.get("Description", "")),
        str(bid.get("Category", "")),
    ]
    haystack = " ".join(text_parts).lower()
    has_inclusion = any(term in haystack for term in INCLUSION_KEYWORDS)
    has_exclusion = any(term in haystack for term in EXCLUSION_KEYWORDS)
    return has_inclusion, has_exclusion


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
            raise RuntimeError(f"Missing LLM prefilter decision for bid {ref}")
        keep = decision.get("decision") == "YES" or float(decision.get("confidence", 0.0)) >= 0.4
        if keep:
            bid["Prefilter Confidence"] = round(float(decision.get("confidence", 0.0)), 3)
            prefiltered_full.append(bid)

    logger.info("Pipeline1 kept after broad prefilter: %d", len(prefiltered_full))

    combined_candidates = _merge_candidates(prefiltered_full, keyword_new)
    combined_candidates = [b for b in _dedupe_by_ref(combined_candidates) if not tracker.is_processed(str(b.get("Reference No.", "")).strip())]
    logger.info("Merged candidates for final classification: %d", len(combined_candidates))

    final_map = llm.final_classify(combined_candidates) if combined_candidates else {}
    final_coverage = (len(final_map) / len(combined_candidates)) if combined_candidates else 1.0

    relevant: list[dict] = []
    doubtful: list[dict] = []
    rejected: list[dict] = []

    for bid in combined_candidates:
        ref = str(bid.get("Reference No.", "")).strip()
        final_vote = final_map.get(ref)
        if final_vote is None:
            raise RuntimeError(f"Missing final LLM classification for bid {ref}")

        category = str(final_vote.get("category", "REJECTED")).upper()
        confidence = round(float(final_vote.get("confidence", 0.0)), 3)
        reason = str(final_vote.get("reason", ""))
        has_inclusion, has_exclusion = _keyword_flags(bid)

        if has_exclusion and category == "EXTRACTED":
            category = "DOUBTFUL"
            reason = f"{reason} | Exclusion keyword detected; moved to DOUBTFUL.".strip(" |")
        elif has_exclusion and category == "REJECTED" and confidence >= 0.75:
            category = "DOUBTFUL"
            reason = f"{reason} | Exclusion keyword detected with strong cyber confidence; moved to DOUBTFUL.".strip(" |")
        elif has_inclusion and not has_exclusion and category != "EXTRACTED":
            category = "EXTRACTED"
            reason = f"{reason} | Inclusion keyword detected; promoted to EXTRACTED.".strip(" |")

        bid["Final Category"] = category
        bid["LLM Confidence"] = confidence
        bid["LLM Reason"] = reason
        bid["Inclusion Match"] = has_inclusion
        bid["Exclusion Match"] = has_exclusion

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
    tracker.save()

    logger.info("LLM prefilter coverage: %.2f", prefilter_coverage)
    logger.info("LLM final coverage: %.2f", final_coverage)
    logger.info("Results -> relevant: %d, doubtful: %d, rejected: %d", len(relevant), len(doubtful), len(rejected))
    logger.info("Saved -> main: %d, doubtful: %d", added_main, added_doubtful)

    return {
        "new": len(combined_candidates),
        "relevant": len(relevant),
        "doubtful": len(doubtful),
        "rejected": len(rejected),
        "saved_main": added_main,
        "saved_doubtful": added_doubtful,
        "prefilter_kept": len(prefiltered_full),
        "merged_candidates": len(combined_candidates),
        "llm_prefilter_coverage": round(prefilter_coverage, 4),
        "llm_final_coverage": round(final_coverage, 4),
        "supabase_sync": db_sync_ok,
    }
