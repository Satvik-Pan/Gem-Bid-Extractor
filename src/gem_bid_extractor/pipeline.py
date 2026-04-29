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
        bid["Pipeline Source"] = "pipeline2_llm"
        merged[ref] = bid

    for bid in keyword_bids:
        ref = str(bid.get("Reference No.", "")).strip()
        if not ref:
            continue
        if ref in merged:
            src = str(merged[ref].get("Pipeline Source", "pipeline2_llm"))
            merged[ref]["Pipeline Source"] = "pipeline2+pipeline3" if "pipeline3_keyword" not in src else src
            continue
        bid["Pipeline Source"] = "pipeline3_keyword"
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

    logger.info("Pipeline 1/5: Fetching GEM bids (last 3 days, max 5 pages)")
    scraper.init_session()
    try:
        # Pipeline 1: fetch bids from last 3 days, max 5 pages.
        pipeline1_bids = scraper.search_full(cutoff)
    finally:
        scraper.close()

    pipeline1_new = [b for b in _dedupe_by_ref(pipeline1_bids) if not tracker.is_processed(str(b.get("Reference No.", "")).strip())]
    logger.info("Pipeline 1/5 complete: %d new bids", len(pipeline1_new))

    # Pipeline 2: independent LLM relevance pass over Pipeline 1 output.
    logger.info("Pipeline 2/5: Running independent LLM relevance over Pipeline 1 output")
    relevance_map = llm.prefilter(pipeline1_new) if pipeline1_new else {}
    pipeline2_llm: list[dict] = []
    for bid in pipeline1_new:
        ref = str(bid.get("Reference No.", "")).strip()
        decision = relevance_map.get(ref)
        if decision is None:
            decision = {"decision": "YES", "confidence": 0.5}
            logger.warning("Pipeline2 missing LLM decision for %s; defaulting to YES", ref)
        keep = decision.get("decision") == "YES" or float(decision.get("confidence", 0.0)) >= 0.4
        if keep:
            bid["Pipeline2 LLM Confidence"] = round(float(decision.get("confidence", 0.0)), 3)
            pipeline2_llm.append(bid)
    logger.info("Pipeline 2/5 complete: %d selected bids", len(pipeline2_llm))

    # Pipeline 3: independent keyword extraction over Pipeline 1 output only.
    logger.info("Pipeline 3/5: Running independent keyword extraction over Pipeline 1 output")
    pipeline3_keyword: list[dict] = []
    for bid in pipeline1_new:
        has_inclusion, _, inclusion_hits, _ = _keyword_flags(bid)
        if has_inclusion:
            bid["Inclusion Hits"] = ", ".join(inclusion_hits[:6])
            pipeline3_keyword.append(bid)
    logger.info("Pipeline 3/5 complete: %d selected bids", len(pipeline3_keyword))

    # Pipeline 4: combine + dedupe pipeline2 and pipeline3 results.
    logger.info("Pipeline 4/5: Combining Pipeline 2 and Pipeline 3, then deduping")
    pipeline4_candidates = _merge_candidates(pipeline2_llm, pipeline3_keyword)
    pipeline4_candidates = _dedupe_by_ref(pipeline4_candidates)
    logger.info("Pipeline 4/5 complete: %d merged+deduped bids", len(pipeline4_candidates))

    # Pipeline 5: final LLM categorization with in-stage exclusion handling.
    logger.info("Pipeline 5/5: Running final LLM categorization (EXTRACTED/DOUBTFUL)")
    final_map = llm.final_classify(pipeline4_candidates) if pipeline4_candidates else {}
    final_coverage = (len(final_map) / len(pipeline4_candidates)) if pipeline4_candidates else 1.0

    relevant: list[dict] = []
    doubtful: list[dict] = []
    ignored: list[dict] = []
    final_fallback_count = 0

    selected_refs = {str(b.get("Reference No.", "")).strip() for b in pipeline4_candidates}

    for bid in pipeline4_candidates:
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

        category = str(final_vote.get("category", "DOUBTFUL")).upper()
        confidence = round(float(final_vote.get("confidence", 0.0)), 3)
        reason = str(final_vote.get("reason", ""))
        has_inclusion, has_exclusion, inclusion_hits, exclusion_hits = _keyword_flags(bid)

        # Exclusion is intentionally handled inside Pipeline 5.
        if has_exclusion:
            category = "DOUBTFUL"
            reason = f"{reason} | Exclusion keyword match forced DOUBTFUL.".strip(" |")
        elif has_inclusion and category != "EXTRACTED":
            category = "EXTRACTED"
            reason = f"{reason} | Inclusion keyword detected; promoted to EXTRACTED.".strip(" |")

        if category not in {"EXTRACTED", "DOUBTFUL"}:
            category = "DOUBTFUL"
            reason = f"{reason} | Final class normalized to DOUBTFUL.".strip(" |")

        bid["Final Category"] = category
        bid["LLM Confidence"] = confidence
        bid["LLM Reason"] = reason
        bid["Inclusion Match"] = has_inclusion
        bid["Exclusion Match"] = has_exclusion
        bid["Inclusion Hits"] = ", ".join(inclusion_hits[:6])
        bid["Exclusion Hits"] = ", ".join(exclusion_hits[:6])

        if category == "EXTRACTED":
            relevant.append(bid)
        else:
            doubtful.append(bid)

    # Mark non-selected Pipeline1 bids as ignored to avoid infinite reprocessing.
    for bid in pipeline1_new:
        ref = str(bid.get("Reference No.", "")).strip()
        if ref and ref not in selected_refs:
            ignored.append(bid)

    added_main = writer_main.save(relevant)
    added_doubtful = writer_doubtful.save(doubtful)

    db_sync_ok = db.sync_with_retry([*relevant, *doubtful])
    if db_sync_ok:
        logger.info("Supabase sync: enabled")
    else:
        logger.warning("Supabase sync: queued for retry (%s)", db.last_error or "connectivity/config issue")

    for bid in relevant:
        tracker.mark(bid.get("Reference No.", ""), "extracted", 100, bid.get("LLM Confidence", 0))
    for bid in doubtful:
        tracker.mark(bid.get("Reference No.", ""), "doubtful", 60, bid.get("LLM Confidence", 0))
    for bid in ignored:
        tracker.mark(bid.get("Reference No.", ""), "ignored", 0, 0)
    tracker.save()

    logger.info("LLM final coverage: %.2f", final_coverage)
    logger.info("LLM final fallback count: %d", final_fallback_count)
    logger.info("Pipeline 5/5 complete -> extracted: %d, doubtful: %d", len(relevant), len(doubtful))
    logger.info("Saved -> main: %d, doubtful: %d", added_main, added_doubtful)

    return {
        "new": len(pipeline4_candidates),
        "pipeline1_count": len(pipeline1_new),
        "pipeline2_count": len(pipeline2_llm),
        "pipeline3_count": len(pipeline3_keyword),
        "pipeline4_merged": len(pipeline4_candidates),
        "relevant": len(relevant),
        "doubtful": len(doubtful),
        "saved_main": added_main,
        "saved_doubtful": added_doubtful,
        "llm_final_coverage": round(final_coverage, 4),
        "llm_final_fallback_count": final_fallback_count,
        "supabase_sync": db_sync_ok,
    }
