from __future__ import annotations

import logging
import re
import unicodedata

from .anthropic_llm import AnthropicClaudeClassifier
from .excel_writer import ExcelWriter
from .gem_client import GemScraper
from .settings import (
    DOUBTFUL_FILE,
    EXCLUSION_KEYWORDS,
    EXCEL_FILE,
    INCLUSION_KEYWORDS,
    P2_DOUBTFUL_MIN_CONFIDENCE,
    P4_PROMOTE_EXTRACTED_MIN_CONFIDENCE,
    P4_REJECT_BELOW_CONFIDENCE,
)
from .storage import BidTracker
from .supabase_store import SupabaseStore

logger = logging.getLogger(__name__)
_ILLEGAL_EXCEL_CHARS = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")


def _tokenize_keyword_phrase(term: str) -> list[str]:
    return [p for p in re.split(r"[^a-z0-9]+", term.strip().lower()) if p]


def _single_token_pattern(token: str) -> re.Pattern[str]:
    """Match a token as a whole word (handles ASCII alnum boundaries; PDF noise tolerant)."""
    t = re.escape(token)
    return re.compile(rf"(?<![a-z0-9]){t}(?![a-z0-9])", re.IGNORECASE)


def _flexible_phrase_pattern(term: str) -> re.Pattern[str]:
    """
    Match multi-word phrases when words are separated by spaces, hyphens, slashes, commas, etc.
    Avoids the old \\b...\\s+...\\b rule that missed 'next-generation-firewall' and 'firewall' vs 'fire wall'.
    """
    parts = _tokenize_keyword_phrase(term)
    if not parts:
        return re.compile(r"$^")
    if len(parts) == 1:
        return _single_token_pattern(parts[0])
    sep = r"[^\w]+"  # any run of non-word chars between word tokens
    body = sep.join(re.escape(p) for p in parts)
    return re.compile(rf"(?<![a-z0-9]){body}(?![a-z0-9])", re.IGNORECASE)


def _patterns_for_keyword_term(term: str) -> list[re.Pattern[str]]:
    """Strict whole-phrase keyword matching."""
    tnorm = " ".join(term.strip().lower().split())
    if not tnorm:
        return [re.compile(r"$^")]
    return [_flexible_phrase_pattern(tnorm)]


def _compile_keyword_sets(terms: list[str]) -> list[tuple[str, list[re.Pattern[str]]]]:
    seen: set[str] = set()
    out: list[tuple[str, list[re.Pattern[str]]]] = []
    for raw in terms:
        label = " ".join(raw.strip().split())
        if not label:
            continue
        key = label.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append((label, _patterns_for_keyword_term(label)))
    return out


_INCLUSION_PATTERN_SET = _compile_keyword_sets(INCLUSION_KEYWORDS)
_EXCLUSION_PATTERN_SET = _compile_keyword_sets(EXCLUSION_KEYWORDS)


def _refresh_keyword_patterns() -> None:
    """Recompile from settings so CSV/inclusion list edits apply on each run."""
    global _INCLUSION_PATTERN_SET, _EXCLUSION_PATTERN_SET
    from .settings import EXCLUSION_KEYWORDS as _exc
    from .settings import INCLUSION_KEYWORDS as _inc

    _INCLUSION_PATTERN_SET = _compile_keyword_sets(list(_inc))
    _EXCLUSION_PATTERN_SET = _compile_keyword_sets(list(_exc))


def _unicode_normalize_text(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.replace("\u00ad", "").replace("\u200b", "").replace("\u200c", "")


def _build_keyword_haystack(bid: dict) -> str:
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


def _match_keyword_set(pattern_set: list[tuple[str, list[re.Pattern[str]]]], haystack: str) -> list[str]:
    hits: list[str] = []
    for label, pats in pattern_set:
        if any(p.search(haystack) for p in pats):
            hits.append(label)
    return hits


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
    haystack = _build_keyword_haystack(bid)
    inclusion_hits = _match_keyword_set(_INCLUSION_PATTERN_SET, haystack)
    exclusion_hits = _match_keyword_set(_EXCLUSION_PATTERN_SET, haystack)
    has_inclusion = bool(inclusion_hits)
    has_exclusion = bool(exclusion_hits)
    return has_inclusion, has_exclusion, inclusion_hits, exclusion_hits


def _build_reason(base_reason: str, inclusion_hits: list[str], exclusion_hits: list[str]) -> str:
    parts: list[str] = []
    if base_reason.strip():
        parts.append(base_reason.strip())
    if inclusion_hits:
        parts.append(f"Inclusion keywords: {', '.join(inclusion_hits[:6])}.")
    if exclusion_hits:
        parts.append(f"Exclusion keywords: {', '.join(exclusion_hits[:6])}.")
    return " | ".join(parts)[:500]


def _sanitize_for_excel(value: object) -> object:
    if not isinstance(value, str):
        return value
    return _ILLEGAL_EXCEL_CHARS.sub("", value)


def _sanitize_bid_strings(bid: dict) -> dict:
    for key, val in list(bid.items()):
        bid[key] = _sanitize_for_excel(val)
    return bid


def run() -> dict:
    _refresh_keyword_patterns()

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

    logger.info("Pipeline 1/4: Fetching exactly 50 bids from first 5 pages (all-bids)")
    scraper.init_session()
    try:
        pipeline1_bids = scraper.search_full(max_pages=5, target_rows=50)
    finally:
        scraper.close()

    # Keep raw order/count from first 5 pages; only skip already processed refs.
    pipeline1_new = [b for b in pipeline1_bids if not tracker.is_processed(str(b.get("Reference No.", "")).strip())]
    pdf_stats = scraper.enrich_with_pdf_text(pipeline1_new)
    pipeline1_pdf_ready = [b for b in pipeline1_new if str(b.get("PDF Text", "")).strip()]
    logger.info("Pipeline 1/4 complete: %d new bids", len(pipeline1_new))
    logger.info(
        "Pipeline 1/4 PDF status -> downloaded: %d, failed: %d, skipped: %d, ready: %d",
        pdf_stats["downloaded"],
        pdf_stats["failed"],
        pdf_stats["skipped"],
        len(pipeline1_pdf_ready),
    )

    # Pipeline 2: LLM relevance over all PDF-backed bids.
    # Routing:
    # - inclusion keyword hit => EXTRACTED immediately
    # - else YES / high-confidence => DOUBTFUL
    # - else => REJECT
    logger.info("Pipeline 2/4: Running Sonnet over all Pipeline1 PDF-backed bids")
    relevance_map = llm.prefilter(pipeline1_pdf_ready) if pipeline1_pdf_ready else {}
    pipeline2_extracted: list[dict] = []
    pipeline2_doubtful: list[dict] = []
    pipeline2_rejected = 0
    for bid in pipeline1_pdf_ready:
        ref = str(bid.get("Reference No.", "")).strip()
        decision = relevance_map.get(ref)
        if decision is None:
            decision = {"decision": "NO", "confidence": 0.15}
            logger.warning("Pipeline2 missing LLM decision for %s; defaulting to NO", ref)
        conf = round(float(decision.get("confidence", 0.0) or 0.0), 3)
        has_inclusion, _, inclusion_hits, _ = _keyword_flags(bid)
        if has_inclusion:
            bid["Final Category"] = "EXTRACTED"
            bid["Pipeline Source"] = "pipeline2_inclusion_forced"
            bid["LLM Confidence"] = conf
            bid["LLM Reason"] = _build_reason("Pipeline2 inclusion keyword hit -> EXTRACTED.", inclusion_hits, [])
            bid["Inclusion Match"] = True
            bid["Exclusion Match"] = False
            bid["Inclusion Hits"] = ", ".join(inclusion_hits[:6])
            bid["Exclusion Hits"] = ""
            pipeline2_extracted.append(_sanitize_bid_strings(bid))
            continue
        if str(decision.get("decision", "")).upper() == "YES" or conf >= P2_DOUBTFUL_MIN_CONFIDENCE:
            bid["Pipeline2 LLM Confidence"] = conf
            pipeline2_doubtful.append(bid)
        else:
            pipeline2_rejected += 1
    logger.info(
        "Pipeline 2/4 complete -> extracted: %d, doubtful: %d, rejected: %d",
        len(pipeline2_extracted),
        len(pipeline2_doubtful),
        pipeline2_rejected,
    )

    # Pipeline 3: exclusion keyword rejection over doubtful set from Pipeline 2.
    logger.info("Pipeline 3/4: Applying strict exclusion keyword rejection over doubtful bids")
    pipeline3_rejected = 0
    pipeline3_doubtful: list[dict] = []
    for bid in pipeline2_doubtful:
        _, has_exclusion, _, _ = _keyword_flags(bid)
        if has_exclusion:
            pipeline3_rejected += 1
            continue
        pipeline3_doubtful.append(bid)
    logger.info(
        "Pipeline 3/4 complete -> rejected_by_exclusion: %d, doubtful_to_pipeline4: %d",
        pipeline3_rejected,
        len(pipeline3_doubtful),
    )

    # Pipeline 4: Sonnet final review on remaining doubtful.
    logger.info("Pipeline 4/4: Sonnet final review on remaining doubtful bids")
    final_map = llm.final_classify(pipeline3_doubtful) if pipeline3_doubtful else {}
    final_coverage = (len(final_map) / len(pipeline3_doubtful)) if pipeline3_doubtful else 1.0

    relevant: list[dict] = list(pipeline2_extracted)
    doubtful: list[dict] = []
    ignored: list[dict] = []
    final_fallback_count = 0
    pipeline4_promoted = 0
    pipeline4_rejected = 0

    selected_refs = {str(b.get("Reference No.", "")).strip() for b in [*pipeline2_extracted, *pipeline3_doubtful]}

    for bid in pipeline3_doubtful:
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

        if category not in {"EXTRACTED", "DOUBTFUL"}:
            category = "DOUBTFUL"
            reason = f"{reason} | Final class normalized to DOUBTFUL.".strip(" |")

        # Safety: if exclusion appears here, reject regardless.
        if has_exclusion:
            pipeline4_rejected += 1
            ignored.append(bid)
            continue
        if category == "EXTRACTED":
            if confidence >= P4_PROMOTE_EXTRACTED_MIN_CONFIDENCE or has_inclusion:
                pipeline4_promoted += 1
            else:
                category = "DOUBTFUL"
                reason = f"{reason} | EXTRACTED confidence below promote threshold; kept as DOUBTFUL.".strip(" |")
        elif confidence < P4_REJECT_BELOW_CONFIDENCE and not has_inclusion:
            pipeline4_rejected += 1
            ignored.append(bid)
            continue

        reason = _build_reason(reason, inclusion_hits, exclusion_hits)

        bid["Final Category"] = category
        bid["LLM Confidence"] = confidence
        bid["LLM Reason"] = reason
        bid["Inclusion Match"] = has_inclusion
        bid["Exclusion Match"] = has_exclusion
        bid["Inclusion Hits"] = ", ".join(inclusion_hits[:6])
        bid["Exclusion Hits"] = ", ".join(exclusion_hits[:6])

        if category == "EXTRACTED":
            relevant.append(_sanitize_bid_strings(bid))
        else:
            doubtful.append(_sanitize_bid_strings(bid))

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
    logger.info(
        "Pipeline 4 notes -> promoted_to_extracted: %d, rejected: %d",
        pipeline4_promoted,
        pipeline4_rejected,
    )
    logger.info("Pipeline 4/4 complete -> extracted: %d, doubtful: %d", len(relevant), len(doubtful))
    logger.info("Saved -> main: %d, doubtful: %d", added_main, added_doubtful)

    return {
        "new": len(pipeline1_new),
        "pipeline1_count": len(pipeline1_new),
        "pipeline1_pdf_ready": len(pipeline1_pdf_ready),
        "pdf_downloaded": pdf_stats["downloaded"],
        "pdf_failed": pdf_stats["failed"],
        "pdf_skipped": pdf_stats["skipped"],
        "pipeline2_extracted": len(pipeline2_extracted),
        "pipeline3_doubtful": len(pipeline3_doubtful),
        "pipeline2_rejected": pipeline2_rejected,
        "pipeline3_rejected": pipeline3_rejected,
        "pipeline4_reviewed": len(pipeline3_doubtful),
        "pipeline4_promoted": pipeline4_promoted,
        "pipeline4_rejected": pipeline4_rejected,
        "relevant": len(relevant),
        "doubtful": len(doubtful),
        "saved_main": added_main,
        "saved_doubtful": added_doubtful,
        "llm_final_coverage": round(final_coverage, 4),
        "llm_final_fallback_count": final_fallback_count,
        "supabase_sync": db_sync_ok,
    }
