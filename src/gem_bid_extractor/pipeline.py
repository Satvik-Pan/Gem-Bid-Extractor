from __future__ import annotations

import logging
import re

from .anthropic_llm import AnthropicClaudeClassifier
from .excel_writer import ExcelWriter
from .gem_client import GemScraper
from .settings import (
    DOUBTFUL_FILE,
    EXCLUSION_KEYWORDS,
    EXCLUSION_REJECT_IF_CONFIDENCE_BELOW,
    EXCEL_FILE,
    INCLUSION_KEYWORDS,
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
    """One or more regexes per keyword (e.g. 'fire wall' also tries 'firewall')."""
    tnorm = " ".join(term.strip().lower().split())
    if not tnorm:
        return [re.compile(r"$^")]
    pats: list[re.Pattern[str]] = [_flexible_phrase_pattern(tnorm)]
    parts = _tokenize_keyword_phrase(tnorm)
    if len(parts) == 2:
        joined = "".join(parts)
        if len(joined) <= 32:
            pats.append(_single_token_pattern(joined))
    return pats


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


def _build_keyword_haystack(bid: dict) -> str:
    parts = [
        str(bid.get("Name", "")),
        str(bid.get("Description", "")),
        str(bid.get("Category", "")),
        str(bid.get("Department", "")),
        str(bid.get("PDF Text", "")),
    ]
    raw = " | ".join(parts)
    s = raw.lower()
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


def _alnum_glue(haystack: str) -> str:
    """Letters/digits only, lowercased — catches PDF lines with no spaces (e.g. ...NGFW...firewall...)."""
    return "".join(ch.lower() for ch in haystack if ch.isalnum())


def _glued_substring_hits(
    pattern_set: list[tuple[str, list[re.Pattern[str]]]], glue: str, already: list[str]
) -> list[str]:
    seen = set(already)
    extra: list[str] = []
    for label, _ in pattern_set:
        if label in seen:
            continue
        parts = _tokenize_keyword_phrase(label)
        if not parts:
            continue
        compact = "".join(parts)
        if len(compact) < 3:
            continue
        if compact in glue:
            extra.append(label)
            seen.add(label)
    return extra


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
    glue = _alnum_glue(haystack)
    inclusion_hits = _match_keyword_set(_INCLUSION_PATTERN_SET, haystack)
    inclusion_hits.extend(_glued_substring_hits(_INCLUSION_PATTERN_SET, glue, inclusion_hits))
    exclusion_hits = _match_keyword_set(_EXCLUSION_PATTERN_SET, haystack)
    exclusion_hits.extend(_glued_substring_hits(_EXCLUSION_PATTERN_SET, glue, exclusion_hits))
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

    logger.info("Pipeline 1/5: Fetching GEM bids (first 5 pages from all-bids)")
    scraper.init_session()
    try:
        # Pipeline 1: fetch bids from first 5 pages only.
        pipeline1_bids = scraper.search_full()
    finally:
        scraper.close()

    pipeline1_new = [b for b in _dedupe_by_ref(pipeline1_bids) if not tracker.is_processed(str(b.get("Reference No.", "")).strip())]
    pdf_stats = scraper.enrich_with_pdf_text(pipeline1_new)
    pipeline1_pdf_ready = [b for b in pipeline1_new if str(b.get("PDF Text", "")).strip()]
    logger.info("Pipeline 1/5 complete: %d new bids", len(pipeline1_new))
    logger.info(
        "Pipeline 1/5 PDF status -> downloaded: %d, failed: %d, skipped: %d, ready: %d",
        pdf_stats["downloaded"],
        pdf_stats["failed"],
        pdf_stats["skipped"],
        len(pipeline1_pdf_ready),
    )

    # Pipeline 2: independent LLM relevance pass over Pipeline 1 output.
    logger.info("Pipeline 2/5: Running independent LLM relevance over Pipeline 1 output")
    relevance_map = llm.prefilter(pipeline1_pdf_ready) if pipeline1_pdf_ready else {}
    pipeline2_llm: list[dict] = []
    for bid in pipeline1_pdf_ready:
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
    for bid in pipeline1_pdf_ready:
        has_inclusion, _, inclusion_hits, _ = _keyword_flags(bid)
        if has_inclusion:
            bid["Inclusion Hits"] = ", ".join(inclusion_hits[:6])
            pipeline3_keyword.append(bid)
    logger.info("Pipeline 3/5 complete: %d selected bids", len(pipeline3_keyword))
    if not pipeline3_keyword and pipeline1_pdf_ready:
        sample = pipeline1_pdf_ready[0]
        plen = len(str(sample.get("PDF Text", "")))
        logger.warning(
            "Pipeline 3 matched 0 bids; check PDF text (sample ref %s PDF chars=%d).",
            str(sample.get("Reference No.", ""))[:40],
            plen,
        )

    # Pipeline 4: merge Pipeline 2 + Pipeline 3 and dedupe only (no exclusion or confidence filtering here).
    logger.info("Pipeline 4/5: Combining Pipeline 2 and Pipeline 3, then deduping")
    pipeline4_candidates = _merge_candidates(pipeline2_llm, pipeline3_keyword)
    pipeline4_ready = _dedupe_by_ref(pipeline4_candidates)
    logger.info("Pipeline 4/5 complete: %d merged+deduped bids", len(pipeline4_ready))

    # Pipeline 5: LLM split + inclusion forces EXTRACTED; exclusion without inclusion → doubtful or reject.
    logger.info("Pipeline 5/5: Running final LLM categorization (EXTRACTED/DOUBTFUL)")
    final_map = llm.final_classify(pipeline4_ready) if pipeline4_ready else {}
    final_coverage = (len(final_map) / len(pipeline4_ready)) if pipeline4_ready else 1.0

    relevant: list[dict] = []
    doubtful: list[dict] = []
    ignored: list[dict] = []
    final_fallback_count = 0
    exclusion_to_doubtful = 0
    exclusion_rejected_low_confidence = 0

    selected_refs = {str(b.get("Reference No.", "")).strip() for b in pipeline4_ready}

    for bid in pipeline4_ready:
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

        if has_inclusion:
            category = "EXTRACTED"
            if has_exclusion:
                reason = (
                    f"{reason} | Inclusion keyword match → EXTRACTED (dashboard); "
                    f"exclusion terms also present — review notes in Exclusion Hits."
                ).strip(" |")
            else:
                reason = f"{reason} | Inclusion keyword match → EXTRACTED.".strip(" |")
        elif has_exclusion:
            if confidence < EXCLUSION_REJECT_IF_CONFIDENCE_BELOW:
                exclusion_rejected_low_confidence += 1
                ignored.append(bid)
                continue
            exclusion_to_doubtful += 1
            category = "DOUBTFUL"
            reason = (
                f"{reason} | Exclusion keyword(s) with model confidence "
                f"{confidence:.2f} ≥ {EXCLUSION_REJECT_IF_CONFIDENCE_BELOW:.2f} → DOUBTFUL for review."
            ).strip(" |")
        else:
            if category not in {"EXTRACTED", "DOUBTFUL"}:
                category = "DOUBTFUL"
                reason = f"{reason} | Final class normalized to DOUBTFUL.".strip(" |")

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
        "Pipeline 5 notes -> exclusion_to_doubtful: %d, exclusion_rejected_low_confidence: %d",
        exclusion_to_doubtful,
        exclusion_rejected_low_confidence,
    )
    logger.info("Pipeline 5/5 complete -> extracted: %d, doubtful: %d", len(relevant), len(doubtful))
    logger.info("Saved -> main: %d, doubtful: %d", added_main, added_doubtful)

    return {
        "new": len(pipeline4_candidates),
        "pipeline1_count": len(pipeline1_new),
        "pipeline1_pdf_ready": len(pipeline1_pdf_ready),
        "pdf_downloaded": pdf_stats["downloaded"],
        "pdf_failed": pdf_stats["failed"],
        "pdf_skipped": pdf_stats["skipped"],
        "pipeline2_count": len(pipeline2_llm),
        "pipeline3_count": len(pipeline3_keyword),
        "pipeline4_merged": len(pipeline4_candidates),
        "pipeline4_ready": len(pipeline4_ready),
        "relevant": len(relevant),
        "doubtful": len(doubtful),
        "saved_main": added_main,
        "saved_doubtful": added_doubtful,
        "llm_final_coverage": round(final_coverage, 4),
        "llm_final_fallback_count": final_fallback_count,
        "exclusion_to_doubtful": exclusion_to_doubtful,
        "exclusion_rejected_low_confidence": exclusion_rejected_low_confidence,
        "supabase_sync": db_sync_ok,
    }
