from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone

from .claude import ClaudeSonnetClassifier
from .embeddings import EmbeddingEngine
from .excel_writer import ExcelWriter
from .feedback import FeedbackTracker
from .gem_client import GemScraper
from .settings import (
    DOUBTFUL_FILE,
    EXCEL_FILE,
    EXCLUDE_DOMAINS,
    KEYWORDS,
    LOOKBACK_DAYS,
)
from .storage import BidTracker
from .thresholds import ThresholdTuner

logger = logging.getLogger(__name__)


def _keyword_matches(text: str, keywords: list[str]) -> int:
    text = text.lower()
    count = 0
    for kw in keywords:
        k = kw.lower()
        if len(k) <= 3:
            if re.search(r"\b" + re.escape(k) + r"\b", text):
                count += 1
        elif k in text:
            count += 1
    return count


def _excluded(text: str) -> bool:
    low = text.lower()
    return any(token in low for token in EXCLUDE_DOMAINS)


def _score_bid(bid: dict, kw_matches: int, embedding_similarity: float, llm_vote: dict | None) -> tuple[float, float]:
    keyword_points = min(25.0, kw_matches * 5.0)
    emb_points = embedding_similarity * 100.0 * 0.55

    if llm_vote:
        conf = float(llm_vote.get("c", 0.5))
        llm_points = 30.0 * conf if llm_vote.get("v") == "y" else -30.0 * conf
    else:
        conf = min(0.95, max(0.2, embedding_similarity))
        llm_points = 0.0

    final = max(0.0, min(100.0, emb_points + keyword_points + llm_points))
    return final, conf


def run() -> dict:
    scraper = GemScraper()
    embeddings = EmbeddingEngine()
    claude = ClaudeSonnetClassifier()
    tracker = BidTracker()
    feedback = FeedbackTracker()
    tuner = ThresholdTuner()
    thresholds = tuner.tune()

    writer_main = ExcelWriter(EXCEL_FILE)
    writer_doubtful = ExcelWriter(DOUBTFUL_FILE)

    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)

    scraper.init_session()
    try:
        raw_bids = scraper.search_all(KEYWORDS, cutoff)
    finally:
        scraper.close()

    new_bids = [b for b in raw_bids if not tracker.is_processed(b.get("Reference No.", ""))]
    if not new_bids:
        return {
            "new": 0,
            "relevant": 0,
            "doubtful": 0,
            "rejected": 0,
            "thresholds": {"relevant": thresholds.relevant, "doubtful": thresholds.doubtful},
        }

    for bid in new_bids:
        text = EmbeddingEngine.text_for_bid(bid)
        bid["Keyword Matches"] = _keyword_matches(text, KEYWORDS)
        bid["Embedding Similarity"] = round(embeddings.similarity(text).similarity, 4)

    candidates = [b for b in new_bids if not _excluded(EmbeddingEngine.text_for_bid(b))]
    llm_map = claude.classify(candidates) if claude.enabled else {}

    relevant: list[dict] = []
    doubtful: list[dict] = []
    rejected: list[dict] = []

    for bid in new_bids:
        ref = bid.get("Reference No.", "")
        text = EmbeddingEngine.text_for_bid(bid)
        if _excluded(text):
            bid["Final Score"] = 0.0
            bid["Confidence"] = 0.99
            rejected.append(bid)
            continue

        score, conf = _score_bid(
            bid,
            int(bid.get("Keyword Matches", 0)),
            float(bid.get("Embedding Similarity", 0.0)),
            llm_map.get(ref),
        )
        bid["Final Score"] = round(score, 2)
        bid["Confidence"] = round(conf, 3)

        if score >= thresholds.relevant:
            relevant.append(bid)
        elif score >= thresholds.doubtful:
            doubtful.append(bid)
        else:
            rejected.append(bid)

    added_main = writer_main.save(relevant)
    added_doubtful = writer_doubtful.save(doubtful)

    for bid in relevant:
        tracker.mark(bid.get("Reference No.", ""), "relevant", bid.get("Final Score", 0), bid.get("Confidence", 0))
    for bid in doubtful:
        tracker.mark(bid.get("Reference No.", ""), "doubtful", bid.get("Final Score", 0), bid.get("Confidence", 0))
    for bid in rejected:
        tracker.mark(bid.get("Reference No.", ""), "rejected", bid.get("Final Score", 0), bid.get("Confidence", 0))
    tracker.save()

    feedback.log_run(relevant, doubtful, rejected)
    feedback.update_false_negative_watchlist(rejected, doubtful)
    stats = feedback.stats()

    logger.info("Thresholds used -> relevant: %.1f, doubtful: %.1f", thresholds.relevant, thresholds.doubtful)
    logger.info("Results -> relevant: %d, doubtful: %d, rejected: %d", len(relevant), len(doubtful), len(rejected))
    logger.info("Saved -> main: %d, doubtful: %d", added_main, added_doubtful)
    logger.info("Feedback stats -> false_positives: %d, false_negatives: %d", stats["false_positives"], stats["false_negatives"])

    return {
        "new": len(new_bids),
        "relevant": len(relevant),
        "doubtful": len(doubtful),
        "rejected": len(rejected),
        "saved_main": added_main,
        "saved_doubtful": added_doubtful,
        "thresholds": {"relevant": thresholds.relevant, "doubtful": thresholds.doubtful},
        "feedback": stats,
    }
