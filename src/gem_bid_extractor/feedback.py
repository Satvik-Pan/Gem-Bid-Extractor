from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from .settings import FEEDBACK_FILE, WATCHLIST_EMBEDDING_MIN, WATCHLIST_FILE, WATCHLIST_SCORE_MIN


class FeedbackTracker:
    def __init__(self, feedback_file: Path = FEEDBACK_FILE, watchlist_file: Path = WATCHLIST_FILE):
        self.feedback_file = feedback_file
        self.watchlist_file = watchlist_file

    def _load_json(self, path: Path, default: dict) -> dict:
        if not path.exists():
            return default
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return default

    def log_run(self, relevant: list[dict], doubtful: list[dict], rejected: list[dict]):
        data = self._load_json(self.feedback_file, {"runs": [], "false_positives": [], "false_negatives": []})
        data["runs"].append(
            {
                "ts": datetime.now(timezone.utc).isoformat(),
                "relevant": len(relevant),
                "doubtful": len(doubtful),
                "rejected": len(rejected),
            }
        )
        self.feedback_file.write_text(json.dumps(data, indent=2), encoding="utf-8")

    def update_false_negative_watchlist(self, rejected: list[dict], doubtful: list[dict]):
        candidates = []
        for bid in rejected + doubtful:
            score = float(bid.get("Final Score", 0))
            emb = float(bid.get("Embedding Similarity", 0))
            kw = int(bid.get("Keyword Matches", 0))
            if score >= WATCHLIST_SCORE_MIN and (emb >= WATCHLIST_EMBEDDING_MIN or kw >= 1):
                candidates.append(
                    {
                        "ref": bid.get("Reference No.", ""),
                        "name": bid.get("Name", ""),
                        "score": score,
                        "embedding": emb,
                        "keywords": kw,
                        "ts": datetime.now(timezone.utc).isoformat(),
                    }
                )

        payload = {"items": sorted(candidates, key=lambda x: x["score"], reverse=True)[:200]}
        self.watchlist_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def stats(self) -> dict:
        data = self._load_json(self.feedback_file, {"false_positives": [], "false_negatives": []})
        return {
            "false_positives": len(data.get("false_positives", [])),
            "false_negatives": len(data.get("false_negatives", [])),
        }
