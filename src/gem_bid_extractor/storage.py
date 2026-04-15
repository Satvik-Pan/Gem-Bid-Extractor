from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from .settings import PROCESSED_FILE


class BidTracker:
    def __init__(self, filepath: Path = PROCESSED_FILE):
        self.filepath = filepath
        self.data = self._load()

    def _load(self) -> dict:
        if not self.filepath.exists():
            return {}
        try:
            return json.loads(self.filepath.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}

    def is_processed(self, ref: str) -> bool:
        return ref in self.data

    def mark(self, ref: str, status: str, score: float, confidence: float):
        self.data[ref] = {
            "status": status,
            "score": round(float(score), 2),
            "confidence": round(float(confidence), 3),
            "ts": datetime.now(timezone.utc).isoformat(),
        }

    def save(self):
        self.filepath.write_text(json.dumps(self.data, indent=2), encoding="utf-8")
