from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from .settings import (
    DOUBTFUL_THRESHOLD_DEFAULT,
    FEEDBACK_FILE,
    RELEVANT_THRESHOLD_DEFAULT,
    THRESHOLDS_FILE,
)


@dataclass
class Thresholds:
    relevant: float
    doubtful: float


class ThresholdTuner:
    def __init__(self, thresholds_file: Path = THRESHOLDS_FILE, feedback_file: Path = FEEDBACK_FILE):
        self.thresholds_file = thresholds_file
        self.feedback_file = feedback_file

    def load(self) -> Thresholds:
        if not self.thresholds_file.exists():
            return Thresholds(RELEVANT_THRESHOLD_DEFAULT, DOUBTFUL_THRESHOLD_DEFAULT)
        data = json.loads(self.thresholds_file.read_text(encoding="utf-8"))
        return Thresholds(float(data.get("relevant", RELEVANT_THRESHOLD_DEFAULT)), float(data.get("doubtful", DOUBTFUL_THRESHOLD_DEFAULT)))

    def tune(self) -> Thresholds:
        t = self.load()
        if not self.feedback_file.exists():
            return t

        data = json.loads(self.feedback_file.read_text(encoding="utf-8"))
        fp = len(data.get("false_positives", []))
        fn = len(data.get("false_negatives", []))

        if fp >= fn + 2:
            t.relevant = min(85.0, t.relevant + 2.0)
        elif fn >= fp + 2:
            t.relevant = max(60.0, t.relevant - 2.0)

        t.doubtful = max(35.0, min(t.relevant - 10.0, t.relevant - 20.0))

        self.thresholds_file.write_text(
            json.dumps({"relevant": t.relevant, "doubtful": t.doubtful}, indent=2),
            encoding="utf-8",
        )
        return t
