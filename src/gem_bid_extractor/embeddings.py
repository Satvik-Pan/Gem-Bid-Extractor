from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from sentence_transformers import SentenceTransformer, util

from .settings import EMBEDDING_MODEL, EMBEDDING_REF_TEXTS


@dataclass
class EmbeddingResult:
    similarity: float


class EmbeddingEngine:
    def __init__(self):
        self.model = SentenceTransformer(EMBEDDING_MODEL)
        self.ref_vectors = self.model.encode(EMBEDDING_REF_TEXTS, convert_to_tensor=True)

    def similarity(self, text: str) -> EmbeddingResult:
        vec = self.model.encode(text, convert_to_tensor=True)
        sims = util.pytorch_cos_sim(vec, self.ref_vectors)[0]
        max_sim = float(sims.max().item()) if sims.numel() else 0.0
        return EmbeddingResult(similarity=max_sim)

    @staticmethod
    def text_for_bid(bid: dict) -> str:
        return " ".join(
            [
                str(bid.get("Category", "")),
                str(bid.get("Name", "")),
                str(bid.get("Description", "")),
                str(bid.get("Department", "")),
            ]
        ).strip()
