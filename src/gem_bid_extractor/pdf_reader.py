from __future__ import annotations

import logging
import re
from pathlib import Path

from pypdf import PdfReader

logger = logging.getLogger(__name__)
_WHITESPACE_RE = re.compile(r"\s+")


def extract_pdf_text(path: Path) -> str:
    if not path.exists():
        return ""
    try:
        reader = PdfReader(str(path))
    except Exception as exc:
        logger.warning("Failed to open PDF %s: %s", path, exc)
        return ""

    pages: list[str] = []
    for page in reader.pages:
        try:
            txt = page.extract_text() or ""
        except Exception:
            txt = ""
        txt = _WHITESPACE_RE.sub(" ", txt).strip()
        if txt:
            pages.append(txt)
    return "\n".join(pages).strip()
