from __future__ import annotations

import logging
import re
from pathlib import Path

import pypdfium2 as pdfium
import pytesseract
from pypdf import PdfReader

from .settings import OCR_LANG, OCR_TIMEOUT_SECONDS, TESSERACT_CMD

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


def _ocr_image_file(path: Path) -> str:
    try:
        txt = pytesseract.image_to_string(str(path), lang=OCR_LANG, timeout=OCR_TIMEOUT_SECONDS)
    except Exception as exc:
        logger.warning("OCR image extraction failed for %s: %s", path, exc)
        return ""
    txt = _WHITESPACE_RE.sub(" ", txt or "").strip()
    return txt


def _ocr_pdf_file(path: Path) -> str:
    texts: list[str] = []
    try:
        doc = pdfium.PdfDocument(str(path))
    except Exception as exc:
        logger.warning("OCR PDF open failed for %s: %s", path, exc)
        return ""
    try:
        for i in range(len(doc)):
            try:
                page = doc[i]
                bitmap = page.render(scale=2.0)
                pil_image = bitmap.to_pil()
                txt = pytesseract.image_to_string(pil_image, lang=OCR_LANG, timeout=OCR_TIMEOUT_SECONDS)
                txt = _WHITESPACE_RE.sub(" ", txt or "").strip()
                if txt:
                    texts.append(txt)
            except Exception:
                continue
    finally:
        doc.close()
    return "\n".join(texts).strip()


def extract_pdf_text_with_ocr(path: Path) -> tuple[str, str]:
    """Return extracted text and source: native|ocr|none."""
    pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD
    native = extract_pdf_text(path)
    if native:
        return native, "native"

    # Some GEM downloads are image payloads with .pdf extension.
    try:
        with path.open("rb") as fh:
            sig = fh.read(8)
    except OSError:
        sig = b""
    if sig.startswith(b"\xff\xd8\xff") or sig.startswith(b"\x89PNG"):
        ocr_txt = _ocr_image_file(path)
    else:
        ocr_txt = _ocr_pdf_file(path)
    if ocr_txt:
        return ocr_txt, "ocr"
    return "", "none"
