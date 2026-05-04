import argparse
import json
import logging
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from gem_bid_extractor.pipeline import run
from gem_bid_extractor.settings import (
    DOUBTFUL_FILE,
    EXCEL_FILE,
    FEEDBACK_FILE,
    LOG_FILE,
    PDF_CACHE_DIR,
    PROCESSED_FILE,
    RUN_STATUS_FILE,
    SYNC_QUEUE_FILE,
    THRESHOLDS_FILE,
    WATCHLIST_FILE,
)
from gem_bid_extractor.supabase_store import SupabaseStore


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
        ],
    )


def full_reset(logger: logging.Logger) -> None:
    """Remove local run state, cached PDFs, Excel outputs, sync queue, and truncate Supabase worklist."""
    for path in (
        PROCESSED_FILE,
        SYNC_QUEUE_FILE,
        RUN_STATUS_FILE,
        FEEDBACK_FILE,
        THRESHOLDS_FILE,
        WATCHLIST_FILE,
        EXCEL_FILE,
        DOUBTFUL_FILE,
    ):
        if path.exists():
            path.unlink()
            logger.info("Removed %s", path)
    if PDF_CACHE_DIR.exists():
        shutil.rmtree(PDF_CACHE_DIR)
        PDF_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        logger.info("Cleared PDF cache directory")

    db = SupabaseStore()
    if db.clear_worklist():
        logger.info("Dashboard DB worklist reset (or sync disabled)")
    else:
        logger.warning("Dashboard DB worklist reset skipped or failed: %s", db.last_error)


def main() -> None:
    parser = argparse.ArgumentParser(description="GEM bid extractor")
    parser.add_argument(
        "--full-reset",
        action="store_true",
        help="Delete local persistence, PDF cache, Excel outputs, and truncate bid_worklist before running.",
    )
    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)
    if args.full_reset:
        full_reset(logger)

    started_at = datetime.now(timezone.utc)
    try:
        result = run()
        status_payload = {
            "status": "ok",
            "started_at": started_at.isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "summary": result,
        }
        logger.info("Run summary: %s", result)
    except Exception as exc:
        status_payload = {
            "status": "error",
            "started_at": started_at.isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "error": str(exc),
        }
        RUN_STATUS_FILE.write_text(json.dumps(status_payload, indent=2), encoding="utf-8")
        raise
    RUN_STATUS_FILE.write_text(json.dumps(status_payload, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
