import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from gem_bid_extractor.pipeline import run
from gem_bid_extractor.settings import LOG_FILE, RUN_STATUS_FILE


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
        ],
    )


def main() -> None:
    setup_logging()
    logger = logging.getLogger(__name__)
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
