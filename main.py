import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from gem_bid_extractor.pipeline import run
from gem_bid_extractor.settings import LOG_FILE


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
    result = run()
    logging.getLogger(__name__).info("Run summary: %s", result)


if __name__ == "__main__":
    main()
