import argparse
import json
import logging
import os
import shutil
import sys
import uuid
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
    LOG_FILE,
    PDF_CACHE_DIR,
    RUN_LOCK_FILE,
    RUN_LOG_DIR,
    PROCESSED_FILE,
    RUN_STATUS_FILE,
    SYNC_QUEUE_FILE,
)
from gem_bid_extractor.supabase_store import SupabaseStore


class _RunContextFilter(logging.Filter):
    def __init__(self, run_id: str):
        super().__init__()
        self.run_id = run_id

    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = self.run_id
        return True


class _RunLock:
    def __init__(self, lock_path: Path, run_id: str):
        self.lock_path = lock_path
        self.run_id = run_id
        self.fd: int | None = None

    def acquire(self) -> bool:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        try:
            self.fd = os.open(str(self.lock_path), flags)
        except FileExistsError:
            return False
        payload = {
            "pid": os.getpid(),
            "run_id": self.run_id,
            "started_at": datetime.now(timezone.utc).isoformat(),
        }
        os.write(self.fd, json.dumps(payload, ensure_ascii=True).encode("utf-8"))
        os.fsync(self.fd)
        return True

    def release(self) -> None:
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None
        if self.lock_path.exists():
            self.lock_path.unlink(missing_ok=True)


def setup_logging(run_id: str) -> None:
    RUN_LOG_DIR.mkdir(parents=True, exist_ok=True)
    run_log_file = RUN_LOG_DIR / f"{run_id}.log"
    run_filter = _RunContextFilter(run_id)
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] [run:%(run_id)s] %(message)s")
    stream_handler = logging.StreamHandler(
        open(sys.stdout.fileno(), mode="w", encoding="utf-8", closefd=False)
    )
    stream_handler.setFormatter(formatter)
    stream_handler.addFilter(run_filter)
    run_file_handler = logging.FileHandler(run_log_file, encoding="utf-8")
    run_file_handler.setFormatter(formatter)
    run_file_handler.addFilter(run_filter)
    summary_handler = logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8")
    summary_handler.setFormatter(formatter)
    summary_handler.addFilter(run_filter)
    root_logger.addHandler(stream_handler)
    root_logger.addHandler(run_file_handler)
    root_logger.addHandler(summary_handler)

    logging.getLogger(__name__).info("Run log file: %s", run_log_file)


def full_reset(logger: logging.Logger) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(
                open(sys.stdout.fileno(), mode="w", encoding="utf-8", closefd=False)
            ),
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
        ],
    )


def full_reset(logger: logging.Logger) -> None:
    """Remove local run state, cached PDFs, Excel outputs, sync queue, and truncate Supabase worklist."""
    for path in (
        PROCESSED_FILE,
        SYNC_QUEUE_FILE,
        RUN_STATUS_FILE,
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
    parser.add_argument(
        "--run-source",
        default="manual",
        choices=["manual", "scheduled"],
        help="Identifies run trigger source for logging and status payload.",
    )
    args = parser.parse_args()

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8]
    setup_logging(run_id)
    logger = logging.getLogger(__name__)
    lock = _RunLock(RUN_LOCK_FILE, run_id)
    if not lock.acquire():
        lock_info = ""
        try:
            lock_info = RUN_LOCK_FILE.read_text(encoding="utf-8").strip()
        except OSError:
            lock_info = "unknown"
        raise RuntimeError(f"Another extractor run is already active. Lock info: {lock_info}")

    started_at = datetime.now(timezone.utc)
    try:
        logger.info("Run source: %s", args.run_source)
        if args.full_reset:
            full_reset(logger)

        result = run()
        status_payload = {
            "status": "ok",
            "run_id": run_id,
            "run_source": args.run_source,
            "started_at": started_at.isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "summary": result,
        }
        logger.info("Run summary: %s", result)
    except Exception as exc:
        status_payload = {
            "status": "error",
            "run_id": run_id,
            "run_source": args.run_source,
            "started_at": started_at.isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "error": str(exc),
        }
        RUN_STATUS_FILE.write_text(json.dumps(status_payload, indent=2), encoding="utf-8")
        raise
    finally:
        lock.release()
    RUN_STATUS_FILE.write_text(json.dumps(status_payload, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
