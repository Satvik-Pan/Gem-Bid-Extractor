from __future__ import annotations

import os
import subprocess
import threading
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException

ROOT = Path(__file__).resolve().parents[1]
RUN_LOCK = threading.Lock()
RUNNER_TOKEN = os.environ.get("GEM_RUNNER_TOKEN", "")

app = FastAPI(title="GEM Bid Extractor Runner")


@app.get("/health")
def health() -> dict:
    return {"ok": True}


@app.post("/run")
def run_job(authorization: str | None = Header(default=None)) -> dict:
    if not RUNNER_TOKEN:
        raise HTTPException(status_code=500, detail="GEM_RUNNER_TOKEN is not configured")

    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")

    token = authorization.split(" ", 1)[1].strip()
    if token != RUNNER_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid token")

    if not RUN_LOCK.acquire(blocking=False):
        return {"status": "busy", "statusCode": 429, "message": "Run already in progress"}

    try:
        result = subprocess.run(
            ["python", "main.py"],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=1800,
            check=False,
        )

        return {
            "status": "success" if result.returncode == 0 else "failed",
            "statusCode": 200 if result.returncode == 0 else 500,
            "returnCode": result.returncode,
            "stdout": result.stdout[-3000:],
            "stderr": result.stderr[-3000:],
        }
    finally:
        RUN_LOCK.release()
