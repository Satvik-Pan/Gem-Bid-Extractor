from __future__ import annotations

import json
import logging
import os
from typing import Iterable

import psycopg2
from psycopg2.extras import RealDictCursor

from .settings import (
    DB_DSN,
    DB_HOST,
    DB_NAME,
    DB_PASSWORD,
    DB_PORT,
    DB_SSLMODE,
    DB_USER,
    SYNC_QUEUE_FILE,
)

logger = logging.getLogger(__name__)


class SupabaseStore:
    def __init__(self):
        self.enabled = bool(DB_DSN or (DB_HOST and DB_USER and DB_PASSWORD))
        self.last_error = ""
        self.queue_path = SYNC_QUEUE_FILE
        self._schema_ready = False

    def _connect(self):
        if not self.enabled:
            raise RuntimeError("Supabase DB credentials are not configured.")
        if DB_DSN:
            return psycopg2.connect(DB_DSN, sslmode=DB_SSLMODE, connect_timeout=12)
        return psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            sslmode=DB_SSLMODE,
            connect_timeout=12,
        )

    def _load_queue(self) -> list[dict]:
        if not self.queue_path.exists():
            return []
        items: list[dict] = []
        try:
            with self.queue_path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(row, dict):
                        items.append(row)
        except OSError:
            return []
        return items

    def _save_queue(self, rows: list[dict]) -> None:
        self.queue_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.queue_path.with_suffix(f"{self.queue_path.suffix}.tmp")
        with temp_path.open("w", encoding="utf-8") as fh:
            for row in rows:
                fh.write(json.dumps(row, ensure_ascii=True) + "\n")
            fh.flush()
            os.fsync(fh.fileno())
        temp_path.replace(self.queue_path)

    @staticmethod
    def _dedupe_rows(rows: Iterable[dict]) -> list[dict]:
        deduped: dict[str, dict] = {}
        for row in rows:
            bid_id = str(row.get("Bid ID") or row.get("Reference No.") or "").strip()
            ref = str(row.get("Reference No.", "")).strip()
            if not bid_id or not ref:
                continue
            deduped[bid_id] = row
        return list(deduped.values())

    def queue_rows(self, bids: Iterable[dict]) -> int:
        incoming = self._dedupe_rows(bids)
        if not incoming:
            return 0
        existing = self._load_queue()
        merged = self._dedupe_rows([*existing, *incoming])
        self._save_queue(merged)
        return len(merged)

    def clear_worklist(self) -> bool:
        """Delete all rows from bid_worklist (dashboard). Returns True if DB was cleared or sync disabled."""
        if not self.enabled:
            return True
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("delete from bid_worklist")
                conn.commit()
            self.last_error = ""
            logger.info("Supabase bid_worklist cleared")
            return True
        except psycopg2.Error as exc:
            self.last_error = str(exc)
            logger.warning("Could not truncate bid_worklist: %s", exc)
            return False

    def ensure_schema(self) -> None:
        if not self.enabled:
            return
        if self._schema_ready:
            return
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        create table if not exists bid_worklist (
                            bid_id text primary key,
                            reference_no text not null,
                            category text not null,
                            status text not null default 'ACTIVE',
                            llm_confidence double precision,
                            llm_reason text,
                            pipeline_source text,
                            payload jsonb not null,
                            first_seen_at timestamptz not null default now(),
                            last_seen_at timestamptz not null default now(),
                            resolved_at timestamptz null
                        );
                        create index if not exists idx_bid_worklist_status on bid_worklist(status);
                        create index if not exists idx_bid_worklist_category on bid_worklist(category);
                        """
                    )
                conn.commit()
            self._schema_ready = True
            self.last_error = ""
        except psycopg2.Error as exc:
            self.last_error = str(exc)
            logger.warning("Supabase schema sync unavailable: %s", exc)

    def _upsert_rows(self, bids: Iterable[dict]) -> None:
        if not self.enabled:
            return

        with self._connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                for bid in self._dedupe_rows(bids):
                    bid_id = str(bid.get("Bid ID") or bid.get("Reference No.") or "").strip()
                    ref = str(bid.get("Reference No.", "")).strip()
                    if not bid_id or not ref:
                        continue

                    incoming_category = str(bid.get("Final Category", "DOUBTFUL")).upper()
                    if incoming_category not in {"EXTRACTED", "DOUBTFUL"}:
                        incoming_category = "DOUBTFUL"
                    confidence = float(bid.get("LLM Confidence", 0.0) or 0.0)
                    reason = str(bid.get("LLM Reason", ""))
                    pipeline_source = str(bid.get("Pipeline Source", ""))
                    payload = json.dumps(bid)

                    status = "ACTIVE"

                    cur.execute("select status from bid_worklist where bid_id = %s", (bid_id,))
                    row = cur.fetchone()
                    if row and row["status"] in {"RESOLVED", "REVIEW_REJECTED"}:
                        continue

                    cur.execute(
                        """
                        insert into bid_worklist (
                            bid_id, reference_no, category, status, llm_confidence,
                            llm_reason, pipeline_source, payload
                        ) values (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                        on conflict (bid_id) do update
                        set reference_no = excluded.reference_no,
                            category = case
                                when bid_worklist.category = 'EXTRACTED' and bid_worklist.status = 'ACTIVE' then 'EXTRACTED'
                                else excluded.category
                            end,
                            status = case
                                when bid_worklist.status in ('RESOLVED', 'REVIEW_REJECTED') then bid_worklist.status
                                else excluded.status
                            end,
                            llm_confidence = excluded.llm_confidence,
                            llm_reason = excluded.llm_reason,
                            pipeline_source = excluded.pipeline_source,
                            payload = excluded.payload,
                            last_seen_at = now()
                        """,
                        (
                            bid_id,
                            ref,
                            incoming_category,
                            status,
                            confidence,
                            reason,
                            pipeline_source,
                            payload,
                        ),
                    )
            conn.commit()

    def sync_with_retry(self, bids: Iterable[dict]) -> bool:
        pending_count = self.queue_rows(bids)
        if not self.enabled:
            logger.warning("Supabase sync not configured; queued rows retained: %d", pending_count)
            return False

        self.ensure_schema()
        if not self._schema_ready:
            logger.warning("Supabase schema unavailable; queued rows retained: %d", pending_count)
            return False

        queued_rows = self._load_queue()
        if not queued_rows:
            return True

        try:
            self._upsert_rows(queued_rows)
            self._save_queue([])
            self.last_error = ""
            logger.info("Supabase sync flushed %d queued rows", len(queued_rows))
            return True
        except psycopg2.Error as exc:
            self.last_error = str(exc)
            logger.warning("Supabase upsert unavailable; keeping %d queued rows: %s", len(queued_rows), exc)
            return False
