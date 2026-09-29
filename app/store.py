"""SQLite-backed result store.

Why SQLite and not a JSON file: the producer service and the consumer service are
separate OS processes writing the same record. A JSON file needs hand-rolled
locking and loses writes to read-modify-write races. SQLite in WAL mode handles
concurrent writers itself, and it is in the standard library, so it adds no
dependency to the lab.

WAL is also what makes exercise 1.3 and harness criterion 2 work: when the
consumer is killed mid-processing, the `processing` row it never got to finish
is still on disk for the restarted consumer to complete.

Every operation opens its own short-lived connection. Nothing holds a write
lock across the ~10s AI call, which would serialise the whole pipeline.
"""

import json
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from . import config
from .schemas import (
    STATUS_COMPLETED,
    STATUS_ERROR,
    STATUS_PROCESSING,
    ProcessResult,
)

_CREATE = """
CREATE TABLE IF NOT EXISTS results (
    id          TEXT PRIMARY KEY,
    text        TEXT NOT NULL,
    status      TEXT NOT NULL,
    result      TEXT,
    error       TEXT,
    retries     INTEGER NOT NULL DEFAULT 0,
    latency_s   REAL,
    metadata    TEXT,
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_results_status ON results(status);
"""


def utc_now() -> str:
    """ISO-8601 UTC with a Z suffix, matching the envelope in exercise 1.1."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def new_request_id() -> str:
    return uuid.uuid4().hex


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(config.DB_PATH, timeout=5.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init() -> None:
    with _connect() as conn:
        conn.executescript(_CREATE)


def create_processing(
    request_id: str,
    text: str,
    metadata: Optional[dict[str, Any]] = None,
) -> str:
    """Record the request as in-flight.

    Called *before* publishing. The harness posts to /process and then
    immediately GETs /result/{id}; writing first means that call can never 404.
    """
    now = utc_now()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO results (id, text, status, metadata, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO NOTHING
            """,
            (request_id, text, STATUS_PROCESSING, json.dumps(metadata) if metadata else None, now, now),
        )
    return now


def mark_completed(request_id: str, result: str, latency_s: float, retries: int) -> None:
    with _connect() as conn:
        conn.execute(
            """
            UPDATE results
               SET status = ?, result = ?, error = NULL, latency_s = ?, retries = ?, updated_at = ?
             WHERE id = ?
            """,
            (STATUS_COMPLETED, result, latency_s, retries, utc_now(), request_id),
        )


def mark_error(request_id: str, error: str, retries: int) -> None:
    with _connect() as conn:
        conn.execute(
            """
            UPDATE results
               SET status = ?, error = ?, retries = ?, updated_at = ?
             WHERE id = ?
            """,
            (STATUS_ERROR, error, retries, utc_now(), request_id),
        )


def set_retries(request_id: str, retries: int) -> None:
    with _connect() as conn:
        conn.execute(
            "UPDATE results SET retries = ?, updated_at = ? WHERE id = ?",
            (retries, utc_now(), request_id),
        )


def get(request_id: str) -> Optional[ProcessResult]:
    with _connect() as conn:
        row = conn.execute("SELECT * FROM results WHERE id = ?", (request_id,)).fetchone()
    if row is None:
        return None
    return ProcessResult(
        id=row["id"],
        status=row["status"],
        text=row["text"],
        result=row["result"],
        error=row["error"],
        retries=row["retries"],
        latency_s=row["latency_s"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        metadata=json.loads(row["metadata"]) if row["metadata"] else None,
    )


def purge() -> int:
    """Empty the store. Handy between lab runs so GET /result/{id} is unambiguous."""
    with _connect() as conn:
        return conn.execute("DELETE FROM results").rowcount
