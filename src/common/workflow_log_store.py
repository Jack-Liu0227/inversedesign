from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional

from .db_paths import WORKFLOW_AUDIT_LOG_DB


def _enabled() -> bool:
    value = os.getenv("APP_LOG_SQLITE_ENABLED", "true").strip().lower()
    return value in {"1", "true", "yes", "on"}


def _retention_days() -> int:
    raw = os.getenv("APP_LOG_RETENTION_DAYS", "30").strip()
    try:
        value = int(raw)
    except ValueError:
        return 30
    return max(1, value)


def _auto_cleanup_enabled() -> bool:
    value = os.getenv("APP_LOG_AUTO_CLEANUP", "true").strip().lower()
    return value in {"1", "true", "yes", "on"}


def _jsonable_payload(value: Any, *, depth: int = 0) -> Any:
    if value is None:
        return None
    if depth >= 24:
        return str(value)
    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for k, v in value.items():
            out[str(k)] = _jsonable_payload(v, depth=depth + 1)
        return out
    if isinstance(value, list):
        return [_jsonable_payload(v, depth=depth + 1) for v in value]
    if isinstance(value, (int, float, bool)):
        return value
    return str(value)


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS workflow_io_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            trace_id TEXT,
            workflow_name TEXT NOT NULL,
            session_id TEXT,
            run_id TEXT,
            user_id TEXT,
            step_name TEXT,
            event_type TEXT NOT NULL,
            payload_json TEXT,
            latency_ms INTEGER,
            success INTEGER,
            error_text TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_workflow_io_logs_created_at ON workflow_io_logs(created_at DESC)")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_workflow_io_logs_session_created ON workflow_io_logs(session_id, created_at DESC)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_workflow_io_logs_run_created ON workflow_io_logs(run_id, created_at DESC)")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_workflow_io_logs_user_created ON workflow_io_logs(user_id, created_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_workflow_io_logs_trace_created ON workflow_io_logs(trace_id, created_at DESC)"
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS workflow_step_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            trace_id TEXT,
            workflow_name TEXT NOT NULL,
            session_id TEXT,
            run_id TEXT,
            user_id TEXT,
            step_name TEXT NOT NULL,
            status TEXT NOT NULL,
            input_json TEXT,
            output_json TEXT,
            latency_ms INTEGER,
            success INTEGER,
            error_text TEXT
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_workflow_step_logs_trace_created ON workflow_step_logs(trace_id, created_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_workflow_step_logs_run_created ON workflow_step_logs(run_id, created_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_workflow_step_logs_step_created ON workflow_step_logs(step_name, created_at DESC)"
    )
    conn.commit()


def _db_path() -> Path:
    raw = os.getenv("APP_LOG_SQLITE_DB", "").strip()
    if raw:
        return Path(raw)
    return WORKFLOW_AUDIT_LOG_DB


def _connect() -> sqlite3.Connection:
    path = _db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    _ensure_schema(conn)
    return conn


def log_workflow_event(
    *,
    workflow_name: str,
    trace_id: Optional[str],
    session_id: Optional[str],
    run_id: Optional[str],
    user_id: Optional[str],
    step_name: Optional[str],
    event_type: str,
    payload: Optional[Dict[str, Any]],
    latency_ms: Optional[int] = None,
    success: Optional[bool] = None,
    error_text: Optional[str] = None,
) -> Optional[int]:
    if not _enabled():
        return None
    normalized_run_id = str(run_id or "").strip() or str(session_id or "").strip() or str(trace_id or "").strip()
    if not normalized_run_id:
        return None
    conn = _connect()
    try:
        cursor = conn.execute(
            """
            INSERT INTO workflow_io_logs (
                created_at, trace_id, workflow_name, session_id, run_id, user_id,
                step_name, event_type, payload_json, latency_ms, success, error_text
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                datetime.now(timezone.utc).isoformat(),
                trace_id,
                workflow_name,
                session_id,
                normalized_run_id,
                user_id,
                step_name,
                event_type,
                json.dumps(_jsonable_payload(payload or {}), ensure_ascii=False),
                latency_ms,
                None if success is None else int(success),
                error_text,
            ),
        )
        conn.commit()
        return int(cursor.lastrowid)
    finally:
        conn.close()


def cleanup_workflow_logs() -> int:
    if not _enabled() or not _auto_cleanup_enabled():
        return 0
    if not _db_path().exists():
        return 0
    threshold = (datetime.now(timezone.utc) - timedelta(days=_retention_days())).isoformat()
    conn = _connect()
    try:
        cursor = conn.execute("DELETE FROM workflow_io_logs WHERE created_at < ?", (threshold,))
        deleted_io = int(cursor.rowcount or 0)
        cursor = conn.execute("DELETE FROM workflow_step_logs WHERE created_at < ?", (threshold,))
        conn.commit()
        return deleted_io + int(cursor.rowcount or 0)
    finally:
        conn.close()


def log_workflow_step(
    *,
    workflow_name: str,
    trace_id: Optional[str],
    session_id: Optional[str],
    run_id: Optional[str],
    user_id: Optional[str],
    step_name: str,
    status: str,
    input_payload: Optional[Dict[str, Any]] = None,
    output_payload: Optional[Dict[str, Any]] = None,
    latency_ms: Optional[int] = None,
    success: Optional[bool] = None,
    error_text: Optional[str] = None,
) -> Optional[int]:
    if not _enabled():
        return None
    normalized_run_id = str(run_id or "").strip() or str(session_id or "").strip() or str(trace_id or "").strip()
    if not normalized_run_id:
        return None
    conn = _connect()
    try:
        cursor = conn.execute(
            """
            INSERT INTO workflow_step_logs (
                created_at, trace_id, workflow_name, session_id, run_id, user_id,
                step_name, status, input_json, output_json, latency_ms, success, error_text
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                datetime.now(timezone.utc).isoformat(),
                trace_id,
                workflow_name,
                session_id,
                normalized_run_id,
                user_id,
                step_name,
                status,
                json.dumps(_jsonable_payload(input_payload or {}), ensure_ascii=False),
                json.dumps(_jsonable_payload(output_payload or {}), ensure_ascii=False),
                latency_ms,
                None if success is None else int(success),
                error_text,
            ),
        )
        conn.commit()
        return int(cursor.lastrowid)
    finally:
        conn.close()
