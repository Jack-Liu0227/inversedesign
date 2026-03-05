from __future__ import annotations

from datetime import datetime, timezone
import json
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional

from .db_paths import PROMPT_LLMRESPONSE_DB


def _enabled() -> bool:
    value = os.getenv("PROMPT_LLMRESPONSE_LOG_ENABLED", "true").strip().lower()
    return value in {"1", "true", "yes", "on"}


def _db_path() -> Path:
    raw = os.getenv("PROMPT_LLMRESPONSE_DB", "").strip()
    if raw:
        return Path(raw)
    return PROMPT_LLMRESPONSE_DB


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS prompt_llmresponse_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            workflow_name TEXT NOT NULL,
            trace_id TEXT,
            session_id TEXT,
            run_id TEXT,
            step_name TEXT,
            agent_name TEXT,
            model_id TEXT,
            prompt_text TEXT NOT NULL,
            llm_response_text TEXT,
            response_json TEXT,
            success INTEGER,
            error_text TEXT,
            latency_ms INTEGER
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_prompt_llm_step_created ON prompt_llmresponse_logs(step_name, created_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_prompt_llm_session_created ON prompt_llmresponse_logs(session_id, created_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_prompt_llm_run_created ON prompt_llmresponse_logs(run_id, created_at DESC)"
    )
    conn.commit()


def _connect() -> sqlite3.Connection:
    path = _db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    _ensure_schema(conn)
    return conn


def _json_text(payload: Optional[Dict[str, Any]]) -> str:
    return json.dumps(payload or {}, ensure_ascii=False)


def log_prompt_llm_response(
    *,
    workflow_name: str,
    trace_id: str | None,
    session_id: str | None,
    run_id: str | None,
    step_name: str | None,
    agent_name: str | None,
    model_id: str | None,
    prompt_text: str,
    llm_response_text: str | None,
    response_json: Optional[Dict[str, Any]],
    success: bool,
    error_text: str | None = None,
    latency_ms: int | None = None,
) -> Optional[int]:
    if not _enabled():
        return None

    conn = _connect()
    try:
        cursor = conn.execute(
            """
            INSERT INTO prompt_llmresponse_logs (
                created_at,
                workflow_name,
                trace_id,
                session_id,
                run_id,
                step_name,
                agent_name,
                model_id,
                prompt_text,
                llm_response_text,
                response_json,
                success,
                error_text,
                latency_ms
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                datetime.now(timezone.utc).isoformat(),
                str(workflow_name or "").strip(),
                trace_id,
                session_id,
                run_id,
                step_name,
                agent_name,
                model_id,
                str(prompt_text or ""),
                str(llm_response_text or ""),
                _json_text(response_json),
                int(bool(success)),
                str(error_text or ""),
                int(latency_ms) if latency_ms is not None else None,
            ),
        )
        conn.commit()
        return int(cursor.lastrowid)
    finally:
        conn.close()

