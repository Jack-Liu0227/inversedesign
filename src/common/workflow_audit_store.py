from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional

from .db_paths import WORKFLOW_AUDIT_LOG_DB


def _connect(path: Optional[Path] = None) -> sqlite3.Connection:
    db_path = path or WORKFLOW_AUDIT_LOG_DB
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    _ensure_schema(conn)
    return conn


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def _truncate_text(value: Any, max_chars: int = 2000) -> str:
    text = str(value or "")
    return text


def _jsonable_payload(value: Any, *, depth: int = 0) -> Any:
    if value is None:
        return None
    if depth >= 12:
        return _truncate_text(value, max_chars=200000)
    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for k, v in value.items():
            out[str(k)] = _jsonable_payload(v, depth=depth + 1)
        return out
    if isinstance(value, list):
        return [_jsonable_payload(v, depth=depth + 1) for v in value]
    if isinstance(value, (int, float, bool)):
        return value
    return _truncate_text(value, max_chars=200000)


def _execute_update(query: str, params: tuple[Any, ...]) -> None:
    conn = _connect()
    try:
        conn.execute(query, params)
        conn.commit()
    finally:
        conn.close()


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS workflow_run_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            workflow_name TEXT NOT NULL,
            session_id TEXT,
            workflow_run_id TEXT,
            user_id TEXT,
            decision TEXT,
            should_stop INTEGER,
            summary_json TEXT,
            final_result_json TEXT,
            step_outputs_json TEXT,
            input_json TEXT NOT NULL,
            error_text TEXT
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_workflow_run_audit_workflow_created "
        "ON workflow_run_audit(workflow_name, created_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_workflow_run_audit_session_created "
        "ON workflow_run_audit(session_id, created_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_workflow_run_audit_user_created "
        "ON workflow_run_audit(user_id, created_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_workflow_run_audit_workflow_run_created "
        "ON workflow_run_audit(workflow_run_id, created_at DESC)"
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS agent_tool_call_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            workflow_name TEXT,
            trace_id TEXT,
            session_id TEXT,
            workflow_run_id TEXT,
            execution_id INTEGER,
            step_name TEXT,
            agent_name TEXT,
            agent_source TEXT,
            tool_name TEXT,
            tool_args_json TEXT,
            tool_result_json TEXT,
            success INTEGER,
            error_text TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS agent_execution_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            workflow_name TEXT,
            trace_id TEXT,
            session_id TEXT,
            workflow_run_id TEXT,
            step_name TEXT,
            agent_name TEXT,
            agent_source TEXT,
            prompt_text TEXT,
            response_text TEXT,
            response_json TEXT,
            success INTEGER,
            error_text TEXT,
            latency_ms INTEGER,
            tool_call_count INTEGER
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_agent_exec_logs_trace_created "
        "ON agent_execution_logs(trace_id, created_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_agent_exec_logs_session_created "
        "ON agent_execution_logs(session_id, created_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_agent_exec_logs_workflow_run_created "
        "ON agent_execution_logs(workflow_run_id, created_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_agent_exec_logs_step_created "
        "ON agent_execution_logs(step_name, created_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_agent_exec_logs_agent_created "
        "ON agent_execution_logs(agent_name, created_at DESC)"
    )
    # Backward-compatible schema upgrade for existing local DBs.
    columns = {
        str(row[1]).strip().lower()
        for row in conn.execute("PRAGMA table_info(agent_tool_call_logs)").fetchall()
        if len(row) > 1
    }
    if "agent_source" not in columns:
        conn.execute("ALTER TABLE agent_tool_call_logs ADD COLUMN agent_source TEXT")
    if "execution_id" not in columns:
        conn.execute("ALTER TABLE agent_tool_call_logs ADD COLUMN execution_id INTEGER")
    if "trace_id" not in columns:
        conn.execute("ALTER TABLE agent_tool_call_logs ADD COLUMN trace_id TEXT")
    if "workflow_name" not in columns:
        conn.execute("ALTER TABLE agent_tool_call_logs ADD COLUMN workflow_name TEXT")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_agent_tool_logs_session_created "
        "ON agent_tool_call_logs(session_id, created_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_agent_tool_logs_trace_created "
        "ON agent_tool_call_logs(trace_id, created_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_agent_tool_logs_workflow_run_created "
        "ON agent_tool_call_logs(workflow_run_id, created_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_agent_tool_logs_exec_created "
        "ON agent_tool_call_logs(execution_id, created_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_agent_tool_logs_agent_created "
        "ON agent_tool_call_logs(agent_name, created_at DESC)"
    )
    conn.commit()


def create_workflow_run_audit(
    *,
    workflow_name: str,
    session_id: Optional[str],
    workflow_run_id: Optional[str],
    user_id: Optional[str],
    input_payload: Dict[str, Any],
) -> int:
    normalized_workflow_run_id = str(workflow_run_id or "").strip() or str(session_id or "").strip()
    if not normalized_workflow_run_id:
        raise ValueError("create_workflow_run_audit requires non-empty workflow_run_id or session_id")
    conn = _connect()
    try:
        cur = conn.execute(
            """
            INSERT INTO workflow_run_audit (
                created_at, workflow_name, session_id, workflow_run_id, user_id, input_json
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                _utc_now_iso(),
                workflow_name,
                session_id,
                normalized_workflow_run_id,
                user_id,
                _json_dumps(input_payload),
            ),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


def finalize_workflow_run_audit(
    *,
    audit_id: int,
    decision: str,
    should_stop: bool,
    summary: list[str],
    final_result: Dict[str, Any],
    step_outputs: Dict[str, Any],
) -> None:
    _execute_update(
        """
        UPDATE workflow_run_audit
        SET decision = ?, should_stop = ?, summary_json = ?, final_result_json = ?, step_outputs_json = ?
        WHERE id = ?
        """,
        (
            decision,
            int(should_stop),
            _json_dumps(summary),
            _json_dumps(_jsonable_payload(final_result)),
            _json_dumps({str(k): _jsonable_payload(v) for k, v in (step_outputs or {}).items()}),
            audit_id,
        ),
    )


def fail_workflow_run_audit(*, audit_id: int, error_text: str) -> None:
    _execute_update(
        "UPDATE workflow_run_audit SET error_text = ? WHERE id = ?",
        (error_text, audit_id),
    )


def log_workflow_run_audit(
    *,
    workflow_name: str,
    session_id: Optional[str],
    workflow_run_id: Optional[str],
    user_id: Optional[str],
    input_payload: Dict[str, Any],
    decision: Optional[str] = None,
    should_stop: Optional[bool] = None,
    summary: Optional[list[str]] = None,
    final_result: Optional[Dict[str, Any]] = None,
    step_outputs: Optional[Dict[str, Any]] = None,
    error_text: Optional[str] = None,
) -> int:
    audit_id = create_workflow_run_audit(
        workflow_name=workflow_name,
        session_id=session_id,
        workflow_run_id=workflow_run_id,
        user_id=user_id,
        input_payload=input_payload,
    )
    if error_text:
        fail_workflow_run_audit(audit_id=audit_id, error_text=error_text)
        return audit_id
    if decision is not None and should_stop is not None and summary is not None and final_result is not None:
        finalize_workflow_run_audit(
            audit_id=audit_id,
            decision=decision,
            should_stop=should_stop,
            summary=summary,
            final_result=final_result,
            step_outputs=step_outputs or {},
        )
    return audit_id


def log_agent_tool_call(
    *,
    workflow_name: Optional[str],
    trace_id: Optional[str],
    session_id: Optional[str],
    workflow_run_id: Optional[str],
    execution_id: Optional[int],
    step_name: Optional[str],
    agent_name: str,
    agent_source: Optional[str],
    tool_name: Optional[str],
    tool_args: Optional[Dict[str, Any]],
    tool_result: Optional[Dict[str, Any]],
    success: bool,
    error_text: Optional[str] = None,
) -> Optional[int]:
    normalized_workflow_run_id = str(workflow_run_id or "").strip() or str(session_id or "").strip() or str(trace_id or "").strip()
    if not normalized_workflow_run_id:
        return None
    conn = _connect()
    try:
        cur = conn.execute(
            """
            INSERT INTO agent_tool_call_logs (
                created_at, workflow_name, trace_id, session_id, workflow_run_id, execution_id,
                step_name, agent_name, agent_source, tool_name,
                tool_args_json, tool_result_json, success, error_text
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                _utc_now_iso(),
                workflow_name,
                trace_id,
                session_id,
                normalized_workflow_run_id,
                execution_id,
                step_name,
                agent_name,
                agent_source,
                tool_name,
                _json_dumps(_jsonable_payload(tool_args or {})),
                _json_dumps(_jsonable_payload(tool_result or {})),
                int(bool(success)),
                error_text,
            ),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


def log_agent_execution(
    *,
    workflow_name: Optional[str],
    trace_id: Optional[str],
    session_id: Optional[str],
    workflow_run_id: Optional[str],
    step_name: Optional[str],
    agent_name: str,
    agent_source: Optional[str],
    prompt_text: str,
    response_text: str,
    response_json: Optional[Dict[str, Any]],
    success: bool,
    error_text: Optional[str],
    latency_ms: Optional[int],
    tool_call_count: int,
) -> Optional[int]:
    normalized_workflow_run_id = str(workflow_run_id or "").strip() or str(session_id or "").strip() or str(trace_id or "").strip()
    if not normalized_workflow_run_id:
        return None
    conn = _connect()
    try:
        cur = conn.execute(
            """
            INSERT INTO agent_execution_logs (
                created_at, workflow_name, trace_id, session_id, workflow_run_id, step_name, agent_name, agent_source,
                prompt_text, response_text, response_json, success, error_text, latency_ms, tool_call_count
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                _utc_now_iso(),
                workflow_name,
                trace_id,
                session_id,
                normalized_workflow_run_id,
                step_name,
                agent_name,
                agent_source,
                _truncate_text(prompt_text, max_chars=200000),
                _truncate_text(response_text, max_chars=200000),
                _json_dumps(_jsonable_payload(response_json or {})),
                int(bool(success)),
                error_text,
                latency_ms,
                int(max(0, tool_call_count)),
            ),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()
