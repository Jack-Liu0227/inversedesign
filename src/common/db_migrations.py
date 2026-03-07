from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable

from src.common.db_paths import (
    MATERIAL_DISCOVERY_WORKFLOW_DB,
    PREDICTION_PROMPT_LOG_DB,
    PROMPT_LLMRESPONSE_DB,
    ROOT,
    WORKFLOW_AUDIT_LOG_DB,
)


def _ensure_run_status_column(db_path: Path) -> bool:
    if not db_path.exists():
        return False

    changed = False
    conn = sqlite3.connect(str(db_path))
    try:
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='agno_approvals'")
        if cur.fetchone() is None:
            return False

        cur.execute("PRAGMA table_info(agno_approvals)")
        columns = {row[1] for row in cur.fetchall()}
        if "run_status" not in columns:
            # Keep migration minimal and backwards-compatible for existing rows.
            cur.execute("ALTER TABLE agno_approvals ADD COLUMN run_status VARCHAR")
            conn.commit()
            changed = True
    finally:
        conn.close()
    return changed


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (str(table),),
    ).fetchone()
    return bool(row)


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    if not _table_exists(conn, table):
        return set()
    return {str(row[1]).strip().lower() for row in conn.execute(f"PRAGMA table_info({table})").fetchall() if len(row) > 1}


def _ensure_run_id_column(
    *,
    conn: sqlite3.Connection,
    table: str,
    fallback_column: str | None = None,
) -> bool:
    cols = _table_columns(conn, table)
    if not cols:
        return False
    changed = False
    if "run_id" not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN run_id TEXT NOT NULL DEFAULT ''")
        changed = True
        cols = _table_columns(conn, table)
    if "run_id" in cols and fallback_column and fallback_column.lower() in cols:
        conn.execute(f"UPDATE {table} SET run_id = {fallback_column} WHERE run_id = ''")
    return changed


def _ensure_workflow_run_id_columns(db_path: Path) -> bool:
    if not db_path.exists():
        return False
    changed = False
    conn = sqlite3.connect(str(db_path))
    try:
        changed |= _ensure_run_id_column(conn=conn, table="workflow_run_audit", fallback_column="session_id")
        changed |= _ensure_run_id_column(conn=conn, table="agent_tool_call_logs", fallback_column="session_id")
        changed |= _ensure_run_id_column(conn=conn, table="agent_execution_logs", fallback_column="session_id")
        changed |= _ensure_run_id_column(conn=conn, table="workflow_io_logs", fallback_column="session_id")
        changed |= _ensure_run_id_column(conn=conn, table="workflow_step_logs", fallback_column="session_id")
        changed |= _ensure_run_id_column(conn=conn, table="prompt_llmresponse_logs", fallback_column="session_id")
        changed |= _ensure_run_id_column(conn=conn, table="prediction_prompt_logs", fallback_column=None)
        changed |= _ensure_run_id_column(conn=conn, table="material_samples", fallback_column="workflow_run_id")
        changed |= _ensure_run_id_column(conn=conn, table="material_dataset_rows", fallback_column="workflow_run_id")
        changed |= _ensure_run_id_column(conn=conn, table="material_doc_knowledge", fallback_column="workflow_run_id")
        changed |= _ensure_run_id_column(conn=conn, table="material_doc_segments", fallback_column="workflow_run_id")
        if _table_exists(conn, "agent_tool_call_logs"):
            conn.execute("UPDATE agent_tool_call_logs SET run_id = trace_id WHERE run_id = '' AND COALESCE(trace_id, '') <> ''")
        if _table_exists(conn, "agent_execution_logs"):
            conn.execute("UPDATE agent_execution_logs SET run_id = trace_id WHERE run_id = '' AND COALESCE(trace_id, '') <> ''")
        if _table_exists(conn, "prompt_llmresponse_logs"):
            conn.execute("UPDATE prompt_llmresponse_logs SET run_id = trace_id WHERE run_id = '' AND COALESCE(trace_id, '') <> ''")
        conn.commit()
    finally:
        conn.close()
    return changed


def run_local_db_migrations() -> list[Path]:
    updated: list[Path] = []
    candidates: Iterable[Path] = (
        MATERIAL_DISCOVERY_WORKFLOW_DB,
        WORKFLOW_AUDIT_LOG_DB,
        PROMPT_LLMRESPONSE_DB,
        PREDICTION_PROMPT_LOG_DB,
        ROOT / "src" / "workflow_material_discovery.db",  # legacy location used by older revisions
    )
    for db_path in candidates:
        if _ensure_run_status_column(db_path):
            updated.append(db_path)
        if _ensure_workflow_run_id_columns(db_path) and db_path not in updated:
            updated.append(db_path)
    return updated

