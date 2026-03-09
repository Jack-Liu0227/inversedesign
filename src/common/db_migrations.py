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


def _ensure_workflow_run_id_column(
    *,
    conn: sqlite3.Connection,
    table: str,
    fallback_column: str | None = None,
) -> bool:
    cols = _table_columns(conn, table)
    if not cols:
        return False
    changed = False
    if "workflow_run_id" not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN workflow_run_id TEXT NOT NULL DEFAULT ''")
        changed = True
        cols = _table_columns(conn, table)
    if "workflow_run_id" in cols:
        if "run_id" in cols:
            conn.execute(f"UPDATE {table} SET workflow_run_id = run_id WHERE workflow_run_id = ''")
        if fallback_column and fallback_column.lower() in cols:
            conn.execute(f"UPDATE {table} SET workflow_run_id = {fallback_column} WHERE workflow_run_id = ''")
    return changed


def _ensure_workflow_run_id_columns(db_path: Path) -> bool:
    if not db_path.exists():
        return False
    changed = False
    conn = sqlite3.connect(str(db_path))
    try:
        changed |= _ensure_workflow_run_id_column(conn=conn, table="workflow_run_audit", fallback_column="session_id")
        changed |= _ensure_workflow_run_id_column(conn=conn, table="agent_tool_call_logs", fallback_column="session_id")
        changed |= _ensure_workflow_run_id_column(conn=conn, table="agent_execution_logs", fallback_column="session_id")
        changed |= _ensure_workflow_run_id_column(conn=conn, table="workflow_io_logs", fallback_column="session_id")
        changed |= _ensure_workflow_run_id_column(conn=conn, table="workflow_step_logs", fallback_column="session_id")
        changed |= _ensure_workflow_run_id_column(conn=conn, table="prompt_llmresponse_logs", fallback_column="session_id")
        changed |= _ensure_workflow_run_id_column(conn=conn, table="prediction_prompt_logs", fallback_column=None)
        changed |= _ensure_workflow_run_id_column(conn=conn, table="material_samples", fallback_column="session_id")
        changed |= _ensure_workflow_run_id_column(conn=conn, table="material_dataset_rows", fallback_column="session_id")
        changed |= _ensure_workflow_run_id_column(conn=conn, table="material_doc_knowledge", fallback_column="session_id")
        changed |= _ensure_workflow_run_id_column(conn=conn, table="material_doc_segments", fallback_column="session_id")
        if _table_exists(conn, "agent_tool_call_logs"):
            conn.execute("UPDATE agent_tool_call_logs SET workflow_run_id = trace_id WHERE workflow_run_id = '' AND COALESCE(trace_id, '') <> ''")
        if _table_exists(conn, "agent_execution_logs"):
            conn.execute("UPDATE agent_execution_logs SET workflow_run_id = trace_id WHERE workflow_run_id = '' AND COALESCE(trace_id, '') <> ''")
        if _table_exists(conn, "prompt_llmresponse_logs"):
            conn.execute("UPDATE prompt_llmresponse_logs SET workflow_run_id = trace_id WHERE workflow_run_id = '' AND COALESCE(trace_id, '') <> ''")
        conn.commit()
    finally:
        conn.close()
    return changed


def _ensure_material_run_note_and_meta(db_path: Path) -> bool:
    if not db_path.exists():
        return False
    changed = False
    conn = sqlite3.connect(str(db_path))
    try:
        dataset_cols = _table_columns(conn, "material_dataset_rows")
        if dataset_cols and "run_note" not in dataset_cols:
            conn.execute("ALTER TABLE material_dataset_rows ADD COLUMN run_note TEXT NOT NULL DEFAULT ''")
            changed = True
        if not _table_exists(conn, "workflow_run_meta"):
            conn.execute(
                """
                CREATE TABLE workflow_run_meta (
                    workflow_run_id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL DEFAULT '',
                    material_type TEXT NOT NULL DEFAULT '',
                    run_note TEXT NOT NULL DEFAULT '',
                    mounted_run_ids_json TEXT NOT NULL DEFAULT '[]',
                    created_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_workflow_run_meta_session_created "
                "ON workflow_run_meta(session_id, created_at DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_workflow_run_meta_material_created "
                "ON workflow_run_meta(material_type, created_at DESC)"
            )
            changed = True
        elif _table_exists(conn, "workflow_run_meta"):
            meta_cols = _table_columns(conn, "workflow_run_meta")
            if "run_note" not in meta_cols:
                conn.execute("ALTER TABLE workflow_run_meta ADD COLUMN run_note TEXT NOT NULL DEFAULT ''")
                changed = True
            if "mounted_run_ids_json" not in meta_cols:
                conn.execute("ALTER TABLE workflow_run_meta ADD COLUMN mounted_run_ids_json TEXT NOT NULL DEFAULT '[]'")
                changed = True
            if "material_type" not in meta_cols:
                conn.execute("ALTER TABLE workflow_run_meta ADD COLUMN material_type TEXT NOT NULL DEFAULT ''")
                changed = True
            if "session_id" not in meta_cols:
                conn.execute("ALTER TABLE workflow_run_meta ADD COLUMN session_id TEXT NOT NULL DEFAULT ''")
                changed = True
            if "created_at" not in meta_cols:
                conn.execute("ALTER TABLE workflow_run_meta ADD COLUMN created_at TEXT NOT NULL DEFAULT ''")
                changed = True
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
        if _ensure_material_run_note_and_meta(db_path) and db_path not in updated:
            updated.append(db_path)
    return updated

