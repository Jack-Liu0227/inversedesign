from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3
from pathlib import Path
from typing import Any

from .db_paths import MATERIAL_DISCOVERY_WORKFLOW_DB


def _connect(path: Path | None = None) -> sqlite3.Connection:
    db_path = path or MATERIAL_DISCOVERY_WORKFLOW_DB
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    _ensure_schema(conn)
    return conn


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_run_ids(raw: Any) -> list[str]:
    if isinstance(raw, str):
        items = [part.strip() for part in raw.split(",")]
    elif isinstance(raw, list):
        items = [str(item or "").strip() for item in raw]
    else:
        items = [str(raw or "").strip()] if raw is not None else []
    normalized: list[str] = []
    for item in items:
        if item and item not in normalized:
            normalized.append(item)
    return normalized


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS workflow_run_meta (
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
    conn.commit()


def upsert_workflow_run_meta(
    *,
    workflow_run_id: str,
    session_id: str = "",
    material_type: str = "",
    run_note: str = "",
    mounted_run_ids: list[str] | str | None = None,
) -> None:
    normalized_run_id = _normalize_text(workflow_run_id)
    if not normalized_run_id:
        return
    normalized_session_id = _normalize_text(session_id)
    normalized_material_type = _normalize_text(material_type).lower()
    normalized_run_note = _normalize_text(run_note)
    normalized_mounted_run_ids = _normalize_run_ids(mounted_run_ids)

    conn = _connect()
    try:
        conn.execute(
            """
            INSERT INTO workflow_run_meta (
                workflow_run_id, session_id, material_type, run_note, mounted_run_ids_json, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(workflow_run_id) DO UPDATE SET
                session_id = CASE
                    WHEN excluded.session_id <> '' THEN excluded.session_id
                    ELSE workflow_run_meta.session_id
                END,
                material_type = CASE
                    WHEN excluded.material_type <> '' THEN excluded.material_type
                    ELSE workflow_run_meta.material_type
                END,
                run_note = excluded.run_note,
                mounted_run_ids_json = excluded.mounted_run_ids_json,
                created_at = CASE
                    WHEN workflow_run_meta.created_at = '' THEN excluded.created_at
                    ELSE workflow_run_meta.created_at
                END
            """,
            (
                normalized_run_id,
                normalized_session_id,
                normalized_material_type,
                normalized_run_note,
                json.dumps(normalized_mounted_run_ids, ensure_ascii=False),
                _utc_now_iso(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def list_workflow_run_meta(*, limit: int = 100) -> list[dict[str, Any]]:
    conn = _connect()
    try:
        rows = conn.execute(
            """
            SELECT workflow_run_id, session_id, material_type, run_note, mounted_run_ids_json, created_at
            FROM workflow_run_meta
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (max(1, int(limit)),),
        ).fetchall()
    finally:
        conn.close()

    output: list[dict[str, Any]] = []
    for workflow_run_id, session_id, material_type, run_note, mounted_json, created_at in rows:
        try:
            mounted_run_ids = json.loads(str(mounted_json or "[]"))
        except Exception:
            mounted_run_ids = []
        output.append(
            {
                "workflow_run_id": _normalize_text(workflow_run_id),
                "session_id": _normalize_text(session_id),
                "material_type": _normalize_text(material_type),
                "run_note": _normalize_text(run_note),
                "mounted_workflow_run_ids": mounted_run_ids if isinstance(mounted_run_ids, list) else [],
                "created_at": _normalize_text(created_at),
            }
        )
    return output
