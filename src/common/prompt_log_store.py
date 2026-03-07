from __future__ import annotations

from datetime import datetime, timezone
import json
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional

from .db_paths import PREDICTION_PROMPT_LOG_DB
DEFAULT_DB_PATH = PREDICTION_PROMPT_LOG_DB


def _enabled() -> bool:
    value = os.getenv("PREDICT_PROMPT_LOG_ENABLED", "false").strip().lower()
    return value in {"1", "true", "yes", "on"}


def _db_path() -> Path:
    raw = os.getenv("PREDICT_PROMPT_LOG_DB", "").strip()
    if raw:
        return Path(raw)
    return DEFAULT_DB_PATH


def _connect() -> sqlite3.Connection:
    path = _db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    _ensure_schema(conn)
    return conn


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS prediction_prompt_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            run_id TEXT NOT NULL DEFAULT '',
            material_type_input TEXT,
            material_type_resolved TEXT NOT NULL,
            composition_json TEXT,
            processing_json TEXT,
            features_json TEXT,
            top_k INTEGER,
            prompt_text TEXT NOT NULL,
            llm_response TEXT,
            predicted_values_json TEXT,
            confidence TEXT
        )
        """
    )
    columns = {
        str(row[1]).strip().lower()
        for row in conn.execute("PRAGMA table_info(prediction_prompt_logs)").fetchall()
        if isinstance(row, tuple) and len(row) > 1
    }
    if "run_id" not in columns:
        conn.execute("ALTER TABLE prediction_prompt_logs ADD COLUMN run_id TEXT NOT NULL DEFAULT ''")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_prediction_prompt_logs_run_created ON prediction_prompt_logs(run_id, created_at DESC)")
    conn.commit()


def log_prediction_prompt(
    *,
    run_id: str = "",
    material_type_input: str,
    material_type_resolved: str,
    composition: Optional[Dict[str, Any]],
    processing: Optional[Dict[str, Any]],
    features: Optional[Dict[str, Any]],
    top_k: int,
    prompt: str,
    llm_response: str,
    predicted_values: Dict[str, Any],
    confidence: str,
) -> Optional[int]:
    if not _enabled():
        return None

    payload = (
        datetime.now(timezone.utc).isoformat(),
        str(run_id or "").strip(),
        material_type_input,
        material_type_resolved,
        json.dumps(composition or {}, ensure_ascii=False),
        json.dumps(processing or {}, ensure_ascii=False),
        json.dumps(features or {}, ensure_ascii=False),
        int(top_k),
        prompt,
        llm_response,
        json.dumps(predicted_values or {}, ensure_ascii=False),
        confidence,
    )

    conn = _connect()
    try:
        cursor = conn.execute(
            """
            INSERT INTO prediction_prompt_logs (
                created_at,
                run_id,
                material_type_input,
                material_type_resolved,
                composition_json,
                processing_json,
                features_json,
                top_k,
                prompt_text,
                llm_response,
                predicted_values_json,
                confidence
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )
        conn.commit()
        return int(cursor.lastrowid)
    finally:
        conn.close()
