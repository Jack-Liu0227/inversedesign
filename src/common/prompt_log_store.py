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
    value = os.getenv("PREDICT_PROMPT_LOG_ENABLED", "true").strip().lower()
    return value in {"1", "true", "yes", "on"}


def _db_path() -> Path:
    raw = os.getenv("PREDICT_PROMPT_LOG_DB", "").strip()
    if raw:
        return Path(raw)
    return DEFAULT_DB_PATH


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS prediction_prompt_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
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
    conn.commit()


def log_prediction_prompt(
    *,
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

    path = _db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (
        datetime.now(timezone.utc).isoformat(),
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

    conn = sqlite3.connect(path)
    try:
        _ensure_schema(conn)
        cursor = conn.execute(
            """
            INSERT INTO prediction_prompt_logs (
                created_at,
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
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )
        conn.commit()
        return int(cursor.lastrowid)
    finally:
        conn.close()
