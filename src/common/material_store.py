from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any

from src.common.db_paths import MATERIAL_DISCOVERY_WORKFLOW_DB


@dataclass
class MaterialSampleRow:
    workflow_run_id: str
    session_id: str
    material_type: str
    goal: str
    round_index: int
    candidate_index: int
    composition: dict[str, Any]
    processing: dict[str, Any]
    predicted_values: dict[str, Any]
    confidence: str
    prediction_error: str
    is_valid: bool
    judge_score: float
    judge_reasons: list[str]
    risk_tags: list[str]
    judge_model: str


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(MATERIAL_DISCOVERY_WORKFLOW_DB))
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS material_samples (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            workflow_run_id TEXT NOT NULL,
            session_id TEXT NOT NULL,
            material_type TEXT NOT NULL,
            goal TEXT NOT NULL,
            round_index INTEGER NOT NULL,
            candidate_index INTEGER NOT NULL,
            composition_json TEXT NOT NULL,
            processing_json TEXT NOT NULL,
            predicted_values_json TEXT NOT NULL,
            confidence TEXT NOT NULL,
            prediction_error TEXT NOT NULL,
            is_valid INTEGER NOT NULL,
            judge_score REAL NOT NULL,
            judge_reasons_json TEXT NOT NULL,
            risk_tags_json TEXT NOT NULL,
            judge_model TEXT NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_samples_valid_round ON material_samples(is_valid, round_index DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_samples_goal_material ON material_samples(material_type, is_valid)"
    )
    conn.execute(
        """
        CREATE VIEW IF NOT EXISTS retrieval_samples_view AS
        SELECT * FROM material_samples WHERE is_valid = 1
        """
    )


def insert_sample_rows(rows: list[MaterialSampleRow]) -> int:
    if not rows:
        return 0
    conn = _connect()
    try:
        _ensure_schema(conn)
        conn.executemany(
            """
            INSERT INTO material_samples (
                workflow_run_id, session_id, material_type, goal, round_index, candidate_index,
                composition_json, processing_json, predicted_values_json, confidence, prediction_error,
                is_valid, judge_score, judge_reasons_json, risk_tags_json, judge_model
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row.workflow_run_id,
                    row.session_id,
                    row.material_type,
                    row.goal,
                    row.round_index,
                    row.candidate_index,
                    json.dumps(row.composition, ensure_ascii=False),
                    json.dumps(row.processing, ensure_ascii=False),
                    json.dumps(row.predicted_values, ensure_ascii=False),
                    row.confidence,
                    row.prediction_error,
                    1 if row.is_valid else 0,
                    float(row.judge_score),
                    json.dumps(row.judge_reasons, ensure_ascii=False),
                    json.dumps(row.risk_tags, ensure_ascii=False),
                    row.judge_model,
                )
                for row in rows
            ],
        )
        conn.commit()
        return len(rows)
    finally:
        conn.close()


def fetch_valid_samples_context(material_type: str, limit: int = 12) -> list[dict[str, Any]]:
    conn = _connect()
    try:
        _ensure_schema(conn)
        rows = conn.execute(
            """
            SELECT composition_json, processing_json, predicted_values_json, judge_score
            FROM retrieval_samples_view
            WHERE material_type = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (material_type, max(1, int(limit))),
        ).fetchall()
    finally:
        conn.close()

    output: list[dict[str, Any]] = []
    for comp_json, proc_json, pred_json, score in rows:
        try:
            composition = json.loads(comp_json)
        except Exception:
            composition = {}
        try:
            processing = json.loads(proc_json)
        except Exception:
            processing = {}
        try:
            predicted = json.loads(pred_json)
        except Exception:
            predicted = {}
        output.append(
            {
                "composition": composition if isinstance(composition, dict) else {},
                "processing": processing if isinstance(processing, dict) else {},
                "predicted_values": predicted if isinstance(predicted, dict) else {},
                "judge_score": float(score or 0.0),
            }
        )
    return output


def fetch_round_samples_context(
    workflow_run_id: str,
    material_type: str,
    round_index: int,
    limit: int = 12,
) -> list[dict[str, Any]]:
    conn = _connect()
    try:
        _ensure_schema(conn)
        rows = conn.execute(
            """
            SELECT candidate_index, composition_json, processing_json, predicted_values_json, is_valid, judge_score
            FROM material_samples
            WHERE workflow_run_id = ? AND material_type = ? AND round_index = ?
            ORDER BY candidate_index ASC, id ASC
            LIMIT ?
            """,
            (str(workflow_run_id), str(material_type), int(round_index), max(1, int(limit))),
        ).fetchall()
    finally:
        conn.close()

    output: list[dict[str, Any]] = []
    for candidate_index, comp_json, proc_json, pred_json, is_valid, score in rows:
        try:
            composition = json.loads(comp_json)
        except Exception:
            composition = {}
        try:
            processing = json.loads(proc_json)
        except Exception:
            processing = {}
        try:
            predicted = json.loads(pred_json)
        except Exception:
            predicted = {}
        output.append(
            {
                "candidate_index": int(candidate_index),
                "composition": composition if isinstance(composition, dict) else {},
                "processing": processing if isinstance(processing, dict) else {},
                "predicted_values": predicted if isinstance(predicted, dict) else {},
                "is_valid": bool(is_valid),
                "judge_score": float(score or 0.0),
            }
        )
    return output


def next_round_index(workflow_run_id: str) -> int:
    conn = _connect()
    try:
        _ensure_schema(conn)
        row = conn.execute(
            "SELECT COALESCE(MAX(round_index), 0) FROM material_samples WHERE workflow_run_id = ?",
            (workflow_run_id,),
        ).fetchone()
    finally:
        conn.close()
    max_round = int(row[0] or 0) if row else 0
    return max_round + 1
