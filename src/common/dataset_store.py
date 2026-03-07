from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any

import pandas as pd

from src.common.db_paths import MATERIAL_DISCOVERY_WORKFLOW_DB
from src.fewshot.data_processing import DataProcessor
from src.fewshot.dataset_registry import get_dataset_registry


@dataclass
class DatasetMaterialRow:
    material_type: str
    source: str
    source_name: str
    source_row_key: str
    composition: dict[str, Any]
    processing: dict[str, Any]
    features: dict[str, Any]
    target_values: dict[str, Any]
    predicted_values: dict[str, Any]
    is_valid: bool
    judge_score: float
    judge_reasons: list[str]
    risk_tags: list[str]
    iteration: int = 0
    workflow_run_id: str = ""
    run_id: str = ""
    session_id: str = ""


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(MATERIAL_DISCOVERY_WORKFLOW_DB))
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS material_dataset_rows (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            material_type TEXT NOT NULL,
            source TEXT NOT NULL,
            source_name TEXT NOT NULL,
            source_row_key TEXT NOT NULL,
            composition_json TEXT NOT NULL,
            processing_json TEXT NOT NULL,
            features_json TEXT NOT NULL DEFAULT '{}',
            target_values_json TEXT NOT NULL,
            predicted_values_json TEXT NOT NULL,
            iteration INTEGER NOT NULL DEFAULT 0,
            is_valid INTEGER NOT NULL,
            judge_score REAL NOT NULL,
            judge_reasons_json TEXT NOT NULL,
            risk_tags_json TEXT NOT NULL,
            workflow_run_id TEXT NOT NULL DEFAULT '',
            run_id TEXT NOT NULL DEFAULT '',
            session_id TEXT NOT NULL DEFAULT '',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source, source_name, source_row_key)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_material_dataset_type_source
        ON material_dataset_rows(material_type, source, created_at DESC)
        """
    )
    table_columns = {
        str(row[1])
        for row in conn.execute("PRAGMA table_info('material_dataset_rows')").fetchall()
    }
    if "features_json" not in table_columns:
        conn.execute("ALTER TABLE material_dataset_rows ADD COLUMN features_json TEXT NOT NULL DEFAULT '{}'")
    if "iteration" not in table_columns:
        conn.execute("ALTER TABLE material_dataset_rows ADD COLUMN iteration INTEGER NOT NULL DEFAULT 0")
    if "run_id" not in table_columns:
        conn.execute("ALTER TABLE material_dataset_rows ADD COLUMN run_id TEXT NOT NULL DEFAULT ''")
        conn.execute("UPDATE material_dataset_rows SET run_id = workflow_run_id WHERE run_id = ''")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_material_dataset_run_id ON material_dataset_rows(run_id, created_at DESC)"
    )


def insert_dataset_rows(rows: list[DatasetMaterialRow]) -> int:
    if not rows:
        return 0
    conn = _connect()
    try:
        _ensure_schema(conn)
        conn.executemany(
            """
            INSERT OR REPLACE INTO material_dataset_rows (
                material_type, source, source_name, source_row_key,
                composition_json, processing_json, features_json, target_values_json, predicted_values_json, iteration,
                is_valid, judge_score, judge_reasons_json, risk_tags_json,
                workflow_run_id, run_id, session_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row.material_type,
                    row.source,
                    row.source_name,
                    row.source_row_key,
                    json.dumps(row.composition, ensure_ascii=False),
                    json.dumps(row.processing, ensure_ascii=False),
                    json.dumps(row.features, ensure_ascii=False),
                    json.dumps(row.target_values, ensure_ascii=False),
                    json.dumps(row.predicted_values, ensure_ascii=False),
                    int(row.iteration),
                    1 if row.is_valid else 0,
                    float(row.judge_score),
                    json.dumps(row.judge_reasons, ensure_ascii=False),
                    json.dumps(row.risk_tags, ensure_ascii=False),
                    row.workflow_run_id,
                    str(row.run_id or row.workflow_run_id),
                    row.session_id,
                )
                for row in rows
            ],
        )
        conn.commit()
        return len(rows)
    finally:
        conn.close()


def _normalize_df_value(value: Any) -> Any:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if pd.isna(value):
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped if stripped else None
    return value


def _split_storage_columns(df: pd.DataFrame, candidate_cols: list[str]) -> tuple[str, list[str]]:
    if not candidate_cols:
        return "", []
    preferred_names = ("heat treatment method", "processing_description")
    lower_to_original = {str(c).strip().lower(): c for c in candidate_cols}
    for name in preferred_names:
        matched = lower_to_original.get(str(name).strip().lower())
        if matched:
            features = [c for c in candidate_cols if c != matched]
            return str(matched), features
    return "", list(candidate_cols)


def _normalize_processing_text(value: Any) -> str:
    parsed = _normalize_df_value(value)
    if parsed is None:
        return ""
    text = str(parsed).strip()
    return text


def import_csv_datasets_to_db() -> dict[str, Any]:
    registry = get_dataset_registry()
    rows_to_insert: list[DatasetMaterialRow] = []
    imported_files = 0
    imported_rows = 0

    for material_type, spec in registry.items():
        processor = DataProcessor(input_file=str(spec.dataset_path), target_cols=spec.target_cols)
        df = processor.load_data()
        columns = processor.identify_columns(df)
        excluded_cols = set(columns.element_cols) | set(columns.target_cols)
        candidate_cols = [c for c in df.columns.tolist() if c not in excluded_cols]
        processing_col, storage_feature_cols = _split_storage_columns(df, candidate_cols)
        imported_files += 1
        for idx, (_, row) in enumerate(df.iterrows()):
            composition = {
                col: _normalize_df_value(row.get(col))
                for col in columns.element_cols
                if _normalize_df_value(row.get(col)) not in (None, 0, 0.0)
            }
            processing_text = _normalize_processing_text(row.get(processing_col)) if processing_col else ""
            processing = {"heat treatment method": processing_text or "No processing"}
            features = {
                col: _normalize_df_value(row.get(col))
                for col in storage_feature_cols
                if _normalize_df_value(row.get(col)) is not None
            }
            target_values = {
                col: _normalize_df_value(row.get(col))
                for col in columns.target_cols
                if _normalize_df_value(row.get(col)) is not None
            }
            rows_to_insert.append(
                DatasetMaterialRow(
                    material_type=material_type,
                    source="csv",
                    source_name=spec.dataset_path.name,
                    source_row_key=str(idx),
                    composition=composition,
                    processing=processing,
                    features=features,
                    target_values=target_values,
                    predicted_values={},
                    iteration=0,
                    is_valid=True,
                    judge_score=1.0,
                    judge_reasons=[],
                    risk_tags=[],
                )
            )
            imported_rows += 1

    written = insert_dataset_rows(rows_to_insert)
    return {
        "files": imported_files,
        "rows_scanned": imported_rows,
        "rows_written": written,
    }
