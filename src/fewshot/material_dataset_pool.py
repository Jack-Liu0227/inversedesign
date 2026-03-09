from __future__ import annotations

import json
from pathlib import Path
import sqlite3
from typing import Any

import pandas as pd

from .data_processing import DataProcessor
from .dataset_registry import resolve_dataset

ROOT = Path(__file__).resolve().parents[2]
MATERIAL_DISCOVERY_WORKFLOW_DB = ROOT / "db" / "material_agent_shared.db"


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(MATERIAL_DISCOVERY_WORKFLOW_DB))
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (str(table),),
    ).fetchone()
    return bool(row)


def _parse_json_dict(raw: Any) -> dict[str, Any]:
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        data = json.loads(text)
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _normalize_mounted_run_ids(raw: list[str] | str | None, *, current_run_id: str = "") -> list[str]:
    if isinstance(raw, str):
        values = [part.strip() for part in raw.split(",")]
    elif isinstance(raw, list):
        values = [str(item or "").strip() for item in raw]
    else:
        values = []
    normalized: list[str] = []
    current = str(current_run_id or "").strip()
    for value in values:
        if not value or value == current or value in normalized:
            continue
        normalized.append(value)
    return normalized


def _load_db_rows(
    *,
    material_type: str,
    mounted_workflow_run_ids: list[str],
) -> list[dict[str, Any]]:
    conn = _connect()
    try:
        if not _table_exists(conn, "material_dataset_rows"):
            return []
        where = ["material_type = ?"]
        params: list[Any] = [str(material_type or "").strip().lower()]
        mounted_clause = ""
        if mounted_workflow_run_ids:
            placeholders = ",".join("?" for _ in mounted_workflow_run_ids)
            mounted_clause = f" OR (source = 'workflow' AND workflow_run_id IN ({placeholders}))"
            params.extend(mounted_workflow_run_ids)
        rows = conn.execute(
            f"""
            SELECT source, source_name, source_row_key, workflow_run_id, run_note,
                   composition_json, processing_json, features_json, target_values_json, predicted_values_json
            FROM material_dataset_rows
            WHERE {" AND ".join(where)} AND (
                source = 'csv'
                {mounted_clause}
            )
            ORDER BY created_at DESC, id DESC
            """,
            params,
        ).fetchall()
    finally:
        conn.close()

    output: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for row in rows:
        key = (
            str(row["source"] or "").strip(),
            str(row["source_name"] or "").strip(),
            str(row["source_row_key"] or "").strip(),
        )
        if key in seen:
            continue
        seen.add(key)
        target_values = _parse_json_dict(row["target_values_json"])
        predicted_values = _parse_json_dict(row["predicted_values_json"])
        output.append(
            {
                "source": str(row["source"] or "").strip(),
                "source_name": str(row["source_name"] or "").strip(),
                "source_row_key": str(row["source_row_key"] or "").strip(),
                "workflow_run_id": str(row["workflow_run_id"] or "").strip(),
                "run_note": str(row["run_note"] or "").strip(),
                "composition": _parse_json_dict(row["composition_json"]),
                "processing": _parse_json_dict(row["processing_json"]),
                "features": _parse_json_dict(row["features_json"]),
                "target_values": target_values if target_values else predicted_values,
            }
        )
    return output


def _load_registry_rows(material_type: str) -> list[dict[str, Any]]:
    spec = resolve_dataset(material_type)
    processor = DataProcessor(
        input_file=str(spec.dataset_path),
        target_cols=spec.target_cols,
    )
    df = processor.load_data()
    columns = processor.identify_columns(df)
    rows: list[dict[str, Any]] = []
    for idx, (_, row) in enumerate(df.iterrows()):
        composition = {
            col: row.get(col)
            for col in columns.element_cols
            if pd.notna(row.get(col)) and str(row.get(col)).strip() not in {"", "0", "0.0"}
        }
        processing = {
            col: row.get(col)
            for col in columns.processing_cols
            if pd.notna(row.get(col)) and str(row.get(col)).strip()
        }
        features = {
            col: row.get(col)
            for col in columns.feature_cols
            if pd.notna(row.get(col)) and str(row.get(col)).strip()
        }
        target_values = {
            col: row.get(col)
            for col in columns.target_cols
            if pd.notna(row.get(col))
        }
        rows.append(
            {
                "source": "csv",
                "source_name": spec.dataset_path.name,
                "source_row_key": str(idx),
                "workflow_run_id": "",
                "run_note": "",
                "composition": composition,
                "processing": processing,
                "features": features,
                "target_values": target_values,
            }
        )
    return rows


def load_prediction_pool(
    *,
    material_type: str,
    mounted_workflow_run_ids: list[str] | str | None = None,
    current_workflow_run_id: str = "",
) -> list[dict[str, Any]]:
    normalized_material_type = str(material_type or "").strip().lower()
    normalized_mounted_run_ids = _normalize_mounted_run_ids(
        mounted_workflow_run_ids,
        current_run_id=current_workflow_run_id,
    )
    db_rows = _load_db_rows(
        material_type=normalized_material_type,
        mounted_workflow_run_ids=normalized_mounted_run_ids,
    )
    has_base_pool = any(str(row.get("source") or "").strip() == "csv" for row in db_rows)
    if has_base_pool:
        return db_rows

    registry_rows = _load_registry_rows(normalized_material_type)
    combined = registry_rows[:]
    seen = {
        (
            str(row.get("source") or "").strip(),
            str(row.get("source_name") or "").strip(),
            str(row.get("source_row_key") or "").strip(),
        )
        for row in combined
    }
    for row in db_rows:
        key = (
            str(row.get("source") or "").strip(),
            str(row.get("source_name") or "").strip(),
            str(row.get("source_row_key") or "").strip(),
        )
        if key in seen:
            continue
        seen.add(key)
        combined.append(row)
    return combined
