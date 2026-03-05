from __future__ import annotations

import sqlite3
from typing import Any

from ui.db.connection import db_manager
from ui.services.json_decode_service import decode_maybe_double_json
from ui.services.timezone_service import normalize_row_datetimes


class PredictionRepository:
    DB_KEY = "prediction_prompt_logs"

    def list_predictions(
        self,
        *,
        page: int,
        page_size: int,
        material_type: str | None = None,
        confidence: str | None = None,
        top_k: int | None = None,
        q: str | None = None,
        created_from: str | None = None,
        created_to: str | None = None,
    ) -> tuple[list[dict[str, Any]], int]:
        where: list[str] = []
        params: list[Any] = []

        if material_type:
            where.append("(material_type_input = ? OR material_type_resolved = ?)")
            params.extend([material_type, material_type])
        if confidence:
            where.append("confidence = ?")
            params.append(confidence)
        if top_k is not None:
            where.append("top_k = ?")
            params.append(top_k)
        if q:
            where.append("(prompt_text LIKE ? OR llm_response LIKE ?)")
            like = f"%{q}%"
            params.extend([like, like])
        if created_from:
            where.append("created_at >= ?")
            params.append(created_from)
        if created_to:
            where.append("created_at <= ?")
            params.append(created_to)

        where_sql = f"WHERE {' AND '.join(where)}" if where else ""

        try:
            with db_manager.connect(self.DB_KEY, readonly=True) as conn:
                total = conn.execute(
                    f"SELECT COUNT(*) AS c FROM prediction_prompt_logs {where_sql}", params
                ).fetchone()["c"]

                offset = (page - 1) * page_size
                rows = conn.execute(
                    f"""
                    SELECT id, created_at, material_type_input, material_type_resolved,
                           top_k, confidence, composition_json, processing_json,
                           features_json, predicted_values_json
                    FROM prediction_prompt_logs
                    {where_sql}
                    ORDER BY created_at DESC, id DESC
                    LIMIT ? OFFSET ?
                    """,
                    [*params, page_size, offset],
                ).fetchall()
        except sqlite3.OperationalError:
            return [], 0

        items: list[dict[str, Any]] = []
        for row in rows:
            item = normalize_row_datetimes(dict(row))
            item["composition"] = decode_maybe_double_json(item.pop("composition_json"))
            item["processing"] = decode_maybe_double_json(item.pop("processing_json"))
            item["features"] = decode_maybe_double_json(item.pop("features_json"))
            item["predicted_values"] = decode_maybe_double_json(item.pop("predicted_values_json"))
            items.append(item)
        return items, int(total)


prediction_repo = PredictionRepository()
