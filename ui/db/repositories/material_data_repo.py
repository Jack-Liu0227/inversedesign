from __future__ import annotations

import json
from decimal import Decimal, InvalidOperation
from typing import Any

from src.fewshot.dataset_registry import get_dataset_registry
from ui.db.connection import db_manager
from ui.services.timezone_service import beijing_range_to_utc_sql, normalize_row_datetimes


class MaterialDataRepository:
    DB_KEY = "material_agent_shared"
    TABLE = "material_dataset_rows"

    def table_exists(self) -> bool:
        with db_manager.connect(self.DB_KEY, readonly=True) as conn:
            row = conn.execute(
                """
                SELECT 1 AS ok
                FROM sqlite_master
                WHERE type='table' AND name=?
                LIMIT 1
                """,
                (self.TABLE,),
            ).fetchone()
        return bool(row)

    def list_filter_options(self) -> dict[str, list[str]]:
        if not self.table_exists():
            return {"material_types": [], "sources": []}
        with db_manager.connect(self.DB_KEY, readonly=True) as conn:
            mt_rows = conn.execute(
                f'SELECT DISTINCT material_type FROM "{self.TABLE}" WHERE material_type <> \'\' ORDER BY material_type'
            ).fetchall()
            src_rows = conn.execute(
                f'SELECT DISTINCT source FROM "{self.TABLE}" WHERE source <> \'\' ORDER BY source'
            ).fetchall()
        return {
            "material_types": [str(r["material_type"]) for r in mt_rows],
            "sources": [str(r["source"]) for r in src_rows],
        }

    def list_rows(
        self,
        *,
        page: int = 1,
        page_size: int = 50,
        material_type: str = "",
        source: str = "",
        q: str = "",
        created_from: str = "",
        created_to: str = "",
        valid_only: bool = False,
        sort_by: str = "id",
        sort_order: str = "desc",
    ) -> tuple[list[dict[str, Any]], int]:
        if not self.table_exists():
            return [], 0
        where = []
        params: list[Any] = []
        if material_type.strip():
            where.append("material_type = ?")
            params.append(material_type.strip())
        if source.strip():
            where.append("source = ?")
            params.append(source.strip())
        if valid_only:
            where.append("is_valid = 1")
        if q.strip():
            like = f"%{q.strip()}%"
            where.append(
                "("
                "material_type LIKE ? OR source LIKE ? OR source_name LIKE ? OR "
                "composition_json LIKE ? OR processing_json LIKE ? OR features_json LIKE ? OR target_values_json LIKE ? OR "
                "predicted_values_json LIKE ? OR judge_reasons_json LIKE ? OR risk_tags_json LIKE ?"
                ")"
            )
            params.extend([like] * 10)
        utc_from, utc_to_exclusive = beijing_range_to_utc_sql(
            created_from=str(created_from or ""),
            created_to=str(created_to or ""),
        )
        if utc_from:
            where.append("created_at >= ?")
            params.append(utc_from)
        if utc_to_exclusive:
            where.append("created_at < ?")
            params.append(utc_to_exclusive)
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        current_page = max(1, int(page))
        current_size = max(10, min(int(page_size), 200))
        offset = (current_page - 1) * current_size
        normalized_order = "asc" if str(sort_order).lower() == "asc" else "desc"
        target_sort_key = self._extract_target_sort_key(sort_by)
        with db_manager.connect(self.DB_KEY, readonly=True) as conn:
            total = int(
                conn.execute(
                    f'SELECT COUNT(*) AS c FROM "{self.TABLE}" {where_sql}',
                    params,
                ).fetchone()["c"]
            )
            base_sql = f"""
                SELECT id, material_type, source, source_name, source_row_key,
                       composition_json, processing_json, features_json, target_values_json, predicted_values_json,
                       iteration, is_valid, judge_score, judge_reasons_json, risk_tags_json, workflow_run_id, session_id, created_at
                FROM "{self.TABLE}"
                {where_sql}
            """
            rows = conn.execute(base_sql, params).fetchall()
            parsed_rows = [self._format_row(dict(r)) for r in rows]
            if target_sort_key:
                parsed_rows.sort(
                    key=lambda row: self._auto_sort_tuple(row.get("target_values_map", {}).get(target_sort_key)),
                    reverse=(normalized_order == "desc"),
                )
            else:
                order_col = self._normalize_sort_column(sort_by)
                parsed_rows.sort(
                    key=lambda row: self._auto_sort_tuple(row.get(order_col)),
                    reverse=(normalized_order == "desc"),
                )
            page_rows = parsed_rows[offset : offset + current_size]
            return page_rows, total

    def delete_rows(self, ids: list[int]) -> int:
        if not self.table_exists():
            return 0
        unique_ids_set: set[int] = set()
        for raw in ids:
            try:
                value = int(raw)
            except (TypeError, ValueError):
                continue
            if value > 0:
                unique_ids_set.add(value)
        unique_ids = sorted(unique_ids_set)
        if not unique_ids:
            return 0
        placeholders = ",".join("?" for _ in unique_ids)
        with db_manager.connect(self.DB_KEY, readonly=False) as conn:
            cur = conn.execute(
                f'DELETE FROM "{self.TABLE}" WHERE id IN ({placeholders})',
                unique_ids,
            )
            conn.commit()
            return int(cur.rowcount or 0)

    def list_target_columns(self, material_type: str = "") -> list[str]:
        columns: set[str] = set()
        try:
            registry = get_dataset_registry()
            if material_type and material_type in registry:
                columns.update([str(c) for c in registry[material_type].target_cols])
            else:
                for spec in registry.values():
                    columns.update([str(c) for c in spec.target_cols])
        except Exception:
            pass

        if self.table_exists():
            where = "WHERE source='csv'"
            params: list[Any] = []
            if material_type.strip():
                where += " AND material_type=?"
                params.append(material_type.strip())
            with db_manager.connect(self.DB_KEY, readonly=True) as conn:
                rows = conn.execute(
                    f'SELECT target_values_json FROM "{self.TABLE}" {where} ORDER BY id DESC LIMIT 2000',
                    params,
                ).fetchall()
            for row in rows:
                data = self._parse_json_dict(row["target_values_json"])
                columns.update([str(k) for k in data.keys()])

        return sorted([c for c in columns if c.strip()])

    @staticmethod
    def _format_json_compact(raw: Any) -> str:
        text = str(raw or "")
        if not text.strip():
            return "{}"
        try:
            data = json.loads(text)
            return json.dumps(data, ensure_ascii=False, separators=(",", ":"))
        except Exception:
            return text

    def _format_row(self, row: dict[str, Any]) -> dict[str, Any]:
        row = normalize_row_datetimes(row)
        target_map = self._parse_json_dict(row.get("target_values_json"))
        predicted_map = self._parse_json_dict(row.get("predicted_values_json"))
        display_target_map = dict(target_map)
        for key, value in predicted_map.items():
            if key not in display_target_map or display_target_map.get(key) in {"", None}:
                display_target_map[key] = value
        processing_method = self._extract_heat_treatment_method(row.get("processing_json"))
        row["composition_text"] = self._format_json_compact(row.get("composition_json"))
        row["processing_text"] = processing_method
        row["features_text"] = self._format_json_compact(row.get("features_json"))
        row["target_values_text"] = self._format_json_compact(row.get("target_values_json"))
        row["target_values_map"] = target_map
        row["display_target_values_map"] = display_target_map
        row["predicted_values_text"] = self._format_json_compact(row.get("predicted_values_json"))
        row["judge_reasons_text"] = self._format_json_compact(row.get("judge_reasons_json"))
        row["risk_tags_text"] = self._format_json_compact(row.get("risk_tags_json"))
        row["is_valid"] = int(row.get("is_valid") or 0)
        return row

    @staticmethod
    def _parse_json_dict(raw: Any) -> dict[str, Any]:
        text = str(raw or "").strip()
        if not text:
            return {}
        try:
            data = json.loads(text)
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    @staticmethod
    def _extract_heat_treatment_method(raw: Any) -> str:
        text = str(raw or "").strip()
        if not text:
            return ""
        parsed: Any
        try:
            parsed = json.loads(text)
        except Exception:
            return text
        if isinstance(parsed, str):
            return parsed.strip()
        if not isinstance(parsed, dict):
            return text

        normalized_items = {
            str(k).strip().lower().replace("_", " "): v
            for k, v in parsed.items()
            if str(k).strip()
        }
        for key in ("heat treatment method", "processing description", "process description", "process", "method", "route"):
            value = normalized_items.get(key)
            if value is None:
                continue
            method = str(value).strip()
            if method:
                return method

        values = [str(v).strip() for v in parsed.values() if str(v or "").strip()]
        return "; ".join(values)

    def normalize_processing_rows(self) -> int:
        if not self.table_exists():
            return 0
        with db_manager.connect(self.DB_KEY, readonly=False) as conn:
            rows = conn.execute(f'SELECT id, processing_json FROM "{self.TABLE}"').fetchall()
            updates: list[tuple[str, int]] = []
            for row in rows:
                row_id = int(row["id"])
                method = self._extract_heat_treatment_method(row["processing_json"])
                normalized = {"heat treatment method": method} if method else {}
                normalized_json = json.dumps(normalized, ensure_ascii=False)
                if str(row["processing_json"] or "") != normalized_json:
                    updates.append((normalized_json, row_id))
            if not updates:
                return 0
            conn.executemany(
                f'UPDATE "{self.TABLE}" SET processing_json = ? WHERE id = ?',
                updates,
            )
            conn.commit()
            return len(updates)

    @staticmethod
    def _normalize_sort_column(sort_by: str) -> str:
        aliases = {
            "id": "id",
            "material_type": "material_type",
            "source": "source",
            "source_name": "source_name",
            "source_row_key": "source_row_key",
            "iteration": "iteration",
            "is_valid": "is_valid",
            "judge_score": "judge_score",
            "composition": "composition_json",
            "processing": "processing_json",
            "features": "features_json",
            "target_values": "target_values_json",
            "predicted_values": "predicted_values_json",
            "judge_reasons": "judge_reasons_json",
            "risk_tags": "risk_tags_json",
            "workflow_run_id": "workflow_run_id",
            "session_id": "session_id",
            "created_at": "created_at",
        }
        key = str(sort_by or "").strip()
        return aliases.get(key, "id")

    @staticmethod
    def _extract_target_sort_key(sort_by: str) -> str:
        text = str(sort_by or "").strip()
        if text.startswith("target:"):
            return text.split("target:", 1)[1].strip()
        return ""

    @staticmethod
    def _target_sort_tuple(value: Any) -> tuple[int, float, str]:
        return MaterialDataRepository._auto_sort_tuple(value)

    @staticmethod
    def _auto_sort_tuple(value: Any) -> tuple[int, int, float, str]:
        if value is None:
            return (1, 2, 0.0, "")

        if isinstance(value, bool):
            return (0, 0, float(int(value)), "")
        if isinstance(value, (int, float)):
            return (0, 0, float(value), "")

        text = str(value).strip()
        if text == "":
            return (1, 2, 0.0, "")

        try:
            numeric = float(Decimal(text))
            return (0, 0, numeric, "")
        except (InvalidOperation, ValueError):
            return (0, 1, 0.0, text.lower())


material_data_repo = MaterialDataRepository()
