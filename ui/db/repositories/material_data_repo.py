from __future__ import annotations

import json
import math
from typing import Any, Callable

from src.fewshot.dataset_registry import get_dataset_registry
from ui.db.connection import db_manager
from ui.services.sort_service import auto_sort_tuple, normalize_sort_order
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

    def table_columns(self) -> set[str]:
        if not self.table_exists():
            return set()
        with db_manager.connect(self.DB_KEY, readonly=True) as conn:
            return {
                str(row["name"]).strip().lower()
                for row in conn.execute(f'PRAGMA table_info("{self.TABLE}")').fetchall()
            }

    def list_filter_options(self) -> dict[str, list[str]]:
        if not self.table_exists():
            return {"material_types": [], "sources": [], "workflow_run_ids": [], "run_notes": []}
        columns = self.table_columns()
        with db_manager.connect(self.DB_KEY, readonly=True) as conn:
            mt_rows = conn.execute(
                f'SELECT DISTINCT material_type FROM "{self.TABLE}" WHERE material_type <> \'\' ORDER BY material_type'
            ).fetchall()
            src_rows = conn.execute(
                f'SELECT DISTINCT source FROM "{self.TABLE}" WHERE source <> \'\' ORDER BY source'
            ).fetchall()
            run_rows = (
                conn.execute(
                    f'SELECT DISTINCT workflow_run_id FROM "{self.TABLE}" WHERE workflow_run_id <> \'\' ORDER BY workflow_run_id DESC'
                ).fetchall()
                if "workflow_run_id" in columns
                else []
            )
            note_rows = (
                conn.execute(
                    f'SELECT DISTINCT run_note FROM "{self.TABLE}" WHERE run_note <> \'\' ORDER BY run_note'
                ).fetchall()
                if "run_note" in columns
                else []
            )
        return {
            "material_types": [str(r["material_type"]) for r in mt_rows],
            "sources": [str(r["source"]) for r in src_rows],
            "workflow_run_ids": [str(r["workflow_run_id"]) for r in run_rows],
            "run_notes": [str(r["run_note"]) for r in note_rows],
        }

    def list_rows(
        self,
        *,
        page: int = 1,
        page_size: int = 50,
        material_type: str = "",
        source: str = "",
        q: str = "",
        workflow_run_id: str = "",
        run_note: str = "",
        created_from: str = "",
        created_to: str = "",
        valid_only: bool = False,
        sort_by: str = "id",
        sort_order: str = "desc",
    ) -> tuple[list[dict[str, Any]], int]:
        if not self.table_exists():
            return [], 0
        parsed_rows = self._load_filtered_rows(
            material_type=material_type,
            source=source,
            q=q,
            workflow_run_id=workflow_run_id,
            run_note=run_note,
            created_from=created_from,
            created_to=created_to,
            valid_only=valid_only,
        )
        current_page = max(1, int(page))
        current_size = max(10, min(int(page_size), 200))
        offset = (current_page - 1) * current_size
        normalized_order = normalize_sort_order(sort_order, default="desc")
        target_sort_key = self._extract_target_sort_key(sort_by)
        total = len(parsed_rows)
        if target_sort_key:
            parsed_rows = self._sort_rows_with_nulls_last(
                parsed_rows,
                value_getter=lambda row: row.get("display_target_values_map", {}).get(target_sort_key),
                descending=(normalized_order == "desc"),
            )
        else:
            order_col = self._normalize_sort_column(sort_by)
            parsed_rows = self._sort_rows_with_nulls_last(
                parsed_rows,
                value_getter=lambda row: row.get(order_col),
                descending=(normalized_order == "desc"),
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

    def build_analytics(
        self,
        *,
        material_type: str = "",
        source: str = "",
        q: str = "",
        workflow_run_id: str = "",
        run_note: str = "",
        created_from: str = "",
        created_to: str = "",
        valid_only: bool = False,
        properties: list[str] | None = None,
        pareto_x: str = "",
        pareto_y: str = "",
    ) -> dict[str, Any]:
        rows = self._load_filtered_rows(
            material_type=material_type,
            source=source,
            q=q,
            workflow_run_id=workflow_run_id,
            run_note=run_note,
            created_from=created_from,
            created_to=created_to,
            valid_only=valid_only,
        )
        available_properties = self._collect_available_properties(rows, material_type=material_type)
        selected_properties = self._resolve_selected_properties(
            available_properties,
            requested=properties or [],
        )
        objective_map = {prop: self._objective_direction(prop) for prop in available_properties}
        return {
            "available_properties": available_properties,
            "selected_properties": selected_properties,
            "objective_map": objective_map,
            "row_count": len(rows),
            "trend": self._build_trend_series(
                rows,
                properties=selected_properties,
                objective_map=objective_map,
            ),
            "pareto": self._build_pareto_series(
                rows,
                available_properties=available_properties,
                objective_map=objective_map,
                pareto_x=pareto_x,
                pareto_y=pareto_y,
            ),
        }

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
        row["predicted_values_map"] = predicted_map
        row["predicted_values_text"] = self._format_json_compact(row.get("predicted_values_json"))
        row["judge_reasons_text"] = self._format_json_compact(row.get("judge_reasons_json"))
        row["risk_tags_text"] = self._format_json_compact(row.get("risk_tags_json"))
        row["is_valid"] = int(row.get("is_valid") or 0)
        row["run_note"] = str(row.get("run_note") or "").strip()
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

    def _load_filtered_rows(
        self,
        *,
        material_type: str = "",
        source: str = "",
        q: str = "",
        workflow_run_id: str = "",
        run_note: str = "",
        created_from: str = "",
        created_to: str = "",
        valid_only: bool = False,
    ) -> list[dict[str, Any]]:
        columns = self.table_columns()
        where = []
        params: list[Any] = []
        if material_type.strip():
            where.append("material_type = ?")
            params.append(material_type.strip())
        if source.strip():
            where.append("source = ?")
            params.append(source.strip())
        if workflow_run_id.strip() and "workflow_run_id" in columns:
            where.append("workflow_run_id = ?")
            params.append(workflow_run_id.strip())
        if run_note.strip() and "run_note" in columns:
            where.append("run_note = ?")
            params.append(run_note.strip())
        if valid_only:
            where.append("is_valid = 1")
        if q.strip():
            like = f"%{q.strip()}%"
            q_clauses = [
                "material_type LIKE ?",
                "source LIKE ?",
                "source_name LIKE ?",
            ]
            q_params: list[Any] = [like, like, like]
            if "workflow_run_id" in columns:
                q_clauses.append("workflow_run_id LIKE ?")
                q_params.append(like)
            if "run_note" in columns:
                q_clauses.append("run_note LIKE ?")
                q_params.append(like)
            q_clauses.extend(
                [
                    "composition_json LIKE ?",
                    "processing_json LIKE ?",
                    "features_json LIKE ?",
                    "target_values_json LIKE ?",
                    "predicted_values_json LIKE ?",
                    "judge_reasons_json LIKE ?",
                    "risk_tags_json LIKE ?",
                ]
            )
            q_params.extend([like] * 7)
            where.append(
                "(" + " OR ".join(q_clauses) + ")"
            )
            params.extend(q_params)
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
        run_note_select = "run_note" if "run_note" in columns else "'' AS run_note"
        workflow_run_id_select = "workflow_run_id" if "workflow_run_id" in columns else "'' AS workflow_run_id"
        with db_manager.connect(self.DB_KEY, readonly=True) as conn:
            base_sql = f"""
                SELECT id, material_type, source, source_name, source_row_key,
                       composition_json, processing_json, features_json, target_values_json, predicted_values_json,
                       iteration, is_valid, judge_score, judge_reasons_json, risk_tags_json, {workflow_run_id_select}, session_id, {run_note_select}, created_at
                FROM "{self.TABLE}"
                {where_sql}
            """
            rows = conn.execute(base_sql, params).fetchall()
        return [self._format_row(dict(r)) for r in rows]

    def list_recent_runs(self, limit: int = 100) -> list[dict[str, Any]]:
        with db_manager.connect(self.DB_KEY, readonly=True) as conn:
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='workflow_run_meta' LIMIT 1"
            ).fetchone()
            if not exists:
                return []
            rows = conn.execute(
                """
                SELECT workflow_run_id, session_id, material_type, run_note, mounted_run_ids_json, created_at
                FROM workflow_run_meta
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
        output: list[dict[str, Any]] = []
        for row in rows:
            payload = normalize_row_datetimes(dict(row))
            try:
                mounted_ids = json.loads(str(payload.get("mounted_run_ids_json") or "[]"))
            except Exception:
                mounted_ids = []
            payload["mounted_workflow_run_ids"] = mounted_ids if isinstance(mounted_ids, list) else []
            output.append(payload)
        return output

    def _collect_available_properties(
        self,
        rows: list[dict[str, Any]],
        *,
        material_type: str = "",
    ) -> list[str]:
        columns: set[str] = set(self.list_target_columns(material_type))
        for row in rows:
            predicted = row.get("predicted_values_map", {})
            if not isinstance(predicted, dict):
                continue
            columns.update(str(k) for k in predicted.keys() if str(k).strip())
        return sorted(columns)

    @staticmethod
    def _resolve_selected_properties(available_properties: list[str], requested: list[str]) -> list[str]:
        available_set = {str(item) for item in available_properties}
        selected: list[str] = []
        for prop in requested:
            key = str(prop or "").strip()
            if key and key in available_set and key not in selected:
                selected.append(key)
        if selected:
            return selected
        return available_properties[: min(4, len(available_properties))]

    def _build_trend_series(
        self,
        rows: list[dict[str, Any]],
        *,
        properties: list[str],
        objective_map: dict[str, str],
    ) -> dict[str, Any]:
        iteration_values: dict[int, dict[str, list[dict[str, Any]]]] = {}
        for row in rows:
            iteration = self._coerce_iteration(row.get("iteration"))
            if iteration is None:
                continue
            predicted_map = row.get("predicted_values_map", {})
            if not isinstance(predicted_map, dict):
                continue
            sample_key = self._build_sample_key(row)
            sample_label = str(row.get("source_name") or row.get("source_row_key") or row.get("id") or "").strip()
            bucket = iteration_values.setdefault(iteration, {})
            for prop in properties:
                value = self._coerce_float(predicted_map.get(prop))
                if value is None:
                    continue
                bucket.setdefault(prop, []).append(
                    {
                        "value": value,
                        "sample_key": sample_key,
                        "sample_label": sample_label,
                        "row_id": int(row.get("id") or 0),
                    }
                )

        iterations = sorted(iteration_values.keys())
        series: list[dict[str, Any]] = []
        for prop in properties:
            direction = objective_map.get(prop, "max")
            points: list[dict[str, Any]] = []
            all_points: list[dict[str, Any]] = []
            raw_values: list[float] = []
            for iteration in iterations:
                entries = iteration_values.get(iteration, {}).get(prop, [])
                if not entries:
                    continue
                values = [float(item["value"]) for item in entries]
                best_value = max(values) if direction == "max" else min(values)
                avg_value = sum(values) / len(values)
                raw_values.append(best_value)
                sorted_entries = sorted(
                    entries,
                    key=lambda item: float(item["value"]),
                    reverse=(direction == "max"),
                )
                best_entry = sorted_entries[0]
                for rank, entry in enumerate(sorted_entries, start=1):
                    all_points.append(
                        {
                            "iteration": iteration,
                            "value": round(float(entry["value"]), 6),
                            "rank": rank,
                            "count": len(sorted_entries),
                            "sample_key": str(entry["sample_key"]),
                            "sample_label": str(entry["sample_label"]),
                            "row_id": int(entry["row_id"]),
                        }
                    )
                points.append(
                    {
                        "iteration": iteration,
                        "best": round(best_value, 6),
                        "avg": round(avg_value, 6),
                        "count": len(values),
                        "sample_key": str(best_entry["sample_key"]),
                        "sample_label": str(best_entry["sample_label"]),
                        "row_id": int(best_entry["row_id"]),
                    }
                )
            if not points:
                continue
            series.append(
                {
                    "property": prop,
                    "direction": direction,
                    "points": points,
                    "all_points": all_points,
                    "raw_min": round(min(raw_values), 6),
                    "raw_max": round(max(raw_values), 6),
                }
            )
        return {"iterations": iterations, "series": series}

    @staticmethod
    def _build_sample_key(row: dict[str, Any]) -> str:
        row_id = int(row.get("id") or 0)
        source = str(row.get("source") or "").strip()
        source_name = str(row.get("source_name") or "").strip()
        source_row_key = str(row.get("source_row_key") or "").strip()
        if source and source_name and source_row_key:
            return f"{source}:{source_name}:{source_row_key}"
        if source_name and source_row_key:
            return f"{source_name}:{source_row_key}"
        if source_name:
            return source_name
        if row_id > 0:
            return f"row:{row_id}"
        return "unknown-sample"

    def _build_pareto_series(
        self,
        rows: list[dict[str, Any]],
        *,
        available_properties: list[str],
        objective_map: dict[str, str],
        pareto_x: str = "",
        pareto_y: str = "",
    ) -> dict[str, Any]:
        x_prop, y_prop = self._resolve_pareto_axes(
            available_properties,
            pareto_x=pareto_x,
            pareto_y=pareto_y,
        )
        if not x_prop or not y_prop or x_prop == y_prop:
            return {
                "x_property": x_prop,
                "y_property": y_prop,
                "x_direction": objective_map.get(x_prop or "", "max"),
                "y_direction": objective_map.get(y_prop or "", "max"),
                "iterations": [],
                "points": [],
                "frontier_by_iteration": [],
                "initial_frontier_ids": [],
                "has_initial_dataset": False,
                "initial_dataset_count": 0,
                "predicted_count": 0,
            }

        points: list[dict[str, Any]] = []
        predicted_points: list[dict[str, Any]] = []
        initial_points: list[dict[str, Any]] = []
        initial_dataset_count = 0
        iterations_set: set[int] = set()
        for row in rows:
            predicted_map = row.get("predicted_values_map", {})
            iteration = self._coerce_iteration(row.get("iteration"))
            row_id = int(row.get("id") or 0)
            source = str(row.get("source") or "").strip().lower()
            source_name = str(row.get("source_name") or "")
            material_type = str(row.get("material_type") or "")

            if isinstance(predicted_map, dict) and iteration is not None:
                x_value = self._coerce_float(predicted_map.get(x_prop))
                y_value = self._coerce_float(predicted_map.get(y_prop))
                if x_value is not None and y_value is not None:
                    iterations_set.add(iteration)
                    point = {
                        "id": row_id,
                        "iteration": iteration,
                        "x": round(x_value, 6),
                        "y": round(y_value, 6),
                        "source_name": source_name,
                        "material_type": material_type,
                        "point_kind": "predicted",
                        "source_group": "predicted",
                    }
                    points.append(point)
                    predicted_points.append(point)

            target_map = row.get("target_values_map", {})
            if source == "csv" and isinstance(target_map, dict):
                x_value = self._coerce_float(target_map.get(x_prop))
                y_value = self._coerce_float(target_map.get(y_prop))
                if x_value is not None and y_value is not None:
                    points.append(
                        {
                            "id": row_id,
                            "iteration": 0,
                            "x": round(x_value, 6),
                            "y": round(y_value, 6),
                            "source_name": source_name,
                            "material_type": material_type,
                            "point_kind": "initial_dataset",
                            "source_group": "initial_dataset",
                        }
                    )
                    initial_points.append(points[-1])
                    initial_dataset_count += 1
        iterations = sorted(iterations_set)
        sorted_points = sorted(
            points,
            key=lambda item: (
                1 if str(item.get("point_kind") or "") == "initial_dataset" else 0,
                int(item["iteration"]),
                int(item["id"]),
            ),
        )
        sorted_predicted_points = sorted(predicted_points, key=lambda item: (int(item["iteration"]), int(item["id"])))
        sorted_initial_points = sorted(initial_points, key=lambda item: int(item["id"]))
        frontier_by_iteration: list[dict[str, Any]] = []
        x_direction = objective_map.get(x_prop, "max")
        y_direction = objective_map.get(y_prop, "max")
        initial_frontier_ids = self._pareto_frontier_ids(
            sorted_initial_points,
            x_direction=x_direction,
            y_direction=y_direction,
        )
        for iteration in iterations:
            visible_points = [item for item in sorted_predicted_points if int(item["iteration"]) <= iteration]
            combined_points = [*visible_points, *sorted_initial_points]
            frontier_by_iteration.append(
                {
                    "iteration": iteration,
                    "visible_ids": [int(item["id"]) for item in visible_points],
                    "predicted_frontier_ids": self._pareto_frontier_ids(
                        visible_points,
                        x_direction=x_direction,
                        y_direction=y_direction,
                    ),
                    "combined_frontier_ids": self._pareto_frontier_ids(
                        combined_points,
                        x_direction=x_direction,
                        y_direction=y_direction,
                    ),
                }
            )
        return {
            "x_property": x_prop,
            "y_property": y_prop,
            "x_direction": x_direction,
            "y_direction": y_direction,
            "iterations": iterations,
            "points": sorted_points,
            "frontier_by_iteration": frontier_by_iteration,
            "initial_frontier_ids": initial_frontier_ids,
            "has_initial_dataset": initial_dataset_count > 0,
            "initial_dataset_count": initial_dataset_count,
            "predicted_count": len(sorted_predicted_points),
        }

    @staticmethod
    def _resolve_pareto_axes(
        available_properties: list[str],
        *,
        pareto_x: str,
        pareto_y: str,
    ) -> tuple[str, str]:
        if not available_properties:
            return "", ""
        normalized = [str(item) for item in available_properties if str(item).strip()]
        x = pareto_x if pareto_x in normalized else (normalized[0] if normalized else "")
        y_candidates = [item for item in normalized if item != x]
        y = pareto_y if pareto_y in y_candidates else (y_candidates[0] if y_candidates else "")
        return x, y

    @staticmethod
    def _pareto_frontier_ids(
        points: list[dict[str, Any]],
        *,
        x_direction: str,
        y_direction: str,
    ) -> list[int]:
        frontier: list[int] = []
        for index, point in enumerate(points):
            dominated = False
            for other_index, other in enumerate(points):
                if index == other_index:
                    continue
                if MaterialDataRepository._dominates(
                    other,
                    point,
                    x_direction=x_direction,
                    y_direction=y_direction,
                ):
                    dominated = True
                    break
            if not dominated:
                frontier.append(int(point["id"]))
        return frontier

    @staticmethod
    def _dominates(
        left: dict[str, Any],
        right: dict[str, Any],
        *,
        x_direction: str,
        y_direction: str,
    ) -> bool:
        left_x = float(left["x"])
        left_y = float(left["y"])
        right_x = float(right["x"])
        right_y = float(right["y"])
        better_x = left_x >= right_x if x_direction == "max" else left_x <= right_x
        better_y = left_y >= right_y if y_direction == "max" else left_y <= right_y
        strict_x = left_x > right_x if x_direction == "max" else left_x < right_x
        strict_y = left_y > right_y if y_direction == "max" else left_y < right_y
        return better_x and better_y and (strict_x or strict_y)

    @staticmethod
    def _objective_direction(property_name: str) -> str:
        key = str(property_name or "").strip().lower()
        minimize_tokens = (
            "cost",
            "density",
            "wear",
            "loss",
            "error",
            "uncertainty",
            "roughness",
            "resistivity",
            "corrosion rate",
            "degradation",
        )
        return "min" if any(token in key for token in minimize_tokens) else "max"

    @staticmethod
    def _coerce_iteration(value: Any) -> int | None:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _coerce_float(value: Any) -> float | None:
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        if math.isnan(number) or math.isinf(number):
            return None
        return number

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
            "run_note": "run_note",
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
    def _target_sort_tuple(value: Any) -> tuple[int, int, float, str]:
        return auto_sort_tuple(value)

    @staticmethod
    def _auto_sort_tuple(value: Any) -> tuple[int, int, float, str]:
        return auto_sort_tuple(value)

    @staticmethod
    def _sort_rows_with_nulls_last(
        rows: list[dict[str, Any]],
        *,
        value_getter: Callable[[dict[str, Any]], Any],
        descending: bool,
    ) -> list[dict[str, Any]]:
        non_empty_rows: list[dict[str, Any]] = []
        empty_rows: list[dict[str, Any]] = []
        for row in rows:
            value = value_getter(row)
            if auto_sort_tuple(value)[0] == 1:
                empty_rows.append(row)
            else:
                non_empty_rows.append(row)
        non_empty_rows.sort(key=lambda row: auto_sort_tuple(value_getter(row)), reverse=descending)
        return non_empty_rows + empty_rows


material_data_repo = MaterialDataRepository()
