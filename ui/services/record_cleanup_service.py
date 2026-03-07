from __future__ import annotations

from typing import Any

from ui.db.connection import db_manager
from ui.db.repositories.explorer_repo import ExplorerRepository

CLEANUP_FILTER_OPTIONS = [
    {"value": "id", "label": "Primary ID"},
    {"value": "workflow_id", "label": "Workflow ID"},
    {"value": "session_id", "label": "Session ID"},
    {"value": "trace_id", "label": "Trace ID"},
]


class RecordCleanupService:
    _LOGICAL_COLUMNS = {
        "workflow_id": ("workflow_run_id", "run_id"),
    }

    def normalize_filter_col(self, filter_col: str | None) -> str:
        raw = str(filter_col or "id").strip()
        allowed = {item["value"] for item in CLEANUP_FILTER_OPTIONS}
        return raw if raw in allowed else "id"

    def normalize_filter_values(self, filter_values: list[str] | tuple[str, ...] | None) -> list[str]:
        output: list[str] = []
        for value in filter_values or []:
            text = str(value or "").strip()
            if text and text not in output:
                output.append(text)
        return output

    def _resolve_actual_column(self, *, filter_col: str, available_columns: list[str]) -> str:
        normalized = self.normalize_filter_col(filter_col)
        available = [str(col or "").strip() for col in available_columns]
        if normalized in self._LOGICAL_COLUMNS:
            for candidate in self._LOGICAL_COLUMNS[normalized]:
                if candidate in available:
                    return candidate
            return ""
        return normalized if normalized in available else ""

    def _record_key_col(self, columns: list[dict[str, Any]]) -> str:
        pk_cols = [str(col.get("name") or "").strip() for col in columns if col.get("pk")]
        key_col = pk_cols[0] if pk_cols else "__rowid__"
        names = {str(col.get("name") or "").strip() for col in columns}
        if key_col != "__rowid__" and key_col not in names:
            return "__rowid__"
        return key_col

    def preview(self, *, explorer_repository: ExplorerRepository, filter_col: str, filter_value: str, sample_limit: int = 6) -> dict[str, Any]:
        normalized_col = self.normalize_filter_col(filter_col)
        normalized_value = str(filter_value or "").strip()
        if not normalized_value:
            return {
                "filter_col": normalized_col,
                "filter_value": normalized_value,
                "total_matches": 0,
                "matched_tables": 0,
                "scanned_tables": 0,
                "details": [],
                "errors": [],
            }

        details: list[dict[str, Any]] = []
        errors: list[dict[str, str]] = []
        total_matches = 0
        scanned_tables = 0

        for db_item in explorer_repository.list_databases():
            db_key = str(db_item.get("key") or "").strip()
            if not db_key or db_key == "ui_classifications":
                continue
            try:
                tables = explorer_repository.list_tables(db_key)
            except Exception as exc:
                errors.append({"db": db_key, "table": "*", "error": str(exc)})
                continue

            for table in tables:
                scanned_tables += 1
                try:
                    columns = explorer_repository.get_table_columns(db_key, table)
                    col_names = [str(col.get("name") or "").strip() for col in columns]
                    actual_col = self._resolve_actual_column(filter_col=normalized_col, available_columns=col_names)
                    if not actual_col:
                        continue
                    key_col = self._record_key_col(columns)
                    with db_manager.connect(db_key, readonly=True) as conn:
                        count_row = conn.execute(
                            f'SELECT COUNT(*) AS c FROM "{table}" WHERE CAST("{actual_col}" AS TEXT) = ?',
                            [normalized_value],
                        ).fetchone()
                        matched = int(count_row["c"] if count_row else 0)
                        if matched <= 0:
                            continue
                        total_matches += matched
                        if key_col == "__rowid__":
                            sample_rows = conn.execute(
                                f'SELECT DISTINCT CAST(rowid AS TEXT) AS sample_key FROM "{table}" WHERE CAST("{actual_col}" AS TEXT) = ? ORDER BY rowid DESC LIMIT ?',
                                [normalized_value, max(1, min(int(sample_limit), 20))],
                            ).fetchall()
                        else:
                            sample_rows = conn.execute(
                                f'SELECT DISTINCT CAST("{key_col}" AS TEXT) AS sample_key FROM "{table}" WHERE CAST("{actual_col}" AS TEXT) = ? ORDER BY "{key_col}" DESC LIMIT ?',
                                [normalized_value, max(1, min(int(sample_limit), 20))],
                            ).fetchall()
                    details.append(
                        {
                            "filter_value": normalized_value,
                            "db": db_key,
                            "table": table,
                            "key_col": key_col,
                            "matched_column": actual_col,
                            "matched": matched,
                            "sample_keys": [str(row["sample_key"]).strip() for row in sample_rows if str(row["sample_key"]).strip()],
                        }
                    )
                except Exception as exc:
                    errors.append({"db": db_key, "table": str(table), "error": str(exc)})

        return {
            "filter_col": normalized_col,
            "filter_value": normalized_value,
            "total_matches": total_matches,
            "matched_tables": len(details),
            "scanned_tables": scanned_tables,
            "details": details,
            "errors": errors,
        }

    def preview_many(self, *, explorer_repository: ExplorerRepository, filter_col: str, filter_values: list[str], sample_limit: int = 6) -> dict[str, Any]:
        normalized_col = self.normalize_filter_col(filter_col)
        normalized_values = self.normalize_filter_values(filter_values)
        if not normalized_values:
            return {
                "filter_col": normalized_col,
                "filter_values": [],
                "total_matches": 0,
                "matched_tables": 0,
                "scanned_tables": 0,
                "details": [],
                "errors": [],
            }
        total_matches = 0
        scanned_tables = 0
        details: list[dict[str, Any]] = []
        errors: list[dict[str, str]] = []
        for value in normalized_values:
            result = self.preview(
                explorer_repository=explorer_repository,
                filter_col=normalized_col,
                filter_value=value,
                sample_limit=sample_limit,
            )
            total_matches += int(result.get("total_matches", 0) or 0)
            scanned_tables = max(scanned_tables, int(result.get("scanned_tables", 0) or 0))
            details.extend(result.get("details", []))
            errors.extend(result.get("errors", []))
        return {
            "filter_col": normalized_col,
            "filter_values": normalized_values,
            "total_matches": total_matches,
            "matched_tables": len(details),
            "scanned_tables": scanned_tables,
            "details": details,
            "errors": errors,
        }

    def suggestions(self, *, explorer_repository: ExplorerRepository, filter_col: str, query: str = "", limit: int = 60) -> dict[str, Any]:
        normalized_col = self.normalize_filter_col(filter_col)
        normalized_query = str(query or "").strip()
        safe_limit = max(1, min(int(limit), 200))
        values: set[str] = set()
        errors: list[dict[str, str]] = []

        for db_item in explorer_repository.list_databases():
            db_key = str(db_item.get("key") or "").strip()
            if not db_key or db_key == "ui_classifications":
                continue
            try:
                tables = explorer_repository.list_tables(db_key)
            except Exception as exc:
                errors.append({"db": db_key, "table": "*", "error": str(exc)})
                continue

            for table in tables:
                try:
                    columns = explorer_repository.get_table_columns(db_key, table)
                    col_names = [str(col.get("name") or "").strip() for col in columns]
                    actual_col = self._resolve_actual_column(filter_col=normalized_col, available_columns=col_names)
                    if not actual_col:
                        continue
                    with db_manager.connect(db_key, readonly=True) as conn:
                        where = [f'"{actual_col}" IS NOT NULL', f'CAST("{actual_col}" AS TEXT) <> \"\"']
                        params: list[Any] = []
                        if normalized_query:
                            where.append(f'CAST("{actual_col}" AS TEXT) LIKE ?')
                            params.append(f"%{normalized_query}%")
                        where_sql = ' AND '.join(where)
                        rows = conn.execute(
                            f'SELECT DISTINCT CAST("{actual_col}" AS TEXT) AS v FROM "{table}" WHERE {where_sql} ORDER BY v ASC LIMIT ?',
                            [*params, safe_limit],
                        ).fetchall()
                    for row in rows:
                        value = str(row["v"] or "").strip()
                        if value:
                            values.add(value)
                    if len(values) >= safe_limit:
                        break
                except Exception as exc:
                    errors.append({"db": db_key, "table": str(table), "error": str(exc)})
            if len(values) >= safe_limit:
                break

        output = sorted(values)
        if normalized_query:
            output.sort(key=lambda item: (0 if item == normalized_query else 1 if item.startswith(normalized_query) else 2, item))
        return {"filter_col": normalized_col, "values": output[:safe_limit], "errors": errors}

    def delete(self, *, explorer_repository: ExplorerRepository, filter_col: str, filter_value: str) -> dict[str, Any]:
        normalized_col = self.normalize_filter_col(filter_col)
        normalized_value = str(filter_value or "").strip()
        if not normalized_value:
            return {
                "filter_col": normalized_col,
                "filter_value": normalized_value,
                "deleted": 0,
                "details": [],
                "errors": [],
                "scanned_tables": 0,
            }

        deleted_total = 0
        details: list[dict[str, Any]] = []
        errors: list[dict[str, str]] = []
        scanned_tables = 0

        for db_item in explorer_repository.list_databases():
            db_key = str(db_item.get("key") or "").strip()
            if not db_key or db_key == "ui_classifications":
                continue
            try:
                tables = explorer_repository.list_tables(db_key)
            except Exception as exc:
                errors.append({"db": db_key, "table": "*", "error": str(exc)})
                continue

            for table in tables:
                scanned_tables += 1
                try:
                    columns = explorer_repository.get_table_columns(db_key, table)
                    col_names = [str(col.get("name") or "").strip() for col in columns]
                    actual_col = self._resolve_actual_column(filter_col=normalized_col, available_columns=col_names)
                    if not actual_col:
                        continue
                    result = explorer_repository.delete_rows_by_column_value_to_recycle_bin(
                        db_key=db_key,
                        table=table,
                        filter_col=actual_col,
                        filter_value=normalized_value,
                    )
                    deleted = int(result.get("deleted", 0) or 0)
                    if deleted > 0:
                        details.append({"filter_value": normalized_value, "db": db_key, "table": table, "matched_column": actual_col, "deleted": deleted})
                        deleted_total += deleted
                except Exception as exc:
                    errors.append({"db": db_key, "table": str(table), "error": str(exc)})

        return {
            "filter_col": normalized_col,
            "filter_value": normalized_value,
            "deleted": deleted_total,
            "details": details,
            "errors": errors,
            "scanned_tables": scanned_tables,
        }

    def delete_many(self, *, explorer_repository: ExplorerRepository, filter_col: str, filter_values: list[str]) -> dict[str, Any]:
        normalized_col = self.normalize_filter_col(filter_col)
        normalized_values = self.normalize_filter_values(filter_values)
        if not normalized_values:
            return {
                "filter_col": normalized_col,
                "filter_values": [],
                "deleted": 0,
                "details": [],
                "errors": [],
                "scanned_tables": 0,
            }
        deleted_total = 0
        scanned_tables = 0
        details: list[dict[str, Any]] = []
        errors: list[dict[str, str]] = []
        for value in normalized_values:
            result = self.delete(
                explorer_repository=explorer_repository,
                filter_col=normalized_col,
                filter_value=value,
            )
            deleted_total += int(result.get("deleted", 0) or 0)
            scanned_tables = max(scanned_tables, int(result.get("scanned_tables", 0) or 0))
            details.extend(result.get("details", []))
            errors.extend(result.get("errors", []))
        return {
            "filter_col": normalized_col,
            "filter_values": normalized_values,
            "deleted": deleted_total,
            "details": details,
            "errors": errors,
            "scanned_tables": scanned_tables,
        }


record_cleanup_service = RecordCleanupService()
