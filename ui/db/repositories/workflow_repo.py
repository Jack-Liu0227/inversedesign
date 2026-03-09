from __future__ import annotations

import sqlite3
from typing import Any

from ui.db.connection import db_manager
from ui.services.json_decode_service import decode_maybe_double_json
from ui.services.timezone_service import normalize_row_datetimes


class WorkflowRepository:
    DB_KEY = "workflow_audit"

    def list_workflow_events(
        self,
        *,
        page: int,
        page_size: int,
        workflow_name: str | None = None,
        step_name: str | None = None,
        event_type: str | None = None,
        success: int | None = None,
        trace_id: str | None = None,
        session_id: str | None = None,
        latency_min: int | None = None,
        latency_max: int | None = None,
    ) -> tuple[list[dict[str, Any]], int]:
        where: list[str] = []
        params: list[Any] = []

        if workflow_name:
            where.append("workflow_name = ?")
            params.append(workflow_name)
        if step_name:
            where.append("step_name = ?")
            params.append(step_name)
        if event_type:
            where.append("event_type = ?")
            params.append(event_type)
        if success is not None:
            where.append("success = ?")
            params.append(success)
        if trace_id:
            where.append("trace_id = ?")
            params.append(trace_id)
        if session_id:
            where.append("session_id = ?")
            params.append(session_id)
        if latency_min is not None:
            where.append("latency_ms >= ?")
            params.append(latency_min)
        if latency_max is not None:
            where.append("latency_ms <= ?")
            params.append(latency_max)

        where_sql = f"WHERE {' AND '.join(where)}" if where else ""

        try:
            with db_manager.connect(self.DB_KEY, readonly=True) as conn:
                total = conn.execute(
                    f"SELECT COUNT(*) AS c FROM workflow_io_logs {where_sql}", params
                ).fetchone()["c"]

                offset = (page - 1) * page_size
                rows = conn.execute(
                    f"""
                    SELECT id, created_at, trace_id, workflow_name, session_id, workflow_run_id,
                           step_name, event_type, payload_json, latency_ms, success, error_text
                    FROM workflow_io_logs
                    {where_sql}
                    ORDER BY created_at DESC, id DESC
                    LIMIT ? OFFSET ?
                    """,
                    [*params, page_size, offset],
                ).fetchall()
        except sqlite3.OperationalError:
            return [], 0

        items = []
        for row in rows:
            item = normalize_row_datetimes(dict(row))
            item["payload"] = decode_maybe_double_json(item.pop("payload_json"))
            items.append(item)
        return items, int(total)

    def find_lineage_events(self, trace_or_session_id: str) -> list[dict[str, Any]]:
        try:
            with db_manager.connect(self.DB_KEY, readonly=True) as conn:
                rows = conn.execute(
                    """
                    SELECT id, created_at, trace_id, workflow_name, session_id, workflow_run_id,
                    step_name, event_type, payload_json, latency_ms, success, error_text
                    FROM workflow_io_logs
                    WHERE trace_id = ? OR session_id = ? OR workflow_run_id = ?
                    ORDER BY created_at ASC, id ASC
                    """,
                    [trace_or_session_id, trace_or_session_id, trace_or_session_id],
                ).fetchall()
        except sqlite3.OperationalError:
            return []

        items = []
        for row in rows:
            item = normalize_row_datetimes(dict(row))
            item["payload"] = decode_maybe_double_json(item.pop("payload_json"))
            items.append(item)
        return items

    def find_run_audits(self, trace_or_session_id: str) -> list[dict[str, Any]]:
        try:
            with db_manager.connect(self.DB_KEY, readonly=True) as conn:
                rows = conn.execute(
                    """
                    SELECT id, created_at, workflow_name, session_id, workflow_run_id,
                           decision, should_stop, summary_json, final_result_json,
                           step_outputs_json, input_json, error_text
                    FROM workflow_run_audit
                    WHERE session_id = ? OR workflow_run_id = ?
                    ORDER BY created_at ASC, id ASC
                    """,
                    [trace_or_session_id, trace_or_session_id],
                ).fetchall()
        except sqlite3.OperationalError:
            return []

        items = []
        for row in rows:
            item = normalize_row_datetimes(dict(row))
            item["summary"] = decode_maybe_double_json(item.pop("summary_json"))
            item["final_result"] = decode_maybe_double_json(item.pop("final_result_json"))
            item["step_outputs"] = decode_maybe_double_json(item.pop("step_outputs_json"))
            item["input"] = decode_maybe_double_json(item.pop("input_json"))
            items.append(item)
        return items


workflow_repo = WorkflowRepository()
