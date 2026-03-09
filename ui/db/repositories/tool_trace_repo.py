from __future__ import annotations

import sqlite3
from collections import defaultdict
from typing import Any

from ui.db.connection import db_manager
from ui.services.timezone_service import normalize_row_datetimes


class ToolTraceRepository:
    DB_KEY = "workflow_audit"
    _WORKFLOW_STEP_ORDER = [
        "Router Agent",
        "Recommender Agent",
        "Predictor Agent",
        "Rationality Judge",
        "Persistence",
        "Human Feedback",
        "Final Decision",
    ]
    def list_tool_calls(
        self,
        *,
        session_id: str | None = None,
        workflow_run_id: str | None = None,
        step_name: str | None = None,
        tool_name: str | None = None,
        success: int | None = None,
        sort_order: str = "desc",
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        where: list[str] = []
        params: list[Any] = []

        if session_id:
            where.append("session_id = ?")
            params.append(session_id)
        if workflow_run_id:
            where.append("workflow_run_id = ?")
            params.append(workflow_run_id)
        if step_name:
            where.append("step_name = ?")
            params.append(step_name)
        if tool_name:
            where.append("tool_name = ?")
            params.append(tool_name)
        if success is not None:
            where.append("success = ?")
            params.append(int(success))

        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        order = "ASC" if str(sort_order).lower() == "asc" else "DESC"

        try:
            with db_manager.connect(self.DB_KEY, readonly=True) as conn:
                rows = conn.execute(
                    f"""
                    SELECT id, created_at, session_id, workflow_run_id, step_name, agent_name,
                           workflow_name, trace_id, execution_id, agent_source, tool_name, tool_args_json, tool_result_json,
                           success, error_text
                    FROM agent_tool_call_logs
                    {where_sql}
                    ORDER BY created_at {order}, id {order}
                    LIMIT ?
                    """,
                    [*params, max(1, min(limit, 2000))],
                ).fetchall()
        except sqlite3.OperationalError:
            return []

        return [normalize_row_datetimes(dict(r)) for r in rows]

    def get_tool_call_by_id(self, call_id: int) -> dict[str, Any] | None:
        try:
            with db_manager.connect(self.DB_KEY, readonly=True) as conn:
                row = conn.execute(
                    """
                    SELECT id, created_at, session_id, workflow_run_id, step_name, agent_name,
                           workflow_name, trace_id, execution_id, agent_source, tool_name, tool_args_json, tool_result_json,
                           success, error_text
                    FROM agent_tool_call_logs
                    WHERE id = ?
                    """,
                    [call_id],
                ).fetchone()
        except sqlite3.OperationalError:
            return None
        return normalize_row_datetimes(dict(row)) if row else None

    def list_distinct_filters(self) -> dict[str, list[str]]:
        try:
            with db_manager.connect(self.DB_KEY, readonly=True) as conn:
                step_rows = conn.execute(
                    "SELECT DISTINCT step_name FROM agent_tool_call_logs WHERE step_name IS NOT NULL AND step_name <> '' ORDER BY step_name"
                ).fetchall()
                tool_rows = conn.execute(
                    "SELECT DISTINCT tool_name FROM agent_tool_call_logs WHERE tool_name IS NOT NULL AND tool_name <> '' ORDER BY tool_name"
                ).fetchall()
        except sqlite3.OperationalError:
            return {"step_names": [], "agent_names": [], "tool_names": []}

        return {
            "step_names": self._ordered_step_names(
                sorted(
                    {
                        *[str(r[0]) for r in step_rows],
                        *self._WORKFLOW_STEP_ORDER,
                    }
                )
            ),
            "agent_names": [],
            "tool_names": [str(r[0]) for r in tool_rows],
        }

    def list_cascaded_filters(
        self,
        *,
        session_id: str | None = None,
        workflow_run_id: str | None = None,
        step_name: str | None = None,
        success: int | None = None,
    ) -> dict[str, list[str]]:
        try:
            with db_manager.connect(self.DB_KEY, readonly=True) as conn:
                step_names = self._distinct_values(
                    conn=conn,
                    column="step_name",
                    session_id=session_id,
                    workflow_run_id=workflow_run_id,
                    success=success,
                )
                tool_names = self._distinct_values(
                    conn=conn,
                    column="tool_name",
                    session_id=session_id,
                    workflow_run_id=workflow_run_id,
                    step_name=step_name,
                    success=success,
                )
        except sqlite3.OperationalError:
            return {"step_names": [], "agent_names": [], "tool_names": []}

        return {
            "step_names": self._ordered_step_names(
                sorted(
                    {
                        *step_names,
                        *self._WORKFLOW_STEP_ORDER,
                    }
                )
            ),
            "agent_names": [],
            "tool_names": tool_names,
        }

    @staticmethod
    def _distinct_values(
        *,
        conn: sqlite3.Connection,
        column: str,
        session_id: str | None = None,
        workflow_run_id: str | None = None,
        step_name: str | None = None,
        success: int | None = None,
    ) -> list[str]:
        where: list[str] = [f"{column} IS NOT NULL", f"{column} <> ''"]
        params: list[Any] = []
        if session_id:
            where.append("session_id = ?")
            params.append(session_id)
        if workflow_run_id:
            where.append("workflow_run_id = ?")
            params.append(workflow_run_id)
        if step_name:
            where.append("step_name = ?")
            params.append(step_name)
        if success is not None:
            where.append("success = ?")
            params.append(int(success))
        where_sql = " AND ".join(where)
        rows = conn.execute(
            f"SELECT DISTINCT {column} FROM agent_tool_call_logs WHERE {where_sql} ORDER BY {column}",
            params,
        ).fetchall()
        return [str(r[0]) for r in rows]

    @staticmethod
    def group_by_step(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            key = str(row.get("step_name") or "Unknown Step")
            grouped[key].append(row)
        order_map = {name: idx for idx, name in enumerate(ToolTraceRepository._WORKFLOW_STEP_ORDER)}
        ordered_steps = sorted(grouped.keys(), key=lambda v: (order_map.get(v, 10_000), v.lower()))
        return [{"step_name": step, "rows": grouped[step]} for step in ordered_steps]

    @classmethod
    def _ordered_step_names(cls, values: list[str]) -> list[str]:
        order_map = {name: idx for idx, name in enumerate(cls._WORKFLOW_STEP_ORDER)}
        return sorted(values, key=lambda v: (order_map.get(v, 10_000), v.lower()))


tool_trace_repo = ToolTraceRepository()
