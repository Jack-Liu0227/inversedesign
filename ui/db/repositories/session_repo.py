from __future__ import annotations

import sqlite3
from typing import Any

from ui.db.connection import db_manager
from ui.services.json_decode_service import decode_maybe_double_json
from ui.services.timezone_service import normalize_row_datetimes


class SessionRepository:
    DB_KEYS = ("material_agent_shared",)

    def find_sessions(self, trace_or_session_id: str) -> list[dict[str, Any]]:
        all_rows: list[dict[str, Any]] = []
        for key in self.DB_KEYS:
            try:
                with db_manager.connect(key, readonly=True) as conn:
                    rows = conn.execute(
                        """
                        SELECT session_id, session_type, agent_id, team_id, workflow_id,
                               user_id, session_data, agent_data, team_data, workflow_data,
                               metadata, runs, summary, created_at, updated_at
                        FROM agno_sessions
                        WHERE session_id = ?
                        ORDER BY created_at ASC
                        """,
                        [trace_or_session_id],
                    ).fetchall()
            except sqlite3.OperationalError:
                continue
            for row in rows:
                item = normalize_row_datetimes(dict(row))
                item["source_db"] = key
                for col in (
                    "session_data",
                    "agent_data",
                    "team_data",
                    "workflow_data",
                    "metadata",
                    "runs",
                    "summary",
                ):
                    item[col] = decode_maybe_double_json(item[col])
                all_rows.append(item)
        return all_rows


session_repo = SessionRepository()
