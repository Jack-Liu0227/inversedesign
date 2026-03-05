from __future__ import annotations

import json
import sqlite3
from typing import Any

from ui.config import get_config
from ui.db.connection import db_manager
from ui.services.timezone_service import normalize_row_datetimes


class ExplorerRepository:
    _WORKFLOW_STEP_ORDER = [
        "Router Agent",
        "Recommender Agent",
        "Predictor Agent",
        "Rationality Judge",
        "Persistence",
        "Human Feedback",
        "Final Decision",
    ]

    def __init__(self) -> None:
        self._cfg = get_config()

    def list_databases(self) -> list[dict[str, str]]:
        items = []
        for key, path in self._cfg.db_paths.items():
            if not path.exists():
                continue
            items.append({"key": key, "file": path.name})
        return items

    def list_tables(self, db_key: str) -> list[str]:
        with db_manager.connect(db_key, readonly=True) as conn:
            rows = conn.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name ASC
                """
            ).fetchall()
        return [str(r["name"]) for r in rows]

    def get_table_columns(self, db_key: str, table: str) -> list[dict[str, Any]]:
        with db_manager.connect(db_key, readonly=True) as conn:
            rows = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
        return [dict(r) for r in rows]

    def list_rows(
        self,
        *,
        db_key: str,
        table: str,
        page: int,
        page_size: int,
        q: str | None = None,
        identifier: str | None = None,
        sort_order: str = "desc",
        extra_filters: dict[str, str] | None = None,
    ) -> tuple[list[dict[str, Any]], int, list[str], str]:
        if table not in self.list_tables(db_key):
            raise ValueError(f"Unknown table: {table}")

        columns = self.get_table_columns(db_key, table)
        col_names = [c["name"] for c in columns]
        if not col_names:
            return [], 0, [], "rowid"

        pk_cols = [c["name"] for c in columns if c.get("pk")]
        record_key = pk_cols[0] if pk_cols else "__rowid__"

        where_clauses: list[str] = []
        params: list[Any] = []

        if q:
            clauses = [f'CAST("{col}" AS TEXT) LIKE ?' for col in col_names]
            where_clauses.append(f"({' OR '.join(clauses)})")
            like = f"%{q}%"
            params.extend([like for _ in col_names])

        if extra_filters:
            for key, value in extra_filters.items():
                col = str(key or "").strip()
                val = str(value or "").strip()
                if not col or not val:
                    continue
                if col not in col_names:
                    continue
                where_clauses.append(f'CAST("{col}" AS TEXT) = ?')
                params.append(val)

        if identifier:
            id_cols = [
                c
                for c in col_names
                if any(token in c.lower() for token in ("id", "trace", "session", "run", "code", "barcode"))
            ]
            if not id_cols:
                id_cols = [record_key] if record_key != "__rowid__" else ["__rowid__"]

            id_clauses = []
            for col in id_cols:
                if col == "__rowid__":
                    id_clauses.append("CAST(rowid AS TEXT) = ?")
                else:
                    id_clauses.append(f'CAST("{col}" AS TEXT) = ?')
                params.append(identifier)
            where_clauses.append(f"({' OR '.join(id_clauses)})")

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

        with db_manager.connect(db_key, readonly=True) as conn:
            total = conn.execute(
                f'SELECT COUNT(*) AS c FROM "{table}" {where_sql}', params
            ).fetchone()["c"]

            offset = (page - 1) * page_size
            select_cols = ', '.join([f'"{c}"' for c in col_names])
            normalized_order = "ASC" if str(sort_order).lower() == "asc" else "DESC"
            if "created_at" in col_names:
                order_sql = f'ORDER BY "created_at" {normalized_order}'
            elif "id" in col_names:
                order_sql = f'ORDER BY "id" {normalized_order}'
            else:
                order_sql = f"ORDER BY rowid {normalized_order}"

            if record_key == "__rowid__":
                sql = (
                    f'SELECT rowid AS __rowid__, {select_cols} FROM "{table}" '
                    f"{where_sql} {order_sql} LIMIT ? OFFSET ?"
                )
            else:
                sql = f'SELECT {select_cols} FROM "{table}" {where_sql} {order_sql} LIMIT ? OFFSET ?'

            rows = conn.execute(sql, [*params, page_size, offset]).fetchall()

        output_rows = [normalize_row_datetimes(dict(r)) for r in rows]
        if db_key == "workflow_audit" and table == "workflow_step_logs":
            output_rows = self._enrich_workflow_step_rows(output_rows)
        return output_rows, int(total), col_names, record_key

    def list_distinct_values(
        self,
        *,
        db_key: str,
        table: str,
        column: str,
        extra_filters: dict[str, str] | None = None,
        limit: int = 500,
    ) -> list[str]:
        if table not in self.list_tables(db_key):
            return []
        columns = self.get_table_columns(db_key, table)
        col_names = [c["name"] for c in columns]
        if column not in col_names:
            return []
        where: list[str] = [f'"{column}" IS NOT NULL', f'CAST("{column}" AS TEXT) <> \'\'']
        params: list[Any] = []
        if extra_filters:
            for key, value in extra_filters.items():
                col = str(key or "").strip()
                val = str(value or "").strip()
                if not col or not val or col == column or col not in col_names:
                    continue
                where.append(f'CAST("{col}" AS TEXT) = ?')
                params.append(val)
        where_sql = f"WHERE {' AND '.join(where)}"
        with db_manager.connect(db_key, readonly=True) as conn:
            rows = conn.execute(
                f'SELECT DISTINCT CAST("{column}" AS TEXT) AS v FROM "{table}" {where_sql} ORDER BY v ASC LIMIT ?',
                [*params, max(1, min(limit, 2000))],
            ).fetchall()
        return [str(r["v"]) for r in rows if str(r["v"]).strip()]

    def viewer_filter_options(
        self,
        *,
        db_key: str,
        table: str,
        trace_id: str | None = None,
        session_id: str | None = None,
        run_id: str | None = None,
        step_name: str | None = None,
        agent_name: str | None = None,
        event_type: str | None = None,
        decision: str | None = None,
        should_stop: str | None = None,
        success: str | None = None,
    ) -> dict[str, list[str]]:
        known_tables = [
            "workflow_step_logs",
            "agent_tool_call_logs",
            "agent_execution_logs",
            "workflow_io_logs",
            "workflow_run_audit",
        ]
        if db_key != "workflow_audit" or table not in known_tables:
            return {
                "step_names": [],
                "agent_names": [],
                "tool_names": [],
                "statuses": [],
                "event_types": [],
                "decisions": [],
                "should_stop_values": [],
                "success_values": [],
                "trace_ids": [],
                "session_ids": [],
                "run_ids": [],
            }
        with db_manager.connect(db_key, readonly=True) as conn:
            context_filters = {
                "trace_id": trace_id or "",
                "session_id": session_id or "",
                "run_id": run_id or "",
            }
            chain_filters = {
                **context_filters,
                "step_name": step_name or "",
                "agent_name": agent_name or "",
                "event_type": event_type or "",
                "decision": decision or "",
                "should_stop": should_stop or "",
                "success": success or "",
            }

            return {
                "step_names": self._ordered_step_names(
                    self._distinct_from_tables(
                        conn=conn,
                        tables=known_tables,
                        column="step_name",
                        filters=context_filters,
                    )
                ),
                "agent_names": self._distinct_from_tables(
                    conn=conn,
                    tables=known_tables,
                    column="agent_name",
                    filters=chain_filters,
                ),
                "tool_names": self._distinct_from_tables(
                    conn=conn,
                    tables=known_tables,
                    column="tool_name",
                    filters=chain_filters,
                ),
                "statuses": self._distinct_from_tables(
                    conn=conn,
                    tables=known_tables,
                    column="status",
                    filters=chain_filters,
                ),
                "event_types": self._distinct_from_tables(
                    conn=conn,
                    tables=known_tables,
                    column="event_type",
                    filters=chain_filters,
                ),
                "decisions": self._distinct_from_tables(
                    conn=conn,
                    tables=known_tables,
                    column="decision",
                    filters=chain_filters,
                ),
                "should_stop_values": self._distinct_from_tables(
                    conn=conn,
                    tables=known_tables,
                    column="should_stop",
                    filters=chain_filters,
                ),
                "success_values": self._distinct_from_tables(
                    conn=conn,
                    tables=known_tables,
                    column="success",
                    filters=context_filters,
                ),
                "trace_ids": self._distinct_from_tables(
                    conn=conn,
                    tables=known_tables,
                    column="trace_id",
                    filters={},
                ),
                "session_ids": self._distinct_from_tables(
                    conn=conn,
                    tables=known_tables,
                    column="session_id",
                    filters={},
                ),
                "run_ids": self._distinct_from_tables(
                    conn=conn,
                    tables=known_tables,
                    column="run_id",
                    filters=context_filters,
                ),
            }

    @staticmethod
    def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
        try:
            rows = conn.execute(f'PRAGMA table_info("{table}")').fetchall()
        except Exception:
            return set()
        return {str(r["name"]) for r in rows if "name" in r.keys()}

    def _distinct_from_tables(
        self,
        *,
        conn: sqlite3.Connection,
        tables: list[str],
        column: str,
        filters: dict[str, str],
        limit: int = 500,
    ) -> list[str]:
        values: set[str] = set()
        for table in tables:
            cols = self._table_columns(conn, table)
            if column not in cols:
                continue
            where = [f'"{column}" IS NOT NULL', f'CAST("{column}" AS TEXT) <> \'\'']
            params: list[Any] = []
            for fk, fv in (filters or {}).items():
                key = str(fk or "").strip()
                val = str(fv or "").strip()
                if not key or not val:
                    continue
                if key not in cols or key == column:
                    continue
                where.append(f'CAST("{key}" AS TEXT) = ?')
                params.append(val)
            where_sql = f"WHERE {' AND '.join(where)}"
            try:
                rows = conn.execute(
                    f'SELECT DISTINCT CAST("{column}" AS TEXT) AS v FROM "{table}" {where_sql} LIMIT ?',
                    [*params, max(1, min(limit, 2000))],
                ).fetchall()
            except Exception:
                continue
            for row in rows:
                val = str(row["v"]).strip()
                if val:
                    values.add(val)
        return sorted(values)

    def _ordered_step_names(self, values: list[str]) -> list[str]:
        order_map = {name: idx for idx, name in enumerate(self._WORKFLOW_STEP_ORDER)}
        return sorted(values, key=lambda v: (order_map.get(v, 10_000), v.lower()))

    def get_row_by_key(
        self,
        *,
        db_key: str,
        table: str,
        key_col: str,
        key_val: str,
    ) -> dict[str, Any] | None:
        if table not in self.list_tables(db_key):
            return None

        columns = self.get_table_columns(db_key, table)
        col_names = [c["name"] for c in columns]
        if not col_names:
            return None

        select_cols = ", ".join([f'"{c}"' for c in col_names])
        with db_manager.connect(db_key, readonly=True) as conn:
            if key_col == "__rowid__":
                row = conn.execute(
                    f'SELECT rowid AS __rowid__, {select_cols} FROM "{table}" WHERE rowid = ?',
                    [key_val],
                ).fetchone()
            else:
                row = conn.execute(
                    f'SELECT {select_cols} FROM "{table}" WHERE "{key_col}" = ?',
                    [key_val],
                ).fetchone()
        parsed = normalize_row_datetimes(dict(row)) if row else None
        if parsed and db_key == "workflow_audit" and table == "workflow_step_logs":
            enriched = self._enrich_workflow_step_rows([parsed])
            return enriched[0] if enriched else parsed
        return parsed

    def delete_rows_to_recycle_bin(
        self,
        *,
        db_key: str,
        table: str,
        key_col: str,
        key_values: list[str],
    ) -> dict[str, int]:
        if not key_values:
            return {"deleted": 0}
        if table not in self.list_tables(db_key):
            raise ValueError(f"Unknown table: {table}")

        deleted = 0
        with db_manager.connect(db_key, readonly=False) as src_conn, db_manager.connect(
            "ui_classifications", readonly=False
        ) as ui_conn:
            columns = self.get_table_columns(db_key, table)
            col_names = [c["name"] for c in columns]
            select_cols = ", ".join([f'"{c}"' for c in col_names])

            for key_val in key_values:
                if key_col == "__rowid__":
                    row = src_conn.execute(
                        f'SELECT rowid AS __rowid__, {select_cols} FROM "{table}" WHERE rowid = ?',
                        [key_val],
                    ).fetchone()
                else:
                    row = src_conn.execute(
                        f'SELECT {select_cols} FROM "{table}" WHERE "{key_col}" = ?',
                        [key_val],
                    ).fetchone()

                if not row:
                    continue

                row_data = dict(row)
                ui_conn.execute(
                    """
                    INSERT INTO ui_deleted_records(source_db, source_table, key_col, key_val, row_json)
                    VALUES(?, ?, ?, ?, ?)
                    """,
                    [db_key, table, key_col, str(key_val), json.dumps(row_data, ensure_ascii=False)],
                )

                if key_col == "__rowid__":
                    src_conn.execute(f'DELETE FROM "{table}" WHERE rowid = ?', [key_val])
                else:
                    src_conn.execute(f'DELETE FROM "{table}" WHERE "{key_col}" = ?', [key_val])
                deleted += 1

            src_conn.commit()
            ui_conn.commit()

        return {"deleted": deleted}

    def list_recycle_bin(self, *, limit: int = 200) -> list[dict[str, Any]]:
        with db_manager.connect("ui_classifications", readonly=True) as conn:
            rows = conn.execute(
                """
                SELECT id, source_db, source_table, key_col, key_val, row_json, deleted_at, restored_at
                FROM ui_deleted_records
                WHERE restored_at IS NULL
                ORDER BY deleted_at DESC, id DESC
                LIMIT ?
                """,
                [limit],
            ).fetchall()
        return [normalize_row_datetimes(dict(r)) for r in rows]

    @staticmethod
    def _try_parse_json_dict(text: Any) -> dict[str, Any]:
        if not isinstance(text, str):
            return {}
        raw = text.strip()
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    @classmethod
    def _is_summary_payload(cls, raw_json: Any) -> bool:
        payload = cls._try_parse_json_dict(raw_json)
        if not payload:
            return False
        keys = set(payload.keys())
        if keys == {"type", "keys", "size"}:
            return True
        if keys == {"type", "keys", "size", "sample"}:
            return True
        return False

    def _enrich_workflow_step_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not rows:
            return rows
        enriched: list[dict[str, Any]] = []
        with db_manager.connect("workflow_audit", readonly=True) as conn:
            for row in rows:
                out = dict(row)
                need_output = self._is_summary_payload(out.get("output_json"))
                need_input = self._is_summary_payload(out.get("input_json"))
                if not (need_output or need_input):
                    enriched.append(out)
                    continue

                step_name = str(out.get("step_name") or "").strip()
                session_id = str(out.get("session_id") or "").strip()
                run_id = str(out.get("run_id") or "").strip()
                created_at = str(out.get("created_at") or "").strip()

                where = ["step_name = ?"]
                params: list[Any] = [step_name]
                if run_id:
                    where.append("run_id = ?")
                    params.append(run_id)
                elif session_id:
                    where.append("session_id = ?")
                    params.append(session_id)
                if created_at:
                    where.append("created_at <= ?")
                    params.append(created_at)

                match = conn.execute(
                    f"""
                    SELECT prompt_text, response_json
                    FROM agent_execution_logs
                    WHERE {' AND '.join(where)}
                    ORDER BY created_at DESC, id DESC
                    LIMIT 1
                    """,
                    params,
                ).fetchone()

                if match:
                    if need_output:
                        response_json = str(match["response_json"] or "").strip()
                        if response_json:
                            out["output_json"] = json.dumps({"output": self._try_parse_json_dict(response_json)}, ensure_ascii=False)
                    if need_input:
                        prompt_text = str(match["prompt_text"] or "").strip()
                        if prompt_text:
                            out["input_json"] = json.dumps({"prompt_text": prompt_text}, ensure_ascii=False)
                enriched.append(out)
        return enriched

    def restore_from_recycle_bin(self, *, recycle_ids: list[int]) -> dict[str, int]:
        if not recycle_ids:
            return {"restored": 0}

        restored = 0
        with db_manager.connect("ui_classifications", readonly=False) as ui_conn:
            for rid in recycle_ids:
                record = ui_conn.execute(
                    """
                    SELECT id, source_db, source_table, key_col, key_val, row_json
                    FROM ui_deleted_records
                    WHERE id = ? AND restored_at IS NULL
                    """,
                    [rid],
                ).fetchone()
                if not record:
                    continue

                source_db = record["source_db"]
                source_table = record["source_table"]
                key_col = record["key_col"]
                row_data = json.loads(record["row_json"])
                data_cols = [k for k in row_data.keys() if k != "__rowid__"]

                with db_manager.connect(source_db, readonly=False) as src_conn:
                    placeholders = ", ".join(["?" for _ in data_cols])
                    col_sql = ", ".join([f'"{c}"' for c in data_cols])
                    if key_col == "__rowid__" and "__rowid__" in row_data:
                        src_conn.execute(
                            f'INSERT OR REPLACE INTO "{source_table}"(rowid, {col_sql}) VALUES(?, {placeholders})',
                            [row_data["__rowid__"], *[row_data[c] for c in data_cols]],
                        )
                    else:
                        src_conn.execute(
                            f'INSERT OR REPLACE INTO "{source_table}"({col_sql}) VALUES({placeholders})',
                            [row_data[c] for c in data_cols],
                        )
                    src_conn.commit()

                ui_conn.execute(
                    "UPDATE ui_deleted_records SET restored_at = CURRENT_TIMESTAMP WHERE id = ?",
                    [rid],
                )
                restored += 1

            ui_conn.commit()
        return {"restored": restored}


explorer_repo = ExplorerRepository()
