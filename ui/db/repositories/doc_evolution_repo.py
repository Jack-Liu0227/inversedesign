from __future__ import annotations

import difflib
import importlib.util
import os
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

from ui.db.connection import db_manager
from ui.db.repositories.explorer_repo import ExplorerRepository, explorer_repo
from ui.services.timezone_service import beijing_range_to_utc_sql, normalize_row_datetimes


class DocEvolutionRepository:
    DB_KEY = "material_agent_shared"
    TABLE = "material_doc_knowledge"

    def __init__(self, explorer: ExplorerRepository | None = None) -> None:
        self._explorer = explorer or explorer_repo

    @staticmethod
    def _ensure_bootstrap_docs() -> None:
        try:
            module_path = Path(__file__).resolve().parents[3] / "src" / "common" / "material_doc_store.py"
            spec = importlib.util.spec_from_file_location("material_doc_store_for_ui_evolution", str(module_path))
            if spec is None or spec.loader is None:
                return
            module = importlib.util.module_from_spec(spec)
            sys.modules[spec.name] = module
            spec.loader.exec_module(module)
            fn = getattr(module, "ensure_bootstrap_material_docs", None)
            if callable(fn):
                fn()
            candidate_backfill_fn = getattr(module, "backfill_iteration_candidate_docs", None)
            if callable(candidate_backfill_fn):
                candidate_backfill_fn(max_rounds=500)
            backfill_fn = getattr(module, "ensure_iteration_theory_snapshots", None)
            # Avoid regenerating deleted round snapshots on every page load.
            auto_backfill = str(os.getenv("DOC_EVOLUTION_AUTO_BACKFILL", "")).strip().lower() in {
                "1",
                "true",
                "yes",
                "on",
            }
            if auto_backfill and callable(backfill_fn):
                backfill_fn(max_rounds=500)
        except Exception:
            return

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
        self._ensure_bootstrap_docs()
        if not self.table_exists():
            return {"material_types": [], "run_ids": []}
        with db_manager.connect(self.DB_KEY, readonly=True) as conn:
            mt_rows = conn.execute(
                f'SELECT DISTINCT material_type FROM "{self.TABLE}" WHERE material_type <> \'\' ORDER BY material_type'
            ).fetchall()
            run_rows = conn.execute(
                f"""
                SELECT DISTINCT workflow_run_id
                FROM "{self.TABLE}"
                WHERE workflow_run_id <> ''
                ORDER BY workflow_run_id DESC
                """
            ).fetchall()
        return {
            "material_types": [str(r["material_type"]) for r in mt_rows],
            "run_ids": [str(r["workflow_run_id"]) for r in run_rows],
        }

    @staticmethod
    def _preview_text(text: str, max_chars: int = 220) -> str:
        normalized = " ".join(str(text or "").split())
        if len(normalized) <= max_chars:
            return normalized
        return normalized[: max_chars - 3] + "..."

    @staticmethod
    def _parse_dt(raw: str) -> datetime:
        text = str(raw or "").strip()
        if not text:
            return datetime.min
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except Exception:
            return datetime.min

    def _aggregate_cells(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        if not rows:
            return {
                "text_preview": "",
                "full_text": "",
                "doc_ids": [],
                "chunk_count": 0,
                "created_at": "",
            }
        def _source_order(source_name: str) -> int:
            text = str(source_name or "")
            if text.endswith(".summary.md"):
                return 0
            if text.endswith(".theory_evolution.md"):
                return 1
            if text.endswith(".candidates.md"):
                return 2
            return 3
        ordered = sorted(
            rows,
            key=lambda x: (
                _source_order(str(x.get("source_name") or "")),
                str(x.get("source_name") or ""),
                int(x.get("chunk_index") or 0),
                int(x.get("id") or 0),
            ),
        )
        sections: list[str] = []
        for row in ordered:
            title = str(row.get("title") or "").strip()
            content = str(row.get("content") or "").strip()
            if title:
                sections.append(f"## {title}")
            if content:
                sections.append(content)
        full_text = "\n\n".join([x for x in sections if x]).strip()
        created_at = ""
        newest = max(rows, key=lambda x: self._parse_dt(str(x.get("created_at") or "")))
        if isinstance(newest, dict):
            created_at = str(newest.get("created_at") or "")
        return {
            "text_preview": self._preview_text(full_text),
            "full_text": full_text,
            "doc_ids": [int(r.get("id")) for r in ordered if str(r.get("id", "")).strip()],
            "chunk_count": len(ordered),
            "created_at": created_at,
        }

    def list_evolution_matrix(
        self,
        *,
        material_type: str = "",
        workflow_run_id: str = "",
        q: str = "",
        created_from: str = "",
        created_to: str = "",
        limit_runs: int = 30,
    ) -> dict[str, Any]:
        self._ensure_bootstrap_docs()
        if not self.table_exists():
            return {"columns": ["bootstrap"], "rows": [], "total_runs": 0}

        where = ["source_kind = 'iteration_feedback'"]
        params: list[Any] = []
        if material_type.strip():
            where.append("material_type = ?")
            params.append(material_type.strip().lower())
        if workflow_run_id.strip():
            where.append("workflow_run_id = ?")
            params.append(workflow_run_id.strip())
        if q.strip():
            like = f"%{q.strip()}%"
            where.append("(title LIKE ? OR content LIKE ? OR source_name LIKE ? OR tags_json LIKE ?)")
            params.extend([like, like, like, like])
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

        with db_manager.connect(self.DB_KEY, readonly=True) as conn:
            rows = conn.execute(
                f"""
                SELECT id, material_type, source_name, chunk_index, source_kind,
                       workflow_run_id, session_id, round_index, title, content, tags_json, created_at
                FROM "{self.TABLE}"
                {where_sql}
                ORDER BY created_at DESC, id DESC
                """,
                params,
            ).fetchall()
        parsed_feedback = [normalize_row_datetimes(dict(r)) for r in rows]

        by_run: dict[str, dict[str, Any]] = {}
        round_indices: set[int] = set()
        for row in parsed_feedback:
            workflow_run_id_value = str(row.get("workflow_run_id") or "").strip()
            mtype = str(row.get("material_type") or "").strip().lower()
            source_kind = str(row.get("source_kind") or "").strip().lower()
            source_name = str(row.get("source_name") or "").strip()
            if source_kind != "iteration_feedback" or not workflow_run_id_value:
                continue
            if workflow_run_id_value not in by_run:
                by_run[workflow_run_id_value] = {
                    "workflow_run_id": workflow_run_id_value,
                    "material_type": mtype,
                    "session_id": str(row.get("session_id") or ""),
                    "round_rows": defaultdict(list),
                }
            rdx = int(row.get("round_index") or 0)
            if rdx > 0:
                if (
                    source_name == f"{mtype}.theory_evolution.md"
                    or source_name.endswith(".summary.md")
                ):
                    round_indices.add(rdx)
                    by_run[workflow_run_id_value]["round_rows"][rdx].append(row)

        sorted_runs = sorted(by_run.values(), key=lambda x: x["workflow_run_id"], reverse=True)[: max(1, int(limit_runs))]
        selected_material_types = sorted({str(r.get("material_type") or "").strip().lower() for r in sorted_runs if r.get("material_type")})
        bootstrap_by_type: dict[str, list[dict[str, Any]]] = defaultdict(list)
        bootstrap_material_types: list[str] = []
        if workflow_run_id.strip():
            bootstrap_material_types = selected_material_types
        elif material_type.strip():
            bootstrap_material_types = [material_type.strip().lower()]
        else:
            with db_manager.connect(self.DB_KEY, readonly=True) as conn:
                mt_rows = conn.execute(
                    f"""
                    SELECT DISTINCT material_type
                    FROM "{self.TABLE}"
                    WHERE source_kind = 'bootstrap' AND material_type <> ''
                    ORDER BY material_type ASC
                    """
                ).fetchall()
            bootstrap_material_types = [str(r["material_type"]).strip().lower() for r in mt_rows if str(r["material_type"]).strip()]

        if bootstrap_material_types:
            placeholders = ",".join("?" for _ in bootstrap_material_types)
            with db_manager.connect(self.DB_KEY, readonly=True) as conn:
                bootstrap_rows = conn.execute(
                    f"""
                    SELECT id, material_type, source_name, chunk_index, source_kind,
                           workflow_run_id, session_id, round_index, title, content, tags_json, created_at
                    FROM "{self.TABLE}"
                    WHERE source_kind = 'bootstrap'
                      AND material_type IN ({placeholders})
                    ORDER BY source_name ASC, chunk_index ASC, id ASC
                    """,
                    bootstrap_material_types,
                ).fetchall()
            for row in bootstrap_rows:
                normalized = normalize_row_datetimes(dict(row))
                mtype = str(normalized.get("material_type") or "").strip().lower()
                if mtype:
                    bootstrap_by_type[mtype].append(normalized)

        sorted_rounds = sorted(round_indices)
        columns = ["bootstrap"] + [f"round_{i}" for i in sorted_rounds]

        output_rows: list[dict[str, Any]] = []
        for row in sorted_runs:
            mtype = str(row.get("material_type") or "")
            bootstrap = self._aggregate_cells(bootstrap_by_type.get(mtype, []))
            rounds_payload = []
            for rdx in sorted_rounds:
                cell = self._aggregate_cells(row["round_rows"].get(rdx, []))
                rounds_payload.append({"round_index": rdx, **cell})
            round_count = len([x for x in rounds_payload if x.get("chunk_count", 0) > 0])
            output_rows.append(
                {
                    "workflow_run_id": row["workflow_run_id"],
                    "material_type": mtype,
                    "session_id": row.get("session_id", ""),
                    "round_count": round_count,
                    "bootstrap": bootstrap,
                    "rounds": rounds_payload,
                }
            )
        # Show materials that only have bootstrap docs (round-0) even without iteration runs.
        if not workflow_run_id.strip():
            existing_types = {str(r.get("material_type") or "").strip().lower() for r in output_rows}
            for mtype in sorted(bootstrap_by_type.keys()):
                if not mtype or mtype in existing_types:
                    continue
                bootstrap = self._aggregate_cells(bootstrap_by_type.get(mtype, []))
                full_text = str(bootstrap.get("full_text") or "").lower()
                if q.strip() and q.strip().lower() not in full_text:
                    continue
                rounds_payload = [{"round_index": rdx, **self._aggregate_cells([])} for rdx in sorted_rounds]
                output_rows.append(
                    {
                        "workflow_run_id": f"bootstrap-only:{mtype}",
                        "material_type": mtype,
                        "session_id": "",
                        "round_count": 0,
                        "bootstrap": bootstrap,
                        "rounds": rounds_payload,
                    }
                )
        return {
            "columns": columns,
            "rows": output_rows,
            "total_runs": len(output_rows),
        }

    def batch_delete_docs(self, doc_ids: list[int]) -> dict[str, int]:
        unique_ids_set: set[int] = set()
        for raw in doc_ids:
            try:
                value = int(raw)
            except (TypeError, ValueError):
                continue
            if value > 0:
                unique_ids_set.add(value)
        if not unique_ids_set:
            return {"deleted": 0}
        key_values = [str(v) for v in sorted(unique_ids_set)]
        return self._explorer.delete_rows_to_recycle_bin(
            db_key=self.DB_KEY,
            table=self.TABLE,
            key_col="id",
            key_values=key_values,
        )

    def batch_delete_by_workflow_run_ids(self, workflow_run_ids: list[str]) -> dict[str, Any]:
        unique_run_ids: list[str] = []
        for raw in workflow_run_ids:
            value = str(raw or "").strip()
            if not value or value.startswith("bootstrap-only:") or value in unique_run_ids:
                continue
            unique_run_ids.append(value)
        if not unique_run_ids:
            return {
                "filter_col": "workflow_run_id",
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
        for workflow_run_id in unique_run_ids:
            result = self._explorer.delete_by_workflow_run_id_across_workflow_dbs_to_recycle_bin(
                workflow_run_id=workflow_run_id
            )
            deleted_total += int(result.get("deleted", 0) or 0)
            scanned_tables = max(scanned_tables, int(result.get("scanned_tables", 0) or 0))
            for item in result.get("details", []) or []:
                if isinstance(item, dict):
                    details.append({"workflow_run_id": workflow_run_id, **item})
            for item in result.get("errors", []) or []:
                if isinstance(item, dict):
                    errors.append({"workflow_run_id": workflow_run_id, **item})

        return {
            "filter_col": "workflow_run_id",
            "filter_values": unique_run_ids,
            "deleted": int(deleted_total),
            "details": details,
            "errors": errors,
            "scanned_tables": int(scanned_tables),
        }

    def build_diff(self, *, left_doc_ids: list[int], right_doc_ids: list[int]) -> dict[str, Any]:
        left = self._load_docs_by_ids(left_doc_ids)
        right = self._load_docs_by_ids(right_doc_ids)
        left_text = self._aggregate_cells(left).get("full_text", "")
        right_text = self._aggregate_cells(right).get("full_text", "")
        diff_lines = difflib.unified_diff(
            left_text.splitlines(),
            right_text.splitlines(),
            fromfile="left",
            tofile="right",
            lineterm="",
        )
        return {
            "left_text": left_text,
            "right_text": right_text,
            "unified_diff": "\n".join(diff_lines),
        }

    def _load_docs_by_ids(self, ids: list[int]) -> list[dict[str, Any]]:
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
            return []
        placeholders = ",".join("?" for _ in unique_ids)
        with db_manager.connect(self.DB_KEY, readonly=True) as conn:
            rows = conn.execute(
                f"""
                SELECT id, source_name, chunk_index, title, content, created_at
                FROM "{self.TABLE}"
                WHERE id IN ({placeholders})
                ORDER BY source_name ASC, chunk_index ASC, id ASC
                """,
                unique_ids,
            ).fetchall()
        return [normalize_row_datetimes(dict(r)) for r in rows]


doc_evolution_repo = DocEvolutionRepository()
