from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from fastapi import APIRouter, Body, Depends, HTTPException
from starlette.concurrency import run_in_threadpool

from ui.db.repositories.doc_evolution_repo import DocEvolutionRepository
from ui.db.connection import db_manager
from ui.db.repositories.explorer_repo import ExplorerRepository
from ui.db.repositories.material_data_repo import MaterialDataRepository
from ui.dependencies import (
    get_doc_evolution_repository,
    get_explorer_repository,
    get_material_data_repository,
)


router = APIRouter(prefix="/material-data", tags=["material-data"])


def _parse_id_csv(raw: str) -> list[int]:
    values: list[int] = []
    for token in str(raw or "").split(","):
        text = token.strip()
        if not text:
            continue
        try:
            value = int(text)
        except (TypeError, ValueError):
            continue
        if value > 0:
            values.append(value)
    return values


def _parse_text_csv(raw: str) -> list[str]:
    values: list[str] = []
    for token in str(raw or "").split(","):
        text = token.strip()
        if text and text not in values:
            values.append(text)
    return values


def _load_doc_store_module():
    module_path = Path(__file__).resolve().parents[2] / "src" / "common" / "material_doc_store.py"
    spec = importlib.util.spec_from_file_location("material_doc_store_ui", str(module_path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module: {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@router.post("/import-csv")
async def import_csv_to_db() -> dict[str, object]:
    from src.common.dataset_store import import_csv_datasets_to_db

    result = await run_in_threadpool(import_csv_datasets_to_db)
    return {"ok": True, **result}


@router.get("/rows")
async def list_rows(
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
    material_data_repository: MaterialDataRepository = Depends(get_material_data_repository),
) -> dict[str, object]:
    rows, total = await run_in_threadpool(
        material_data_repository.list_rows,
        page=page,
        page_size=page_size,
        material_type=material_type,
        source=source,
        q=q,
        workflow_run_id=workflow_run_id,
        run_note=run_note,
        created_from=created_from,
        created_to=created_to,
        valid_only=valid_only,
        sort_by=sort_by,
        sort_order=sort_order,
    )
    return {
        "rows": rows,
        "total": total,
        "page": page,
        "page_size": page_size,
        "sort_by": sort_by,
        "sort_order": "asc" if str(sort_order).lower() == "asc" else "desc",
    }


@router.get("/analytics")
async def material_data_analytics(
    material_type: str = "",
    source: str = "",
    q: str = "",
    workflow_run_id: str = "",
    run_note: str = "",
    created_from: str = "",
    created_to: str = "",
    valid_only: bool = False,
    properties: str = "",
    pareto_x: str = "",
    pareto_y: str = "",
    material_data_repository: MaterialDataRepository = Depends(get_material_data_repository),
) -> dict[str, object]:
    result = await run_in_threadpool(
        material_data_repository.build_analytics,
        material_type=material_type,
        source=source,
        q=q,
        workflow_run_id=workflow_run_id,
        run_note=run_note,
        created_from=created_from,
        created_to=created_to,
        valid_only=valid_only,
        properties=_parse_text_csv(properties),
        pareto_x=str(pareto_x or "").strip(),
        pareto_y=str(pareto_y or "").strip(),
)
    return {"ok": True, **result}


@router.get("/runs")
async def list_recent_runs(
    limit: int = 100,
    material_data_repository: MaterialDataRepository = Depends(get_material_data_repository),
) -> dict[str, object]:
    rows = await run_in_threadpool(material_data_repository.list_recent_runs, max(1, min(int(limit), 500)))
    return {"ok": True, "rows": rows}


@router.post("/batch-delete")
async def batch_delete_rows(
    payload: dict[str, object] | None = Body(None),
    explorer_repository: ExplorerRepository = Depends(get_explorer_repository),
) -> dict[str, object]:
    body = payload or {}
    ids_raw = body.get("ids", [])
    if not isinstance(ids_raw, list):
        raise HTTPException(status_code=400, detail="ids must be a list")
    key_values = [str(v) for v in ids_raw]
    result = await run_in_threadpool(
        explorer_repository.delete_rows_to_recycle_bin,
        db_key="material_agent_shared",
        table="material_dataset_rows",
        key_col="id",
        key_values=key_values,
    )
    return {"ok": True, **result}


@router.post("/normalize-processing")
async def normalize_processing_rows(
    material_data_repository: MaterialDataRepository = Depends(get_material_data_repository),
) -> dict[str, object]:
    updated = await run_in_threadpool(material_data_repository.normalize_processing_rows)
    return {"ok": True, "updated": updated}


@router.post("/import-docs")
async def import_material_docs() -> dict[str, object]:
    module = _load_doc_store_module()
    fn = getattr(module, "upsert_material_docs_from_dir", None)
    if not callable(fn):
        raise HTTPException(status_code=500, detail="Document import function not available")
    written = await run_in_threadpool(fn)
    return {"ok": True, "rows_written": int(written)}


@router.get("/docs")
async def list_material_docs(
    page: int = 1,
    page_size: int = 50,
    material_type: str = "",
    source_kind: str = "",
    workflow_run_id: str = "",
    q: str = "",
) -> dict[str, object]:
    where = []
    params: list[object] = []
    if material_type.strip():
        where.append("material_type = ?")
        params.append(material_type.strip().lower())
    if source_kind.strip():
        where.append("source_kind = ?")
        params.append(source_kind.strip().lower())
    if workflow_run_id.strip():
        where.append("workflow_run_id = ?")
        params.append(workflow_run_id.strip())
    if q.strip():
        like = f"%{q.strip()}%"
        where.append("(title LIKE ? OR content LIKE ? OR source_name LIKE ? OR tags_json LIKE ?)")
        params.extend([like, like, like, like])
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    current_page = max(1, int(page))
    current_size = max(10, min(int(page_size), 200))
    offset = (current_page - 1) * current_size
    with db_manager.connect("material_agent_shared", readonly=True) as conn:
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='material_doc_knowledge' LIMIT 1"
        ).fetchone()
        if not exists:
            return {"rows": [], "total": 0, "page": current_page, "page_size": current_size}
        total = int(
            conn.execute(f"SELECT COUNT(*) AS c FROM material_doc_knowledge {where_sql}", params).fetchone()["c"]
        )
        rows = conn.execute(
            f"""
            SELECT id, material_type, source_name, chunk_index, source_kind, workflow_run_id, session_id, round_index,
                   title, content, tags_json, created_at
            FROM material_doc_knowledge
            {where_sql}
            ORDER BY created_at DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            [*params, current_size, offset],
        ).fetchall()
    output = [dict(r) for r in rows]
    return {"rows": output, "total": total, "page": current_page, "page_size": current_size}


@router.get("/docs/full")
async def get_full_material_doc(
    source_name: str,
    material_type: str = "",
    source_kind: str = "",
    workflow_run_id: str = "",
    round_index: int | None = None,
) -> dict[str, object]:
    source_name = str(source_name or "").strip()
    if not source_name:
        raise HTTPException(status_code=400, detail="source_name is required")

    where = ["source_name = ?"]
    params: list[object] = [source_name]
    if material_type.strip():
        where.append("material_type = ?")
        params.append(material_type.strip().lower())
    if source_kind.strip():
        where.append("source_kind = ?")
        params.append(source_kind.strip().lower())
    if workflow_run_id.strip():
        where.append("workflow_run_id = ?")
        params.append(workflow_run_id.strip())
    if round_index is not None:
        where.append("round_index = ?")
        params.append(int(round_index))
    where_sql = f"WHERE {' AND '.join(where)}"

    with db_manager.connect("material_agent_shared", readonly=True) as conn:
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='material_doc_knowledge' LIMIT 1"
        ).fetchone()
        if not exists:
            raise HTTPException(status_code=404, detail="material_doc_knowledge table not found")

        rows = conn.execute(
            f"""
            SELECT material_type, source_name, source_kind, workflow_run_id, session_id, round_index,
                   chunk_index, title, content, created_at
            FROM material_doc_knowledge
            {where_sql}
            ORDER BY chunk_index ASC, id ASC
            """,
            params,
        ).fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail="No document chunks found for given filters")

    records = [dict(r) for r in rows]
    # Rebuild markdown-style full doc for easier reading.
    sections: list[str] = []
    for row in records:
        title = str(row.get("title") or "").strip()
        content = str(row.get("content") or "").strip()
        if title:
            sections.append(f"## {title}")
        if content:
            sections.append(content)
    full_text = "\n\n".join([x for x in sections if x]).strip()

    head = records[0]
    return {
        "source_name": source_name,
        "material_type": head.get("material_type", ""),
        "source_kind": head.get("source_kind", ""),
        "workflow_run_id": head.get("workflow_run_id", ""),
        "session_id": head.get("session_id", ""),
        "round_index": int(head.get("round_index") or 0),
        "chunk_count": len(records),
        "full_text": full_text,
        "chunks": records,
    }


@router.get("/docs/evolution")
async def list_doc_evolution(
    material_type: str = "",
    workflow_run_id: str = "",
    q: str = "",
    created_from: str = "",
    created_to: str = "",
    limit_runs: int = 30,
    doc_evolution_repository: DocEvolutionRepository = Depends(get_doc_evolution_repository),
) -> dict[str, object]:
    result = await run_in_threadpool(
        doc_evolution_repository.list_evolution_matrix,
        material_type=material_type,
        workflow_run_id=workflow_run_id,
        q=q,
        created_from=created_from,
        created_to=created_to,
        limit_runs=max(1, min(int(limit_runs), 200)),
    )
    return {"ok": True, **result}


@router.post("/docs/batch-delete")
async def batch_delete_docs(
    payload: dict[str, object] | None = Body(None),
    doc_evolution_repository: DocEvolutionRepository = Depends(get_doc_evolution_repository),
) -> dict[str, object]:
    body = payload or {}
    raw_ids = body.get("doc_ids", [])
    if not isinstance(raw_ids, list):
        raise HTTPException(status_code=400, detail="doc_ids must be a list")
    ids: list[int] = []
    for raw in raw_ids:
        try:
            value = int(raw)
        except (TypeError, ValueError):
            continue
        if value > 0:
            ids.append(value)
    if not ids:
        raise HTTPException(status_code=400, detail="doc_ids is empty or invalid")
    result = await run_in_threadpool(doc_evolution_repository.batch_delete_docs, ids)
    return {"ok": True, **result}


@router.get("/docs/diff")
async def diff_docs(
    left_doc_ids: str = "",
    right_doc_ids: str = "",
    doc_evolution_repository: DocEvolutionRepository = Depends(get_doc_evolution_repository),
) -> dict[str, object]:
    left_ids = _parse_id_csv(left_doc_ids)
    right_ids = _parse_id_csv(right_doc_ids)
    if not left_ids and not right_ids:
        raise HTTPException(status_code=400, detail="left_doc_ids/right_doc_ids cannot both be empty")
    result = await run_in_threadpool(
        doc_evolution_repository.build_diff,
        left_doc_ids=left_ids,
        right_doc_ids=right_ids,
    )
    return {"ok": True, **result}
