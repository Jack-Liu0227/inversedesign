from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates

from ui.config import get_config
from ui.db.connection import db_manager
from ui.db.repositories.doc_evolution_repo import doc_evolution_repo
from ui.db.repositories.explorer_repo import explorer_repo
from ui.db.repositories.material_data_repo import material_data_repo
from ui.db.repositories.tool_trace_repo import tool_trace_repo
from ui.routers.page_utils import (
    is_workflow_filterable_table,
    parse_success_query,
    to_detail_sections,
    to_record_card,
    tool_trace_detail_payload,
    viewer_extra_filters,
)
from ui.services.classification_service import classification_service
from ui.services.stats_service import stats_service


cfg = get_config()
router = APIRouter(tags=["pages"])
templates = Jinja2Templates(directory=str(cfg.templates_dir))


def _aggregate_doc_rows_for_full_view(rows: list[dict]) -> list[dict]:
    grouped: dict[tuple[str, str, str, str, int], list[dict]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        key = (
            str(row.get("material_type") or "").strip().lower(),
            str(row.get("source_name") or "").strip(),
            str(row.get("source_kind") or "").strip().lower(),
            str(row.get("workflow_run_id") or "").strip(),
            int(row.get("round_index") or 0),
        )
        grouped.setdefault(key, []).append(row)

    output: list[dict] = []
    for key, items in grouped.items():
        ordered = sorted(items, key=lambda x: int(x.get("chunk_index") or 0))
        first = ordered[0]
        sections: list[str] = []
        for item in ordered:
            title = str(item.get("title") or "").strip()
            content = str(item.get("content") or "").strip()
            if title:
                sections.append(f"## {title}")
            if content:
                sections.append(content)
        full_text = "\n\n".join([x for x in sections if x]).strip()
        created_at_values = [str(x.get("created_at") or "") for x in ordered if str(x.get("created_at") or "").strip()]
        output.append(
            {
                "id": first.get("id"),
                "material_type": key[0],
                "source_name": key[1],
                "source_kind": key[2],
                "workflow_run_id": key[3],
                "round_index": key[4],
                "session_id": first.get("session_id"),
                "chunk_index": -1,
                "chunk_count": len(ordered),
                "title": f"{key[1]} (full)",
                "content": full_text,
                "tags_json": first.get("tags_json", "[]"),
                "created_at": max(created_at_values) if created_at_values else first.get("created_at"),
            }
        )

    output.sort(key=lambda x: str(x.get("created_at") or ""), reverse=True)
    return output


def _tool_trace_step_log_fallback_rows(
    *,
    session_id: str | None,
    step_name: str | None,
    success_value: int | None,
    sort_order: str,
    limit: int = 500,
) -> list[dict]:
    if not step_name:
        return []
    extra_filters = {"step_name": str(step_name or "").strip()}
    if session_id:
        extra_filters["session_id"] = str(session_id).strip()
    if success_value is not None:
        extra_filters["success"] = str(int(success_value))
    rows, _, _, _ = explorer_repo.list_rows(
        db_key="workflow_audit",
        table="workflow_step_logs",
        page=1,
        page_size=max(1, min(int(limit), 1000)),
        q=None,
        identifier=None,
        sort_order=sort_order,
        extra_filters=extra_filters,
    )
    output: list[dict] = []
    for row in rows:
        output.append(
            {
                "id": int(row.get("id") or 0),
                "source_type": "step_log",
                "created_at": row.get("created_at"),
                "session_id": row.get("session_id"),
                "run_id": row.get("run_id"),
                "step_name": row.get("step_name"),
                "agent_name": row.get("step_name"),
                "tool_name": "no-tool",
                "success": row.get("success"),
                "error_text": row.get("error_text"),
                "tool_args_json": row.get("input_json"),
                "tool_result_json": row.get("output_json"),
            }
        )
    return output


def _default_db_key(db_keys: list[str], preferred: str | None = None) -> str:
    if preferred and preferred in db_keys:
        return preferred
    if "workflow_audit" in db_keys:
        return "workflow_audit"
    return db_keys[0] if db_keys else ""


def _default_table_name(tables: list[str]) -> str:
    if "workflow_step_logs" in tables:
        return "workflow_step_logs"
    return tables[0] if tables else ""


@router.get("/")
def dashboard(request: Request):
    data = stats_service.dashboard()
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "title": "Material Data Dashboard",
            "stats": data,
            "active": "dashboard",
        },
    )


@router.get("/explorer")
def explorer_page(
    request: Request,
    db: str | None = None,
    table: str | None = None,
    q: str | None = None,
    code: str | None = None,
    material_type: str | None = None,
    source_kind: str | None = None,
    workflow_run_id: str | None = None,
    trace_id: str | None = None,
    session_id: str | None = None,
    run_id: str | None = None,
    step_name: str | None = None,
    agent_name: str | None = None,
    tool_name: str | None = None,
    event_type: str | None = None,
    status: str | None = None,
    decision: str | None = None,
    success: str | None = None,
    created_from: str | None = None,
    created_to: str | None = None,
    sort_order: str = "desc",
    page: int = 1,
    page_size: int = 50,
):
    databases = explorer_repo.list_databases()
    db_keys = [item["key"] for item in databases]
    selected_db = db if db in db_keys else _default_db_key(db_keys, preferred="prediction_prompt_logs")
    tables = explorer_repo.list_tables(selected_db) if selected_db else []
    selected_table = table if table in tables else _default_table_name(tables)

    rows = []
    total = 0
    columns: list[str] = []
    record_key = "rowid"
    explorer_filter_columns = [
        "material_type",
        "source_kind",
        "workflow_run_id",
        "trace_id",
        "session_id",
        "run_id",
        "step_name",
        "agent_name",
        "tool_name",
        "event_type",
        "status",
        "decision",
        "success",
    ]
    col_names: list[str] = []
    explorer_filter_options: dict[str, list[str]] = {}
    explorer_filters = {
        "material_type": str(material_type or "").strip(),
        "source_kind": str(source_kind or "").strip(),
        "workflow_run_id": str(workflow_run_id or "").strip(),
        "trace_id": str(trace_id or "").strip(),
        "session_id": str(session_id or "").strip(),
        "run_id": str(run_id or "").strip(),
        "step_name": str(step_name or "").strip(),
        "agent_name": str(agent_name or "").strip(),
        "tool_name": str(tool_name or "").strip(),
        "event_type": str(event_type or "").strip(),
        "status": str(status or "").strip(),
        "decision": str(decision or "").strip(),
        "success": str(success or "").strip(),
    }
    if selected_db and selected_table:
        table_columns = explorer_repo.get_table_columns(selected_db, selected_table)
        col_names = [str(c.get("name")) for c in table_columns]
        effective_extra_filters: dict[str, str] = {}
        for k, v in explorer_filters.items():
            if v and k in col_names:
                effective_extra_filters[k] = v
        for col in explorer_filter_columns:
            if col in col_names:
                explorer_filter_options[col] = explorer_repo.list_distinct_values(
                    db_key=selected_db,
                    table=selected_table,
                    column=col,
                    extra_filters=effective_extra_filters,
                    limit=200,
                )
        rows, total, columns, record_key = explorer_repo.list_rows(
            db_key=selected_db,
            table=selected_table,
            page=max(page, 1),
            page_size=min(max(page_size, 10), 200),
            q=q,
            identifier=code,
            sort_order=sort_order,
            extra_filters=effective_extra_filters,
            created_from=created_from,
            created_to=created_to,
        )

    return templates.TemplateResponse(
        request,
        "explorer.html",
        {
            "title": "Database Explorer",
            "active": "explorer",
            "databases": databases,
            "tables": tables,
            "selected_db": selected_db,
            "selected_table": selected_table,
            "db": selected_db,
            "table": selected_table,
            "rows": rows,
            "columns": columns,
            "record_key": record_key,
            "total": total,
            "q": q or "",
            "code": code or "",
            "created_from": created_from or "",
            "created_to": created_to or "",
            "page": max(page, 1),
            "page_size": min(max(page_size, 10), 200),
            "sort_order": "asc" if str(sort_order).lower() == "asc" else "desc",
            "explorer_filter_options": explorer_filter_options,
            "explorer_filters": explorer_filters,
            "explorer_filter_columns": [c for c in explorer_filter_columns if c in col_names],
            "tags": classification_service.list_tags(),
        },
    )


@router.get("/viewer")
def viewer_page(
    request: Request,
    db: str | None = None,
    table: str | None = None,
    q: str | None = None,
    code: str | None = None,
    trace_id: str | None = None,
    session_id: str | None = None,
    run_id: str | None = None,
    material_type: str | None = None,
    step_name: str | None = None,
    agent_name: str | None = None,
    tool_name: str | None = None,
    status: str | None = None,
    event_type: str | None = None,
    decision: str | None = None,
    should_stop: str | None = None,
    success: str | None = None,
    doc_view: str = "chunk",
    page_size: int = 30,
    sort_order: str = "desc",
):
    databases = explorer_repo.list_databases()
    db_keys = [item["key"] for item in databases]
    selected_db = db if db in db_keys else _default_db_key(db_keys)
    tables = explorer_repo.list_tables(selected_db) if selected_db else []
    selected_table = table if table in tables else _default_table_name(tables)

    rows: list[dict] = []
    total = 0
    columns: list[str] = []
    record_key = "__rowid__"
    selected_table_columns = explorer_repo.get_table_columns(selected_db, selected_table) if selected_db and selected_table else []
    selected_col_names = [str(c.get("name")) for c in selected_table_columns]
    has_material_type_column = "material_type" in selected_col_names
    material_type_values = (
        explorer_repo.list_distinct_values(
            db_key=selected_db,
            table=selected_table,
            column="material_type",
        )
        if selected_db and selected_table and has_material_type_column
        else []
    )
    effective_material_type = str(material_type or "").strip() if has_material_type_column else ""
    extra_filters = viewer_extra_filters(
        trace_id=trace_id,
        session_id=session_id,
        run_id=run_id,
        material_type=effective_material_type,
        step_name=step_name,
        agent_name=agent_name,
        tool_name=tool_name,
        status=status,
        event_type=event_type,
        decision=decision,
        should_stop=should_stop,
        success=success,
    )
    if selected_db and selected_table:
        rows, total, columns, record_key = explorer_repo.list_rows(
            db_key=selected_db,
            table=selected_table,
            page=1,
            page_size=min(max(page_size, 10), 200),
            q=q,
            identifier=code,
            sort_order=sort_order,
            extra_filters=extra_filters,
        )
        if selected_table == "material_doc_knowledge" and str(doc_view).strip().lower() == "full":
            rows = _aggregate_doc_rows_for_full_view(rows)
            total = len(rows)
            if "chunk_count" not in columns:
                columns = [*columns, "chunk_count"]

    records = [to_record_card(row, columns, record_key, selected_table) for row in rows]
    detail_sections = to_detail_sections(rows[0]) if rows else []
    detail_key = str(rows[0].get(record_key)) if rows else ""
    detail_row = rows[0] if rows else {}
    recycle_items = explorer_repo.list_recycle_bin(limit=100)
    workflow_filterable = is_workflow_filterable_table(selected_db, selected_table)
    viewer_filters = (
        explorer_repo.viewer_filter_options(
            db_key=selected_db,
            table=selected_table,
            trace_id=trace_id,
            session_id=session_id,
            run_id=run_id,
            step_name=step_name,
            agent_name=agent_name,
            event_type=event_type,
            decision=decision,
            should_stop=should_stop,
            success=success,
        )
        if workflow_filterable
        else {"step_names": [], "agent_names": [], "tool_names": [], "statuses": [], "success_values": []}
    )

    return templates.TemplateResponse(
        request,
        "viewer.html",
        {
            "title": "Result Viewer",
            "active": "viewer",
            "databases": databases,
            "tables": tables,
            "selected_db": selected_db,
            "selected_table": selected_table,
            "q": q or "",
            "code": code or "",
            "page_size": min(max(page_size, 10), 200),
            "sort_order": "asc" if str(sort_order).lower() == "asc" else "desc",
            "total": total,
            "records": records,
            "detail_sections": detail_sections,
            "detail_row": detail_row,
            "detail_key": detail_key,
            "detail_key_col": record_key,
            "recycle_items": recycle_items,
            "db": selected_db,
            "table": selected_table,
            "record_key": record_key,
            "doc_view": "full" if str(doc_view).strip().lower() == "full" else "chunk",
            "workflow_filterable": workflow_filterable,
            "viewer_filters": viewer_filters,
            "has_material_type_column": has_material_type_column,
            "material_type_values": material_type_values,
            "filter_query": {
                "trace_id": trace_id or "",
                "session_id": session_id or "",
                "run_id": run_id or "",
                "material_type": effective_material_type,
                "step_name": step_name or "",
                "agent_name": agent_name or "",
                "tool_name": tool_name or "",
                "status": status or "",
                "event_type": event_type or "",
                "decision": decision or "",
                "should_stop": should_stop or "",
                "success": success or "",
                "doc_view": "full" if str(doc_view).strip().lower() == "full" else "chunk",
            },
        },
    )


@router.get("/recycle-bin")
def recycle_bin_page(request: Request):
    recycle_items = explorer_repo.list_recycle_bin(limit=500)
    return templates.TemplateResponse(
        request,
        "recycle_bin.html",
        {
            "title": "Recycle Bin",
            "active": "recycle",
            "recycle_items": recycle_items,
        },
    )


@router.get("/tool-trace")
def tool_trace_page(
    request: Request,
    session_id: str | None = None,
    step_name: str | None = None,
    tool_name: str | None = None,
    success: str | None = None,
    sort_order: str = "desc",
):
    success_value = parse_success_query(success)
    step_log_total = 0
    filters = tool_trace_repo.list_cascaded_filters(
        session_id=session_id,
        step_name=step_name,
        success=success_value,
    )
    rows = tool_trace_repo.list_tool_calls(
        session_id=session_id,
        step_name=step_name,
        tool_name=tool_name,
        success=success_value,
        sort_order=sort_order,
        limit=500,
    )
    if not rows and step_name and not tool_name:
        rows = _tool_trace_step_log_fallback_rows(
            session_id=session_id,
            step_name=step_name,
            success_value=success_value,
            sort_order=sort_order,
            limit=500,
        )
        step_log_total = len(rows)
    grouped = tool_trace_repo.group_by_step(rows)
    selected = rows[0] if rows else None
    detail = tool_trace_detail_payload(selected)
    return templates.TemplateResponse(
        request,
        "tool_trace.html",
        {
            "title": "Tool Trace",
            "active": "tool_trace",
            "filters": filters,
            "grouped": grouped,
            "total": len(rows),
            "sort_order": "asc" if str(sort_order).lower() == "asc" else "desc",
            "selected": selected,
            "detail": detail,
            "step_log_total": int(step_log_total or 0),
            "query": {
                "session_id": session_id or "",
                "step_name": step_name or "",
                "tool_name": tool_name or "",
                "success": "" if success_value is None else str(success_value),
                "sort_order": "asc" if str(sort_order).lower() == "asc" else "desc",
            },
        },
    )


@router.get("/partials/tool-trace-list")
def tool_trace_list_partial(
    request: Request,
    session_id: str | None = None,
    step_name: str | None = None,
    tool_name: str | None = None,
    success: str | None = None,
    sort_order: str = "desc",
):
    success_value = parse_success_query(success)
    step_log_total = 0
    rows = tool_trace_repo.list_tool_calls(
        session_id=session_id,
        step_name=step_name,
        tool_name=tool_name,
        success=success_value,
        sort_order=sort_order,
        limit=500,
    )
    if not rows and step_name and not tool_name:
        rows = _tool_trace_step_log_fallback_rows(
            session_id=session_id,
            step_name=step_name,
            success_value=success_value,
            sort_order=sort_order,
            limit=500,
        )
        step_log_total = len(rows)
    grouped = tool_trace_repo.group_by_step(rows)
    return templates.TemplateResponse(
        request,
        "partials/tool_trace_list.html",
        {
            "grouped": grouped,
            "total": len(rows),
            "sort_order": "asc" if str(sort_order).lower() == "asc" else "desc",
            "step_log_total": int(step_log_total or 0),
        },
    )


@router.get("/material-data")
def material_data_page(
    request: Request,
    material_type: str | None = None,
    source: str | None = None,
    q: str | None = None,
    created_from: str | None = None,
    created_to: str | None = None,
    valid_only: bool = False,
    page: int = 1,
    page_size: int = 50,
    sort_by: str = "id",
    sort_order: str = "desc",
):
    filters = material_data_repo.list_filter_options()
    target_columns = material_data_repo.list_target_columns(str(material_type or ""))
    rows, total = material_data_repo.list_rows(
        page=max(1, page),
        page_size=min(max(page_size, 10), 200),
        material_type=str(material_type or ""),
        source=str(source or ""),
        q=str(q or ""),
        created_from=str(created_from or ""),
        created_to=str(created_to or ""),
        valid_only=bool(valid_only),
        sort_by=str(sort_by or "id"),
        sort_order=str(sort_order or "desc"),
    )
    return templates.TemplateResponse(
        request,
        "material_data.html",
        {
            "title": "Material Data Preview",
            "active": "material_data",
            "rows": rows,
            "total": total,
            "target_columns": target_columns,
            "material_types": filters["material_types"],
            "sources": filters["sources"],
            "query": {
                "material_type": material_type or "",
                "source": source or "",
                "q": q or "",
                "created_from": created_from or "",
                "created_to": created_to or "",
                "valid_only": bool(valid_only),
                "page": max(1, page),
                "page_size": min(max(page_size, 10), 200),
                "sort_by": str(sort_by or "id"),
                "sort_order": "asc" if str(sort_order).lower() == "asc" else "desc",
            },
        },
    )


@router.get("/doc-evolution")
def doc_evolution_page(
    request: Request,
    material_type: str | None = None,
    workflow_run_id: str | None = None,
    q: str | None = None,
    created_from: str | None = None,
    created_to: str | None = None,
    limit_runs: int = 30,
):
    filters = doc_evolution_repo.list_filter_options()
    matrix = doc_evolution_repo.list_evolution_matrix(
        material_type=str(material_type or ""),
        workflow_run_id=str(workflow_run_id or ""),
        q=str(q or ""),
        created_from=str(created_from or ""),
        created_to=str(created_to or ""),
        limit_runs=max(1, min(int(limit_runs), 200)),
    )
    return templates.TemplateResponse(
        request,
        "doc_evolution.html",
        {
            "title": "Doc Evolution",
            "active": "doc_evolution",
            "material_types": filters["material_types"],
            "run_ids": filters["run_ids"],
            "columns": matrix.get("columns", ["bootstrap"]),
            "rows": matrix.get("rows", []),
            "total_runs": int(matrix.get("total_runs", 0) or 0),
            "query": {
                "material_type": material_type or "",
                "workflow_run_id": workflow_run_id or "",
                "q": q or "",
                "created_from": created_from or "",
                "created_to": created_to or "",
                "limit_runs": max(1, min(int(limit_runs), 200)),
            },
        },
    )


@router.get("/partials/tool-trace-detail")
def tool_trace_detail_partial(request: Request, id: int):
    item = tool_trace_repo.get_tool_call_by_id(id)
    detail = tool_trace_detail_payload(item)
    return templates.TemplateResponse(
        request,
        "partials/tool_trace_detail.html",
        {"selected": item, "detail": detail},
    )


@router.get("/partials/tool-trace-step-log-detail")
def tool_trace_step_log_detail_partial(request: Request, id: int):
    item = explorer_repo.get_row_by_key(
        db_key="workflow_audit",
        table="workflow_step_logs",
        key_col="id",
        key_val=str(int(id)),
    )
    selected = None
    detail = {"tool_input": "", "tool_output": "", "error_text": ""}
    if item:
        selected = {
            "id": item.get("id"),
            "source_type": "step_log",
            "created_at": item.get("created_at"),
            "session_id": item.get("session_id"),
            "run_id": item.get("run_id"),
            "step_name": item.get("step_name"),
            "agent_name": item.get("step_name"),
            "tool_name": "no-tool",
            "success": item.get("success"),
        }
        detail = tool_trace_detail_payload(
            {
                "tool_args_json": item.get("input_json"),
                "tool_result_json": item.get("output_json"),
                "error_text": item.get("error_text"),
            }
        )
    return templates.TemplateResponse(
        request,
        "partials/tool_trace_detail.html",
        {"selected": selected, "detail": detail},
    )


@router.get("/partials/viewer-results")
def viewer_results_partial(
    request: Request,
    db: str,
    table: str,
    q: str | None = None,
    code: str | None = None,
    trace_id: str | None = None,
    session_id: str | None = None,
    run_id: str | None = None,
    material_type: str | None = None,
    step_name: str | None = None,
    agent_name: str | None = None,
    tool_name: str | None = None,
    status: str | None = None,
    event_type: str | None = None,
    decision: str | None = None,
    should_stop: str | None = None,
    success: str | None = None,
    doc_view: str = "chunk",
    page_size: int = 30,
    sort_order: str = "desc",
):
    valid_tables = explorer_repo.list_tables(db)
    safe_table = table if table in valid_tables else (valid_tables[0] if valid_tables else "")
    safe_table_columns = explorer_repo.get_table_columns(db, safe_table) if safe_table else []
    safe_col_names = [str(c.get("name")) for c in safe_table_columns]
    has_material_type_column = "material_type" in safe_col_names
    effective_material_type = str(material_type or "").strip() if has_material_type_column else ""
    extra_filters = viewer_extra_filters(
        trace_id=trace_id,
        session_id=session_id,
        run_id=run_id,
        material_type=effective_material_type,
        step_name=step_name,
        agent_name=agent_name,
        tool_name=tool_name,
        status=status,
        event_type=event_type,
        decision=decision,
        should_stop=should_stop,
        success=success,
    )
    if safe_table:
        rows, total, columns, record_key = explorer_repo.list_rows(
            db_key=db,
            table=safe_table,
            page=1,
            page_size=min(max(page_size, 10), 200),
            q=q,
            identifier=code,
            sort_order=sort_order,
            extra_filters=extra_filters,
        )
    else:
        rows, total, columns, record_key = [], 0, [], "__rowid__"
    if safe_table == "material_doc_knowledge" and str(doc_view).strip().lower() == "full":
        rows = _aggregate_doc_rows_for_full_view(rows)
        total = len(rows)
        if "chunk_count" not in columns:
            columns = [*columns, "chunk_count"]
    records = [to_record_card(row, columns, record_key, safe_table) for row in rows]
    return templates.TemplateResponse(
        request,
        "partials/viewer_results.html",
        {
            "records": records,
            "total": total,
            "db": db,
            "table": safe_table,
            "record_key": record_key,
            "doc_view": "full" if str(doc_view).strip().lower() == "full" else "chunk",
            "selected": [],
            "sort_order": "asc" if str(sort_order).lower() == "asc" else "desc",
        },
    )


@router.get("/partials/viewer-detail")
def viewer_detail_partial(
    request: Request,
    db: str,
    table: str,
    key_col: str,
    key_val: str,
    doc_view: str = "chunk",
):
    row = explorer_repo.get_row_by_key(db_key=db, table=table, key_col=key_col, key_val=key_val)
    normalized_view = "full" if str(doc_view).strip().lower() == "full" else "chunk"
    if row and table == "material_doc_knowledge" and normalized_view == "full":
        source_name = str(row.get("source_name") or "").strip()
        material_type = str(row.get("material_type") or "").strip().lower()
        source_kind = str(row.get("source_kind") or "").strip().lower()
        workflow_run_id = str(row.get("workflow_run_id") or "").strip()
        round_index = int(row.get("round_index") or 0)
        if source_name:
            where = ["source_name = ?"]
            params: list[object] = [source_name]
            if material_type:
                where.append("material_type = ?")
                params.append(material_type)
            if source_kind:
                where.append("source_kind = ?")
                params.append(source_kind)
            if workflow_run_id:
                where.append("workflow_run_id = ?")
                params.append(workflow_run_id)
            if round_index > 0:
                where.append("round_index = ?")
                params.append(round_index)
            where_sql = f"WHERE {' AND '.join(where)}"
            with db_manager.connect("material_agent_shared", readonly=True) as conn:
                parts = conn.execute(
                    f"""
                    SELECT title, content
                    FROM material_doc_knowledge
                    {where_sql}
                    ORDER BY chunk_index ASC, id ASC
                    """,
                    params,
                ).fetchall()
            sections: list[str] = []
            for p in parts:
                title = str(p["title"] or "").strip()
                content = str(p["content"] or "").strip()
                if title:
                    sections.append(f"## {title}")
                if content:
                    sections.append(content)
            row["full_document"] = "\n\n".join([x for x in sections if x]).strip()
    detail_sections = to_detail_sections(row) if row else []
    return templates.TemplateResponse(
        request,
        "partials/viewer_detail.html",
        {
            "detail_sections": detail_sections,
            "detail_row": row or {},
            "key_val": key_val,
            "db": db,
            "table": table,
            "doc_view": normalized_view,
        },
    )


@router.get("/partials/explorer-table")
def explorer_table_partial(
    request: Request,
    db: str,
    table: str,
    q: str | None = None,
    code: str | None = None,
    material_type: str | None = None,
    source_kind: str | None = None,
    workflow_run_id: str | None = None,
    trace_id: str | None = None,
    session_id: str | None = None,
    run_id: str | None = None,
    step_name: str | None = None,
    agent_name: str | None = None,
    tool_name: str | None = None,
    event_type: str | None = None,
    status: str | None = None,
    decision: str | None = None,
    success: str | None = None,
    created_from: str | None = None,
    created_to: str | None = None,
    sort_order: str = "desc",
    page: int = 1,
    page_size: int = 50,
):
    safe_table = table
    table_columns = explorer_repo.get_table_columns(db, safe_table) if safe_table else []
    col_names = [str(c.get("name")) for c in table_columns]
    raw_filters = {
        "material_type": str(material_type or "").strip(),
        "source_kind": str(source_kind or "").strip(),
        "workflow_run_id": str(workflow_run_id or "").strip(),
        "trace_id": str(trace_id or "").strip(),
        "session_id": str(session_id or "").strip(),
        "run_id": str(run_id or "").strip(),
        "step_name": str(step_name or "").strip(),
        "agent_name": str(agent_name or "").strip(),
        "tool_name": str(tool_name or "").strip(),
        "event_type": str(event_type or "").strip(),
        "status": str(status or "").strip(),
        "decision": str(decision or "").strip(),
        "success": str(success or "").strip(),
    }
    extra_filters = {k: v for k, v in raw_filters.items() if v and k in col_names}
    try:
        rows, total, columns, record_key = explorer_repo.list_rows(
            db_key=db,
            table=safe_table,
            page=max(page, 1),
            page_size=min(max(page_size, 10), 200),
            q=q,
            identifier=code,
            sort_order=sort_order,
            extra_filters=extra_filters,
            created_from=created_from,
            created_to=created_to,
        )
    except ValueError:
        valid_tables = explorer_repo.list_tables(db)
        safe_table = valid_tables[0] if valid_tables else ""
        if safe_table:
            rows, total, columns, record_key = explorer_repo.list_rows(
                db_key=db,
                table=safe_table,
                page=max(page, 1),
                page_size=min(max(page_size, 10), 200),
                q=q,
                identifier=code,
                sort_order=sort_order,
                extra_filters=extra_filters,
                created_from=created_from,
                created_to=created_to,
            )
        else:
            rows, total, columns, record_key = [], 0, [], "__rowid__"
    return templates.TemplateResponse(
        request,
        "partials/explorer_table.html",
        {
            "rows": rows,
            "columns": columns,
            "record_key": record_key,
            "total": total,
            "db": db,
            "table": safe_table,
            "q": q or "",
            "page": max(page, 1),
            "page_size": min(max(page_size, 10), 200),
        },
    )

