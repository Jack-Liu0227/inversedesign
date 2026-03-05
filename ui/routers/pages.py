from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates

from ui.config import get_config
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
    if selected_db and selected_table:
        rows, total, columns, record_key = explorer_repo.list_rows(
            db_key=selected_db,
            table=selected_table,
            page=max(page, 1),
            page_size=min(max(page_size, 10), 200),
            q=q,
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
            "rows": rows,
            "columns": columns,
            "record_key": record_key,
            "total": total,
            "q": q or "",
            "page": max(page, 1),
            "page_size": min(max(page_size, 10), 200),
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
    step_name: str | None = None,
    agent_name: str | None = None,
    tool_name: str | None = None,
    status: str | None = None,
    event_type: str | None = None,
    decision: str | None = None,
    should_stop: str | None = None,
    success: str | None = None,
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
    extra_filters = viewer_extra_filters(
        trace_id=trace_id,
        session_id=session_id,
        run_id=run_id,
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

    records = [to_record_card(row, columns, record_key, selected_table) for row in rows]
    detail_sections = to_detail_sections(rows[0]) if rows else []
    detail_key = str(rows[0].get(record_key)) if rows else ""
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
            "detail_key": detail_key,
            "detail_key_col": record_key,
            "recycle_items": recycle_items,
            "db": selected_db,
            "table": selected_table,
            "record_key": record_key,
            "workflow_filterable": workflow_filterable,
            "viewer_filters": viewer_filters,
            "filter_query": {
                "trace_id": trace_id or "",
                "session_id": session_id or "",
                "run_id": run_id or "",
                "step_name": step_name or "",
                "agent_name": agent_name or "",
                "tool_name": tool_name or "",
                "status": status or "",
                "event_type": event_type or "",
                "decision": decision or "",
                "should_stop": should_stop or "",
                "success": success or "",
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
    agent_name: str | None = None,
    tool_name: str | None = None,
    success: str | None = None,
    sort_order: str = "desc",
):
    success_value = parse_success_query(success)
    filters = tool_trace_repo.list_cascaded_filters(
        session_id=session_id,
        step_name=step_name,
        agent_name=agent_name,
        success=success_value,
    )
    rows = tool_trace_repo.list_tool_calls(
        session_id=session_id,
        step_name=step_name,
        agent_name=agent_name,
        tool_name=tool_name,
        success=success_value,
        sort_order=sort_order,
        limit=500,
    )
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
            "query": {
                "session_id": session_id or "",
                "step_name": step_name or "",
                "agent_name": agent_name or "",
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
    agent_name: str | None = None,
    tool_name: str | None = None,
    success: str | None = None,
    sort_order: str = "desc",
):
    success_value = parse_success_query(success)
    rows = tool_trace_repo.list_tool_calls(
        session_id=session_id,
        step_name=step_name,
        agent_name=agent_name,
        tool_name=tool_name,
        success=success_value,
        sort_order=sort_order,
        limit=500,
    )
    grouped = tool_trace_repo.group_by_step(rows)
    return templates.TemplateResponse(
        request,
        "partials/tool_trace_list.html",
        {
            "grouped": grouped,
            "total": len(rows),
            "sort_order": "asc" if str(sort_order).lower() == "asc" else "desc",
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


@router.get("/partials/tool-trace-detail")
def tool_trace_detail_partial(request: Request, id: int):
    item = tool_trace_repo.get_tool_call_by_id(id)
    detail = tool_trace_detail_payload(item)
    return templates.TemplateResponse(
        request,
        "partials/tool_trace_detail.html",
        {"selected": item, "detail": detail},
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
    step_name: str | None = None,
    agent_name: str | None = None,
    tool_name: str | None = None,
    status: str | None = None,
    event_type: str | None = None,
    decision: str | None = None,
    should_stop: str | None = None,
    success: str | None = None,
    page_size: int = 30,
    sort_order: str = "desc",
):
    safe_table = table
    extra_filters = viewer_extra_filters(
        trace_id=trace_id,
        session_id=session_id,
        run_id=run_id,
        step_name=step_name,
        agent_name=agent_name,
        tool_name=tool_name,
        status=status,
        event_type=event_type,
        decision=decision,
        should_stop=should_stop,
        success=success,
    )
    try:
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
    except ValueError:
        valid_tables = explorer_repo.list_tables(db)
        safe_table = valid_tables[0] if valid_tables else ""
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
):
    row = explorer_repo.get_row_by_key(db_key=db, table=table, key_col=key_col, key_val=key_val)
    detail_sections = to_detail_sections(row) if row else []
    return templates.TemplateResponse(
        request,
        "partials/viewer_detail.html",
        {"detail_sections": detail_sections, "key_val": key_val, "db": db, "table": table},
    )


@router.get("/partials/explorer-table")
def explorer_table_partial(
    request: Request,
    db: str,
    table: str,
    q: str | None = None,
    page: int = 1,
    page_size: int = 50,
):
    safe_table = table
    try:
        rows, total, columns, record_key = explorer_repo.list_rows(
            db_key=db,
            table=safe_table,
            page=max(page, 1),
            page_size=min(max(page_size, 10), 200),
            q=q,
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

