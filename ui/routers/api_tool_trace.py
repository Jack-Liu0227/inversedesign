from __future__ import annotations

from fastapi import APIRouter, Query

from ui.db.repositories.tool_trace_repo import tool_trace_repo


router = APIRouter(prefix="/api/tool-trace", tags=["tool-trace"])


@router.get('/logs')
def get_tool_trace_logs(
    session_id: str | None = None,
    step_name: str | None = None,
    agent_name: str | None = None,
    tool_name: str | None = None,
    success: int | None = Query(default=None, ge=0, le=1),
    sort_order: str = "desc",
    limit: int = Query(default=200, ge=1, le=2000),
):
    rows = tool_trace_repo.list_tool_calls(
        session_id=session_id,
        step_name=step_name,
        agent_name=agent_name,
        tool_name=tool_name,
        success=success,
        sort_order=sort_order,
        limit=limit,
    )
    grouped = tool_trace_repo.group_by_step(rows)
    return {"total": len(rows), "grouped": grouped, "items": rows}


@router.get('/filter-options')
def get_tool_trace_filter_options(
    session_id: str | None = None,
    step_name: str | None = None,
    agent_name: str | None = None,
    success: str | None = None,
):
    success_value: int | None = None
    if success is not None and str(success).strip() in {"0", "1"}:
        success_value = int(str(success).strip())
    return tool_trace_repo.list_cascaded_filters(
        session_id=session_id,
        step_name=step_name,
        agent_name=agent_name,
        success=success_value,
    )
