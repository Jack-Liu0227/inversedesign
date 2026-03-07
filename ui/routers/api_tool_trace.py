from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from starlette.concurrency import run_in_threadpool

from ui.db.repositories.tool_trace_repo import ToolTraceRepository
from ui.dependencies import get_tool_trace_repository


router = APIRouter(prefix="/tool-trace", tags=["tool-trace"])


@router.get('/logs')
async def get_tool_trace_logs(
    session_id: str | None = None,
    run_id: str | None = None,
    step_name: str | None = None,
    tool_name: str | None = None,
    success: int | None = Query(default=None, ge=0, le=1),
    sort_order: str = "desc",
    limit: int = Query(default=200, ge=1, le=2000),
    tool_trace_repository: ToolTraceRepository = Depends(get_tool_trace_repository),
):
    rows = await run_in_threadpool(
        tool_trace_repository.list_tool_calls,
        session_id=session_id,
        run_id=run_id,
        step_name=step_name,
        tool_name=tool_name,
        success=success,
        sort_order=sort_order,
        limit=limit,
    )
    grouped = await run_in_threadpool(tool_trace_repository.group_by_step, rows)
    return {"total": len(rows), "grouped": grouped, "items": rows}


@router.get('/filter-options')
async def get_tool_trace_filter_options(
    session_id: str | None = None,
    run_id: str | None = None,
    step_name: str | None = None,
    success: str | None = None,
    tool_trace_repository: ToolTraceRepository = Depends(get_tool_trace_repository),
):
    success_value: int | None = None
    if success is not None and str(success).strip() in {"0", "1"}:
        success_value = int(str(success).strip())
    return await run_in_threadpool(
        tool_trace_repository.list_cascaded_filters,
        session_id=session_id,
        run_id=run_id,
        step_name=step_name,
        success=success_value,
    )
