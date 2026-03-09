from __future__ import annotations

from fastapi import APIRouter, Depends
from starlette.concurrency import run_in_threadpool

from ui.db.repositories.explorer_repo import ExplorerRepository
from ui.dependencies import get_explorer_repository


router = APIRouter(prefix="/viewer", tags=["viewer"])


@router.get("/filter-options")
async def get_viewer_filter_options(
    db: str,
    table: str,
    trace_id: str | None = None,
    session_id: str | None = None,
    workflow_run_id: str | None = None,
    step_name: str | None = None,
    agent_name: str | None = None,
    event_type: str | None = None,
    decision: str | None = None,
    should_stop: str | None = None,
    success: str | None = None,
    explorer_repository: ExplorerRepository = Depends(get_explorer_repository),
):
    return await run_in_threadpool(
        explorer_repository.viewer_filter_options,
        db_key=db,
        table=table,
        trace_id=trace_id,
        session_id=session_id,
        workflow_run_id=workflow_run_id,
        step_name=step_name,
        agent_name=agent_name,
        event_type=event_type,
        decision=decision,
        should_stop=should_stop,
        success=success,
    )
