from __future__ import annotations

from fastapi import APIRouter

from ui.db.repositories.explorer_repo import explorer_repo


router = APIRouter(prefix="/api/viewer", tags=["viewer"])


@router.get("/filter-options")
def get_viewer_filter_options(
    db: str,
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
):
    return explorer_repo.viewer_filter_options(
        db_key=db,
        table=table,
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
