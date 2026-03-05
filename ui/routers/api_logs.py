from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from starlette.concurrency import run_in_threadpool

from ui.dependencies import get_app_config, get_prediction_repository, get_workflow_repository
from ui.db.repositories.prediction_repo import PredictionRepository
from ui.db.repositories.workflow_repo import WorkflowRepository
from ui.schemas.models import PageResult


router = APIRouter(prefix="/logs", tags=["logs"])


@router.get("/predictions")
async def get_predictions(
    from_: str | None = Query(default=None, alias="from"),
    to: str | None = Query(default=None, alias="to"),
    material_type: str | None = None,
    confidence: str | None = None,
    top_k: int | None = None,
    q: str | None = None,
    page: int = 1,
    page_size: int = 20,
    prediction_repository: PredictionRepository = Depends(get_prediction_repository),
):
    cfg = get_app_config()
    page = max(page, 1)
    page_size = min(max(page_size, 1), cfg.max_page_size)
    items, total = await run_in_threadpool(
        prediction_repository.list_predictions,
        page=page,
        page_size=page_size,
        material_type=material_type,
        confidence=confidence,
        top_k=top_k,
        q=q,
        created_from=from_,
        created_to=to,
    )
    return {"meta": PageResult(total=total, page=page, page_size=page_size), "items": items}


@router.get("/workflow-events")
async def get_workflow_events(
    workflow_name: str | None = None,
    step_name: str | None = None,
    event_type: str | None = None,
    success: int | None = Query(default=None, ge=0, le=1),
    latency_min: int | None = Query(default=None, ge=0),
    latency_max: int | None = Query(default=None, ge=0),
    trace_id: str | None = None,
    session_id: str | None = None,
    page: int = 1,
    page_size: int = 20,
    workflow_repository: WorkflowRepository = Depends(get_workflow_repository),
):
    cfg = get_app_config()
    page = max(page, 1)
    page_size = min(max(page_size, 1), cfg.max_page_size)
    items, total = await run_in_threadpool(
        workflow_repository.list_workflow_events,
        page=page,
        page_size=page_size,
        workflow_name=workflow_name,
        step_name=step_name,
        event_type=event_type,
        success=success,
        trace_id=trace_id,
        session_id=session_id,
        latency_min=latency_min,
        latency_max=latency_max,
    )
    return {"meta": PageResult(total=total, page=page, page_size=page_size), "items": items}
