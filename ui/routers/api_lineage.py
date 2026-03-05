from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from starlette.concurrency import run_in_threadpool

from ui.dependencies import get_lineage_service
from ui.schemas.models import LineageResponse
from ui.services.lineage_service import LineageService


router = APIRouter(prefix="/lineage", tags=["lineage"])


@router.get("/{trace_or_session_id}", response_model=LineageResponse)
async def get_lineage(
    trace_or_session_id: str,
    include_payload: bool = Query(default=True),
    service: LineageService = Depends(get_lineage_service),
):
    data = await run_in_threadpool(service.build_lineage, trace_or_session_id)
    if not include_payload:
        for item in data["timeline"]:
            item["data"].pop("payload", None)
            item["data"].pop("step_outputs", None)
            item["data"].pop("final_result", None)
    return data
