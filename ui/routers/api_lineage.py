from __future__ import annotations

from fastapi import APIRouter, Query

from ui.schemas.models import LineageResponse
from ui.services.lineage_service import lineage_service


router = APIRouter(prefix="/api/lineage", tags=["lineage"])


@router.get("/{trace_or_session_id}", response_model=LineageResponse)
def get_lineage(trace_or_session_id: str, include_payload: bool = Query(default=True)):
    data = lineage_service.build_lineage(trace_or_session_id)
    if not include_payload:
        for item in data["timeline"]:
            item["data"].pop("payload", None)
            item["data"].pop("step_outputs", None)
            item["data"].pop("final_result", None)
    return data
