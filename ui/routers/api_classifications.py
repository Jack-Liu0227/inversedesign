from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends
from starlette.concurrency import run_in_threadpool

from ui.dependencies import get_classification_service
from ui.schemas.models import AnnotationStateRequest, AssignTagRequest, TagCreateRequest
from ui.services.classification_service import ClassificationService


router = APIRouter(prefix="/classifications", tags=["classifications"])


@router.get("/tags")
async def list_tags(service: Annotated[ClassificationService, Depends(get_classification_service)]):
    items = await run_in_threadpool(service.list_tags)
    return {"items": items}


@router.post("/tags")
async def create_tag(
    req: TagCreateRequest,
    service: Annotated[ClassificationService, Depends(get_classification_service)],
):
    return await run_in_threadpool(
        service.create_tag,
        name=req.name,
        color=req.color,
        group_name=req.group_name,
        description=req.description,
    )


@router.get("/annotations")
async def list_annotations(service: Annotated[ClassificationService, Depends(get_classification_service)]):
    items = await run_in_threadpool(service.list_annotations)
    return {"items": items}


@router.post("/assign")
async def assign_tags(
    req: AssignTagRequest,
    service: Annotated[ClassificationService, Depends(get_classification_service)],
):
    return await run_in_threadpool(
        service.assign_tags,
        source_db=req.source_db,
        source_table=req.source_table,
        source_pk=req.source_pk,
        tag_names=req.tag_names,
    )


@router.post("/state")
async def update_state(
    req: AnnotationStateRequest,
    service: Annotated[ClassificationService, Depends(get_classification_service)],
):
    return await run_in_threadpool(
        service.update_state,
        source_db=req.source_db,
        source_table=req.source_table,
        source_pk=req.source_pk,
        status=req.status,
        priority=req.priority,
        note=req.note,
    )
