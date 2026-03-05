from __future__ import annotations

from fastapi import APIRouter

from ui.schemas.models import AnnotationStateRequest, AssignTagRequest, TagCreateRequest
from ui.services.classification_service import classification_service


router = APIRouter(prefix="/api/classifications", tags=["classifications"])


@router.get("/tags")
def list_tags():
    return {"items": classification_service.list_tags()}


@router.post("/tags")
def create_tag(req: TagCreateRequest):
    return classification_service.create_tag(
        name=req.name,
        color=req.color,
        group_name=req.group_name,
        description=req.description,
    )


@router.get("/annotations")
def list_annotations():
    return {"items": classification_service.list_annotations()}


@router.post("/assign")
def assign_tags(req: AssignTagRequest):
    return classification_service.assign_tags(
        source_db=req.source_db,
        source_table=req.source_table,
        source_pk=req.source_pk,
        tag_names=req.tag_names,
    )


@router.post("/state")
def update_state(req: AnnotationStateRequest):
    return classification_service.update_state(
        source_db=req.source_db,
        source_table=req.source_table,
        source_pk=req.source_pk,
        status=req.status,
        priority=req.priority,
        note=req.note,
    )
