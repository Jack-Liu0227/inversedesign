from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class PageResult(BaseModel):
    total: int
    page: int
    page_size: int


class PredictionLogItem(BaseModel):
    id: int
    created_at: str
    material_type_input: str | None = None
    material_type_resolved: str | None = None
    top_k: int | None = None
    confidence: str | None = None
    predicted_values: dict[str, Any] | list[Any] | None = None


class WorkflowEventItem(BaseModel):
    id: int
    created_at: str
    trace_id: str | None = None
    workflow_name: str | None = None
    session_id: str | None = None
    run_id: str | None = None
    step_name: str | None = None
    event_type: str | None = None
    latency_ms: int | None = None
    success: int | None = None
    error_text: str | None = None


class LineageNode(BaseModel):
    id: str
    kind: Literal["event", "run_audit", "session"]
    label: str
    created_at: str | None = None
    data: dict[str, Any] = Field(default_factory=dict)


class LineageEdge(BaseModel):
    source: str
    target: str
    relation: str


class LineageResponse(BaseModel):
    query: str
    nodes: list[LineageNode]
    edges: list[LineageEdge]
    timeline: list[dict[str, Any]]


class TagCreateRequest(BaseModel):
    name: str
    color: str = "#0a7ea4"
    group_name: str = "default"
    description: str = ""


class AssignTagRequest(BaseModel):
    source_db: str
    source_table: str
    source_pk: str
    tag_names: list[str]


class AnnotationStateRequest(BaseModel):
    source_db: str
    source_table: str
    source_pk: str
    status: Literal["new", "reviewed", "verified", "rejected"] = "new"
    priority: Literal["P0", "P1", "P2", "P3"] = "P2"
    note: str = ""
