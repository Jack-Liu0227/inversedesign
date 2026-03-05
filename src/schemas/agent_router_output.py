from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class RouterTargetThreshold(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(default="")
    operator: str = Field(default="=")
    target: float | None = Field(default=None)


class AgentRouterOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    resolved_material_type: str = Field(default="")
    resolution_reason: str = Field(default="")
    resolved_properties: list[str] = Field(default_factory=list)
    target_thresholds: list[RouterTargetThreshold] = Field(default_factory=list)
