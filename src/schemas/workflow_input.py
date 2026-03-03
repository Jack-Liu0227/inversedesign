from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, field_validator


class WorkflowInput(BaseModel):
    material_type: str = Field(
        default="",
        description="Optional dataset key such as ti/steel/al/hea/hea_pitting; empty means auto-routing by goal",
    )
    goal: str = Field(description="Optimization goal for this run")
    composition: Dict[str, float] = Field(default_factory=dict, description="Candidate composition")
    processing: Dict[str, Any] = Field(default_factory=dict)
    features: Dict[str, Any] = Field(default_factory=dict)
    top_k: Optional[int | str] = Field(default=None, description="Top similar samples to retrieve (1-20)")
    max_iterations: int | str = Field(default=3, description="Max loop iterations (1-10)")

    @field_validator("top_k", mode="before")
    @classmethod
    def _coerce_top_k(cls, value: Any) -> Optional[int]:
        if value is None:
            return None
        if isinstance(value, str):
            value = value.strip()
            if value == "":
                return None
        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError("top_k must be an integer between 1 and 20") from exc
        if not 1 <= parsed <= 20:
            raise ValueError("top_k must be between 1 and 20")
        return parsed

    @field_validator("max_iterations", mode="before")
    @classmethod
    def _coerce_max_iterations(cls, value: Any) -> int:
        if isinstance(value, str):
            value = value.strip()
            if value == "":
                return 3
        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError("max_iterations must be an integer between 1 and 10") from exc
        if not 1 <= parsed <= 10:
            raise ValueError("max_iterations must be between 1 and 10")
        return parsed
