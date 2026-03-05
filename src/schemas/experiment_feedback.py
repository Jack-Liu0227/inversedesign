from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class ExperimentFeedback(BaseModel):
    measured_values: dict[str, float] = Field(default_factory=dict)
    notes: Optional[str] = None
