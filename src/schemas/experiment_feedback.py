from __future__ import annotations

from typing import Dict, Optional

from pydantic import BaseModel, Field


class ExperimentFeedback(BaseModel):
    measured_values: Dict[str, float] = Field(default_factory=dict)
    notes: Optional[str] = None
