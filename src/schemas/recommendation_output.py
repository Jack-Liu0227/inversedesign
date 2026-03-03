from __future__ import annotations

from typing import Dict, List

from pydantic import BaseModel, Field


class RecommendationCandidate(BaseModel):
    composition: Dict[str, float] = Field(default_factory=dict)
    score: float = 0.0
    reason: str = ""


class RecommendationOutput(BaseModel):
    material_type: str
    goal: str
    candidates: List[RecommendationCandidate] = Field(default_factory=list)
