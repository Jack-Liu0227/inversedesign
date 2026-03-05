from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class RecommenderCandidate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    composition: dict[str, float] = Field(default_factory=dict)
    processing: dict[str, object] = Field(default_factory=dict)
    score: float = 0.0
    reason: str = ""
    expected_tradeoff: str = ""


class AgentRecommenderOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    candidates: list[RecommenderCandidate] = Field(default_factory=list)
