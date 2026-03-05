from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from .agent_recommender_output import RecommenderCandidate

RecommendedAction = Literal["keep", "revise", "drop"]


class RationalityItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    candidate_index: int
    is_valid: bool = False
    validity_score: float = 0.0
    reasons: list[str] = Field(default_factory=list)
    risk_tags: list[str] = Field(default_factory=list)
    recommended_action: RecommendedAction = "drop"
    cleaned_candidate: RecommenderCandidate | None = None


class AgentRationalityOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    evaluations: list[RationalityItem] = Field(default_factory=list)
