from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from .agent_recommender_output import RecommenderCandidate


class CandidatePrediction(BaseModel):
    model_config = ConfigDict(extra="forbid")

    candidate_index: int
    predicted_values: dict[str, float | int | str | None] = Field(default_factory=dict)
    confidence: str = "low"
    reasoning: str = ""
    prediction_error: str = ""


class AgentPredictorOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    recommended_candidates: list[RecommenderCandidate] = Field(default_factory=list)
    candidate_predictions: list[CandidatePrediction] = Field(default_factory=list)
    prediction_error: str = ""
