from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from .agent_recommender_output import RecommenderCandidate
from .loop_mode import LoopMode

DecisionValue = Literal["continue", "stop", "await_user_choice"]


class CandidatePredictionBlock(BaseModel):
    model_config = ConfigDict(extra="forbid")

    predicted_values: dict[str, float | int | str | None] = Field(default_factory=dict)
    confidence: str = "low"
    reasoning: str = ""


class CandidateWithPrediction(BaseModel):
    model_config = ConfigDict(extra="forbid")

    candidate_index: int
    composition: dict[str, float] = Field(default_factory=dict)
    processing: dict[str, object] = Field(default_factory=dict)
    score: float = 0.0
    reason: str = ""
    expected_tradeoff: str = ""
    prediction: CandidatePredictionBlock = Field(default_factory=CandidatePredictionBlock)
    prediction_error: str = ""
    is_valid: bool = False
    validity_score: float = 0.0
    judge_reasons: list[str] = Field(default_factory=list)
    risk_tags: list[str] = Field(default_factory=list)
    recommended_action: str = "drop"


class JudgeSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    total: int = 0
    valid_count: int = 0
    invalid_count: int = 0
    top_reasons: list[str] = Field(default_factory=list)


class StopMetricEvaluation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    operator: str = ">="
    target: float | None = None
    predicted: float | None = None
    passed: bool = False
    detail: str = ""


class StopEvaluation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    passed: bool = False
    reason: str = ""
    metrics: list[StopMetricEvaluation] = Field(default_factory=list)


class LoopState(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: LoopMode = "ai_only"
    round_index: int = 1
    max_iterations: int = 3
    remaining_rounds: int = 2
    reached_max_iterations: bool = False
    requires_human_feedback: bool = False


class MaterialDiscoveryResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    decision: DecisionValue
    recommended_candidates: list[CandidateWithPrediction] = Field(default_factory=list)
    valid_candidates: list[CandidateWithPrediction] = Field(default_factory=list)
    judge_summary: JudgeSummary = Field(default_factory=JudgeSummary)
    stop_evaluation: StopEvaluation = Field(default_factory=StopEvaluation)
    summary: list[str] = Field(default_factory=list)
    loop_state: LoopState = Field(default_factory=LoopState)
    debug: dict[str, object] | None = None


def candidate_from_recommender(candidate: RecommenderCandidate, index: int) -> CandidateWithPrediction:
    return CandidateWithPrediction(
        candidate_index=index,
        composition=dict(candidate.composition),
        processing=dict(candidate.processing),
        score=float(candidate.score),
        reason=str(candidate.reason),
        expected_tradeoff=str(candidate.expected_tradeoff),
    )
