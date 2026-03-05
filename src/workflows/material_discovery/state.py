from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from src.schemas.agent_predictor_output import CandidatePrediction
from src.schemas.agent_rationality_output import RationalityItem
from src.schemas.agent_recommender_output import RecommenderCandidate
from src.schemas.loop_mode import LoopMode
from src.schemas.material_discovery_response import StopEvaluation


class MaterialDiscoveryState(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: LoopMode = "ai_only"
    round_index: int = 1
    goal: str = ""
    resolved_material_type: str = ""
    resolution_reason: str = ""

    recommended_candidates: list[RecommenderCandidate] = Field(default_factory=list)
    candidate_predictions: list[CandidatePrediction] = Field(default_factory=list)
    rationality: list[RationalityItem] = Field(default_factory=list)

    measured_values: dict[str, float] = Field(default_factory=dict)
    preference_feedback: str = ""

    decision: str = "continue"
    requires_human_feedback: bool = False
    stop_evaluation: StopEvaluation = Field(default_factory=StopEvaluation)
