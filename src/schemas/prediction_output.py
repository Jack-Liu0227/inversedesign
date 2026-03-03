from __future__ import annotations

from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class SimilarSample(BaseModel):
    sample_text: str
    similarity: float
    properties: Dict[str, float] = Field(default_factory=dict)


class PredictionOutput(BaseModel):
    material_type: str
    predicted_values: Dict[str, Optional[float]]
    confidence: str
    reasoning: str = ""
    similar_samples: List[SimilarSample] = Field(default_factory=list)
