from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, Optional
import json

from ..config import PipelineConfig, create_default_config
from ..predictor import FewshotPredictor
from src.common import build_model


class FewshotPipeline:
    """
    Lightweight few-shot pipeline for a single candidate.

    This version does NOT split train/test; it always retrieves similar samples
    directly from the specified dataset.
    """

    def __init__(self, config: Optional[PipelineConfig] = None, **kwargs) -> None:
        if config is None:
            config = create_default_config(**kwargs)
        self.config = config

    def run_single(
        self,
        material_type: str,
        composition: Dict[str, Any],
        processing: Optional[Dict[str, Any]] = None,
        features: Optional[Dict[str, Any]] = None,
        top_k: Optional[int] = None,
    ) -> Dict[str, Any]:
        output_dir = Path(self.config.data.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Always resolve runtime model from unified project routing config.
        runtime_model = build_model("workflow/fewshot")
        predictor = FewshotPredictor(
            model_name=runtime_model.id,
            temperature=self.config.llm.temperature,
            api_key=runtime_model.api_key,
            base_url=runtime_model.base_url,
            embedding_model=self.config.retrieval.embedding_model,
        )
        result = predictor.predict(
            material_type=material_type,
            composition=composition,
            processing=processing,
            features=features,
            top_k=top_k or self.config.retrieval.top_k,
        )

        payload = {
            "material_type": result.material_type,
            "predicted_values": result.predicted_values,
            "confidence": result.confidence,
            "similar_samples": result.similar_samples,
            "prompt": result.prompt,
            "llm_response": result.llm_response,
            "config": {
                "data": asdict(self.config.data),
                "retrieval": asdict(self.config.retrieval),
                "llm": asdict(self.config.llm),
                "llm_resolved": {
                    "source": "build_model(workflow/fewshot)",
                    "model_name": runtime_model.id,
                    "base_url": runtime_model.base_url,
                },
            },
        }
        output_path = output_dir / "single_prediction.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
        return payload


# Backward compatibility with old imports.
RAGPipeline = FewshotPipeline
