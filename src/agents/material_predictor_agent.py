from __future__ import annotations

import concurrent.futures
import json
from typing import Any, Dict, Optional

from agno.agent import Agent
from agno.db.sqlite import SqliteDb
from agno.tools import tool

from src.common import build_model, log_prediction_prompt, MATERIAL_PREDICTOR_AGENT_DB
from src.fewshot import FewshotPredictor, resolve_material_type_input, supported_material_type_hint


def _build_predictor() -> FewshotPredictor:
    model = build_model("material_predictor/fewshot")
    model_name = model.id
    api_key = model.api_key
    base_url = model.base_url
    return FewshotPredictor(
        model_name=model_name,
        api_key=api_key,
        base_url=base_url,
        allow_mock_on_failure=True,
    )


def _normalize_for_logging(value: Any, label: str) -> Dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return {
            str(k): v
            for k, v in value.items()
            if v is not None and (not isinstance(v, str) or v.strip())
        }
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return {}
        try:
            parsed = json.loads(text)
            if isinstance(parsed, dict):
                return {
                    str(k): v
                    for k, v in parsed.items()
                    if v is not None and (not isinstance(v, str) or v.strip())
                }
        except Exception:
            pass
        return {f"{label}_text": text}
    return {f"{label}_text": str(value)}


@tool
def predict_material_properties(
    material_type: str,
    composition: dict,
    goal: str = "",
    processing: Optional[Any] = None,
    features: Optional[Any] = None,
    top_k: int = 3,
) -> Dict[str, Any]:
    resolved_material_type, route_reason = resolve_material_type_input(
        goal=goal,
        material_type=material_type,
    )
    predictor = _build_predictor()
    try:
        result = predictor.predict(
            material_type=resolved_material_type,
            composition=composition,
            processing=processing,
            features=features,
            top_k=top_k,
        )
    except ValueError as exc:
        if "Unsupported material_type" in str(exc):
            hint = supported_material_type_hint()
            raise ValueError(f"{exc} {hint}") from exc
        raise

    prompt_log_id = log_prediction_prompt(
        material_type_input=material_type,
        material_type_resolved=resolved_material_type,
        composition=composition,
        processing=_normalize_for_logging(processing, "processing"),
        features=_normalize_for_logging(features, "features"),
        top_k=top_k,
        prompt=result.prompt,
        llm_response=result.llm_response,
        predicted_values=result.predicted_values,
        confidence=result.confidence,
    )

    return {
        "material_type": result.material_type,
        "material_type_input": material_type,
        "material_type_route_reason": route_reason,
        "predicted_values": result.predicted_values,
        "confidence": result.confidence,
        "similar_samples": result.similar_samples,
        "llm_response": result.llm_response,
        "prompt_log_id": prompt_log_id,
    }


@tool
def predict_material_properties_batch(
    material_type: str,
    candidates: list[dict],
    goal: str = "",
    top_k: int = 3,
    max_workers: int = 3,
) -> Dict[str, Any]:
    resolved_material_type, route_reason = resolve_material_type_input(
        goal=goal,
        material_type=material_type,
    )
    if not candidates:
        raise ValueError("candidates cannot be empty")

    workers = max(1, min(int(max_workers or 1), 8, len(candidates)))

    def _predict_one(index: int, candidate: Dict[str, Any]) -> Dict[str, Any]:
        composition = candidate.get("composition", {})
        processing = candidate.get("processing", {})
        features = candidate.get("features", {})

        if not isinstance(composition, dict):
            return {
                "index": index,
                "error": "composition must be an object",
                "predicted_values": {},
                "confidence": "low",
            }

        predictor = _build_predictor()
        try:
            result = predictor.predict(
                material_type=resolved_material_type,
                composition=composition,
                processing=processing,
                features=features,
                top_k=top_k,
            )
        except Exception as exc:
            return {
                "index": index,
                "error": str(exc),
                "predicted_values": {},
                "confidence": "low",
            }

        prompt_log_id = log_prediction_prompt(
            material_type_input=material_type,
            material_type_resolved=resolved_material_type,
            composition=composition,
            processing=_normalize_for_logging(processing, "processing"),
            features=_normalize_for_logging(features, "features"),
            top_k=top_k,
            prompt=result.prompt,
            llm_response=result.llm_response,
            predicted_values=result.predicted_values,
            confidence=result.confidence,
        )
        return {
            "index": index,
            "material_type": result.material_type,
            "predicted_values": result.predicted_values,
            "confidence": result.confidence,
            "similar_samples": result.similar_samples,
            "llm_response": result.llm_response,
            "prompt_log_id": prompt_log_id,
        }

    predictions: list[Dict[str, Any]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(_predict_one, idx, cand) for idx, cand in enumerate(candidates)]
        for future in concurrent.futures.as_completed(futures):
            predictions.append(future.result())

    predictions.sort(key=lambda x: int(x.get("index", 0)))
    return {
        "material_type": resolved_material_type,
        "material_type_input": material_type,
        "material_type_route_reason": route_reason,
        "goal": goal,
        "top_k": top_k,
        "count": len(predictions),
        "predictions": predictions,
    }


_predictor_agent_model = build_model("material_predictor/agent")


material_predictor_agent = Agent(
    name="Material Predictor Agent",
    model=_predictor_agent_model,
    db=SqliteDb(db_file=str(MATERIAL_PREDICTOR_AGENT_DB)),
    instructions=[
        "You predict material properties using nearest-sample few-shot retrieval.",
        "Before calling tools, infer and normalize the alloy family to a supported dataset key.",
        "Supported dataset keys are: ti, steel, al, hea, hea_pitting.",
        "Use predict_material_properties for one candidate.",
        "Use predict_material_properties_batch when multiple candidates are provided.",
        "Each tool call must contain exactly one JSON arguments object.",
        "Do not concatenate multiple JSON objects in a single tool call.",
        "Summarize predicted values and explain confidence.",
    ],
    tools=[predict_material_properties, predict_material_properties_batch],
    markdown=True,
)
