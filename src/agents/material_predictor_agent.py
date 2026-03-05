from __future__ import annotations

import concurrent.futures
import json
from typing import Any, Dict, Optional

from agno.agent import Agent
from agno.db.sqlite import SqliteDb
from agno.tools import tool

from src.common import MATERIAL_AGENT_SHARED_DB_ID, MATERIAL_PREDICTOR_AGENT_DB, build_model, log_prediction_prompt
from src.fewshot import FewshotPredictor, resolve_material_type_input, supported_material_type_hint
from src.fewshot.parsing import ResultParser
from src.schemas import AgentPredictorOutput, CandidatePrediction


def _build_predictor() -> FewshotPredictor:
    model = build_model("material_predictor/fewshot")
    return FewshotPredictor(
        model_name=model.id,
        api_key=model.api_key,
        base_url=model.base_url,
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


def _single_tool_error(error: str, hint: str) -> Dict[str, Any]:
    return {
        "ok": False,
        "error": error,
        "hint": hint,
        "predicted_values": {},
        "confidence": "low",
        "reasoning": "",
    }


def _batch_tool_error(error: str, hint: str) -> Dict[str, Any]:
    return {
        "ok": False,
        "error": error,
        "hint": hint,
        "predictions": [],
        "count": 0,
    }


def _prediction_error_result(index: int, error: str) -> Dict[str, Any]:
    return {
        "index": index,
        "error": error,
        "predicted_values": {},
        "confidence": "low",
        "reasoning": f"Prediction failed: {error}",
    }


def _prediction_reasoning_payload(
    *,
    llm_response: str,
    predicted_values: Dict[str, Any],
    confidence: str,
) -> str:
    return _extract_reasoning(
        llm_response=llm_response,
        predicted_values=predicted_values,
        confidence=confidence,
    )


def _fallback_reasoning(*, predicted_values: Dict[str, Any], confidence: str) -> str:
    preview = ", ".join(f"{k}={v}" for k, v in list((predicted_values or {}).items())[:3])
    base = "Fallback estimate generated from nearest reference samples."
    if preview:
        return f"{base} Predicted profile: {preview}. Confidence assessed as {confidence}."
    return f"{base} Confidence assessed as {confidence}."


def _is_mock_reasoning_text(text: str) -> bool:
    normalized = (text or "").strip().lower()
    mock_markers = (
        "mock response used because llm call was unavailable",
        "mock response used because llm call failed",
        "llm call was unavailable",
    )
    return any(marker in normalized for marker in mock_markers)


def _extract_reasoning(*, llm_response: str, predicted_values: Dict[str, Any], confidence: str) -> str:
    parser = ResultParser(list((predicted_values or {}).keys()))
    parsed = parser.extract_reasoning(llm_response)
    if parsed and not _is_mock_reasoning_text(parsed):
        return parsed
    return _fallback_reasoning(predicted_values=predicted_values, confidence=confidence)


@tool
def predict_material_properties(
    material_type: str = "",
    composition: Optional[dict] = None,
    goal: str = "",
    processing: Optional[Any] = None,
    features: Optional[Any] = None,
    top_k: int = 3,
) -> Dict[str, Any]:
    if not (material_type or "").strip():
        return _single_tool_error(
            error="Missing required argument: material_type",
            hint="Provide one of supported keys, e.g. ti/steel/al/hea/hea_pitting.",
        )
    if not isinstance(composition, dict) or not composition:
        return _single_tool_error(
            error="Missing required argument: composition",
            hint="Provide a non-empty composition object, e.g. {\"Ti\": 88, \"Al\": 6, \"V\": 4}.",
        )

    resolved_material_type, _ = resolve_material_type_input(
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

    _ = log_prediction_prompt(
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
        "predicted_values": result.predicted_values,
        "confidence": result.confidence,
        "reasoning": _prediction_reasoning_payload(
            llm_response=result.llm_response,
            predicted_values=result.predicted_values,
            confidence=result.confidence,
        ),
    }


@tool
def predict_material_properties_batch(
    material_type: str = "",
    candidates: Optional[list[dict]] = None,
    goal: str = "",
    top_k: int = 3,
    max_workers: int = 3,
) -> Dict[str, Any]:
    if not (material_type or "").strip():
        return _batch_tool_error(
            error="Missing required argument: material_type",
            hint="Provide one of supported keys, e.g. ti/steel/al/hea/hea_pitting.",
        )
    if not candidates:
        return _batch_tool_error(
            error="Missing required argument: candidates",
            hint="Provide a non-empty candidates array with composition/processing fields.",
        )

    resolved_material_type, _ = resolve_material_type_input(
        goal=goal,
        material_type=material_type,
    )

    workers = max(1, min(int(max_workers or 1), 8, len(candidates)))

    def _predict_one(index: int, candidate: Dict[str, Any]) -> Dict[str, Any]:
        composition = candidate.get("composition", {})
        processing = candidate.get("processing", {})
        features = candidate.get("features", {})

        if not isinstance(composition, dict):
            return _prediction_error_result(index=index, error="composition must be an object")

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
            return _prediction_error_result(index=index, error=str(exc))

        _ = log_prediction_prompt(
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
        payload = CandidatePrediction(
            candidate_index=index,
            predicted_values=result.predicted_values,
            confidence=result.confidence,
            reasoning=_prediction_reasoning_payload(
                llm_response=result.llm_response,
                predicted_values=result.predicted_values,
                confidence=result.confidence,
            ),
            prediction_error="",
        )
        item = payload.model_dump()
        item["index"] = int(item.pop("candidate_index", index))
        item["error"] = item.pop("prediction_error", "")
        return item

    predictions: list[Dict[str, Any]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(_predict_one, idx, cand) for idx, cand in enumerate(candidates)]
        for future in concurrent.futures.as_completed(futures):
            predictions.append(future.result())

    predictions.sort(key=lambda x: int(x.get("index", 0)))
    response = AgentPredictorOutput(
        candidate_predictions=[
            CandidatePrediction(
                candidate_index=int(item.get("index", idx)),
                predicted_values=item.get("predicted_values", {})
                if isinstance(item.get("predicted_values"), dict)
                else {},
                confidence=str(item.get("confidence", "low") or "low"),
                reasoning=str(item.get("reasoning", "") or ""),
                prediction_error=str(item.get("error", "") or ""),
            )
            for idx, item in enumerate(predictions)
            if isinstance(item, dict)
        ],
    ).model_dump()
    return {
        "count": len(response["candidate_predictions"]),
        "predictions": [
            {
                "index": item["candidate_index"],
                "predicted_values": item["predicted_values"],
                "confidence": item["confidence"],
                "reasoning": item["reasoning"],
                "error": item["prediction_error"],
            }
            for item in response["candidate_predictions"]
        ],
    }


_predictor_agent_model = build_model("material_predictor/agent")


material_predictor_agent = Agent(
    name="Material Predictor Agent",
    model=_predictor_agent_model,
    db=SqliteDb(db_file=str(MATERIAL_PREDICTOR_AGENT_DB), id=MATERIAL_AGENT_SHARED_DB_ID),
    instructions=[
        "You predict material properties using nearest-sample few-shot retrieval.",
        "Before calling tools, infer and normalize the alloy family to a supported dataset key.",
        "Supported dataset keys are: ti, steel, al, hea, hea_pitting.",
        "Use predict_material_properties for one candidate.",
        "Use predict_material_properties_batch when multiple candidates are provided.",
        "Never call prediction tools with empty args. material_type and composition/candidates must be present.",
        "Each tool call must contain exactly one JSON arguments object.",
        "Do not concatenate multiple JSON objects in a single tool call.",
        "Summarize predicted values and explain confidence.",
    ],
    tools=[predict_material_properties, predict_material_properties_batch],
    markdown=True,
)
