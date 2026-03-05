from __future__ import annotations

import re
from typing import Any, Dict, List


def dict_or_empty(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def list_or_empty(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def dict_from(data: Dict[str, Any], key: str) -> Dict[str, Any]:
    return dict_or_empty(data.get(key, {}))


def normalize_confidence(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"high", "medium", "low"}:
        return normalized
    return "low"


def fallback_prediction_reasoning(predicted_values: Dict[str, Any], confidence: str) -> str:
    preview = ", ".join(f"{k}={v}" for k, v in list((predicted_values or {}).items())[:3])
    if preview:
        return f"Predicted profile: {preview}. Confidence assessed as {confidence}."
    return f"Confidence assessed as {confidence}."


def normalize_prediction_block(value: Any) -> Dict[str, Any]:
    pred = dict_or_empty(value)
    predicted_values = dict_from(pred, "predicted_values")
    confidence = normalize_confidence(pred.get("confidence", "low"))
    reasoning = str(pred.get("reasoning", "") or "").strip()
    if not reasoning:
        reasoning = fallback_prediction_reasoning(predicted_values, confidence)
    return {
        "predicted_values": predicted_values,
        "confidence": confidence,
        "reasoning": reasoning,
    }


def try_parse_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "").strip()
    if not text:
        return None
    text = text.replace("%", "").strip()
    try:
        return float(text)
    except (TypeError, ValueError):
        pass
    match = re.search(r"[-+]?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        return float(match.group(0))
    except (TypeError, ValueError):
        return None


def compact_recommended_candidates_for_review(candidates: List[Any]) -> List[Dict[str, Any]]:
    compacted: List[Dict[str, Any]] = []
    for item in candidates:
        if not isinstance(item, dict):
            continue
        compacted.append(
            {
                "composition": normalize_composition(item.get("composition", {})),
                "processing": dict_from(item, "processing"),
            }
        )
    return compacted


def compact_candidate_predictions_for_review(predictions: List[Any]) -> List[Dict[str, Any]]:
    compacted: List[Dict[str, Any]] = []
    for item in predictions:
        if not isinstance(item, dict):
            continue
        compacted.append(
            {
                "candidate_index": item.get("candidate_index"),
                "predicted_values": dict_from(item, "predicted_values"),
                "confidence": normalize_confidence(item.get("confidence", "low")),
                "reasoning": str(item.get("reasoning", "") or "").strip(),
                "error": str(item.get("prediction_error", "") or "").strip(),
            }
        )
    return compacted


def normalize_next_iteration_proposals(value: Any) -> List[Dict[str, Any]]:
    proposals = list_or_empty(value)
    normalized: List[Dict[str, Any]] = []
    for item in proposals:
        if not isinstance(item, dict):
            continue
        raw_prediction = item.get("prediction", {})
        if not isinstance(raw_prediction, dict):
            raw_prediction = {
                "predicted_values": item.get("predicted_values", {}),
                "confidence": item.get("confidence", "low"),
                "reasoning": item.get("reasoning", ""),
            }
        prediction = normalize_prediction_block(raw_prediction)
        normalized.append(
            {
                "composition": normalize_composition(item.get("composition", {})),
                "processing": dict_from(item, "processing"),
                "prediction": prediction,
                "expected_tradeoff": str(item.get("expected_tradeoff", "") or "").strip(),
                "reason": str(item.get("reason", "") or "").strip(),
            }
        )
    return normalized


def normalize_composition(value: Any) -> Dict[str, float]:
    if isinstance(value, dict):
        normalized: Dict[str, float] = {}
        for key, raw in value.items():
            parsed = try_parse_float(raw)
            if parsed is None:
                continue
            normalized[str(key)] = parsed
        return normalized
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return {}
        matches = re.findall(r"([A-Z][a-z]?)[\s:=]*([-+]?\d+(?:\.\d+)?)", text)
        parsed_comp: Dict[str, float] = {}
        for element, number in matches:
            try:
                parsed_comp[element] = float(number)
            except (TypeError, ValueError):
                continue
        return parsed_comp
    return {}


_BASE_ELEMENT_BY_MATERIAL_TYPE: Dict[str, str] = {
    "ti": "Ti",
    "steel": "Fe",
    "al": "Al",
}


def enforce_explicit_base_element(composition: Dict[str, float], material_type: str) -> Dict[str, float]:
    base_element = _BASE_ELEMENT_BY_MATERIAL_TYPE.get(str(material_type or "").strip().lower())
    if not base_element or not composition:
        return composition
    if base_element in composition:
        return composition
    total = sum(float(v) for v in composition.values())
    remainder = round(max(0.0, 100.0 - total), 4)
    normalized = dict(composition)
    normalized[base_element] = remainder
    return normalized


def normalize_processing(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        text = _processing_text_from_dict(value)
        if text:
            return {"heat treatment method": text}
        return {}
    if isinstance(value, str):
        text = value.strip()
        if text:
            return {"heat treatment method": text}
    return {}


def _processing_text_from_dict(value: Dict[str, Any]) -> str:
    if not isinstance(value, dict):
        return ""
    normalized_items = {
        str(k).strip().lower().replace("_", " "): v
        for k, v in value.items()
        if str(k).strip()
    }
    preferred_keys = (
        "heat treatment method",
        "processing description",
        "process description",
        "process",
        "method",
        "route",
    )
    for key in preferred_keys:
        if key in normalized_items and normalized_items.get(key) is not None:
            text = str(normalized_items.get(key)).strip()
            if text:
                return text

    parts: List[str] = []
    for key, raw in value.items():
        text = str(raw or "").strip()
        if not text:
            continue
        clean_key = str(key or "").strip()
        parts.append(f"{clean_key}: {text}" if clean_key else text)
    return "; ".join(parts)


def extract_top_level_composition(candidate: Dict[str, Any]) -> Dict[str, float]:
    composition: Dict[str, float] = {}
    skip_keys = {
        "composition",
        "processing",
        "processing_description",
        "score",
        "reason",
        "expected_tradeoff",
        "prediction",
        "candidate_index",
    }
    for key, raw in candidate.items():
        if str(key).strip().lower() in skip_keys:
            continue
        parsed = try_parse_float(raw)
        if parsed is None:
            continue
        composition[str(key)] = parsed
    return composition


def normalize_recommender_candidates(raw_candidates: Any, material_type: str = "") -> List[Dict[str, Any]]:
    raw_list = list_or_empty(raw_candidates)
    normalized: List[Dict[str, Any]] = []
    for item in raw_list:
        if not isinstance(item, dict):
            continue
        composition = normalize_composition(item.get("composition", {}))
        if not composition:
            composition = normalize_composition(item.get("composition_text") or item.get("alloy_composition") or item.get("alloy"))
        if not composition:
            composition = extract_top_level_composition(item)
        if not composition:
            continue
        composition = enforce_explicit_base_element(composition, material_type)

        processing = normalize_processing(item.get("processing", {}))
        if not processing:
            processing = normalize_processing(item.get("processing_description") or item.get("process"))

        normalized.append(
            {
                "composition": composition,
                "processing": processing,
                "score": item.get("score"),
                "reason": str(item.get("reason", "") or "").strip(),
                "expected_tradeoff": str(item.get("expected_tradeoff", "") or "").strip(),
            }
        )
    return normalized


def extract_candidate_list(payload: Dict[str, Any]) -> List[Any]:
    if not isinstance(payload, dict):
        return []
    candidate_keys = ("candidates", "recommended_candidates", "recommendations", "materials")
    for key in candidate_keys:
        value = payload.get(key)
        if isinstance(value, list):
            return value
    nested = payload.get("data")
    if isinstance(nested, dict):
        for key in candidate_keys:
            value = nested.get(key)
            if isinstance(value, list):
                return value
    return []
