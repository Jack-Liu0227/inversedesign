from __future__ import annotations

import json
import math
from typing import Any, Dict, List

from agno.agent import Agent
from agno.db.sqlite import SqliteDb
from agno.tools import tool

from src.agents.material_predictor_agent import predict_material_properties_batch
from src.common import MATERIAL_AGENT_SHARED_DB_ID, MATERIAL_RECOMMENDER_AGENT_DB, build_model
from src.fewshot import resolve_material_type_input
from src.schemas import AgentRecommenderOutput, RecommenderCandidate


def _tool_error(error: str, hint: str) -> Dict[str, Any]:
    return {
        "ok": False,
        "error": error,
        "hint": hint,
        "candidates": [],
    }


def _prediction_input_error(error: str, hint: str) -> Dict[str, Any]:
    return {
        "ok": False,
        "error": error,
        "hint": hint,
        "candidates": [],
        "prediction_summary": {"count": 0, "prediction_success_count": 0},
    }


def _dict_or_empty(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list_or_empty(value: Any) -> List[Any]:
    return value if isinstance(value, list) else []


def _try_parse_json_dict(text: str) -> Dict[str, Any]:
    try:
        parsed = json.loads(text)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _extract_json_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value is None:
        return {}
    text = str(value).strip()
    if not text:
        return {}
    if text.startswith("```"):
        text = text.strip("`")
        if text.lower().startswith("json"):
            text = text[4:].strip()
    parsed = _try_parse_json_dict(text)
    if parsed:
        return parsed
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        return _try_parse_json_dict(text[start : end + 1])
    return {}


def _call_tool(tool_obj: Any, **kwargs: Any) -> Any:
    entrypoint = getattr(tool_obj, "entrypoint", None)
    if callable(entrypoint):
        return entrypoint(**kwargs)
    if callable(tool_obj):
        return tool_obj(**kwargs)
    raise TypeError(f"Tool object is not callable: {type(tool_obj)!r}")


def _normalize_top_n(value: Any, default: int = 3) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(1, min(10, parsed))


def _normalize_composition(value: Any) -> Dict[str, float]:
    if not isinstance(value, dict):
        return {}
    normalized: Dict[str, float] = {}
    for key, raw in value.items():
        try:
            normalized[str(key)] = float(raw)
        except (TypeError, ValueError):
            continue
    return normalized


_BASE_ELEMENT_BY_MATERIAL_TYPE: Dict[str, str] = {
    "ti": "Ti",
    "steel": "Fe",
    "al": "Al",
}


def _enforce_explicit_base_element(composition: Dict[str, float], material_type: str) -> Dict[str, float]:
    base_element = _BASE_ELEMENT_BY_MATERIAL_TYPE.get(str(material_type or "").strip().lower())
    if not base_element or not composition:
        return composition
    if base_element in composition:
        return composition
    total = 0.0
    for value in composition.values():
        if isinstance(value, (int, float)) and math.isfinite(float(value)):
            total += float(value)
    remainder = round(max(0.0, 100.0 - total), 4)
    normalized = dict(composition)
    normalized[base_element] = remainder
    return normalized


def _normalize_processing(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        text = _processing_text_from_dict(value)
        return {"heat treatment method": text} if text else {}
    if isinstance(value, str) and value.strip():
        return {"heat treatment method": value.strip()}
    return {}


def _processing_text_from_dict(value: Dict[str, Any]) -> str:
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


def _candidate_signature(composition: Dict[str, Any], processing: Dict[str, Any]) -> str:
    return json.dumps({"composition": composition, "processing": processing}, sort_keys=True, ensure_ascii=False)


def _normalize_confidence(value: Any) -> str:
    confidence = str(value or "").strip().lower()
    if confidence in {"high", "medium", "low"}:
        return confidence
    return "low"


_recommender_model = build_model("material_recommender/agent")


def _candidate_generator_agent() -> Agent:
    return Agent(
        name="Material Candidate Generator",
        model=_recommender_model,
        db=SqliteDb(db_file=str(MATERIAL_RECOMMENDER_AGENT_DB), id=MATERIAL_AGENT_SHARED_DB_ID),
        instructions=[
            "You design new alloy candidates directly from goal and context.",
            "Return only valid JSON: {\"candidates\": [...]}",
            "Each candidate must have composition(object), processing(object), reason(string), expected_tradeoff(string), score(number).",
            "processing must contain exactly one key: 'heat treatment method'.",
            "The value of 'heat treatment method' must be one complete and practical process route sentence.",
            "Do not copy identical candidates.",
            "Favor practical metallurgical changes over random large jumps.",
        ],
        markdown=True,
    )


@tool
def generate_candidates_with_rag(
    goal: str = "",
    material_type: str = "",
    top_n: int = 3,
    rag_context: str = "",
    preference_feedback: str = "",
) -> Dict[str, Any]:
    if not (goal or "").strip():
        return _tool_error(
            error="Missing required argument: goal",
            hint="Provide a non-empty optimization goal, e.g. 'high strength and good ductility'.",
        )

    resolved_material_type, _ = resolve_material_type_input(
        goal=goal,
        material_type=material_type,
    )
    n = _normalize_top_n(top_n, default=3)
    prompt = (
        "Generate novel material candidates.\n"
        "Return ONLY JSON object with key candidates.\n"
        "Each candidate requires: composition(dict), processing(dict), reason(str), expected_tradeoff(str), score(number).\n"
        "processing must include exactly one field: {'heat treatment method': '<complete route text>'}.\n"
        "Do not output thermo_mechanical, microstructure_target, or other processing sub-keys.\n"
        "Candidates should be diverse and realistic under the given material family.\n"
        f"goal={goal}\n"
        f"material_type={resolved_material_type}\n"
        f"top_n={n}\n"
        f"preference_feedback={preference_feedback}\n"
        f"rag_context={rag_context}"
    )
    generated = _extract_json_dict(_candidate_generator_agent().run(prompt).content)
    raw_candidates = _list_or_empty(generated.get("candidates"))

    candidates: List[Dict[str, Any]] = []
    seen = set()
    for i, item in enumerate(raw_candidates):
        if not isinstance(item, dict):
            continue
        composition = _normalize_composition(item.get("composition", {}))
        if not composition:
            continue
        composition = _enforce_explicit_base_element(composition, resolved_material_type)
        processing = _normalize_processing(item.get("processing", {}))
        signature = _candidate_signature(composition, processing)
        if signature in seen:
            continue
        seen.add(signature)
        try:
            score = float(item.get("score", max(0.0, 1.0 - i * 0.1)))
        except (TypeError, ValueError):
            score = max(0.0, 1.0 - i * 0.1)
        candidates.append(
            {
                "composition": composition,
                "processing": processing,
                "score": score,
                "reason": str(item.get("reason", "") or "").strip(),
                "expected_tradeoff": str(item.get("expected_tradeoff", "") or "").strip(),
            }
        )
        if len(candidates) >= n:
            break

    output = AgentRecommenderOutput(
        candidates=[RecommenderCandidate(**item) for item in candidates],
    )
    return output.model_dump()


@tool
def predict_generated_candidates(
    material_type: str = "",
    candidates: List[Dict[str, Any]] | None = None,
    goal: str = "",
    top_k: int = 3,
    max_workers: int = 3,
) -> Dict[str, Any]:
    if not (material_type or "").strip():
        return _prediction_input_error(
            error="Missing required argument: material_type",
            hint="Provide one of supported keys, e.g. ti/steel/al/hea/hea_pitting.",
        )

    input_candidates = candidates if isinstance(candidates, list) else []
    if not input_candidates:
        return _prediction_input_error(
            error="Missing required argument: candidates",
            hint="Provide non-empty candidate list generated by generate_candidates_with_rag.",
        )

    prediction_result = _call_tool(
        predict_material_properties_batch,
        material_type=material_type,
        candidates=input_candidates,
        goal=goal,
        top_k=top_k,
        max_workers=max(1, min(max_workers, len(input_candidates))),
    )
    prediction_items = _list_or_empty(_dict_or_empty(prediction_result).get("predictions"))

    prediction_map: Dict[int, Dict[str, Any]] = {}
    for item in prediction_items:
        if not isinstance(item, dict):
            continue
        idx = item.get("index")
        if isinstance(idx, int):
            prediction_map[idx] = item

    merged_candidates: List[Dict[str, Any]] = []
    prediction_success_count = 0
    for idx, candidate in enumerate(input_candidates):
        if not isinstance(candidate, dict):
            continue
        output_candidate = dict(candidate)
        pred = prediction_map.get(idx, {})
        predicted_values = pred.get("predicted_values", {}) if isinstance(pred, dict) else {}
        if isinstance(predicted_values, dict) and predicted_values:
            prediction_success_count += 1
        confidence = _normalize_confidence(pred.get("confidence", "low"))
        if isinstance(pred, dict) and pred.get("error"):
            reasoning = f"Prediction failed: {pred.get('error')}"
        else:
            preview = ", ".join(f"{k}={v}" for k, v in list((predicted_values or {}).items())[:3])
            reasoning = f"Predicted profile: {preview}" if preview else f"confidence={confidence}"
        output_candidate["prediction"] = {
            "predicted_values": predicted_values if isinstance(predicted_values, dict) else {},
            "confidence": confidence,
            "reasoning": reasoning,
        }
        merged_candidates.append(output_candidate)

    return {
        "ok": True,
        "material_type": material_type,
        "goal": goal,
        "candidates": merged_candidates,
        "prediction_summary": {
            "count": len(merged_candidates),
            "prediction_success_count": prediction_success_count,
        },
    }


material_recommender_agent = Agent(
    name="Material Recommender Agent",
    model=_recommender_model,
    db=SqliteDb(db_file=str(MATERIAL_RECOMMENDER_AGENT_DB), id=MATERIAL_AGENT_SHARED_DB_ID),
    instructions=[
        "You recommend new material candidates by LLM generation, not by historical row copy.",
        "First normalize material type to one of supported dataset keys.",
        "Use generate_candidates_with_rag to produce candidate composition and processing suggestions.",
        "Always provide processing as exactly one key: 'heat treatment method' with full route text.",
        "If previous_round_feedback_summary is provided, avoid repeating top risk tags and failure reasons.",
        "Never call recommendation tools without goal. If goal is missing, ask a clarification question instead of tool call.",
        "Return concise rationale for each candidate.",
    ],
    tools=[generate_candidates_with_rag],
    markdown=True,
)
