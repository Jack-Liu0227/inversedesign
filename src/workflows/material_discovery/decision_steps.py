from __future__ import annotations

import json
import re
from typing import Any

from agno.workflow.types import StepInput, StepOutput

from src.schemas import MaterialDiscoveryResponse
from src.workflows.material_discovery.state import MaterialDiscoveryState

from .common import as_workflow_input, collect_step_outputs, trace_id
from .response_mapper import build_response, valid_candidates_only

_TARGET_PATTERN = re.compile(
    r"([A-Za-z][A-Za-z0-9_%()/.-]*)\s*(>=|<=|>|<|=|为|到达|达到|不少于|不低于|不高于|不大于)?\s*([-+]?\d+(?:\.\d+)?)",
    re.IGNORECASE,
)


def _dict_or_empty(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list_or_empty(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _normalize_operator(raw: str) -> str:
    text = str(raw or "").strip()
    if text in {">", ">=", "不少于", "不低于"}:
        return ">="
    if text in {"<", "<=", "不高于", "不大于"}:
        return "<="
    return "="


def _normalize_metric_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(name or "").strip().lower())


def _parse_goal_targets(goal: str) -> list[dict[str, Any]]:
    targets: list[dict[str, Any]] = []
    for metric, op, raw_value in _TARGET_PATTERN.findall(str(goal or "")):
        try:
            target = float(raw_value)
        except (TypeError, ValueError):
            continue
        operator = _normalize_operator(op)
        targets.append({"name": metric, "operator": operator, "target": target})
    return targets


def _build_metric_lookup(predicted_values: dict[str, Any]) -> dict[str, tuple[str, float]]:
    lookup: dict[str, tuple[str, float]] = {}
    for key, value in predicted_values.items():
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            continue
        lookup[_normalize_metric_name(str(key))] = (str(key), parsed)
    return lookup


def _evaluate_stop(goal: str, valid_candidates: list[dict[str, Any]]) -> dict[str, Any]:
    if not valid_candidates:
        return {"passed": False, "reason": "no_valid_candidates", "metrics": []}
    primary = valid_candidates[0]
    prediction = _dict_or_empty(primary.get("prediction", {}))
    predicted_values = _dict_or_empty(prediction.get("predicted_values", {}))
    targets = _parse_goal_targets(goal)
    if not targets:
        return {"passed": False, "reason": "no_parseable_targets_in_goal", "metrics": []}
    lookup = _build_metric_lookup(predicted_values)
    metrics: list[dict[str, Any]] = []
    for target in targets:
        metric_name = str(target["name"])
        normalized_name = _normalize_metric_name(metric_name)
        matched_key, predicted = lookup.get(normalized_name, ("", None))
        if predicted is None:
            metrics.append(
                {
                    "name": metric_name,
                    "operator": target["operator"],
                    "target": target["target"],
                    "predicted": None,
                    "passed": False,
                    "detail": "missing_metric_in_prediction",
                }
            )
            continue
        op = str(target["operator"])
        threshold = float(target["target"])
        if op == ">=":
            passed = bool(predicted >= threshold)
        elif op == "<=":
            passed = bool(predicted <= threshold)
        else:
            tolerance = max(1e-6, abs(threshold) * 0.05)
            passed = bool(abs(predicted - threshold) <= tolerance)
        metrics.append(
            {
                "name": matched_key or metric_name,
                "operator": op,
                "target": threshold,
                "predicted": predicted,
                "passed": passed,
                "detail": "matched",
            }
        )
    passed = bool(metrics) and all(m["passed"] for m in metrics)
    return {"passed": passed, "reason": "target_met" if passed else "target_not_met", "metrics": metrics}


def _evaluate_values_against_goal(goal: str, values: dict[str, Any], reason_prefix: str) -> dict[str, Any]:
    targets = _parse_goal_targets(goal)
    if not targets:
        return {"passed": False, "reason": "no_parseable_targets_in_goal", "metrics": []}
    lookup = _build_metric_lookup(values)
    metrics: list[dict[str, Any]] = []
    for target in targets:
        metric_name = str(target["name"])
        normalized_name = _normalize_metric_name(metric_name)
        matched_key, observed = lookup.get(normalized_name, ("", None))
        if observed is None:
            metrics.append(
                {
                    "name": metric_name,
                    "operator": target["operator"],
                    "target": target["target"],
                    "predicted": None,
                    "passed": False,
                    "detail": "missing_metric",
                }
            )
            continue
        op = str(target["operator"])
        threshold = float(target["target"])
        if op == ">=":
            passed = bool(observed >= threshold)
        elif op == "<=":
            passed = bool(observed <= threshold)
        else:
            tolerance = max(1e-6, abs(threshold) * 0.05)
            passed = bool(abs(observed - threshold) <= tolerance)
        metrics.append(
            {
                "name": matched_key or metric_name,
                "operator": op,
                "target": threshold,
                "predicted": observed,
                "passed": passed,
                "detail": f"{reason_prefix}_matched",
            }
        )
    passed = bool(metrics) and all(m["passed"] for m in metrics)
    return {"passed": passed, "reason": f"{reason_prefix}_met" if passed else f"{reason_prefix}_not_met", "metrics": metrics}


def _parse_measured_values_from_feedback(payload: dict[str, Any]) -> dict[str, float]:
    measured_values_input = payload.get("measured_values")
    if measured_values_input is None:
        measured_values_input = payload.get("measured_values_json")
    if measured_values_input is None:
        return {}
    if isinstance(measured_values_input, dict):
        data = measured_values_input
    elif isinstance(measured_values_input, str):
        text = measured_values_input.strip()
        if not text:
            return {}
        data = json.loads(text)
    else:
        return {}
    if not isinstance(data, dict):
        return {}
    output: dict[str, float] = {}
    for key, value in data.items():
        try:
            output[str(key)] = float(value)
        except (TypeError, ValueError):
            continue
    return output


def collect_human_feedback(step_input: StepInput) -> StepOutput:
    request = as_workflow_input(step_input.input)
    additional_data = getattr(step_input, "additional_data", None) or {}
    user_input = _dict_or_empty(_dict_or_empty(additional_data).get("user_input", {}))
    measured_values = _parse_measured_values_from_feedback(user_input)
    if not measured_values and isinstance(request.experiment_feedback, dict):
        measured_values = _parse_measured_values_from_feedback(request.experiment_feedback)
    notes = str(user_input.get("notes", "")).strip()
    preference_feedback = str(user_input.get("preference_feedback") or request.preference_feedback or "").strip()
    return StepOutput(content={"measured_values": measured_values, "notes": notes, "preference_feedback": preference_feedback})


def _resolve_round_index(persistence_payload: dict[str, Any]) -> int:
    value = persistence_payload.get("round_index")
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = 1
    return max(1, parsed)


def final_decision(step_input: StepInput) -> StepOutput:
    request = as_workflow_input(step_input.input)
    previous_outputs = step_input.previous_step_outputs or {}
    router_payload = _dict_or_empty(getattr(previous_outputs.get("Router Agent"), "content", None))
    predictor_payload = _dict_or_empty(getattr(previous_outputs.get("Predictor Agent"), "content", None))
    judge_payload = _dict_or_empty(getattr(previous_outputs.get("Rationality Judge"), "content", None))
    persistence_payload = _dict_or_empty(getattr(previous_outputs.get("Persistence"), "content", None))
    feedback_payload = _dict_or_empty(getattr(previous_outputs.get("Human Feedback"), "content", None))

    round_index = _resolve_round_index(persistence_payload)
    mode = "human_in_the_loop" if bool(request.human_loop) else "ai_only"
    requires_human = bool(mode == "human_in_the_loop")
    recommended_candidates = _list_or_empty(predictor_payload.get("recommended_candidates", []))
    candidate_predictions = _list_or_empty(predictor_payload.get("candidate_predictions", []))
    rationality = _list_or_empty(judge_payload.get("evaluations", []))
    measured_values = _dict_or_empty(feedback_payload.get("measured_values", {}))
    stop_evaluation = _evaluate_stop(request.goal, valid_candidates_only(recommended_candidates, candidate_predictions, rationality))

    if measured_values:
        stop_evaluation = _evaluate_values_against_goal(request.goal, measured_values, "experiment_target")

    reached_max_iterations = bool(round_index >= int(request.max_iterations))
    if stop_evaluation["passed"]:
        decision = "await_user_choice" if requires_human and not measured_values else "stop"
    elif reached_max_iterations:
        decision = "await_user_choice"
    else:
        decision = "continue"

    state = MaterialDiscoveryState(
        mode=mode,
        round_index=round_index,
        goal=request.goal,
        resolved_material_type=str(router_payload.get("resolved_material_type", "")),
        resolution_reason=str(router_payload.get("resolution_reason", "")),
        measured_values=measured_values,
        preference_feedback=str(feedback_payload.get("preference_feedback", "")),
        decision=decision,
        requires_human_feedback=requires_human,
    )

    debug_payload = None
    if request.include_debug:
        debug_payload = {"trace_id": trace_id(step_input, request), "step_outputs": collect_step_outputs(step_input)}
    response = build_response(
        state=state,
        decision=decision,
        max_iterations=int(request.max_iterations),
        recommended_candidates=recommended_candidates,
        candidate_predictions=candidate_predictions,
        rationality=rationality,
        stop_evaluation=stop_evaluation,
        judge_summary={
            "total": int(persistence_payload.get("total_candidates", len(recommended_candidates))),
            "valid_count": int(persistence_payload.get("valid_count", 0)),
            "invalid_count": int(persistence_payload.get("invalid_count", 0)),
            "top_reasons": [str(x) for x in _list_or_empty(persistence_payload.get("top_reasons", []))],
        },
        debug_payload=debug_payload,
    )
    return StepOutput(content=MaterialDiscoveryResponse.model_validate(response).model_dump(exclude_none=True))


def end_when_satisfied(outputs: list[StepOutput]) -> bool:
    for output in reversed(outputs):
        content = output.content
        if not isinstance(content, dict):
            continue
        decision = str(content.get("decision", "")).strip().lower()
        if decision in {"stop", "await_user_choice"}:
            return True
    return False
