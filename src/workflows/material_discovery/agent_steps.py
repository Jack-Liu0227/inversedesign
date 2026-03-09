from __future__ import annotations

import json
import re
from collections import Counter
from typing import Any

from agno.workflow.types import StepInput, StepOutput

from src.agents.material_predictor_agent import material_predictor_agent
from src.agents.material_recommender_agent import material_recommender_agent
from src.agents.material_router_agent import material_router_agent
from src.common import (
    fetch_round_samples_context,
    retrieve_material_doc_segments,
    next_round_index,
)
from src.common.prompt_formatting import (
    format_candidates_for_predictor,
    format_feedback_summary,
    format_previous_round_context,
    format_retrieved_context_blocks,
)
from src.schemas import AgentPredictorOutput, AgentRecommenderOutput, AgentRouterOutput

from .agent_runtime import run_agent_for_json
from .common import as_workflow_input, audit_event, effective_workflow_run_id, sync_workflow_run_meta
from .material_normalization import (
    dict_or_empty,
    extract_candidate_list,
    list_or_empty,
    normalize_composition,
    normalize_confidence,
    normalize_processing,
    normalize_recommender_candidates,
)


def _adaptive_recommend_count(previous_round_context: list[dict[str, Any]]) -> int:
    if not previous_round_context:
        return 4
    total = len(previous_round_context)
    valid = len([x for x in previous_round_context if bool(dict_or_empty(x).get("is_valid", False))])
    valid_rate = float(valid) / float(total) if total > 0 else 0.0
    if valid_rate < 0.34:
        return 5
    if valid_rate < 0.67:
        return 4
    return 3


def _round_feedback_summary(previous_round_context: list[dict[str, Any]]) -> dict[str, Any]:
    valid_examples: list[dict[str, Any]] = []
    risk_counter: Counter[str] = Counter()
    reason_counter: Counter[str] = Counter()
    action_counter: Counter[str] = Counter()

    for item in previous_round_context:
        row = dict_or_empty(item)
        is_valid = bool(row.get("is_valid", False))
        judge_score = float(row.get("judge_score", 0.0) or 0.0)
        reasons = [str(x).strip() for x in list_or_empty(row.get("judge_reasons", [])) if str(x).strip()]
        risk_tags = [str(x).strip() for x in list_or_empty(row.get("risk_tags", [])) if str(x).strip()]
        action = str(row.get("recommended_action", "") or "").strip().lower()
        if action:
            action_counter[action] += 1
        if not is_valid:
            for reason in reasons:
                reason_counter[reason] += 1
            for risk in risk_tags:
                risk_counter[risk] += 1
            continue
        valid_examples.append(
            {
                "candidate_index": row.get("candidate_index"),
                "judge_score": judge_score,
                "composition": dict_or_empty(row.get("composition", {})),
                "processing": dict_or_empty(row.get("processing", {})),
                "predicted_values": dict_or_empty(row.get("predicted_values", {})),
            }
        )

    valid_examples.sort(key=lambda x: float(x.get("judge_score", 0.0) or 0.0), reverse=True)
    return {
        "total_candidates": len(previous_round_context),
        "valid_count": len(valid_examples),
        "invalid_count": max(0, len(previous_round_context) - len(valid_examples)),
        "recommended_action_counts": dict(action_counter),
        "top_invalid_reasons": [x[0] for x in reason_counter.most_common(5)],
        "top_risk_tags_to_avoid": [x[0] for x in risk_counter.most_common(5)],
        "top_valid_examples": valid_examples[:3],
    }


def _looks_garbled_goal(text: str) -> bool:
    value = str(text or "")
    return ("?" in value) or ("�" in value)


def _candidate_composition_signature(composition: dict[str, Any]) -> str:
    normalized = normalize_composition(composition)
    ordered_items: list[tuple[str, float]] = []
    for key in sorted(normalized.keys()):
        try:
            value = round(float(normalized[key]), 6)
        except (TypeError, ValueError):
            continue
        ordered_items.append((str(key), value))
    return json.dumps(ordered_items, ensure_ascii=False, separators=(",", ":"))


def _previous_round_composition_signatures(previous_round_context: list[dict[str, Any]]) -> set[str]:
    signatures: set[str] = set()
    for row in previous_round_context:
        composition = dict_or_empty(dict_or_empty(row).get("composition", {}))
        signature = _candidate_composition_signature(composition)
        if signature:
            signatures.add(signature)
    return signatures


_TARGET_PATTERN = re.compile(
    r"([A-Za-z][A-Za-z0-9_%()/.-]*)\s*(>=|<=|>|<|=)\s*([-+]?\d+(?:\.\d+)?)",
    re.IGNORECASE,
)


def _normalize_goal_operator(raw: str) -> str:
    text = str(raw or "").strip()
    if text in {">", ">="}:
        return ">="
    if text in {"<", "<="}:
        return "<="
    return "="


def _normalize_metric_key(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(name or "").strip().lower())


def _parse_goal_targets(goal: str) -> list[dict[str, Any]]:
    targets: list[dict[str, Any]] = []
    for metric, op, raw_value in _TARGET_PATTERN.findall(str(goal or "")):
        try:
            target_value = float(raw_value)
        except (TypeError, ValueError):
            continue
        targets.append(
            {
                "name": str(metric).strip(),
                "operator": _normalize_goal_operator(op),
                "target": target_value,
            }
        )
    return targets


def _goal_distance(goal: str, predicted_values: dict[str, Any]) -> tuple[float, bool]:
    targets = _parse_goal_targets(goal)
    if not targets:
        return float("inf"), False
    metric_lookup: dict[str, float] = {}
    for key, value in dict_or_empty(predicted_values).items():
        try:
            metric_lookup[_normalize_metric_key(str(key))] = float(value)
        except (TypeError, ValueError):
            continue
    total_gap = 0.0
    all_passed = True
    for target in targets:
        observed = metric_lookup.get(_normalize_metric_key(str(target["name"])))
        if observed is None:
            return float("inf"), False
        threshold = float(target["target"])
        scale = max(abs(threshold), 1.0)
        operator = str(target["operator"])
        if operator == ">=":
            gap = max(0.0, threshold - observed) / scale
            passed = observed >= threshold
        elif operator == "<=":
            gap = max(0.0, observed - threshold) / scale
            passed = observed <= threshold
        else:
            gap = abs(observed - threshold) / scale
            tolerance = max(1e-6, abs(threshold) * 0.05)
            passed = abs(observed - threshold) <= tolerance
        total_gap += gap
        all_passed = all_passed and passed
    return total_gap, all_passed


def _best_previous_round_goal_distance(goal: str, previous_round_context: list[dict[str, Any]]) -> float:
    best = float("inf")
    for row in previous_round_context:
        distance, _ = _goal_distance(goal, dict_or_empty(dict_or_empty(row).get("predicted_values", {})))
        if distance < best:
            best = distance
    return best


def _filter_candidates_by_composition(
    candidates: list[dict[str, Any]],
    forbidden_signatures: set[str],
) -> list[dict[str, Any]]:
    if not forbidden_signatures:
        return list(candidates)
    filtered: list[dict[str, Any]] = []
    seen_current: set[str] = set()
    for candidate in candidates:
        composition = dict_or_empty(candidate.get("composition", {}))
        signature = _candidate_composition_signature(composition)
        if not signature or signature in forbidden_signatures or signature in seen_current:
            continue
        seen_current.add(signature)
        filtered.append(candidate)
    return filtered


def _filter_predictions_by_goal_improvement(
    *,
    goal: str,
    candidates: list[dict[str, Any]],
    predictions: list[dict[str, Any]],
    previous_round_context: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str]:
    if not candidates or not predictions:
        return candidates, predictions, ""
    baseline_distance = _best_previous_round_goal_distance(goal, previous_round_context)
    if baseline_distance == float("inf"):
        return candidates, predictions, ""

    prediction_map = {
        int(item["candidate_index"]): item
        for item in predictions
        if isinstance(item, dict) and isinstance(item.get("candidate_index"), int)
    }
    kept_pairs: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for idx, candidate in enumerate(candidates):
        prediction = prediction_map.get(idx)
        if not prediction:
            continue
        distance, passed = _goal_distance(goal, dict_or_empty(prediction.get("predicted_values", {})))
        if passed or distance + 1e-9 < baseline_distance:
            kept_pairs.append((candidate, prediction))

    if not kept_pairs:
        return [], [], "no_goal_improving_candidates_vs_previous_round"

    filtered_candidates: list[dict[str, Any]] = []
    filtered_predictions: list[dict[str, Any]] = []
    for new_index, (candidate, prediction) in enumerate(kept_pairs):
        filtered_candidates.append(candidate)
        remapped = dict(prediction)
        remapped["candidate_index"] = new_index
        filtered_predictions.append(remapped)
    return filtered_candidates, filtered_predictions, ""


def _canonical_objective_text(
    *,
    raw_goal: str,
    material_type: str,
    resolved_properties: list[Any],
    target_thresholds: list[Any],
) -> str:
    material = str(material_type or "").strip().lower() or "unknown"
    props = [str(x).strip().lower() for x in resolved_properties if str(x).strip()]
    parsed_thresholds = [str(x).strip() for x in target_thresholds if str(x).strip()]
    if parsed_thresholds:
        return f"material={material}; targets={'; '.join(parsed_thresholds)}"

    nums = re.findall(r"\d+(?:\.\d+)?", str(raw_goal or ""))
    if len(nums) >= 2 and "ultimate_tensile_strength" in props and "elongation" in props:
        return f"material={material}; UTS(MPa)>= {nums[0]}; El(%)>= {nums[1]}"
    if len(nums) >= 1 and "ultimate_tensile_strength" in props:
        return f"material={material}; UTS(MPa)>= {nums[0]}"
    if len(nums) >= 1 and "elongation" in props:
        return f"material={material}; El(%)>= {nums[0]}"
    return f"material={material}; optimize {', '.join(props) if props else 'strength/ductility balance'}"


def route_with_agent(step_input: StepInput) -> StepOutput:
    request = as_workflow_input(step_input.input)
    prompt = (
        "Resolve material type and parse optimization targets from goal.\n"
        "Return ONLY valid JSON with keys: goal, resolved_material_type, resolution_reason, resolved_properties, target_thresholds.\n"
        f"goal={request.goal}"
    )
    raw = run_agent_for_json(
        material_router_agent,
        step_input=step_input,
        agent_name="router",
        prompt=prompt,
        include_meta=True,
    )
    parsed = dict_or_empty(raw.get("parsed", {}))
    raw_content = str(raw.get("raw_content", "") or "").strip()
    required_keys = {"resolved_material_type", "resolution_reason", "resolved_properties", "target_thresholds"}
    missing_keys = sorted(list(required_keys - set(parsed.keys())))
    if missing_keys:
        error_text = f"Router output missing required keys: {missing_keys}"
        audit_event(
            step_input=step_input,
            request=request,
            step_name="Router Agent",
            event_type="router_schema_validation_failed",
            payload={
                "error": error_text,
                "missing_keys": missing_keys,
                "parsed_response": parsed,
                "raw_response": raw_content,
            },
            success=False,
            error_text=error_text,
        )
        raise ValueError(error_text)
    try:
        validated = AgentRouterOutput.model_validate(parsed)
    except Exception as exc:
        error_text = f"Router output schema validation failed: {exc}"
        audit_event(
            step_input=step_input,
            request=request,
            step_name="Router Agent",
            event_type="router_schema_validation_failed",
            payload={
                "error": str(exc),
                "parsed_response": parsed,
                "raw_response": raw_content,
            },
            success=False,
            error_text=str(exc),
        )
        raise ValueError(error_text) from exc
    resolved_material_type = str(validated.resolved_material_type or "").strip().lower()
    resolution_reason = str(validated.resolution_reason or "").strip()
    if not resolved_material_type or not resolution_reason:
        error_text = (
            "Router output has empty required fields: "
            f"resolved_material_type='{resolved_material_type}', resolution_reason='{resolution_reason}'"
        )
        audit_event(
            step_input=step_input,
            request=request,
            step_name="Router Agent",
            event_type="router_schema_validation_failed",
            payload={
                "error": error_text,
                "parsed_response": validated.model_dump(),
                "raw_response": raw_content,
            },
            success=False,
            error_text=error_text,
        )
        raise ValueError(error_text)
    output = validated.model_dump()
    output["goal"] = str(output.get("goal", "") or request.goal or "").strip()
    output["resolved_material_type"] = resolved_material_type
    sync_workflow_run_meta(step_input, request, material_type=resolved_material_type)
    return StepOutput(content=output)


def recommend_with_agent(step_input: StepInput) -> StepOutput:
    request = as_workflow_input(step_input.input)
    routed_output = step_input.previous_step_outputs.get("Router Agent")
    routed = dict_or_empty(getattr(routed_output, "content", None))
    resolved_material_type = str(routed.get("resolved_material_type", "")).strip().lower()
    resolved_properties = list_or_empty(routed.get("resolved_properties", []))
    target_thresholds = list_or_empty(routed.get("target_thresholds", []))
    if not resolved_material_type:
        raise ValueError("Missing resolved_material_type from Router Agent step")

    workflow_run_id = effective_workflow_run_id(step_input, request)
    current_round = next_round_index(str(workflow_run_id))
    previous_round = max(0, int(current_round) - 1)
    previous_round_context = (
        fetch_round_samples_context(
            workflow_run_id=str(workflow_run_id),
            material_type=resolved_material_type,
            round_index=previous_round,
            limit=12,
        )
        if previous_round > 0
        else []
    )
    top_n = _adaptive_recommend_count(previous_round_context)
    previous_round_feedback = _round_feedback_summary(previous_round_context)
    forbidden_composition_signatures = _previous_round_composition_signatures(previous_round_context)
    previous_round_constraint_lines: list[str] = []
    for row in previous_round_context:
        composition = dict_or_empty(row.get("composition", {}))
        processing = dict_or_empty(row.get("processing", {}))
        predicted_values = dict_or_empty(row.get("predicted_values", {}))
        if not composition and not processing and not predicted_values:
            continue
        previous_round_constraint_lines.append(
            " | ".join(
                [
                    f"composition={json.dumps(composition, ensure_ascii=False, sort_keys=True)}",
                    f"processing={json.dumps(processing, ensure_ascii=False, sort_keys=True)}",
                    f"predicted_values={json.dumps(predicted_values, ensure_ascii=False, sort_keys=True)}",
                ]
            )
        )
    retrieval_query = (
        f"goal={request.goal}\n"
        f"preference_feedback={request.preference_feedback or ''}\n"
        f"previous_round_feedback_summary={json.dumps(previous_round_feedback, ensure_ascii=False)}"
    )
    retrieved_doc_segments = retrieve_material_doc_segments(
        material_type=resolved_material_type,
        query_text=retrieval_query,
        workflow_run_id=str(workflow_run_id),
        before_round_index=int(current_round),
        top_k=8,
        fetch_k=30,
    )
    retrieved_context_block = format_retrieved_context_blocks(retrieved_doc_segments, max_items=8, max_content_chars=320)
    previous_round_feedback_block = format_feedback_summary(previous_round_feedback)
    canonical_objective = _canonical_objective_text(
        raw_goal=request.goal,
        material_type=resolved_material_type,
        resolved_properties=resolved_properties,
        target_thresholds=target_thresholds,
    )
    objective_line = (
        f"Design objective: {canonical_objective}"
        if _looks_garbled_goal(request.goal)
        else f"Design objective: {request.goal}"
    )
    previous_round_constraints_block = (
        chr(10).join([f"- {line}" for line in previous_round_constraint_lines])
        if previous_round_constraint_lines
        else "- None"
    )
    prompt = (
        "Recommend candidate alloys.\n\n"
        "Return ONLY valid JSON with key: candidates.\n"
        "Each candidate must include: composition, processing, score, reason, expected_tradeoff.\n"
        f"Generate exactly {top_n} candidates.\n\n"
        f"Material family: {resolved_material_type}\n"
        f"{objective_line}\n"
        f"Canonical objective: {canonical_objective}\n"
        f"Preference feedback: {request.preference_feedback or 'None'}\n\n"
        "Hard constraints:\n"
        "- Do not reuse any previous-round composition.\n"
        "- Every candidate must be a new composition and should move closer to the goal than the previous round.\n"
        "- processing must contain exactly one key: 'heat treatment method'.\n"
        "- The value of 'heat treatment method' must be one complete process-route sentence.\n"
        "- Do not output thermo_mechanical or other processing sub-keys.\n"
        "- Do not ask clarifying questions or request additional user input.\n"
        "- If some fields are ambiguous, infer conservatively from the design objective.\n\n"
        "Previous-round constraints:\n"
        f"{previous_round_constraints_block}\n\n"
        "Previous-round feedback summary:\n"
        f"{previous_round_feedback_block}\n\n"
        "Retrieved context:\n"
        f"{retrieved_context_block or 'No retrieved context.'}"
    )
    rec = run_agent_for_json(
        material_recommender_agent,
        step_input=step_input,
        agent_name="recommender",
        prompt=prompt,
    )
    normalized_candidates = normalize_recommender_candidates(
        extract_candidate_list(rec),
        material_type=resolved_material_type,
    )
    filtered_candidates = _filter_candidates_by_composition(
        normalized_candidates,
        forbidden_signatures=forbidden_composition_signatures,
    )
    output = AgentRecommenderOutput(candidates=filtered_candidates).model_dump()
    return StepOutput(content=output)


def _build_predict_jobs(candidates: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    jobs: list[dict[str, Any]] = []
    invalid: list[dict[str, Any]] = []
    for idx, candidate in enumerate(candidates):
        comp = normalize_composition(candidate.get("composition", {}))
        candidate_processing = normalize_processing(candidate.get("processing", {}))
        if not comp:
            invalid.append(
                {
                    "candidate_index": idx,
                    "predicted_values": {},
                    "confidence": "low",
                    "reasoning": "",
                    "prediction_error": "invalid or missing composition",
                }
            )
            continue
        jobs.append({"composition": comp, "processing": candidate_processing, "candidate_index": idx})
    return jobs, invalid


def _map_predictor_items(batch_items: list[Any], jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    predictions: list[dict[str, Any]] = []
    for item in batch_items:
        if not isinstance(item, dict):
            continue
        idx = item.get("index")
        if not isinstance(idx, int) or idx < 0 or idx >= len(jobs):
            continue
        job = jobs[idx]
        predicted_values = item.get("predicted_values", {})
        if not isinstance(predicted_values, dict):
            predicted_values = {}
        predictions.append(
            {
                "candidate_index": job["candidate_index"],
                "predicted_values": predicted_values,
                "confidence": normalize_confidence(item.get("confidence", "low")),
                "reasoning": str(item.get("reasoning", "") or "").strip(),
                "prediction_error": str(item.get("error", "") or "").strip(),
            }
        )
    seen = {int(p.get("candidate_index")) for p in predictions if isinstance(p.get("candidate_index"), int)}
    for job in jobs:
        cidx = int(job["candidate_index"])
        if cidx in seen:
            continue
        predictions.append(
            {
                "candidate_index": cidx,
                "predicted_values": {},
                "confidence": "low",
                "reasoning": "",
                "prediction_error": "missing prediction result",
            }
        )
    predictions.sort(key=lambda x: int(x.get("candidate_index", 10**9)))
    return predictions


def predict_with_agent(step_input: StepInput) -> StepOutput:
    request = as_workflow_input(step_input.input)
    routed_output = step_input.previous_step_outputs.get("Router Agent")
    rec_output = step_input.previous_step_outputs.get("Recommender Agent")
    routed = dict_or_empty(getattr(routed_output, "content", None))
    recommendation = dict_or_empty(getattr(rec_output, "content", None))
    resolved_material_type = str(routed.get("resolved_material_type", "")).strip().lower()
    effective_top_k = int(request.top_k) if request.top_k is not None else 3

    candidates = [c for c in list_or_empty(recommendation.get("candidates", [])) if isinstance(c, dict)]
    jobs, invalid_candidate_predictions = _build_predict_jobs(candidates)
    if not jobs:
        payload = AgentPredictorOutput(
            recommended_candidates=candidates,
            candidate_predictions=invalid_candidate_predictions,
            prediction_error="no_valid_recommender_candidates",
        ).model_dump()
        return StepOutput(content=payload)

    input_candidates = [{"composition": job["composition"], "processing": job["processing"]} for job in jobs]
    candidate_blocks = format_candidates_for_predictor(input_candidates, max_process_chars=260)
    prompt = (
        "Predict properties for all recommended candidates.\n"
        "You must call predict_material_properties_batch exactly once with the candidates below.\n"
        "The tool arguments must include material_type, goal, candidates, and top_k.\n"
        f"Use material_type={resolved_material_type or 'unknown'}.\n"
        f"Use goal={request.goal}.\n"
        f"Use top_k={effective_top_k}.\n"
        f"Use mounted_workflow_run_ids={json.dumps(request.mounted_workflow_run_ids or [], ensure_ascii=False)}.\n"
        "Return ONLY valid JSON with keys: predictions.\n"
        "Candidates:\n"
        f"{candidate_blocks or 'No candidates.'}"
    )
    batch_result = run_agent_for_json(
        material_predictor_agent,
        step_input=step_input,
        agent_name="predictor",
        prompt=prompt,
    )
    batch_items = list_or_empty(batch_result.get("predictions", []))
    candidate_predictions = _map_predictor_items(batch_items, jobs)
    candidate_predictions.extend(invalid_candidate_predictions)
    candidate_predictions.sort(key=lambda x: int(x.get("candidate_index", 10**9)))
    prediction_error = ""

    workflow_run_id = effective_workflow_run_id(step_input, request)
    current_round = next_round_index(str(workflow_run_id))
    previous_round = max(0, int(current_round) - 1)
    previous_round_context = (
        fetch_round_samples_context(
            workflow_run_id=str(workflow_run_id),
            material_type=resolved_material_type,
            round_index=previous_round,
            limit=12,
        )
        if previous_round > 0 and resolved_material_type
        else []
    )
    candidates, candidate_predictions, goal_filter_error = _filter_predictions_by_goal_improvement(
        goal=request.goal,
        candidates=candidates,
        predictions=candidate_predictions,
        previous_round_context=previous_round_context,
    )
    if goal_filter_error:
        prediction_error = goal_filter_error

    if not [p for p in candidate_predictions if not p.get("prediction_error")]:
        prediction_error = prediction_error or "all_parallel_predictor_calls_failed"

    payload = AgentPredictorOutput(
        recommended_candidates=candidates,
        candidate_predictions=candidate_predictions,
        prediction_error=prediction_error,
    ).model_dump()
    return StepOutput(content=payload)
