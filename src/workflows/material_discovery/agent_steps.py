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
from .common import as_workflow_input, audit_event, effective_workflow_run_id
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
    prev_round_context_block = format_previous_round_context(previous_round_context, max_items=6, max_process_chars=220)
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
    prompt = (
        "Recommend candidate alloys.\n"
        "Return ONLY valid JSON with keys: candidates.\n"
        "Each candidate must include composition, processing, score, reason, expected_tradeoff.\n"
        f"Generate exactly {top_n} candidates.\n"
        "Do not ask clarifying questions and do not request additional user input.\n"
        "If any field appears ambiguous or partially truncated, infer conservatively from Design objective and proceed.\n"
        "processing must contain exactly one key: 'heat treatment method'.\n"
        "The value of 'heat treatment method' must be one complete process route sentence.\n"
        "Do not output thermo_mechanical or other processing sub-keys.\n"
        f"Material family: {resolved_material_type}\n"
        f"{objective_line}\n"
        f"Canonical objective: {canonical_objective}\n"
        "When previous_round_context is not empty, use it as hard feedback from previous iteration outcomes.\n"
        "You must consider previous round composition, processing, and predicted_values before proposing new candidates.\n"
        "Use retrieved context below as the only retrieval context source beyond previous round feedback.\n"
        "Use previous_round_feedback_summary to avoid repeated failure modes and preserve high-score valid patterns.\n"
        "Prioritize avoiding top_risk_tags_to_avoid and top_invalid_reasons from the previous round.\n"
        f"Preference feedback: {request.preference_feedback or 'None'}\n\n"
        "Retrieved context:\n"
        f"{retrieved_context_block or 'No retrieved context.'}\n\n"
        "Previous round context:\n"
        f"{prev_round_context_block}\n\n"
        "Previous round feedback summary:\n"
        f"{previous_round_feedback_block}"
    )
    rec = run_agent_for_json(
        material_recommender_agent,
        step_input=step_input,
        agent_name="recommender",
        prompt=prompt,
    )
    output = AgentRecommenderOutput(
        candidates=normalize_recommender_candidates(extract_candidate_list(rec), material_type=resolved_material_type),
    ).model_dump()
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
    rec_output = step_input.previous_step_outputs.get("Recommender Agent")
    recommendation = dict_or_empty(getattr(rec_output, "content", None))

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
        "You must call predict_material_properties_batch with the candidates below.\n"
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
    if not [p for p in candidate_predictions if not p.get("prediction_error")]:
        prediction_error = "all_parallel_predictor_calls_failed"

    payload = AgentPredictorOutput(
        recommended_candidates=candidates,
        candidate_predictions=candidate_predictions,
        prediction_error=prediction_error,
    ).model_dump()
    return StepOutput(content=payload)
