from __future__ import annotations

import json
from collections import Counter
from typing import Any

from agno.workflow.types import StepInput, StepOutput

from src.agents.material_predictor_agent import material_predictor_agent
from src.agents.material_recommender_agent import material_recommender_agent
from src.agents.material_router_agent import material_router_agent
from src.common import (
    fetch_material_doc_context,
    fetch_round_samples_context,
    fetch_valid_samples_context,
    next_round_index,
)
from src.schemas import AgentPredictorOutput, AgentRecommenderOutput, AgentRouterOutput

from .agent_runtime import run_agent_for_json
from .common import as_workflow_input, audit_event, run_id_from_step_input, trace_id
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
    if not resolved_material_type:
        raise ValueError("Missing resolved_material_type from Router Agent step")

    workflow_run_id = run_id_from_step_input(step_input) or trace_id(step_input, request)
    current_round = next_round_index(str(workflow_run_id))
    previous_round = max(0, int(current_round) - 1)
    valid_context = fetch_valid_samples_context(material_type=resolved_material_type, limit=12)
    doc_context = fetch_material_doc_context(
        material_type=resolved_material_type,
        limit=8,
        workflow_run_id=str(workflow_run_id),
        before_round_index=int(current_round),
    )
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
    rag_context = json.dumps(valid_context, ensure_ascii=False)
    doc_context_json = json.dumps(doc_context, ensure_ascii=False)
    prev_round_context_json = json.dumps(previous_round_context, ensure_ascii=False)
    previous_round_feedback_json = json.dumps(previous_round_feedback, ensure_ascii=False)
    prompt = (
        "Recommend candidate alloys.\n"
        "Return ONLY valid JSON with keys: candidates.\n"
        "Each candidate must include composition, processing, score, reason, expected_tradeoff.\n"
        "processing must contain exactly one key: 'heat treatment method'.\n"
        "The value of 'heat treatment method' must be one complete process route sentence.\n"
        "Do not output thermo_mechanical or other processing sub-keys.\n"
        f"goal={request.goal}\n"
        f"material_type={resolved_material_type}\n"
        f"current_round={current_round}\n"
        f"previous_round={previous_round}\n"
        "When previous_round_context is not empty, use it as hard feedback from previous iteration outcomes.\n"
        "You must consider previous round composition, processing, and predicted_values before proposing new candidates.\n"
        "When valid retrieval samples are empty, use bootstrap_doc_context as first-round domain knowledge.\n"
        "Use previous_round_feedback_summary to avoid repeated failure modes and preserve high-score valid patterns.\n"
        "Prioritize avoiding top_risk_tags_to_avoid and top_invalid_reasons from the previous round.\n"
        f"top_n={top_n}\n"
        f"rag_context={rag_context}\n"
        f"bootstrap_doc_context={doc_context_json}\n"
        f"previous_round_context={prev_round_context_json}\n"
        f"previous_round_feedback_summary={previous_round_feedback_json}\n"
        f"preference_feedback={request.preference_feedback or ''}"
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
    request = as_workflow_input(step_input.input)
    routed_output = step_input.previous_step_outputs.get("Router Agent")
    rec_output = step_input.previous_step_outputs.get("Recommender Agent")
    routed = dict_or_empty(getattr(routed_output, "content", None))
    recommendation = dict_or_empty(getattr(rec_output, "content", None))

    resolved_material_type = str(routed.get("resolved_material_type", "")).strip().lower()
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
    prompt = (
        "Predict properties for all recommended candidates.\n"
        "First call predict_material_properties_batch with candidates.\n"
        "Return ONLY valid JSON with keys: predictions.\n"
        f"material_type={resolved_material_type}\n"
        f"goal={request.goal}\n"
        f"top_k={request.top_k or 3}\n"
        f"max_workers={min(3, max(1, len(input_candidates)))}\n"
        f"candidates={json.dumps(input_candidates, ensure_ascii=False)}"
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
