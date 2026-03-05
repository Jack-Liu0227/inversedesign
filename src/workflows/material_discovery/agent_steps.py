from __future__ import annotations

import json
from typing import Any

from agno.workflow.types import StepInput, StepOutput

from src.agents.material_predictor_agent import material_predictor_agent
from src.agents.material_recommender_agent import material_recommender_agent
from src.agents.material_router_agent import material_router_agent, parse_goal_properties, parse_goal_targets
from src.common import fetch_round_samples_context, fetch_valid_samples_context, next_round_index
from src.fewshot import resolve_material_type_input
from src.schemas import AgentPredictorOutput, AgentRecommenderOutput, AgentRouterOutput

from .agent_runtime import run_agent_for_json
from .common import as_workflow_input, run_id_from_step_input, trace_id
from .material_normalization import (
    dict_or_empty,
    extract_candidate_list,
    list_or_empty,
    normalize_composition,
    normalize_confidence,
    normalize_processing,
    normalize_recommender_candidates,
)


def route_with_agent(step_input: StepInput) -> StepOutput:
    request = as_workflow_input(step_input.input)
    prompt = (
        "Resolve material type and parse optimization targets from goal.\n"
        "Return ONLY valid JSON with keys: resolved_material_type, resolution_reason, resolved_properties, target_thresholds.\n"
        f"goal={request.goal}"
    )
    _ = run_agent_for_json(material_router_agent, step_input=step_input, agent_name="router", prompt=prompt)
    local_resolved, local_reason = resolve_material_type_input(goal=request.goal, material_type="")
    local_properties = parse_goal_properties(request.goal)
    local_thresholds = parse_goal_targets(request.goal)
    output = AgentRouterOutput(
        resolved_material_type=local_resolved.strip().lower(),
        resolution_reason=f"local_policy_{local_reason}",
        resolved_properties=local_properties,
        target_thresholds=local_thresholds,
    ).model_dump()
    return StepOutput(content=output)


def recommend_with_agent(step_input: StepInput) -> StepOutput:
    request = as_workflow_input(step_input.input)
    routed_output = step_input.previous_step_outputs.get("Router Agent")
    routed = dict_or_empty(getattr(routed_output, "content", None))
    resolved_material_type = str(routed.get("resolved_material_type", "")).strip().lower()
    if not resolved_material_type:
        raise ValueError("Missing resolved_material_type from Router Agent step")

    # Keep recommender candidate count stable across rounds.
    # `top_k` in request is reserved for predictor retrieval, not candidate count.
    top_n = 3
    workflow_run_id = run_id_from_step_input(step_input) or trace_id(step_input, request)
    current_round = next_round_index(str(workflow_run_id))
    previous_round = max(0, int(current_round) - 1)
    valid_context = fetch_valid_samples_context(material_type=resolved_material_type, limit=12)
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
    rag_context = json.dumps(valid_context, ensure_ascii=False)
    prev_round_context_json = json.dumps(previous_round_context, ensure_ascii=False)
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
        f"top_n={top_n}\n"
        f"rag_context={rag_context}\n"
        f"previous_round_context={prev_round_context_json}\n"
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
