from __future__ import annotations

import json
from collections import Counter
from typing import Any

from agno.workflow.types import StepInput, StepOutput

from src.agents.material_rationality_agent import material_rationality_agent
from src.common import DatasetMaterialRow, MaterialSampleRow, insert_dataset_rows, insert_sample_rows, next_round_index
from src.schemas import AgentRationalityOutput

from .agent_runtime import run_agent_for_json
from .common import as_workflow_input, run_id_from_step_input, session_id_from_step_input, trace_id
from .material_normalization import dict_or_empty, list_or_empty, normalize_composition, normalize_processing

_ROUND_BY_RUN_ID: dict[str, int] = {}


def _normalize_eval_item(item: dict[str, Any]) -> dict[str, Any]:
    validity_score = item.get("validity_score", 0.0)
    try:
        validity_score = float(validity_score)
    except (TypeError, ValueError):
        validity_score = 0.0
    validity_score = min(1.0, max(0.0, validity_score))
    reasons = [str(x).strip() for x in list_or_empty(item.get("reasons", [])) if str(x).strip()]
    risk_tags = [str(x).strip() for x in list_or_empty(item.get("risk_tags", [])) if str(x).strip()]
    action = str(item.get("recommended_action", "drop")).strip().lower()
    if action not in {"keep", "revise", "drop"}:
        action = "drop"
    cleaned = dict_or_empty(item.get("cleaned_candidate", {}))
    if cleaned:
        cleaned["composition"] = normalize_composition(cleaned.get("composition", {}))
        cleaned["processing"] = normalize_processing(cleaned.get("processing", {}))
    return {
        "candidate_index": int(item.get("candidate_index", -1)),
        "is_valid": bool(item.get("is_valid", False)),
        "validity_score": validity_score,
        "reasons": reasons,
        "risk_tags": risk_tags,
        "recommended_action": action,
        "cleaned_candidate": cleaned or None,
    }


def judge_with_agent(step_input: StepInput) -> StepOutput:
    request = as_workflow_input(step_input.input)
    routed_output = step_input.previous_step_outputs.get("Router Agent")
    predictor_output = step_input.previous_step_outputs.get("Predictor Agent")

    routed = dict_or_empty(getattr(routed_output, "content", None))
    predictor = dict_or_empty(getattr(predictor_output, "content", None))

    material_type = str(routed.get("resolved_material_type", "")).strip().lower()
    candidates = [c for c in list_or_empty(predictor.get("recommended_candidates", [])) if isinstance(c, dict)]
    predictions = [p for p in list_or_empty(predictor.get("candidate_predictions", [])) if isinstance(p, dict)]

    prompt = (
        "Judge rationality of candidates and predictions.\n"
        "Return ONLY JSON with key evaluations.\n"
        "Each evaluation item must include: candidate_index, is_valid, validity_score, reasons, risk_tags, "
        "recommended_action, cleaned_candidate.\n"
        f"goal={request.goal}\n"
        f"material_type={material_type}\n"
        f"candidates={json.dumps(candidates, ensure_ascii=False)}\n"
        f"candidate_predictions={json.dumps(predictions, ensure_ascii=False)}"
    )
    raw = run_agent_for_json(
        material_rationality_agent,
        step_input=step_input,
        agent_name="rationality",
        prompt=prompt,
    )
    normalized = []
    for item in list_or_empty(raw.get("evaluations", [])):
        if not isinstance(item, dict):
            continue
        try:
            normalized.append(_normalize_eval_item(item))
        except Exception:
            continue

    output = AgentRationalityOutput(
        evaluations=normalized,
    ).model_dump()
    return StepOutput(content=output)


def persist_candidates(step_input: StepInput) -> StepOutput:
    request = as_workflow_input(step_input.input)
    routed_output = step_input.previous_step_outputs.get("Router Agent")
    predictor_output = step_input.previous_step_outputs.get("Predictor Agent")
    judge_output = step_input.previous_step_outputs.get("Rationality Judge")

    routed = dict_or_empty(getattr(routed_output, "content", None))
    predictor = dict_or_empty(getattr(predictor_output, "content", None))
    judge = dict_or_empty(getattr(judge_output, "content", None))

    material_type = str(routed.get("resolved_material_type", "")).strip().lower()
    candidates = [c for c in list_or_empty(predictor.get("recommended_candidates", [])) if isinstance(c, dict)]
    predictions = [p for p in list_or_empty(predictor.get("candidate_predictions", [])) if isinstance(p, dict)]
    evaluations = [e for e in list_or_empty(judge.get("evaluations", [])) if isinstance(e, dict)]

    prediction_map = {int(p["candidate_index"]): p for p in predictions if isinstance(p.get("candidate_index"), int)}
    judge_map = {int(e["candidate_index"]): e for e in evaluations if isinstance(e.get("candidate_index"), int)}

    trace = trace_id(step_input, request)
    workflow_run_id = run_id_from_step_input(step_input) or trace
    session_id = session_id_from_step_input(step_input) or trace
    db_round = next_round_index(str(workflow_run_id))
    in_memory_round = _ROUND_BY_RUN_ID.get(str(workflow_run_id), 0) + 1
    round_index = max(db_round, in_memory_round)
    _ROUND_BY_RUN_ID[str(workflow_run_id)] = round_index
    rows: list[MaterialSampleRow] = []
    dataset_rows: list[DatasetMaterialRow] = []
    for idx, candidate in enumerate(candidates):
        pred = prediction_map.get(idx, {})
        judge_item = judge_map.get(idx, {})
        cleaned = dict_or_empty(judge_item.get("cleaned_candidate", {}))
        source_candidate = cleaned if cleaned else candidate
        rows.append(
            MaterialSampleRow(
                workflow_run_id=str(workflow_run_id),
                session_id=str(session_id),
                material_type=material_type,
                goal=request.goal,
                round_index=round_index,
                candidate_index=idx,
                composition=normalize_composition(source_candidate.get("composition", {})),
                processing=normalize_processing(source_candidate.get("processing", {})),
                predicted_values=dict_or_empty(pred.get("predicted_values", {})),
                confidence=str(pred.get("confidence", "low") or "low"),
                prediction_error=str(pred.get("prediction_error", "") or ""),
                is_valid=bool(judge_item.get("is_valid", False)),
                judge_score=float(judge_item.get("validity_score", 0.0) or 0.0),
                judge_reasons=[str(x) for x in list_or_empty(judge_item.get("reasons", [])) if str(x).strip()],
                risk_tags=[str(x) for x in list_or_empty(judge_item.get("risk_tags", [])) if str(x).strip()],
                judge_model="material_rationality_agent",
            )
        )
        dataset_rows.append(
            DatasetMaterialRow(
                material_type=material_type,
                source="workflow",
                source_name=str(workflow_run_id),
                source_row_key=f"{round_index}:{idx}",
                composition=normalize_composition(source_candidate.get("composition", {})),
                processing=normalize_processing(source_candidate.get("processing", {})),
                features={},
                target_values={},
                predicted_values=dict_or_empty(pred.get("predicted_values", {})),
                is_valid=bool(judge_item.get("is_valid", False)),
                judge_score=float(judge_item.get("validity_score", 0.0) or 0.0),
                judge_reasons=[str(x) for x in list_or_empty(judge_item.get("reasons", [])) if str(x).strip()],
                risk_tags=[str(x) for x in list_or_empty(judge_item.get("risk_tags", [])) if str(x).strip()],
                iteration=int(round_index),
                workflow_run_id=str(workflow_run_id),
                session_id=str(session_id),
            )
        )

    inserted = insert_sample_rows(rows)
    inserted_dataset = insert_dataset_rows(dataset_rows)
    reason_counter: Counter[str] = Counter()
    valid_count = 0
    for item in evaluations:
        if bool(item.get("is_valid", False)):
            valid_count += 1
        for reason in list_or_empty(item.get("reasons", [])):
            reason_text = str(reason).strip()
            if reason_text:
                reason_counter[reason_text] += 1

    return StepOutput(
        content={
            "round_index": round_index,
            "inserted_count": inserted,
            "inserted_dataset_count": inserted_dataset,
            "total_candidates": len(candidates),
            "valid_count": valid_count,
            "invalid_count": max(0, len(candidates) - valid_count),
            "top_reasons": [x[0] for x in reason_counter.most_common(5)],
        }
    )
