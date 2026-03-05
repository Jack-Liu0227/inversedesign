from __future__ import annotations

from collections import Counter
from typing import Any

from src.schemas import CandidateWithPrediction, MaterialDiscoveryResponse
from src.workflows.material_discovery.state import MaterialDiscoveryState

from .material_normalization import dict_or_empty


def _to_candidate_rows(
    recommended_candidates: list[dict[str, Any]],
    candidate_predictions: list[dict[str, Any]],
    rationality: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    prediction_by_index = {
        int(item["candidate_index"]): item
        for item in candidate_predictions
        if isinstance(item, dict) and isinstance(item.get("candidate_index"), int)
    }
    rationality_by_index = {
        int(item["candidate_index"]): item
        for item in rationality
        if isinstance(item, dict) and isinstance(item.get("candidate_index"), int)
    }

    rows: list[dict[str, Any]] = []
    for idx, candidate in enumerate(recommended_candidates):
        if not isinstance(candidate, dict):
            continue
        pred = prediction_by_index.get(idx, {})
        judge = rationality_by_index.get(idx, {})
        prediction = {
            "predicted_values": dict_or_empty(pred.get("predicted_values", {})),
            "confidence": str(pred.get("confidence", "low") or "low"),
            "reasoning": str(pred.get("reasoning", "") or "").strip(),
        }
        rows.append(
            CandidateWithPrediction(
                candidate_index=idx,
                composition=dict_or_empty(candidate.get("composition", {})),
                processing=dict_or_empty(candidate.get("processing", {})),
                score=float(candidate.get("score", 0.0) or 0.0),
                reason=str(candidate.get("reason", "") or "").strip(),
                expected_tradeoff=str(candidate.get("expected_tradeoff", "") or "").strip(),
                prediction=prediction,
                prediction_error=str(pred.get("prediction_error", "") or "").strip(),
                is_valid=bool(judge.get("is_valid", False)),
                validity_score=float(judge.get("validity_score", 0.0) or 0.0),
                judge_reasons=[str(x) for x in (judge.get("reasons", []) or []) if str(x).strip()],
                risk_tags=[str(x) for x in (judge.get("risk_tags", []) or []) if str(x).strip()],
                recommended_action=str(judge.get("recommended_action", "drop") or "drop"),
            ).model_dump()
        )
    return rows


def valid_candidates_only(
    recommended_candidates: list[dict[str, Any]],
    candidate_predictions: list[dict[str, Any]],
    rationality: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows = _to_candidate_rows(recommended_candidates, candidate_predictions, rationality)
    return [row for row in rows if bool(row.get("is_valid", False))]


def _summary_lines(state: MaterialDiscoveryState, payload: dict[str, Any]) -> list[str]:
    summary = [
        f"mode={state.mode}, round={state.round_index}",
        f"decision={payload.get('decision', '')}",
        f"valid_candidates={len(payload.get('valid_candidates', []))}/{len(payload.get('recommended_candidates', []))}",
        f"stop_reason={dict_or_empty(payload.get('stop_evaluation', {})).get('reason', '')}",
    ]
    judge_summary = dict_or_empty(payload.get("judge_summary", {}))
    top_reasons = [str(x) for x in (judge_summary.get("top_reasons", []) or []) if str(x).strip()]
    if top_reasons:
        summary.append(f"judge_top_reasons={top_reasons[:3]}")
    return summary


def _judge_summary_from_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    reason_counter: Counter[str] = Counter()
    valid_count = 0
    for row in rows:
        if bool(row.get("is_valid", False)):
            valid_count += 1
        for reason in row.get("judge_reasons", []):
            reason_text = str(reason).strip()
            if reason_text:
                reason_counter[reason_text] += 1
    total = len(rows)
    return {
        "total": total,
        "valid_count": valid_count,
        "invalid_count": max(0, total - valid_count),
        "top_reasons": [x[0] for x in reason_counter.most_common(5)],
    }


def build_response(
    *,
    state: MaterialDiscoveryState,
    decision: str,
    max_iterations: int,
    recommended_candidates: list[dict[str, Any]],
    candidate_predictions: list[dict[str, Any]],
    rationality: list[dict[str, Any]],
    stop_evaluation: dict[str, Any],
    judge_summary: dict[str, Any] | None = None,
    debug_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    rows = _to_candidate_rows(recommended_candidates, candidate_predictions, rationality)
    valid_rows = [row for row in rows if bool(row.get("is_valid", False))]
    if not judge_summary:
        judge_summary = _judge_summary_from_rows(rows)
    payload = MaterialDiscoveryResponse(
        decision=decision,
        recommended_candidates=rows,
        valid_candidates=valid_rows,
        judge_summary=judge_summary,
        stop_evaluation=stop_evaluation,
        loop_state={
            "mode": state.mode,
            "round_index": int(state.round_index),
            "max_iterations": int(max_iterations),
            "remaining_rounds": max(0, int(max_iterations) - int(state.round_index)),
            "reached_max_iterations": bool(int(state.round_index) >= int(max_iterations)),
            "requires_human_feedback": bool(state.requires_human_feedback),
        },
        debug=debug_payload,
    ).model_dump(exclude_none=True)
    payload["summary"] = _summary_lines(state, payload)
    return payload
