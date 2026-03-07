from __future__ import annotations

import json
import re
from typing import Any


def _norm_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _truncate_line(value: Any, max_chars: int) -> str:
    text = _norm_text(value)
    if len(text) <= max(1, int(max_chars)):
        return text
    return f"{text[: max(1, int(max_chars)) - 3].rstrip()}..."


def _unit_from_metric(metric: str) -> str:
    text = str(metric or "")
    start = text.rfind("(")
    end = text.rfind(")")
    if start >= 0 and end > start:
        return text[start + 1 : end].strip()
    return ""


def _format_metric_value(metric: str, value: Any) -> str:
    try:
        number = float(value)
        if number.is_integer():
            display = str(int(number))
        else:
            display = f"{number:.4g}"
    except Exception:
        display = _norm_text(value)
    unit = _unit_from_metric(metric)
    if unit and display:
        return f"{display} {unit}".strip()
    return display


def _format_composition_line(composition: dict[str, Any]) -> str:
    items: list[str] = []
    for key in sorted(composition.keys()):
        value = composition.get(key)
        if value is None:
            continue
        try:
            val = float(value)
            text = str(int(val)) if val.is_integer() else f"{val:.6g}"
        except Exception:
            text = _norm_text(value)
        if text:
            items.append(f"{key} {text}")
    return ", ".join(items) if items else "N/A"


def dedupe_doc_segments(segments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, str]] = set()
    out: list[dict[str, Any]] = []
    for seg in segments:
        if not isinstance(seg, dict):
            continue
        source_name = _norm_text(seg.get("source_name", ""))
        title = _norm_text(seg.get("title", ""))
        content = _norm_text(seg.get("content", ""))
        if not content:
            continue
        key = (source_name, title, content)
        if key in seen:
            continue
        seen.add(key)
        out.append(seg)
    return out


def format_retrieved_context_blocks(
    segments: list[dict[str, Any]],
    *,
    max_items: int = 8,
    max_content_chars: int = 320,
) -> str:
    deduped = dedupe_doc_segments(segments)
    lines: list[str] = []
    for idx, seg in enumerate(deduped[: max(1, int(max_items))], start=1):
        lines.extend(
            [
                f"Retrieved context #{idx}",
                f"Source: {_norm_text(seg.get('source_name', 'unknown'))}",
                f"Title: {_norm_text(seg.get('title', '')) or 'N/A'}",
                f"Key facts: {_truncate_line(seg.get('content', ''), max_content_chars)}",
            ]
        )
    return "\n".join(lines).strip()


def format_valid_sample_blocks(
    samples: list[dict[str, Any]],
    *,
    max_items: int = 5,
    max_process_chars: int = 220,
) -> str:
    lines: list[str] = []
    for idx, row in enumerate(samples[: max(1, int(max_items))], start=1):
        composition = row.get("composition", {}) if isinstance(row.get("composition"), dict) else {}
        processing = row.get("processing", {}) if isinstance(row.get("processing"), dict) else {}
        predicted = row.get("predicted_values", {}) if isinstance(row.get("predicted_values"), dict) else {}
        method = _truncate_line(processing.get("heat treatment method", ""), max_process_chars) or "N/A"
        pred_pairs = [
            f"{k}: {_format_metric_value(str(k), predicted.get(k))}"
            for k in sorted(predicted.keys())
            if predicted.get(k) is not None
        ]
        lines.extend(
            [
                f"Valid sample #{idx}",
                f"Judge score: {float(row.get('judge_score', 0.0) or 0.0):.3f}",
                f"Composition: {_format_composition_line(composition)}",
                f"Heat Treatment Method: {method}",
                f"Predicted values: {', '.join(pred_pairs) if pred_pairs else 'N/A'}",
            ]
        )
    return "\n".join(lines).strip()


def format_previous_round_context(
    context_rows: list[dict[str, Any]],
    *,
    max_items: int = 6,
    max_process_chars: int = 220,
) -> str:
    if not context_rows:
        return "No previous round context."
    lines: list[str] = []
    for row in context_rows[: max(1, int(max_items))]:
        candidate_index = row.get("candidate_index", "N/A")
        composition = row.get("composition", {}) if isinstance(row.get("composition"), dict) else {}
        processing = row.get("processing", {}) if isinstance(row.get("processing"), dict) else {}
        predicted = row.get("predicted_values", {}) if isinstance(row.get("predicted_values"), dict) else {}
        reasons = row.get("judge_reasons", []) if isinstance(row.get("judge_reasons"), list) else []
        lines.extend(
            [
                f"Candidate index: {candidate_index}",
                f"Is valid: {bool(row.get('is_valid', False))}",
                f"Judge score: {float(row.get('judge_score', 0.0) or 0.0):.3f}",
                f"Composition: {_format_composition_line(composition)}",
                f"Heat Treatment Method: {_truncate_line(processing.get('heat treatment method', ''), max_process_chars) or 'N/A'}",
                "Predicted values: "
                + (
                    ", ".join(
                        [f"{k}: {_format_metric_value(str(k), predicted.get(k))}" for k in sorted(predicted.keys())]
                    )
                    if predicted
                    else "N/A"
                ),
                f"Recommended action: {_norm_text(row.get('recommended_action', '')) or 'N/A'}",
                f"Reasons: {', '.join([_norm_text(x) for x in reasons if _norm_text(x)]) or 'N/A'}",
            ]
        )
    return "\n".join(lines).strip()


def format_feedback_summary(summary: dict[str, Any]) -> str:
    if not isinstance(summary, dict):
        return "No previous round feedback summary."
    action_counts = summary.get("recommended_action_counts", {})
    if isinstance(action_counts, dict):
        action_text = ", ".join([f"{k}: {int(v)}" for k, v in sorted(action_counts.items(), key=lambda kv: str(kv[0]))])
    else:
        action_text = "N/A"
    top_invalid = summary.get("top_invalid_reasons", [])
    top_risks = summary.get("top_risk_tags_to_avoid", [])
    lines = [
        f"Total candidates: {int(summary.get('total_candidates', 0) or 0)}",
        f"Valid count: {int(summary.get('valid_count', 0) or 0)}",
        f"Invalid count: {int(summary.get('invalid_count', 0) or 0)}",
        f"Recommended action counts: {action_text or 'N/A'}",
        f"Top invalid reasons: {', '.join([_norm_text(x) for x in top_invalid if _norm_text(x)]) or 'N/A'}",
        f"Top risk tags to avoid: {', '.join([_norm_text(x) for x in top_risks if _norm_text(x)]) or 'N/A'}",
    ]
    return "\n".join(lines)


def format_candidates_for_predictor(candidates: list[dict[str, Any]], *, max_process_chars: int = 260) -> str:
    lines: list[str] = []
    for idx, cand in enumerate(candidates):
        composition = cand.get("composition", {}) if isinstance(cand.get("composition"), dict) else {}
        processing = cand.get("processing", {}) if isinstance(cand.get("processing"), dict) else {}
        lines.extend(
            [
                f"Candidate index: {idx}",
                f"Composition: {_format_composition_line(composition)}",
                f"Heat Treatment Method: {_truncate_line(processing.get('heat treatment method', ''), max_process_chars) or 'N/A'}",
            ]
        )
    return "\n".join(lines).strip()


def format_rationality_pairs(
    candidates: list[dict[str, Any]],
    predictions: list[dict[str, Any]],
    *,
    max_process_chars: int = 260,
) -> str:
    pred_map = {}
    for item in predictions:
        if not isinstance(item, dict):
            continue
        idx = item.get("candidate_index")
        if isinstance(idx, int):
            pred_map[idx] = item
    lines: list[str] = []
    for cand in candidates:
        if not isinstance(cand, dict):
            continue
        idx = cand.get("candidate_index")
        if not isinstance(idx, int):
            continue
        composition = cand.get("composition", {}) if isinstance(cand.get("composition"), dict) else {}
        processing = cand.get("processing", {}) if isinstance(cand.get("processing"), dict) else {}
        pred = pred_map.get(idx, {})
        predicted = pred.get("predicted_values", {}) if isinstance(pred.get("predicted_values"), dict) else {}
        pred_lines = [f"- {k}: {_format_metric_value(str(k), predicted.get(k))}" for k in sorted(predicted.keys())]
        lines.extend(
            [
                f"Candidate index: {idx}",
                f"Composition: {_format_composition_line(composition)}",
                f"Heat Treatment Method: {_truncate_line(processing.get('heat treatment method', ''), max_process_chars) or 'N/A'}",
                "Predicted values:",
                *(pred_lines if pred_lines else ["- N/A"]),
                f"Confidence: {_norm_text(pred.get('confidence', '')) or 'N/A'}",
                f"Error: {_norm_text(pred.get('error', '')) or 'N/A'}",
            ]
        )
    return "\n".join(lines).strip()


def format_theory_retrieved_segments(
    segments: list[dict[str, Any]],
    *,
    max_items: int = 8,
    max_snippet_chars: int = 300,
) -> str:
    deduped = dedupe_doc_segments(segments)
    if not deduped:
        return "No retrieved evidence."
    lines: list[str] = []
    for idx, seg in enumerate(deduped[: max(1, int(max_items))], start=1):
        lines.extend(
            [
                f"Retrieved context #{idx}",
                f"Source: {_norm_text(seg.get('source_name', 'unknown'))}",
                f"Title: {_norm_text(seg.get('title', '')) or 'N/A'}",
                f"Evidence snippet: {_truncate_line(seg.get('content', ''), max_snippet_chars)}",
            ]
        )
    return "\n".join(lines)


def safe_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False)
