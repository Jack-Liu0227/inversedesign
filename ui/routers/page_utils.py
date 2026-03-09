from __future__ import annotations

import json
from typing import Any

from ui.services.json_decode_service import decode_maybe_double_json


def to_record_card(row: dict, columns: list[str], record_key: str, table: str) -> dict:
    preview_priority = {
        "agent_execution_logs": ["prompt_text", "response_text", "response_json"],
        "agent_tool_call_logs": ["tool_args_json", "tool_result_json", "error_text"],
        "workflow_step_logs": ["input_json", "output_json", "error_text"],
        "workflow_io_logs": ["payload_json", "error_text"],
        "workflow_run_audit": ["final_result_json", "summary_json", "error_text"],
        "material_samples": ["predicted_values_json", "judge_reasons_json", "risk_tags_json"],
        "material_doc_knowledge": ["content", "tags_json", "title"],
    }
    preferred = preview_priority.get(str(table or "").strip(), [])
    preview_col = ""
    for col in preferred:
        if col in columns:
            preview_col = col
            break
    if not preview_col:
        text_cols = [c for c in columns if any(x in c.lower() for x in ("summary", "prompt", "response", "payload", "json", "note"))]
        preview_col = text_cols[0] if text_cols else (columns[0] if columns else "")
    preview_raw = str(row.get(preview_col, "")) if preview_col else ""
    preview = humanize_escaped_text(preview_raw).replace("\n", " ")[:180]

    label_col = ""
    for candidate in ("id", "workflow_run_id", "session_id", "trace_id", "created_at", "name"):
        if candidate in row:
            label_col = candidate
            break
    if not label_col and columns:
        label_col = columns[0]
    label = str(row.get(label_col, "")) if label_col else ""

    key_value = str(row.get(record_key, "")) if record_key else ""
    meta = {}
    for k in (
        "created_at",
        "workflow_name",
        "step_name",
        "event_type",
        "status",
        "agent_name",
        "tool_name",
        "success",
        "decision",
        "should_stop",
        "material_type",
        "round_index",
        "candidate_index",
        "source_name",
        "source_kind",
        "chunk_index",
        "chunk_count",
        "is_valid",
        "confidence",
    ):
        if k in row:
            meta[k] = row.get(k)

    return {
        "label": label,
        "label_col": label_col,
        "preview_col": preview_col,
        "preview": preview,
        "key_val": key_value,
        "key_col": record_key,
        "meta": meta,
    }


def collect_structure_stats(value: Any) -> dict[str, float | bool]:
    stats = {
        "leaf_count": 0.0,
        "numeric_count": 0.0,
        "string_count": 0.0,
        "max_text_len": 0.0,
        "metric_like_key": False,
    }

    def walk(v: Any) -> None:
        if isinstance(v, dict):
            for k, subv in v.items():
                key_text = str(k)
                if "(" in key_text and ")" in key_text:
                    stats["metric_like_key"] = True
                walk(subv)
            return
        if isinstance(v, list):
            for subv in v:
                walk(subv)
            return
        stats["leaf_count"] += 1
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            stats["numeric_count"] += 1
            return
        text = str(v or "")
        stats["string_count"] += 1
        stats["max_text_len"] = max(float(len(text)), float(stats["max_text_len"]))

    walk(value)
    leaf_count = float(stats["leaf_count"] or 1.0)
    stats["numeric_ratio"] = float(stats["numeric_count"]) / leaf_count
    stats["string_ratio"] = float(stats["string_count"]) / leaf_count
    return stats


def field_group(key: str, value: Any) -> str:
    lower_key = str(key).lower()
    normalized = decode_nested_json(value)
    if any(
        token in lower_key
        for token in (
            "predicted_values",
            "judge_reasons",
            "risk_tags",
            "valid_candidates",
            "recommended_candidates",
            "stop_evaluation",
            "loop_state",
            "final_result",
            "step_outputs",
        )
    ):
        return "Prediction"
    if isinstance(normalized, (dict, list)):
        stats = collect_structure_stats(normalized)
        numeric_ratio = float(stats.get("numeric_ratio", 0.0) or 0.0)
        string_ratio = float(stats.get("string_ratio", 0.0) or 0.0)
        max_text_len = float(stats.get("max_text_len", 0.0) or 0.0)
        metric_like = bool(stats.get("metric_like_key", False))

        if metric_like or numeric_ratio >= 0.45:
            return "Prediction"
        if string_ratio >= 0.6 and max_text_len >= 600:
            if "input" in lower_key or "args" in lower_key or "prompt" in lower_key:
                return "Prompt Input"
            if "tool" in lower_key or "result" in lower_key:
                return "Tool Output"
            return "LLM Response"

    if "input" in lower_key or "prompt" in lower_key or "args" in lower_key:
        return "Prompt Input"
    if "tool" in lower_key and "result" in lower_key:
        return "Tool Output"
    if "response" in lower_key or "output" in lower_key:
        return "LLM Response"
    return "Metadata"


def render_field_value(value: Any) -> tuple[str, bool, Any]:
    normalized = decode_nested_json(value)
    is_structured = isinstance(normalized, (dict, list))
    if is_structured:
        rendered = humanize_escaped_text(json.dumps(normalized, ensure_ascii=False, indent=2))
    else:
        rendered = humanize_escaped_text(str(normalized))
    return rendered, is_structured, normalized


def field_item(key: str, value: Any) -> dict:
    rendered_pretty, is_structured, normalized = render_field_value(value)
    lower_key = str(key).lower()
    force_scroll = lower_key in {"prompt_text", "llm_response", "input_prompt", "row_json", "response_text"}
    is_scroll_field = force_scroll or len(rendered_pretty) > 900
    return {
        "key": key,
        "is_structured": is_structured,
        "is_scroll_field": is_scroll_field,
        "full": rendered_pretty,
        "normalized": normalized,
    }


def expand_structured_field(key: str, value: Any) -> list[dict]:
    lower_key = str(key).strip().lower()
    # For processing_json we only want the meaningful extracted fields
    # (e.g. processing_json.heat treatment method), not duplicate raw JSON block.
    entries: list[dict] = [] if lower_key == "processing_json" else [field_item(key, value)]
    normalized = decode_nested_json(value)
    if not isinstance(normalized, dict):
        return entries if entries else [field_item(key, value)]

    candidate = normalized.get("output") if "output" in normalized else normalized.get("input")
    root_payload = candidate if isinstance(candidate, dict) else normalized
    root_name = "output" if isinstance(candidate, dict) and "output" in normalized else ("input" if isinstance(candidate, dict) else key)

    for sub_key, sub_val in root_payload.items():
        dotted = f"{root_name}.{sub_key}"
        entries.append(field_item(dotted, sub_val))
        if isinstance(sub_val, dict):
            for sub2_key, sub2_val in sub_val.items():
                entries.append(field_item(f"{dotted}.{sub2_key}", sub2_val))
        elif isinstance(sub_val, list):
            entries.append(field_item(f"{dotted}.__len__", len(sub_val)))
            for idx, item in enumerate(sub_val):
                entries.append(field_item(f"{dotted}[{idx}]", item))
                if isinstance(item, dict):
                    for sub2_key, sub2_val in item.items():
                        entries.append(field_item(f"{dotted}[{idx}].{sub2_key}", sub2_val))
    return entries


def should_auto_expand(value: Any) -> bool:
    normalized = decode_nested_json(value)
    if isinstance(normalized, dict):
        return True
    if isinstance(normalized, list):
        if not normalized:
            return False
        return any(isinstance(item, (dict, list)) for item in normalized)
    return False


def to_detail_sections(row: dict | None) -> list[dict]:
    if not row:
        return []
    grouped: dict[str, list[dict]] = {}
    for key, value in row.items():
        if should_auto_expand(value):
            items = expand_structured_field(str(key), value)
        else:
            items = [field_item(str(key), value)]
        for item in items:
            group = field_group(str(item["key"]), item.get("normalized"))
            item.pop("normalized", None)
            grouped.setdefault(group, []).append(item)
    order = ["Metadata", "Prompt Input", "LLM Response", "Tool Output", "Prediction"]
    sections: list[dict] = []
    for title in order:
        fields = grouped.get(title, [])
        if fields:
            sections.append({"title": title, "fields": fields})
    for title, fields in grouped.items():
        if title in order:
            continue
        sections.append({"title": title, "fields": fields})
    return sections


def is_workflow_filterable_table(db: str, table: str) -> bool:
    return db == "workflow_audit" and table in {
        "workflow_step_logs",
        "agent_tool_call_logs",
        "agent_execution_logs",
        "workflow_io_logs",
        "workflow_run_audit",
    }


def viewer_extra_filters(
    *,
    trace_id: str | None = None,
    session_id: str | None = None,
    workflow_run_id: str | None = None,
    material_type: str | None = None,
    step_name: str | None = None,
    agent_name: str | None = None,
    tool_name: str | None = None,
    status: str | None = None,
    event_type: str | None = None,
    decision: str | None = None,
    should_stop: str | None = None,
    success: str | None = None,
) -> dict[str, str]:
    return {
        "trace_id": str(trace_id or "").strip(),
        "session_id": str(session_id or "").strip(),
        "workflow_run_id": str(workflow_run_id or "").strip(),
        "material_type": str(material_type or "").strip(),
        "step_name": str(step_name or "").strip(),
        "agent_name": str(agent_name or "").strip(),
        "tool_name": str(tool_name or "").strip(),
        "status": str(status or "").strip(),
        "event_type": str(event_type or "").strip(),
        "decision": str(decision or "").strip(),
        "should_stop": str(should_stop or "").strip(),
        "success": str(success or "").strip(),
    }


def decode_nested_json(value, depth: int = 0):
    if depth > 4:
        return value
    if isinstance(value, dict):
        return {k: decode_nested_json(v, depth + 1) for k, v in value.items()}
    if isinstance(value, list):
        return [decode_nested_json(v, depth + 1) for v in value]
    if isinstance(value, str):
        decoded = decode_maybe_double_json(value)
        if decoded is value:
            return value
        return decode_nested_json(decoded, depth + 1)
    return value


def humanize_escaped_text(text: str) -> str:
    if "\\" not in text:
        return text
    return (
        text.replace("\\\\r\\\\n", "\n")
        .replace("\\\\n", "\n")
        .replace("\\\\t", "\t")
        .replace('\\\\\"', '"')
        .replace("\\r\\n", "\n")
        .replace("\\n", "\n")
        .replace("\\t", "\t")
        .replace('\\\"', '"')
    )


def tool_trace_detail_payload(item: dict | None) -> dict[str, str]:
    if not item:
        return {"tool_input": "", "tool_output": "", "error_text": ""}

    tool_input_raw = str(item.get("tool_args_json") or "")
    tool_output_raw = str(item.get("tool_result_json") or "")
    tool_input_decoded = decode_nested_json(tool_input_raw)
    tool_output_decoded = decode_nested_json(tool_output_raw)

    if isinstance(tool_input_decoded, (dict, list)):
        step_name = str(item.get("step_name") or "").strip().lower()
        if "rationality" in step_name and isinstance(tool_input_decoded, dict):
            tool_input = _format_rationality_tool_input(tool_input_decoded)
        else:
            tool_input = json.dumps(tool_input_decoded, ensure_ascii=False, indent=2)
    else:
        tool_input = str(tool_input_decoded)

    if isinstance(tool_output_decoded, (dict, list)):
        tool_output = json.dumps(tool_output_decoded, ensure_ascii=False, indent=2)
    else:
        tool_output = str(tool_output_decoded)

    return {
        "tool_input": humanize_escaped_text(tool_input),
        "tool_output": humanize_escaped_text(tool_output),
        "error_text": str(item.get("error_text") or ""),
    }


def _extract_json_after_prefix(text: str, prefix: str) -> tuple[Any, int, int]:
    start = text.find(prefix)
    if start < 0:
        return None, -1, -1
    idx = start + len(prefix)
    while idx < len(text) and text[idx].isspace():
        idx += 1
    if idx >= len(text) or text[idx] not in "[{":
        return None, -1, -1
    open_ch = text[idx]
    close_ch = "]" if open_ch == "[" else "}"
    depth = 0
    in_str = False
    escape = False
    end = idx
    while end < len(text):
        ch = text[end]
        if in_str:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == "\"":
                in_str = False
        else:
            if ch == "\"":
                in_str = True
            elif ch == open_ch:
                depth += 1
            elif ch == close_ch:
                depth -= 1
                if depth == 0:
                    raw = text[idx : end + 1]
                    try:
                        return json.loads(raw), idx, end + 1
                    except Exception:
                        return None, -1, -1
        end += 1
    return None, -1, -1


def _extract_scalar_line(text: str, key: str) -> str:
    marker = f"{key}="
    pos = text.find(marker)
    if pos < 0:
        return ""
    start = pos + len(marker)
    end = text.find("\n", start)
    if end < 0:
        end = len(text)
    return str(text[start:end]).strip()


def _format_rationality_prompt(prompt: str) -> str:
    raw = str(prompt or "").strip()
    if not raw:
        return ""
    goal = _extract_scalar_line(raw, "goal")
    material_type = _extract_scalar_line(raw, "material_type")
    candidates, c_start, c_end = _extract_json_after_prefix(raw, "candidates=")
    preds, p_start, p_end = _extract_json_after_prefix(raw, "candidate_predictions=")
    if preds is None:
        preds, p_start, p_end = _extract_json_after_prefix(raw, "predictions=")

    cut_positions = [x for x in [c_start, p_start] if x >= 0]
    instruction = raw[: min(cut_positions)] if cut_positions else raw
    parts: list[str] = []
    if instruction.strip():
        parts.append("## Instructions\n" + instruction.strip())
    if goal:
        parts.append("## Goal\n" + goal)
    if material_type:
        parts.append("## Material Type\n" + material_type)
    if candidates is not None:
        count = len(candidates) if isinstance(candidates, list) else 1
        parts.append(f"## Candidates ({count})\n" + json.dumps(candidates, ensure_ascii=False, indent=2))
    if preds is not None:
        count = len(preds) if isinstance(preds, list) else 1
        parts.append(f"## Candidate Predictions ({count})\n" + json.dumps(preds, ensure_ascii=False, indent=2))

    trailing_start = max(c_end, p_end)
    if trailing_start > 0:
        trailing = raw[trailing_start:].strip()
        if trailing:
            parts.append("## Extra\n" + trailing)
    return "\n\n".join([p for p in parts if p]).strip()


def _format_rationality_tool_input(payload: dict[str, Any]) -> str:
    prompt = str(payload.get("prompt") or "").strip()
    formatted_prompt = _format_rationality_prompt(prompt)
    out = dict(payload)
    if formatted_prompt:
        out["prompt"] = formatted_prompt
    return json.dumps(out, ensure_ascii=False, indent=2)


def parse_success_query(value: str | None) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    if text in {"0", "1"}:
        return int(text)
    return None
