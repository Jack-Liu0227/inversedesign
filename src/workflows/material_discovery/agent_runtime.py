from __future__ import annotations

import ast
import json
import time
from typing import Any, Dict

from agno.workflow.types import StepInput

from src.common import log_agent_execution, log_agent_tool_call, log_prompt_llm_response

_AGENT_SOURCE_BY_NAME: Dict[str, str] = {
    "router": "src/agents/material_router_agent.py",
    "recommender": "src/agents/material_recommender_agent.py",
    "predictor": "src/agents/material_predictor_agent.py",
    "rationality": "src/agents/material_rationality_agent.py",
}

_AGENT_STEP_NAME_BY_AGENT: Dict[str, str] = {
    "router": "Router Agent",
    "recommender": "Recommender Agent",
    "predictor": "Predictor Agent",
    "rationality": "Rationality Judge",
}


def _try_parse_json_dict(text: str) -> Dict[str, Any]:
    try:
        parsed = json.loads(text)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _extract_json_object(value: Any) -> Dict[str, Any]:
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


def _extract_structured_payload(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        pass
    try:
        literal = ast.literal_eval(text)
        if isinstance(literal, (dict, list)):
            return literal
    except Exception:
        pass
    return None


def _normalize_tool_exec_item(item: Any) -> Dict[str, Any]:
    if hasattr(item, "to_dict"):
        try:
            payload = item.to_dict()
            if isinstance(payload, dict):
                return payload
        except Exception:
            pass
    if isinstance(item, dict):
        return item
    return {}


def _extract_tool_executions(response: Any) -> list[Dict[str, Any]]:
    candidates: list[Any] = []
    direct_tools = getattr(response, "tools", None)
    if isinstance(direct_tools, list):
        candidates.extend(direct_tools)
    alt_tool_calls = getattr(response, "tool_calls", None)
    if isinstance(alt_tool_calls, list):
        candidates.extend(alt_tool_calls)

    messages = getattr(response, "messages", None)
    if isinstance(messages, list):
        for msg in messages:
            msg_dict = _normalize_tool_exec_item(msg)
            if not msg_dict:
                continue
            if any(k in msg_dict for k in ("tool_name", "tool_args", "result", "tool_call_error")):
                candidates.append(msg_dict)
            msg_tool_calls = msg_dict.get("tool_calls")
            if isinstance(msg_tool_calls, list):
                candidates.extend(msg_tool_calls)

    best_by_key: Dict[str, Dict[str, Any]] = {}
    for item in candidates:
        item_dict = _normalize_tool_exec_item(item)
        if not item_dict:
            continue
        tool_name = str(item_dict.get("tool_name") or "").strip()
        tool_args = item_dict.get("tool_args")
        result = item_dict.get("result")
        err = str(item_dict.get("tool_call_error") or "")
        # Skip empty shell records emitted by some SDK message wrappers.
        if not tool_name and tool_args in (None, {}, []) and result in (None, {}, [], "") and not err:
            continue
        try:
            key = json.dumps(
                {
                    "tool_name": tool_name,
                    "tool_args": tool_args,
                },
                ensure_ascii=False,
                sort_keys=True,
                default=str,
            )
        except Exception:
            key = f"{tool_name}|{str(tool_args)}"

        def _score(payload: Dict[str, Any]) -> tuple[int, int]:
            p_name = str(payload.get("tool_name") or "").strip()
            p_args = payload.get("tool_args")
            p_result = payload.get("result")
            p_err = str(payload.get("tool_call_error") or "")
            score = 0
            if p_name:
                score += 2
            if p_args not in (None, {}, [], ""):
                score += 1
            if p_result not in (None, {}, [], ""):
                score += 3
            if p_err:
                score -= 1
            result_len = len(str(p_result or ""))
            return score, result_len

        current = best_by_key.get(key)
        if current is None or _score(item_dict) > _score(current):
            best_by_key[key] = item_dict
    return list(best_by_key.values())


def _agent_source(agent_name: str) -> str:
    return _AGENT_SOURCE_BY_NAME.get(str(agent_name or "").strip().lower(), "")


def _resolve_step_name(step_input: StepInput, agent_name: str) -> str | None:
    step_obj = getattr(step_input, "step", None)
    candidates = [
        getattr(step_obj, "name", None),
        getattr(step_input, "step_name", None),
        getattr(step_obj, "id", None),
        getattr(step_obj, "__class__", type("Step", (), {})).__name__ if step_obj is not None else None,
    ]
    for value in candidates:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return _AGENT_STEP_NAME_BY_AGENT.get(str(agent_name or "").strip().lower())


def _agent_session_id(step_input: StepInput) -> str:
    workflow_session = getattr(step_input, "workflow_session", None)
    workflow_sid = getattr(workflow_session, "session_id", None)
    if isinstance(workflow_sid, str) and workflow_sid.strip():
        return workflow_sid.strip()
    workflow_run_id = getattr(workflow_session, "current_run_id", None) or getattr(workflow_session, "run_id", None)
    if isinstance(workflow_run_id, str) and workflow_run_id.strip():
        return workflow_run_id.strip()
    return "workflow-shared-session"


def is_timeout_error(exc: Exception) -> bool:
    text = str(exc or "").strip().lower()
    timeout_signals = (
        "timed out",
        "timeout",
        "request timed out",
        "api connection error",
        "read timed out",
    )
    return any(sig in text for sig in timeout_signals)


def run_agent_for_json(
    agent: Any,
    *,
    step_input: StepInput,
    agent_name: str,
    prompt: str,
    include_meta: bool = False,
) -> Dict[str, Any]:
    session_id = _agent_session_id(step_input)
    workflow_session = getattr(step_input, "workflow_session", None)
    run_id = getattr(workflow_session, "current_run_id", None) or getattr(workflow_session, "run_id", None)
    workflow_name = "material_discovery_workflow"
    trace_id = session_id
    step_name = _resolve_step_name(step_input, agent_name)

    start = time.perf_counter()
    try:
        response = agent.run(prompt, session_id=session_id)
        latency_ms = int((time.perf_counter() - start) * 1000)
        raw_content = str(getattr(response, "content", "") or "").strip()
        parsed = _extract_json_object(getattr(response, "content", None))
        tool_executions = _extract_tool_executions(response)
        tool_call_count = len(tool_executions) if isinstance(tool_executions, list) else 0

        execution_id = log_agent_execution(
            workflow_name=workflow_name,
            trace_id=trace_id if isinstance(trace_id, str) else None,
            session_id=session_id,
            run_id=run_id if isinstance(run_id, str) else None,
            step_name=step_name if isinstance(step_name, str) else None,
            agent_name=agent_name,
            agent_source=_agent_source(agent_name),
            prompt_text=prompt,
            response_text=raw_content,
            response_json=parsed,
            success=True,
            error_text=None,
            latency_ms=latency_ms,
            tool_call_count=tool_call_count,
        )
        log_prompt_llm_response(
            workflow_name=workflow_name,
            trace_id=trace_id if isinstance(trace_id, str) else None,
            session_id=session_id,
            run_id=run_id if isinstance(run_id, str) else None,
            step_name=step_name if isinstance(step_name, str) else None,
            agent_name=agent_name,
            model_id=None,
            prompt_text=prompt,
            llm_response_text=raw_content,
            response_json=parsed if isinstance(parsed, dict) else {},
            success=True,
            error_text=None,
            latency_ms=latency_ms,
        )

        if isinstance(tool_executions, list) and tool_executions:
            for tool_exec in tool_executions:
                tool_exec_dict = _normalize_tool_exec_item(tool_exec)

                tool_name = str(tool_exec_dict.get("tool_name") or "").strip() or None
                tool_args = tool_exec_dict.get("tool_args", {})
                if not isinstance(tool_args, dict):
                    tool_args = {"value": str(tool_args)}

                result = tool_exec_dict.get("result")
                structured_result = _extract_structured_payload(result)
                if isinstance(structured_result, (dict, list)):
                    tool_result: Dict[str, Any] = {"result": structured_result}
                elif result is not None:
                    tool_result = {"result_text": str(result)}
                else:
                    tool_result = {}
                if not tool_result and isinstance(parsed, dict) and parsed:
                    tool_result = {"parsed_response": parsed}

                success = not bool(tool_exec_dict.get("tool_call_error"))
                error_text = str(result) if not success and result is not None else None
                log_agent_tool_call(
                    workflow_name=workflow_name,
                    trace_id=trace_id if isinstance(trace_id, str) else None,
                    session_id=session_id,
                    run_id=run_id if isinstance(run_id, str) else None,
                    execution_id=execution_id,
                    step_name=step_name if isinstance(step_name, str) else None,
                    agent_name=agent_name,
                    agent_source=_agent_source(agent_name),
                    tool_name=tool_name,
                    tool_args=tool_args,
                    tool_result=tool_result,
                    success=success,
                    error_text=error_text,
                )
        else:
            log_agent_tool_call(
                workflow_name=workflow_name,
                trace_id=trace_id if isinstance(trace_id, str) else None,
                session_id=session_id,
                run_id=run_id if isinstance(run_id, str) else None,
                execution_id=execution_id,
                step_name=step_name if isinstance(step_name, str) else None,
                agent_name=agent_name,
                agent_source=_agent_source(agent_name),
                tool_name=None,
                tool_args={"prompt": prompt},
                tool_result={"parsed": parsed},
                success=True,
                error_text=None,
            )
        if include_meta:
            return {
                "parsed": parsed,
                "raw_content": raw_content,
                "step_name": step_name,
            }
        return parsed
    except Exception as exc:
        latency_ms = int((time.perf_counter() - start) * 1000)
        execution_id = log_agent_execution(
            workflow_name=workflow_name,
            trace_id=trace_id if isinstance(trace_id, str) else None,
            session_id=session_id,
            run_id=run_id if isinstance(run_id, str) else None,
            step_name=step_name if isinstance(step_name, str) else None,
            agent_name=agent_name,
            agent_source=_agent_source(agent_name),
            prompt_text=prompt,
            response_text="",
            response_json={},
            success=False,
            error_text=str(exc),
            latency_ms=latency_ms,
            tool_call_count=0,
        )
        log_prompt_llm_response(
            workflow_name=workflow_name,
            trace_id=trace_id if isinstance(trace_id, str) else None,
            session_id=session_id,
            run_id=run_id if isinstance(run_id, str) else None,
            step_name=step_name if isinstance(step_name, str) else None,
            agent_name=agent_name,
            model_id=None,
            prompt_text=prompt,
            llm_response_text="",
            response_json={},
            success=False,
            error_text=str(exc),
            latency_ms=latency_ms,
        )
        log_agent_tool_call(
            workflow_name=workflow_name,
            trace_id=trace_id if isinstance(trace_id, str) else None,
            session_id=session_id,
            run_id=run_id if isinstance(run_id, str) else None,
            execution_id=execution_id,
            step_name=step_name if isinstance(step_name, str) else None,
            agent_name=agent_name,
            agent_source=_agent_source(agent_name),
            tool_name=None,
            tool_args={"prompt": prompt},
            tool_result={},
            success=False,
            error_text=str(exc),
        )
        raise
