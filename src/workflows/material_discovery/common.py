from __future__ import annotations

import inspect
import logging
import time
import traceback
from typing import Any, Dict, List, Optional
from uuid import uuid4

from agno.workflow import Step
from agno.workflow.types import StepInput, StepOutput

try:
    from agno.workflow.types import UserInputField  # type: ignore
except Exception:
    try:
        from agno.tools.function import UserInputField  # type: ignore
    except Exception:
        UserInputField = None  # type: ignore

from src.common import (
    apply_request_debug_mode,
    create_workflow_run_audit,
    fail_workflow_run_audit,
    finalize_workflow_run_audit,
    log_agent_tool_call,
    log_workflow_event,
    log_workflow_step,
    run_local_db_migrations,
)
from src.schemas import WorkflowInput

run_local_db_migrations()
LOGGER = logging.getLogger("inversedesign.workflow")
_AUDIT_ROW_BY_TRACE: Dict[str, int] = {}
_WORKFLOW_NO_TOOL_TRACE_STEPS = {"Persistence", "Human Feedback", "Final Decision"}
try:
    _STEP_INIT_PARAMS = set(inspect.signature(Step.__init__).parameters.keys())
except Exception:
    _STEP_INIT_PARAMS = set()


def is_non_empty_dict(value: Any) -> bool:
    return isinstance(value, dict) and bool(value)


def as_workflow_input(payload: Any) -> WorkflowInput:
    if isinstance(payload, WorkflowInput):
        return payload
    return WorkflowInput.model_validate(payload)


def request_from_step_input(step_input: StepInput) -> WorkflowInput:
    return as_workflow_input(step_input.input)


def is_debug_enabled(request: WorkflowInput) -> bool:
    return bool(request.debug)


def debug_level(request: WorkflowInput) -> int:
    level = int(request.debug_level)
    return 2 if level >= 2 else 1


def _workflow_session_attr(step_input: StepInput, *names: str) -> Optional[str]:
    workflow_session = getattr(step_input, "workflow_session", None)
    for name in names:
        value = getattr(workflow_session, name, None)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def session_id_from_step_input(step_input: StepInput) -> Optional[str]:
    return _workflow_session_attr(step_input, "session_id")


def run_id_from_step_input(step_input: StepInput) -> Optional[str]:
    return _workflow_session_attr(step_input, "current_run_id", "run_id")


def trace_id(step_input: StepInput, request: WorkflowInput) -> str:
    if request.log_trace_id:
        return request.log_trace_id
    session_id = session_id_from_step_input(step_input)
    return session_id or f"trace-{uuid4()}"


def effective_workflow_run_id(step_input: StepInput, request: WorkflowInput) -> str:
    if request.resume_run_id:
        return str(request.resume_run_id).strip()
    run_id = run_id_from_step_input(step_input)
    if isinstance(run_id, str) and run_id.strip():
        return run_id.strip()
    return trace_id(step_input, request)


def ensure_run_audit_row(step_input: StepInput, request: WorkflowInput) -> int:
    trace = trace_id(step_input, request)
    existing = _AUDIT_ROW_BY_TRACE.get(trace)
    if existing is not None:
        return existing
    audit_id = create_workflow_run_audit(
        workflow_name="material_discovery_workflow",
        session_id=session_id_from_step_input(step_input),
        run_id=effective_workflow_run_id(step_input, request),
        user_id=request.user_id,
        input_payload=to_jsonable(step_input.input) or {},
    )
    _AUDIT_ROW_BY_TRACE[trace] = audit_id
    return audit_id


def pop_run_audit_row(step_input: StepInput, request: WorkflowInput) -> None:
    _AUDIT_ROW_BY_TRACE.pop(trace_id(step_input, request), None)


def to_jsonable(value: Any, max_chars: int = 3000) -> Any:
    if value is None:
        return None
    if hasattr(value, "model_dump"):
        try:
            value = value.model_dump(mode="json")
        except Exception:
            value = str(value)
    if isinstance(value, dict):
        return {str(k): to_jsonable(v, max_chars=max_chars) for k, v in value.items()}
    if isinstance(value, list):
        return [to_jsonable(v, max_chars=max_chars) for v in value]
    if isinstance(value, (int, float, bool)):
        return value
    text = str(value)
    if len(text) > max_chars:
        return text[:max_chars]
    return text


def _compact_dict(value: Any, max_items: int = 8) -> Dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    compact: Dict[str, Any] = {}
    for idx, (key, val) in enumerate(value.items()):
        if idx >= max_items:
            break
        compact[str(key)] = to_jsonable(val, max_chars=180)
    return compact


def _candidate_preview(items: Any, limit: int = 2) -> List[Dict[str, Any]]:
    if not isinstance(items, list):
        return []
    preview: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        preview.append(
            {
                "candidate_index": item.get("candidate_index"),
                "composition": _compact_dict(item.get("composition", {}), max_items=8),
                "processing": _compact_dict(item.get("processing", {}), max_items=6),
                "score": item.get("score"),
                "confidence": item.get("confidence"),
                "predicted_values": _compact_dict(item.get("predicted_values", {}), max_items=6),
                "prediction_error": str(item.get("prediction_error", "") or ""),
                "is_valid": item.get("is_valid"),
            }
        )
        if len(preview) >= limit:
            break
    return preview


def summarize_request(request: WorkflowInput) -> Dict[str, Any]:
    experiment_feedback = request.experiment_feedback if isinstance(request.experiment_feedback, dict) else {}
    measured_values = experiment_feedback.get("measured_values")
    measured_preview = _compact_dict(measured_values, max_items=8) if isinstance(measured_values, dict) else {}
    feedback_status = "provided" if measured_preview else "none"
    preference_feedback = str(request.preference_feedback or "").strip()
    mode = "human_in_the_loop" if bool(request.human_loop) else "ai_only"
    return {
        "goal": request.goal,
        "mode": mode,
        "human_loop": bool(request.human_loop),
        "max_iterations": request.max_iterations,
        "top_k": request.top_k,
        "recommend_count_policy": request.recommend_count_policy,
        "resume_run_id": request.resume_run_id,
        "experiment_feedback": feedback_status,
        "measured_values": measured_preview,
        "preference_feedback": preference_feedback,
        "debug": bool(request.debug),
        "debug_level": int(request.debug_level),
    }


def summarize_output(content: Any) -> Dict[str, Any]:
    if isinstance(content, dict):
        has_predicted_values = bool(content.get("predicted_values")) if "predicted_values" in content else None
        has_next_iteration_proposals = bool(content.get("next_iteration_proposals")) if "next_iteration_proposals" in content else None
        proposal_meta_raw = content.get("proposal_meta", {})
        proposal_meta = proposal_meta_raw if isinstance(proposal_meta_raw, dict) else {}
        summary = {
            "keys": sorted(content.keys()),
            "decision": content.get("decision"),
            "should_stop": content.get("should_stop"),
            "success_hints": {
                "has_predicted_values": has_predicted_values,
                "has_next_iteration_proposals": has_next_iteration_proposals,
            },
            "proposal_meta": {
                "requested_count": proposal_meta.get("requested_count"),
                "generated_count": proposal_meta.get("generated_count"),
                "prediction_success_count": proposal_meta.get("prediction_success_count"),
                "duplicate_filtered": proposal_meta.get("duplicate_filtered"),
            }
            if proposal_meta
            else {},
        }
        summary["resolved_material_type"] = content.get("resolved_material_type")
        summary["prediction_error"] = content.get("prediction_error")
        summary["summary"] = content.get("summary") if isinstance(content.get("summary"), list) else []
        summary["candidate_count"] = len(content.get("candidates", [])) if isinstance(content.get("candidates"), list) else 0
        summary["recommended_count"] = (
            len(content.get("recommended_candidates", [])) if isinstance(content.get("recommended_candidates"), list) else 0
        )
        summary["valid_count"] = len(content.get("valid_candidates", [])) if isinstance(content.get("valid_candidates"), list) else 0
        summary["preview_candidates"] = _candidate_preview(content.get("candidates"), limit=2)
        summary["preview_recommended"] = _candidate_preview(content.get("recommended_candidates"), limit=2)
        summary["preview_valid"] = _candidate_preview(content.get("valid_candidates"), limit=2)
        summary["preview_predictions"] = _candidate_preview(content.get("candidate_predictions"), limit=3)
        return summary
    if isinstance(content, list):
        return {"type": "list", "size": len(content)}
    return {"type": type(content).__name__, "preview": to_jsonable(content, max_chars=300)}


def collect_step_outputs(step_input: StepInput) -> Dict[str, Any]:
    collected: Dict[str, Any] = {}
    previous = step_input.previous_step_outputs or {}
    for step_name, output in previous.items():
        collected[step_name] = summarize_output(getattr(output, "content", None))
    return collected


def build_response_summary(step_input: StepInput, final_payload: Dict[str, Any]) -> List[str]:
    previous = step_input.previous_step_outputs or {}

    def _step_content(step_name: str) -> Dict[str, Any]:
        step_output = previous.get(step_name)
        content = getattr(step_output, "content", None)
        return content if isinstance(content, dict) else {}

    router_payload = _step_content("Router Agent")
    rec_payload = _step_content("Recommender Agent")
    pred_payload = _step_content("Predictor Agent")
    judge_payload = _step_content("Rationality Judge")
    persistence_payload = _step_content("Persistence")
    feedback_payload = _step_content("Human Feedback")

    summary: List[str] = [f"Router: resolved_material_type={router_payload.get('resolved_material_type', '')}"]
    candidates = rec_payload.get("candidates", [])
    summary.append(f"Recommender: candidates={len(candidates) if isinstance(candidates, list) else 0}")

    predicted_total = 0
    predicted_failed = 0
    candidate_predictions = pred_payload.get("candidate_predictions", [])
    if isinstance(candidate_predictions, list) and candidate_predictions:
        predicted_total = len([x for x in candidate_predictions if isinstance(x, dict)])
        predicted_failed = len(
            [
                x
                for x in candidate_predictions
                if isinstance(x, dict) and str(x.get("prediction_error", "") or "").strip()
            ]
        )
    predicted_success = max(0, predicted_total - predicted_failed)
    summary.append(
        f"Predictor: predicted_candidates={predicted_success}/{predicted_total}, failed={predicted_failed}"
    )

    valid_count = int(judge_payload.get("valid_count", persistence_payload.get("valid_count", 0)) or 0)
    summary.append(f"Judge: valid_candidates={valid_count}")
    feedback_keys = (
        sorted((feedback_payload.get("measured_values") or {}).keys())
        if isinstance(feedback_payload.get("measured_values"), dict)
        else []
    )
    summary.append(f"Feedback: measured_keys={feedback_keys}")
    summary.append(f"Final: decision={final_payload.get('decision')}")
    return summary


def audit_event(
    *,
    step_input: StepInput,
    request: WorkflowInput,
    step_name: Optional[str],
    event_type: str,
    payload: Dict[str, Any],
    latency_ms: Optional[int] = None,
    success: Optional[bool] = None,
    error_text: Optional[str] = None,
) -> None:
    log_workflow_event(
        workflow_name="material_discovery_workflow",
        trace_id=trace_id(step_input, request),
        session_id=session_id_from_step_input(step_input),
        run_id=effective_workflow_run_id(step_input, request),
        user_id=request.user_id,
        step_name=step_name,
        event_type=event_type,
        payload=payload,
        latency_ms=latency_ms,
        success=success,
        error_text=error_text,
    )


def _step_accepts(param: str) -> bool:
    return param in _STEP_INIT_PARAMS


def build_step(name: str, executor: Any, **kwargs: Any) -> Step:
    is_final_step = name == "Final Decision"
    is_router_step = name == "Router Agent"

    def _logged_executor(step_input: StepInput) -> StepOutput:
        request = request_from_step_input(step_input)
        workflow_run_id = effective_workflow_run_id(step_input, request)
        debug = is_debug_enabled(request)
        apply_request_debug_mode(debug=debug, debug_level=debug_level(request))
        audit_id = ensure_run_audit_row(step_input, request)

        if is_router_step:
            audit_event(
                step_input=step_input,
                request=request,
                step_name=name,
                event_type="request",
                payload=summarize_request(request),
            )

        step_input_payload = to_jsonable(step_input.input)
        audit_event(
            step_input=step_input,
            request=request,
            step_name=name,
            event_type="step_start",
            payload={"input": step_input_payload},
        )
        log_workflow_step(
            workflow_name="material_discovery_workflow",
            trace_id=trace_id(step_input, request),
            session_id=session_id_from_step_input(step_input),
            run_id=workflow_run_id,
            user_id=request.user_id,
            step_name=name,
            status="step_start",
            input_payload={"input": step_input_payload if isinstance(step_input_payload, dict) else {"value": step_input_payload}},
        )

        start = time.perf_counter()
        try:
            output = executor(step_input)
            elapsed_ms = int((time.perf_counter() - start) * 1000)
            output_payload = to_jsonable(output.content)
            audit_event(
                step_input=step_input,
                request=request,
                step_name=name,
                event_type="step_end",
                payload={"output": output_payload},
                latency_ms=elapsed_ms,
                success=output.success,
            )
            log_workflow_step(
                workflow_name="material_discovery_workflow",
                trace_id=trace_id(step_input, request),
                session_id=session_id_from_step_input(step_input),
                run_id=workflow_run_id,
                user_id=request.user_id,
                step_name=name,
                status="step_end",
                output_payload={"output": output_payload if isinstance(output_payload, dict) else {"value": output_payload}},
                latency_ms=elapsed_ms,
                success=output.success,
            )
            if name in _WORKFLOW_NO_TOOL_TRACE_STEPS:
                log_agent_tool_call(
                    workflow_name="material_discovery_workflow",
                    trace_id=trace_id(step_input, request),
                    session_id=session_id_from_step_input(step_input),
                    run_id=workflow_run_id,
                    execution_id=None,
                    step_name=name,
                    agent_name=name,
                    agent_source="workflow_step",
                    tool_name=None,
                    tool_args={"input": step_input_payload},
                    tool_result={"output": output_payload},
                    success=bool(output.success),
                    error_text=None,
                )
            LOGGER.info("step=%s success=%s latency_ms=%s", name, output.success, elapsed_ms)

            if is_final_step:
                final_payload = to_jsonable(output.content)
                audit_event(
                    step_input=step_input,
                    request=request,
                    step_name=name,
                    event_type="final",
                    payload=final_payload if isinstance(final_payload, dict) else {"value": final_payload},
                    latency_ms=elapsed_ms,
                    success=output.success,
                )
                output_content = output.content if isinstance(output.content, dict) else {}
                final_decision = str(output_content.get("decision", "")).strip().lower()
                finalize_workflow_run_audit(
                    audit_id=audit_id,
                    decision=str(output_content.get("decision", "")),
                    should_stop=final_decision in {"stop", "await_user_choice"},
                    summary=[str(s) for s in output_content.get("summary", []) if isinstance(s, str)],
                    final_result=to_jsonable(output_content) if isinstance(output_content, dict) else {},
                    step_outputs=collect_step_outputs(step_input),
                )
                pop_run_audit_row(step_input, request)
            return output
        except Exception as exc:
            elapsed_ms = int((time.perf_counter() - start) * 1000)
            error_payload: Dict[str, Any] = {"error": str(exc)}
            if debug:
                error_payload["traceback"] = traceback.format_exc(limit=10)
                error_payload["input"] = to_jsonable(step_input.input)
            audit_event(
                step_input=step_input,
                request=request,
                step_name=name,
                event_type="error",
                payload=error_payload,
                latency_ms=elapsed_ms,
                success=False,
                error_text=str(exc),
            )
            log_workflow_step(
                workflow_name="material_discovery_workflow",
                trace_id=trace_id(step_input, request),
                session_id=session_id_from_step_input(step_input),
                run_id=workflow_run_id,
                user_id=request.user_id,
                step_name=name,
                status="error",
                input_payload={"input": to_jsonable(step_input.input) if debug else summarize_request(request)},
                output_payload={"error": str(exc)},
                latency_ms=elapsed_ms,
                success=False,
                error_text=str(exc),
            )
            if name in _WORKFLOW_NO_TOOL_TRACE_STEPS:
                log_agent_tool_call(
                    workflow_name="material_discovery_workflow",
                    trace_id=trace_id(step_input, request),
                    session_id=session_id_from_step_input(step_input),
                    run_id=workflow_run_id,
                    execution_id=None,
                    step_name=name,
                    agent_name=name,
                    agent_source="workflow_step",
                    tool_name=None,
                    tool_args={"input": to_jsonable(step_input.input) if debug else summarize_request(request)},
                    tool_result={"error": str(exc)},
                    success=False,
                    error_text=str(exc),
                )
            LOGGER.exception("step=%s failed latency_ms=%s error=%s", name, elapsed_ms, exc)
            fail_workflow_run_audit(audit_id=audit_id, error_text=str(exc))
            pop_run_audit_row(step_input, request)
            raise

    step_kwargs: Dict[str, Any] = {"name": name, "executor": _logged_executor}
    for key, value in kwargs.items():
        if _step_accepts(key):
            step_kwargs[key] = value
    return Step(**step_kwargs)


def build_user_input_field(
    name: str,
    field_type: str,
    description: str,
    required: bool,
) -> Any:
    fallback = {
        "name": name,
        "field_type": field_type,
        "required": required,
        "description": description,
    }
    if UserInputField is None:
        return fallback

    constructors = [
        fallback,
        {"name": name, "field_type": field_type, "is_required": required, "description": description},
        {"name": name, "type": field_type, "required": required, "description": description},
        {"name": name, "type": field_type, "description": description},
        {"name": name, "field_type": field_type, "description": description},
        {"name": name, "type": field_type, "is_required": required, "description": description},
    ]
    for kwargs in constructors:
        try:
            field = UserInputField(**kwargs)
            if hasattr(field, "required"):
                setattr(field, "required", required)
            elif hasattr(field, "is_required"):
                setattr(field, "is_required", required)
            return field
        except Exception:
            continue
    return fallback
