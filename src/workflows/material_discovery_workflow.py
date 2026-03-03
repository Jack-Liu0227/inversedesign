from __future__ import annotations

import concurrent.futures
import inspect
import json
from typing import Any, Dict, List, Optional

from agno.db.sqlite import SqliteDb
from agno.workflow import Loop, Step, Workflow
from agno.workflow.types import StepInput, StepOutput

try:
    from agno.workflow.types import UserInputField  # type: ignore
except Exception:
    try:
        from agno.tools.function import UserInputField  # type: ignore
    except Exception:
        UserInputField = None  # type: ignore

from src.agents.material_predictor_agent import material_predictor_agent
from src.agents.material_recommender_agent import material_recommender_agent
from src.agents.material_review_agent import material_review_agent
from src.agents.material_router_agent import material_router_agent
from src.common import MATERIAL_DISCOVERY_WORKFLOW_DB
from src.schemas import WorkflowInput


def _as_workflow_input(payload: Any) -> WorkflowInput:
    if isinstance(payload, WorkflowInput):
        return payload
    return WorkflowInput.model_validate(payload)


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
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            candidate = text[start : end + 1]
            try:
                parsed = json.loads(candidate)
                return parsed if isinstance(parsed, dict) else {}
            except Exception:
                return {}
    return {}


def _agent_session_id(step_input: StepInput, agent_name: str) -> str:
    workflow_session = getattr(step_input, "workflow_session", None)
    workflow_sid = getattr(workflow_session, "session_id", None) or "workflow"
    return f"{workflow_sid}:{agent_name}"


def _run_agent_for_json(agent: Any, *, step_input: StepInput, agent_name: str, prompt: str) -> Dict[str, Any]:
    response = agent.run(
        prompt,
        session_id=_agent_session_id(step_input, agent_name),
    )
    content = getattr(response, "content", None)
    return _extract_json_object(content)


def _run_agent_for_json_with_session_suffix(
    agent: Any,
    *,
    step_input: StepInput,
    agent_name: str,
    session_suffix: str,
    prompt: str,
) -> Dict[str, Any]:
    response = agent.run(
        prompt,
        session_id=f"{_agent_session_id(step_input, agent_name)}:{session_suffix}",
    )
    content = getattr(response, "content", None)
    return _extract_json_object(content)


def _confidence_rank(confidence: str) -> int:
    value = (confidence or "").strip().lower()
    if value == "high":
        return 3
    if value == "medium":
        return 2
    if value == "low":
        return 1
    return 0


def _route_with_agent(step_input: StepInput) -> StepOutput:
    request = _as_workflow_input(step_input.input)
    prompt = (
        "Resolve material type for downstream dataset selection.\n"
        "Return ONLY valid JSON with keys: "
        "material_type_input, goal, resolved_material_type, resolution_reason.\n"
        f"material_type_input={request.material_type}\n"
        f"goal={request.goal}"
    )
    routed = _run_agent_for_json(
        material_router_agent,
        step_input=step_input,
        agent_name="router",
        prompt=prompt,
    )
    resolved = str(routed.get("resolved_material_type", "")).strip().lower()
    if not resolved:
        raise ValueError("Router agent did not return resolved_material_type")
    routed["resolved_material_type"] = resolved
    return StepOutput(content=routed)


def _recommend_with_agent(step_input: StepInput) -> StepOutput:
    request = _as_workflow_input(step_input.input)
    route_step = step_input.previous_step_outputs.get("Router Agent")
    routed = route_step.content if route_step else {}
    resolved_material_type = str((routed or {}).get("resolved_material_type", "")).strip().lower()
    if not resolved_material_type:
        raise ValueError("Missing resolved_material_type from Router Agent step")

    prompt = (
        "Recommend candidate alloys.\n"
        "Return ONLY valid JSON with keys: material_type, goal, candidates.\n"
        "Each item in candidates must include composition, processing, score, reason.\n"
        f"goal={request.goal}\n"
        f"material_type={resolved_material_type}\n"
        "top_n=3"
    )
    rec = _run_agent_for_json(
        material_recommender_agent,
        step_input=step_input,
        agent_name="recommender",
        prompt=prompt,
    )
    rec["material_type"] = resolved_material_type
    rec["goal"] = request.goal
    if not isinstance(rec.get("candidates"), list):
        rec["candidates"] = []
    return StepOutput(content=rec)


def _predict_with_agent(step_input: StepInput) -> StepOutput:
    request = _as_workflow_input(step_input.input)
    routed_output = step_input.previous_step_outputs.get("Router Agent")
    rec_output = step_input.previous_step_outputs.get("Recommender Agent")

    routed = routed_output.content if routed_output else {}
    recommendation = rec_output.content if rec_output else {}
    resolved_material_type = str((routed or {}).get("resolved_material_type", "")).strip().lower()
    candidates = recommendation.get("candidates", []) if isinstance(recommendation, dict) else []

    jobs: List[Dict[str, Any]] = []
    if request.composition:
        jobs.append(
            {
                "composition": request.composition,
                "processing": request.processing or {},
                "candidate_index": -1,
            }
        )
    else:
        for idx, candidate in enumerate(candidates):
            comp = candidate.get("composition", {})
            if not isinstance(comp, dict) or not comp:
                continue
            candidate_processing = candidate.get("processing", {})
            jobs.append(
                {
                    "composition": comp,
                    "processing": candidate_processing if isinstance(candidate_processing, dict) else {},
                    "candidate_index": idx,
                }
            )
    if not jobs:
        jobs.append({"composition": {}, "processing": request.processing or {}, "candidate_index": -1})

    def _predict_single(job_idx: int, job: Dict[str, Any]) -> Dict[str, Any]:
        prompt = (
            "Predict properties using few-shot tool.\n"
            "Make exactly one tool call with one JSON arguments object.\n"
            "Do not concatenate multiple JSON objects in a single tool call.\n"
            "Return ONLY valid JSON with keys: material_type, predicted_values, confidence, "
            "similar_samples, llm_response, prompt_log_id.\n"
            f"goal={request.goal}\n"
            f"material_type={resolved_material_type}\n"
            f"composition={json.dumps(job['composition'], ensure_ascii=False)}\n"
            f"processing={json.dumps(job['processing'], ensure_ascii=False)}\n"
            f"features={json.dumps(request.features, ensure_ascii=False)}\n"
            f"top_k={request.top_k or 3}"
        )
        pred = _run_agent_for_json_with_session_suffix(
            material_predictor_agent,
            step_input=step_input,
            agent_name="predictor",
            session_suffix=f"cand_{job_idx}",
            prompt=prompt,
        )
        pred["material_type"] = resolved_material_type
        pred["goal"] = request.goal
        pred["used_composition"] = job["composition"]
        pred["used_processing"] = job["processing"]
        pred["candidate_index"] = job["candidate_index"]
        if not isinstance(pred.get("predicted_values"), dict):
            pred["predicted_values"] = {}
        return pred

    batch_predictions: List[Dict[str, Any]] = []
    max_workers = min(3, max(1, len(jobs)))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_predict_single, i, job) for i, job in enumerate(jobs)]
        for future in concurrent.futures.as_completed(futures):
            try:
                batch_predictions.append(future.result())
            except Exception as exc:
                batch_predictions.append(
                    {
                        "error": str(exc),
                        "material_type": resolved_material_type,
                        "goal": request.goal,
                        "predicted_values": {},
                        "confidence": "low",
                    }
                )

    successful = [p for p in batch_predictions if not p.get("error")]
    if not successful:
        raise ValueError("All parallel predictor calls failed")

    primary = max(
        successful,
        key=lambda p: (
            _confidence_rank(str(p.get("confidence", ""))),
            len((p.get("predicted_values") or {})),
        ),
    )
    primary_result = dict(primary)
    primary_result["recommended_candidates"] = candidates
    # Clone prediction rows to avoid self-reference when primary comes from batch_predictions.
    primary_result["batch_predictions"] = [dict(item) if isinstance(item, dict) else item for item in batch_predictions]
    return StepOutput(content=primary_result)


def _review_with_agent(step_input: StepInput) -> StepOutput:
    prediction_step = step_input.previous_step_outputs.get("Predictor Agent")
    prediction = prediction_step.content if prediction_step else {}
    if not isinstance(prediction, dict):
        prediction = {}

    prompt = (
        "Review prediction and return concise decision support.\n"
        "Return ONLY valid JSON with keys: review_summary, major_risks, next_actions.\n"
        "major_risks and next_actions must be string arrays.\n"
        f"prediction_payload={json.dumps(prediction, ensure_ascii=False)}"
    )
    review = _run_agent_for_json(
        material_review_agent,
        step_input=step_input,
        agent_name="review",
        prompt=prompt,
    )
    if not isinstance(review.get("major_risks"), list):
        review["major_risks"] = []
    if not isinstance(review.get("next_actions"), list):
        review["next_actions"] = []

    merged = dict(prediction)
    merged["review_summary"] = review.get("review_summary", "")
    merged["major_risks"] = review.get("major_risks", [])
    merged["next_actions"] = review.get("next_actions", [])
    return StepOutput(content=merged)


def _collect_human_feedback(step_input: StepInput) -> StepOutput:
    additional_data = getattr(step_input, "additional_data", None) or {}
    user_input = additional_data.get("user_input", {}) if isinstance(additional_data, dict) else {}
    input_payload = step_input.input if isinstance(step_input.input, dict) else {}
    fallback_feedback: Dict[str, Any] = {}
    if isinstance(input_payload, dict):
        for key in ("experiment_feedback", "human_feedback", "feedback"):
            candidate = input_payload.get(key, {})
            if isinstance(candidate, dict) and candidate:
                fallback_feedback = candidate
                break

    measured_values_input = user_input.get("measured_values")
    if measured_values_input is None:
        measured_values_input = user_input.get("measured_values_json")
    if measured_values_input is None:
        measured_values_input = fallback_feedback.get("measured_values", {})

    if isinstance(measured_values_input, dict):
        measured_values = measured_values_input
    elif isinstance(measured_values_input, str):
        text = measured_values_input.strip()
        if not text:
            measured_values = {}
        else:
            try:
                parsed = json.loads(text)
                if not isinstance(parsed, dict):
                    return StepOutput(
                        content={"error": "measured_values_json must decode to a JSON object"},
                        success=False,
                    )
                measured_values = parsed
            except json.JSONDecodeError as exc:
                return StepOutput(content={"error": f"Invalid measured_values_json: {exc}"}, success=False)
    else:
        return StepOutput(
            content={"error": "measured_values/measured_values_json must be a JSON object or JSON string"},
            success=False,
        )

    notes = user_input.get("notes", fallback_feedback.get("notes", ""))
    return StepOutput(content={"measured_values": measured_values, "notes": notes})


def _final_decision(step_input: StepInput) -> StepOutput:
    router_step = step_input.previous_step_outputs.get("Router Agent")
    recommender_step = step_input.previous_step_outputs.get("Recommender Agent")
    predictor_step = step_input.previous_step_outputs.get("Predictor Agent")
    review_step = step_input.previous_step_outputs.get("Review Agent")
    feedback_step = step_input.previous_step_outputs.get("Human Feedback")
    routed = router_step.content if router_step else {}
    recommendation = recommender_step.content if recommender_step else {}
    prediction = predictor_step.content if predictor_step else {}
    review = review_step.content if review_step else {}
    feedback = feedback_step.content if feedback_step else {}
    predicted_values = review.get("predicted_values", {}) if isinstance(review, dict) else {}
    measured_values = feedback.get("measured_values", {}) if isinstance(feedback, dict) else {}

    abs_errors: Dict[str, float] = {}
    for key, pred_val in predicted_values.items():
        if key not in measured_values:
            continue
        try:
            abs_errors[key] = abs(float(pred_val) - float(measured_values[key]))
        except (TypeError, ValueError):
            continue

    has_feedback = bool(measured_values)
    if not has_feedback:
        return StepOutput(
            content={
                "decision": "await_feedback",
                "should_stop": True,
                "reason": "No experimental feedback was provided. Returning latest recommendation/prediction.",
                "predicted_values": predicted_values,
                "recommended_candidates": review.get("recommended_candidates", []) if isinstance(review, dict) else [],
                "used_composition": review.get("used_composition", {}) if isinstance(review, dict) else {},
                "confidence": review.get("confidence") if isinstance(review, dict) else None,
                "review_summary": review.get("review_summary") if isinstance(review, dict) else "",
                "step_outputs": {
                    "router": routed if isinstance(routed, dict) else {},
                    "recommender": recommendation if isinstance(recommendation, dict) else {},
                    "predictor": prediction if isinstance(prediction, dict) else {},
                    "review": review if isinstance(review, dict) else {},
                    "human_feedback": feedback if isinstance(feedback, dict) else {},
                },
            }
        )

    should_stop = bool(abs_errors) and all(v <= 5.0 for v in abs_errors.values())
    return StepOutput(
        content={
            "abs_errors": abs_errors,
            "should_stop": should_stop,
            "decision": "stop" if should_stop else "continue",
            "predicted_values": predicted_values,
            "measured_values": measured_values,
            "recommended_candidates": review.get("recommended_candidates", []) if isinstance(review, dict) else [],
            "used_composition": review.get("used_composition", {}) if isinstance(review, dict) else {},
            "confidence": review.get("confidence") if isinstance(review, dict) else None,
            "review_summary": review.get("review_summary") if isinstance(review, dict) else "",
            "step_outputs": {
                "router": routed if isinstance(routed, dict) else {},
                "recommender": recommendation if isinstance(recommendation, dict) else {},
                "predictor": prediction if isinstance(prediction, dict) else {},
                "review": review if isinstance(review, dict) else {},
                "human_feedback": feedback if isinstance(feedback, dict) else {},
            },
        }
    )


def _end_when_satisfied(outputs: List[StepOutput]) -> bool:
    for output in reversed(outputs):
        content = output.content
        if isinstance(content, dict) and content.get("should_stop") is True:
            return True
    return False


def _step_accepts(param: str) -> bool:
    try:
        sig = inspect.signature(Step.__init__)
    except Exception:
        return False
    return param in sig.parameters


def _build_step(name: str, executor: Any, **kwargs: Any) -> Step:
    step_kwargs: Dict[str, Any] = {"name": name, "executor": executor}
    for key, value in kwargs.items():
        if _step_accepts(key):
            step_kwargs[key] = value
    return Step(**step_kwargs)


def _build_user_input_field(
    name: str,
    field_type: str,
    description: str,
    required: bool,
) -> Any:
    if UserInputField is None:
        return {
            "name": name,
            "field_type": field_type,
            "required": required,
            "description": description,
        }

    constructors = [
        {"name": name, "field_type": field_type, "required": required, "description": description},
        {"name": name, "field_type": field_type, "is_required": required, "description": description},
        {"name": name, "type": field_type, "required": required, "description": description},
        {"name": name, "type": field_type, "is_required": required, "description": description},
        {"name": name, "field_type": field_type, "description": description},
        {"name": name, "type": field_type, "description": description},
    ]
    for kwargs in constructors:
        try:
            field = UserInputField(**kwargs)
            if hasattr(field, "required"):
                setattr(field, "required", required)
            elif hasattr(field, "is_required"):
                setattr(field, "is_required", required)
            return field
        except TypeError:
            continue
        except Exception:
            continue
    return {
        "name": name,
        "field_type": field_type,
        "required": required,
        "description": description,
    }


def build_material_discovery_workflow() -> Workflow:
    feedback_fields = [
        _build_user_input_field(
            name="measured_values_json",
            field_type="str",
            required=True,
            description='JSON map of measured values, e.g. {"UTS(MPa)": 980, "El(%)": 12.4}',
        ),
        _build_user_input_field(
            name="notes",
            field_type="str",
            required=False,
            description="Optional experiment notes",
        ),
    ]

    return Workflow(
        name="material_discovery_workflow",
        db=SqliteDb(db_file=str(MATERIAL_DISCOVERY_WORKFLOW_DB)),
        input_schema=WorkflowInput,
        steps=[
            Loop(
                name="Router Recommender Predictor Review Feedback Loop",
                max_iterations=3,
                end_condition=_end_when_satisfied,
                steps=[
                    _build_step(name="Router Agent", executor=_route_with_agent),
                    _build_step(name="Recommender Agent", executor=_recommend_with_agent),
                    _build_step(name="Predictor Agent", executor=_predict_with_agent),
                    _build_step(name="Review Agent", executor=_review_with_agent),
                    _build_step(
                        name="Human Feedback",
                        executor=_collect_human_feedback,
                        requires_user_input=True,
                        user_input_message="Provide measured values from lab experiment:",
                        user_input_schema=feedback_fields,
                    ),
                    _build_step(name="Final Decision", executor=_final_decision),
                ],
            ),
        ],
        stream_events=True,
    )


workflow = build_material_discovery_workflow()
