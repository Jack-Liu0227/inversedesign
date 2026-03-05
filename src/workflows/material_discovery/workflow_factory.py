from __future__ import annotations

from typing import Any, List

from src.schemas import LoopMode

from .agent_steps import predict_with_agent, recommend_with_agent, route_with_agent
from .common import build_step, build_user_input_field
from .decision_steps import collect_human_feedback, final_decision
from .judge_steps import judge_with_agent, persist_candidates


def feedback_fields() -> list[object]:
    field_specs = [
        ("measured_values_json", 'Optional JSON map, e.g. {"UTS(MPa)": 980, "EL(%)": 12.4}'),
        ("preference_feedback", "Optional preference feedback for next AI iteration"),
        ("notes", "Optional experiment notes"),
    ]
    return [
        build_user_input_field(name=name, field_type="str", required=False, description=description)
        for name, description in field_specs
    ]


def steps_for_mode(mode: LoopMode) -> List[Any]:
    _ = mode
    steps: List[Any] = [
        build_step(name="Router Agent", executor=route_with_agent),
        build_step(name="Recommender Agent", executor=recommend_with_agent),
        build_step(name="Predictor Agent", executor=predict_with_agent),
        build_step(name="Rationality Judge", executor=judge_with_agent),
        build_step(name="Persistence", executor=persist_candidates),
        build_step(
            name="Human Feedback",
            executor=collect_human_feedback,
            requires_user_input=False,
            user_input_message="Optional: provide lab measured values and preference feedback.",
            user_input_schema=feedback_fields(),
        ),
        build_step(name="Final Decision", executor=final_decision),
    ]
    return steps
