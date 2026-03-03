from __future__ import annotations

from typing import Any, Dict

from agno.agent import Agent
from agno.db.sqlite import SqliteDb
from agno.tools import tool

from src.common import build_model, MATERIAL_ROUTER_AGENT_DB
from src.fewshot import resolve_material_type_input, supported_material_type_hint


@tool
def resolve_material_type(
    goal: str = "",
    material_type: str = "",
) -> Dict[str, Any]:
    resolved, reason = resolve_material_type_input(goal=goal, material_type=material_type)
    return {
        "material_type_input": material_type,
        "goal": goal,
        "resolved_material_type": resolved,
        "resolution_reason": reason,
        "hint": supported_material_type_hint(),
    }


_router_model = build_model("material_router/agent")


material_router_agent = Agent(
    name="Material Router Agent",
    model=_router_model,
    db=SqliteDb(db_file=str(MATERIAL_ROUTER_AGENT_DB)),
    instructions=[
        "You normalize user material descriptions into supported dataset keys.",
        "Always call resolve_material_type tool first.",
        "Supported dataset keys are: ti, steel, al, hea, hea_pitting.",
        "When input is ambiguous, explain why the selected key is chosen.",
    ],
    tools=[resolve_material_type],
    markdown=True,
)
