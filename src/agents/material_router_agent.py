from __future__ import annotations

import re
from typing import Any, Dict

from agno.agent import Agent
from agno.db.sqlite import SqliteDb
from agno.tools import tool

from src.common import MATERIAL_AGENT_SHARED_DB_ID, MATERIAL_ROUTER_AGENT_DB, build_model
from src.fewshot import resolve_material_type_input
from src.schemas import AgentRouterOutput

_TARGET_PATTERN = re.compile(
    r"([A-Za-z][A-Za-z0-9_%()/.-]*)\s*(>=|<=|>|<|=|达到|不低于|不少于|不高于|不大于)?\s*([-+]?\d+(?:\.\d+)?)",
    re.IGNORECASE,
)


def _goal_keyword_route(goal: str) -> str:
    text = str(goal or "").strip().lower()
    if not text:
        return ""
    keyword_map = {
        "hea_pitting": ["pitting", "corrosion", "点蚀", "腐蚀", "氯离子"],
        "hea": ["high entropy", "high-entropy", "multi principal", "hea", "高熵", "多主元"],
        "steel": ["steel", "stainless", "martensitic", "ferritic", "钢", "不锈钢", "马氏体", "铁素体"],
        "al": ["aluminum", "aluminium", "lightweight", "铝", "轻量化"],
        "ti": ["titanium", "钛", "钛合金"],
    }
    for key, keywords in keyword_map.items():
        if any(kw in text for kw in keywords):
            return key
    return ""


def _normalize_operator(raw: str) -> str:
    text = str(raw or "").strip()
    if text in {">", ">=", "不低于", "不少于", "达到"}:
        return ">="
    if text in {"<", "<=", "不高于", "不大于"}:
        return "<="
    return "="


def parse_goal_targets(goal: str) -> list[dict[str, Any]]:
    targets: list[dict[str, Any]] = []
    for metric, op, raw_value in _TARGET_PATTERN.findall(str(goal or "")):
        try:
            target = float(raw_value)
        except (TypeError, ValueError):
            continue
        targets.append(
            {
                "name": str(metric).strip(),
                "operator": _normalize_operator(op),
                "target": target,
            }
        )
    return targets


def parse_goal_properties(goal: str) -> list[str]:
    targets = parse_goal_targets(goal)
    output: list[str] = []
    seen: set[str] = set()
    for item in targets:
        name = str(item.get("name", "")).strip()
        normalized = re.sub(r"\s+", " ", name).lower()
        if not name or normalized in seen:
            continue
        seen.add(normalized)
        output.append(name)
    return output


@tool
def resolve_material_type(
    goal: str = "",
    material_type: str = "",
    material_type_input: str = "",
) -> Dict[str, Any]:
    normalized_material_type = str(material_type or material_type_input or "").strip()
    normalized_goal = str(goal or "")
    resolved, reason = resolve_material_type_input(goal=goal, material_type=normalized_material_type)
    if not normalized_material_type:
        keyword_resolved = _goal_keyword_route(normalized_goal)
        if keyword_resolved and keyword_resolved != resolved:
            resolved = keyword_resolved
            reason = "goal_keyword_override"
    resolved_properties = parse_goal_properties(normalized_goal)
    target_thresholds = parse_goal_targets(normalized_goal)
    _ = normalized_material_type
    _ = normalized_goal
    return AgentRouterOutput(
        goal=normalized_goal,
        resolved_material_type=resolved,
        resolution_reason=reason,
        resolved_properties=resolved_properties,
        target_thresholds=target_thresholds,
    ).model_dump()


material_router_agent = Agent(
    name="Material Router Agent",
    model=build_model("material_router/agent"),
    db=SqliteDb(db_file=str(MATERIAL_ROUTER_AGENT_DB), id=MATERIAL_AGENT_SHARED_DB_ID),
    instructions=[
        "You normalize user material descriptions into supported dataset keys.",
        "Return ONLY valid JSON with exactly these keys: goal, resolved_material_type, resolution_reason, resolved_properties, target_thresholds.",
        "Supported dataset keys are: ti, steel, al, hea, hea_pitting.",
        "When material_type_input is empty, infer from goal semantics and domain terms.",
        "For high-entropy alloy intent, prefer hea; for pitting/corrosion HEA intent, prefer hea_pitting.",
        "target_thresholds must come only from explicit parseable constraints in goal; never invent targets.",
        "If no parseable thresholds exist, return target_thresholds as an empty list and explain in resolution_reason.",
        "When input is ambiguous, explain why the selected key is chosen.",
        "Pay special attention to mechanical and electrochemical property constraints such as UTS (ultimate tensile strength), YS (yield strength), Ep (pitting potential in mV), elongation, hardness, corrosion resistance, etc.",
        "Always include all identified property constraints in resolved_properties to facilitate subsequent material recommendation.",
        "Extract property names accurately from goal text, preserving technical terms and units when present.",
    ],
    tools=[],
    markdown=True,
)
