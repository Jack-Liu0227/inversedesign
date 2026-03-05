from __future__ import annotations

from typing import Any, Dict

from pydantic import BaseModel, ConfigDict, Field, model_validator


class RouterTargetThreshold(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(default="")
    operator: str = Field(default="=")
    target: float | None = Field(default=None)

    @staticmethod
    def _normalize_operator(raw: Any) -> str:
        op = str(raw or "").strip().lower()
        mapping = {
            "gt": ">",
            "gte": ">=",
            "ge": ">=",
            "at least": ">=",
            "lt": "<",
            "lte": "<=",
            "le": "<=",
            "at most": "<=",
            "eq": "=",
            "equals": "=",
        }
        if op in {">", ">=", "<", "<=", "="}:
            return op
        return mapping.get(op, "=")

    @model_validator(mode="before")
    @classmethod
    def _normalize_threshold_payload(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value

        # Accept common LLM variants like property/value/unit, op/comparator/relation.
        name = value.get("name", value.get("property", value.get("metric", "")))
        operator = value.get(
            "operator",
            value.get("op", value.get("comparator", value.get("relation", "="))),
        )
        target = value.get("target", value.get("value", value.get("threshold")))
        return {
            "name": name,
            "operator": cls._normalize_operator(operator),
            "target": target,
        }


class AgentRouterOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    goal: str = Field(default="")
    resolved_material_type: str = Field(default="")
    resolution_reason: str = Field(default="")
    resolved_properties: list[str] = Field(default_factory=list)
    target_thresholds: list[RouterTargetThreshold] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _normalize_router_payload(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value

        payload: Dict[str, Any] = dict(value)

        # Some responses may output get_thresholds/thresholds instead of target_thresholds.
        if "target_thresholds" not in payload:
            if isinstance(payload.get("get_thresholds"), list):
                payload["target_thresholds"] = payload.get("get_thresholds")
            elif isinstance(payload.get("thresholds"), list):
                payload["target_thresholds"] = payload.get("thresholds")

        if not isinstance(payload.get("target_thresholds"), list):
            payload["target_thresholds"] = []
        return payload
