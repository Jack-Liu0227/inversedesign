from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator

_TRUE_STRINGS = {"1", "true", "yes", "on"}
_FALSE_STRINGS = {"0", "false", "no", "off", ""}


class WorkflowInput(BaseModel):
    goal: str = Field(description="Optimization goal for this run")
    human_loop: bool | str = Field(default=False)
    max_iterations: int | str = Field(default=3)
    top_k: Optional[int | str] = Field(default=None)

    experiment_feedback: Optional[dict[str, Any]] = Field(default=None)
    preference_feedback: Optional[str] = Field(default=None)
    user_id: Optional[str] = Field(default=None)

    debug: bool | str = Field(default=False)
    debug_level: int | str = Field(default=1)
    include_debug: bool | str = Field(default=False)
    log_trace_id: Optional[str] = Field(default=None)

    @staticmethod
    def _coerce_min_int(value: Any, *, field_name: str, minimum: int, default: Optional[int]) -> Optional[int]:
        if value is None:
            return default
        if isinstance(value, str):
            value = value.strip()
            if value == "":
                return default
        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{field_name} must be an integer >= {minimum}") from exc
        if parsed < minimum:
            raise ValueError(f"{field_name} must be >= {minimum}")
        return parsed

    @field_validator("goal", mode="before")
    @classmethod
    def _coerce_goal(cls, value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            raise ValueError("goal must be non-empty")
        return text

    @field_validator("top_k", mode="before")
    @classmethod
    def _coerce_top_k(cls, value: Any) -> Optional[int]:
        return cls._coerce_min_int(value, field_name="top_k", minimum=1, default=None)

    @field_validator("max_iterations", mode="before")
    @classmethod
    def _coerce_max_iterations(cls, value: Any) -> int:
        parsed = cls._coerce_min_int(value, field_name="max_iterations", minimum=1, default=3)
        return 3 if parsed is None else parsed

    @field_validator("experiment_feedback", mode="before")
    @classmethod
    def _coerce_experiment_feedback(cls, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
        return value

    @field_validator("preference_feedback", "user_id", "log_trace_id", mode="before")
    @classmethod
    def _coerce_optional_text(cls, value: Any) -> Optional[str]:
        if value is None:
            return None
        text = value.strip() if isinstance(value, str) else str(value).strip()
        return text or None

    @field_validator("debug", "include_debug", "human_loop", mode="before")
    @classmethod
    def _coerce_bool_switch(cls, value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in _TRUE_STRINGS:
                return True
            if lowered in _FALSE_STRINGS:
                return False
        if isinstance(value, (int, float)):
            return bool(value)
        raise ValueError("boolean switch must be a boolean-like value")

    @field_validator("debug_level", mode="before")
    @classmethod
    def _coerce_debug_level(cls, value: Any) -> int:
        parsed = cls._coerce_min_int(value, field_name="debug_level", minimum=1, default=1)
        return 1 if parsed is None else parsed
