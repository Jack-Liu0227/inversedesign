from __future__ import annotations

import logging
import os
from typing import Literal

from agno.utils.log import (
    set_log_level_to_debug,
    set_log_level_to_error,
    set_log_level_to_info,
    set_log_level_to_warning,
)


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _normalize_level(level: str) -> str:
    lowered = (level or "INFO").strip().upper()
    return lowered if lowered in {"DEBUG", "INFO", "WARNING", "ERROR"} else "INFO"


def _set_agno_level_all(level_fn, *, debug_level: int = 1) -> None:
    if level_fn is set_log_level_to_debug:
        level_fn(level=debug_level)
        level_fn(source_type="workflow", level=debug_level)
        level_fn(source_type="team", level=debug_level)
        return
    level_fn()
    level_fn(source_type="workflow")
    level_fn(source_type="team")


def configure_app_logging() -> str:
    level_name = _normalize_level(os.getenv("APP_LOG_LEVEL", "INFO"))
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    for logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logging.getLogger(logger_name).setLevel(level)
    _apply_agno_level(level_name)
    return level_name


def _apply_agno_level(level_name: str) -> None:
    if level_name == "DEBUG":
        _set_agno_level_all(set_log_level_to_debug, debug_level=1)
    elif level_name == "WARNING":
        _set_agno_level_all(set_log_level_to_warning)
    elif level_name == "ERROR":
        _set_agno_level_all(set_log_level_to_error)
    else:
        _set_agno_level_all(set_log_level_to_info)


def apply_request_debug_mode(debug: bool, debug_level: Literal[1, 2] = 1) -> None:
    if debug:
        _set_agno_level_all(set_log_level_to_debug, debug_level=debug_level)
        return
    app_level = _normalize_level(os.getenv("APP_LOG_LEVEL", "INFO"))
    _apply_agno_level(app_level)


def should_force_tracing() -> bool:
    return _env_bool("APP_FORCE_TRACING", False)
