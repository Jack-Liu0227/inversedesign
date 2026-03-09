from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Any

_NUMERIC_WITH_OPTIONAL_UNIT_RE = re.compile(
    r"^\s*(?P<num>[+-]?(?:(?:\d{1,3}(?:,\d{3})+)|\d+)(?:\.\d+)?(?:[eE][+-]?\d+)?)\s*(?:[%a-zA-Z_/.-]+)?\s*$"
)


def normalize_sort_order(raw_order: str, *, default: str = "desc") -> str:
    normalized = str(raw_order or "").strip().lower()
    if normalized in {"asc", "desc"}:
        return normalized
    return "asc" if str(default or "").strip().lower() == "asc" else "desc"


def auto_sort_tuple(value: Any) -> tuple[int, int, float, str]:
    if value is None:
        return (1, 2, 0.0, "")

    if isinstance(value, bool):
        return (0, 0, float(int(value)), "")
    if isinstance(value, (int, float)):
        return (0, 0, float(value), "")

    text = str(value).strip()
    if text == "":
        return (1, 2, 0.0, "")

    matched = _NUMERIC_WITH_OPTIONAL_UNIT_RE.match(text)
    numeric_text = matched.group("num") if matched else text
    numeric_text = numeric_text.replace(",", "")
    try:
        numeric = float(Decimal(numeric_text))
        return (0, 0, numeric, "")
    except (InvalidOperation, ValueError):
        return (0, 1, 0.0, text.lower())


def sqlite_smart_order_clause(*, value_expr: str, sort_order: str, tie_breaker_expr: str = "rowid") -> str:
    text_expr = f"TRIM(CAST({value_expr} AS TEXT))"
    is_numeric_expr = (
        f"({text_expr} <> '' AND {text_expr} NOT GLOB '*[^0-9eE+.-]*' AND {text_expr} GLOB '*[0-9]*')"
    )
    sql_order = "ASC" if normalize_sort_order(sort_order, default="desc") == "asc" else "DESC"
    return (
        "ORDER BY "
        f"CASE WHEN {text_expr} = '' THEN 1 ELSE 0 END ASC, "
        f"CASE WHEN {is_numeric_expr} THEN 0 ELSE 1 END ASC, "
        f"CASE WHEN {is_numeric_expr} THEN CAST({text_expr} AS REAL) END {sql_order}, "
        f"LOWER({text_expr}) {sql_order}, "
        f"{tie_breaker_expr} {sql_order}"
    )
