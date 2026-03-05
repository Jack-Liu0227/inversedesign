from __future__ import annotations

import json
from typing import Any


def decode_maybe_double_json(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list, int, float, bool)):
        return value
    if not isinstance(value, str):
        return value

    text = value.strip()
    if not text:
        return None

    # Avoid forcing plain text into JSON parsing. Only parse when it looks
    # like JSON object/array/string literals.
    looks_like_json = (
        (text.startswith("{") and text.endswith("}"))
        or (text.startswith("[") and text.endswith("]"))
        or (text.startswith("\"") and text.endswith("\""))
    )
    if not looks_like_json:
        return value

    try:
        first = json.loads(text)
    except json.JSONDecodeError:
        return value

    if isinstance(first, str):
        try:
            return json.loads(first)
        except json.JSONDecodeError:
            return first
    return first


def format_json(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, indent=2)
    except TypeError:
        return str(value)
