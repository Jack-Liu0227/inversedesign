from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

BEIJING_TZ = ZoneInfo("Asia/Shanghai")


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def to_beijing_time(value: Any) -> Any:
    dt = _parse_datetime(value)
    if dt is None:
        return value
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    local_dt = dt.astimezone(BEIJING_TZ)
    return local_dt.strftime("%Y-%m-%d %H:%M:%S")


def _looks_like_time_field(name: str) -> bool:
    key = str(name or "").strip().lower()
    if not key:
        return False
    if key.endswith("_at") or key.endswith("_time"):
        return True
    return key in {"timestamp", "time", "created", "updated"}


def normalize_row_datetimes(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    for key, value in out.items():
        if _looks_like_time_field(str(key)):
            out[key] = to_beijing_time(value)
    return out


def parse_beijing_datetime_local(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=BEIJING_TZ)
        except ValueError:
            continue
    parsed = _parse_datetime(text)
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=BEIJING_TZ)
    return parsed.astimezone(BEIJING_TZ)


def to_utc_sql_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def beijing_range_to_utc_sql(
    *,
    created_from: str = "",
    created_to: str = "",
) -> tuple[str, str]:
    utc_from = ""
    utc_to_exclusive = ""
    from_dt = parse_beijing_datetime_local(created_from)
    to_dt = parse_beijing_datetime_local(created_to)
    if from_dt is not None:
        utc_from = to_utc_sql_text(from_dt)
    if to_dt is not None:
        utc_to_exclusive = to_utc_sql_text(to_dt + timedelta(minutes=1))
    return utc_from, utc_to_exclusive
