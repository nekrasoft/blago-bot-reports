from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Iterable
from zoneinfo import ZoneInfo

DRIVER_START_TIME_OPTIONS = ("08:30", "09:00", "09:30")
MOSCOW_TIME_ZONE = ZoneInfo("Europe/Moscow")


def to_moscow_time(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("Timezone-aware datetime is required")
    return value.astimezone(MOSCOW_TIME_ZONE)


def get_moscow_now() -> datetime:
    return to_moscow_time(datetime.now(timezone.utc))


def get_driver_end_time_options(now: datetime) -> list[str]:
    rounded = now.replace(minute=(now.minute // 10) * 10, second=0, microsecond=0)
    return [
        (rounded - timedelta(minutes=offset)).strftime("%H:%M")
        for offset in (20, 10, 0)
    ]


def get_driver_time_buttons(
    options: Iterable[str],
    prefix: str,
) -> list[tuple[str, str]]:
    return [(value, f"{prefix}:{value}") for value in options]
