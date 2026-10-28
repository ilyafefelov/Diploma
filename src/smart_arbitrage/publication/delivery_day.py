"""Kyiv delivery intervals, keyed by UTC instants to preserve repeated hours."""

from datetime import UTC, date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

KYIV = ZoneInfo("Europe/Kyiv")


def delivery_intervals(day: date) -> list[datetime]:
    start = datetime.combine(day, datetime.min.time(), KYIV).astimezone(UTC)
    end = datetime.combine(
        day + timedelta(days=1), datetime.min.time(), KYIV
    ).astimezone(UTC)
    return [
        start + timedelta(hours=hour)
        for hour in range(int((end - start).total_seconds() // 3600))
    ]


def delivery_timestamp(value: Any) -> datetime:
    """Accept legacy unambiguous Kyiv wall times; require offsets across DST gaps/folds."""
    if isinstance(value, str) and value.strip():
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime):
        raise TypeError("timestamp must be a datetime or ISO datetime string")
    if value.tzinfo is not None:
        return value.astimezone(UTC)
    candidates = {
        local.astimezone(UTC)
        for fold in (0, 1)
        if (local := value.replace(tzinfo=KYIV, fold=fold))
        .astimezone(UTC)
        .astimezone(KYIV)
        .replace(tzinfo=None)
        == value
    }
    if len(candidates) != 1:
        raise ValueError(
            "ambiguous or nonexistent Kyiv delivery timestamp requires an explicit UTC offset"
        )
    return candidates.pop()


def delivery_iso(value: datetime) -> str:
    return value.astimezone(KYIV).isoformat()
