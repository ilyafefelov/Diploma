"""Leakage-safe Silver features from public grid-event text signals."""

from __future__ import annotations

from datetime import datetime, timedelta
from collections.abc import Mapping
from typing import Final
from zoneinfo import ZoneInfo

import polars as pl

from smart_arbitrage.assets.gold.baseline_solver import DEFAULT_TIMESTAMP_COLUMN

KYIV_TZ: Final[ZoneInfo] = ZoneInfo("Europe/Kyiv")

TENANT_REGION_BY_ID: Final[dict[str, str]] = {
    "client_001_kyiv_mall": "Kyiv",
    "client_002_lviv_office": "Lviv",
    "client_003_dnipro_factory": "Dnipropetrovsk",
    "client_004_kharkiv_hospital": "Kharkiv",
    "client_005_odesa_hotel": "Odesa",
}

GRID_EVENT_FEATURE_DEFAULTS: Final[dict[str, float]] = {
    "grid_event_count_24h": 0.0,
    "tenant_region_affected": 0.0,
    "national_grid_risk_score": 0.0,
    "days_since_grid_event": 999.0,
    "outage_flag": 0.0,
    "saving_request_flag": 0.0,
    "solar_shift_hint": 0.0,
    "event_source_freshness_hours": 999.0,
}

GRID_EVENT_FEATURE_COLUMNS: Final[tuple[str, ...]] = tuple(GRID_EVENT_FEATURE_DEFAULTS)


def build_grid_event_signal_frame(
    *,
    price_history: pl.DataFrame,
    grid_events: pl.DataFrame,
    tenant_ids: list[str] | None = None,
    timestamp_column: str = DEFAULT_TIMESTAMP_COLUMN,
) -> pl.DataFrame:
    """Build tenant-hour event features using only events published at or before each hour."""

    if timestamp_column not in price_history.columns:
        raise ValueError(f"price_history is missing {timestamp_column}.")
    timestamp_values = _timestamp_values(price_history, timestamp_column=timestamp_column)
    resolved_tenant_ids = tenant_ids if tenant_ids is not None else list(TENANT_REGION_BY_ID)
    event_rows = _grid_event_rows(grid_events)
    rows: list[dict[str, object]] = []
    for timestamp in timestamp_values:
        for tenant_id in resolved_tenant_ids:
            rows.append(
                {
                    "tenant_id": tenant_id,
                    "timestamp": timestamp,
                    **_features_for_tenant_hour(
                        timestamp=timestamp,
                        tenant_id=tenant_id,
                        event_rows=event_rows,
                    ),
                }
            )
    if not rows:
        return _empty_grid_event_signal_frame()
    return pl.DataFrame(rows).sort(["tenant_id", "timestamp"])


def _features_for_tenant_hour(
    *,
    timestamp: datetime,
    tenant_id: str,
    event_rows: list[dict[str, object]],
) -> dict[str, float]:
    operational_events = [event for event in event_rows if is_operational_grid_event_row(event)]
    known_events = [event for event in operational_events if _event_timestamp(event) <= timestamp]
    if not known_events:
        return dict(GRID_EVENT_FEATURE_DEFAULTS)

    window_start = timestamp - timedelta(hours=24)
    window_events = [event for event in known_events if _event_timestamp(event) >= window_start]
    latest_event = max(known_events, key=_event_timestamp)
    tenant_region = TENANT_REGION_BY_ID.get(tenant_id)
    return {
        "grid_event_count_24h": float(len(window_events)),
        "tenant_region_affected": float(
            any(_event_affects_region(event, tenant_region=tenant_region) for event in window_events)
        ),
        "national_grid_risk_score": _window_risk_score(window_events),
        "days_since_grid_event": _hours_since(timestamp, _event_timestamp(latest_event)) / 24.0,
        "outage_flag": float(any(bool(event["outage_or_restriction"]) for event in window_events)),
        "saving_request_flag": float(any(bool(event["evening_saving_request"]) for event in window_events)),
        "solar_shift_hint": float(any(bool(event["solar_shift_advice"]) for event in window_events)),
        "event_source_freshness_hours": _hours_since(timestamp, _event_timestamp(latest_event)),
    }


def _grid_event_rows(grid_events: pl.DataFrame) -> list[dict[str, object]]:
    if grid_events.height == 0:
        return []
    required_columns = {
        "published_at",
        "energy_system_status",
        "shelling_damage",
        "outage_or_restriction",
        "solar_shift_advice",
        "evening_saving_request",
        "affected_oblasts",
    }
    missing_columns = required_columns.difference(grid_events.columns)
    if missing_columns:
        raise ValueError(f"grid_events is missing required columns: {sorted(missing_columns)}")
    rows = []
    for row in grid_events.iter_rows(named=True):
        published_at = row["published_at"]
        if not isinstance(published_at, datetime):
            raise TypeError("grid_events published_at must contain datetime values.")
        rows.append({**row, "published_at": _to_naive_kyiv(published_at)})
    return sorted(rows, key=_event_timestamp)


def _timestamp_values(price_history: pl.DataFrame, *, timestamp_column: str) -> list[datetime]:
    timestamps: list[datetime] = []
    for value in price_history.select(timestamp_column).to_series().to_list():
        if not isinstance(value, datetime):
            raise TypeError(f"{timestamp_column} must contain datetime values.")
        timestamps.append(_to_naive_kyiv(value))
    return sorted(set(timestamps))


def _to_naive_kyiv(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=None)
    return value.astimezone(KYIV_TZ).replace(tzinfo=None)


def _event_timestamp(event: dict[str, object]) -> datetime:
    published_at = event["published_at"]
    if not isinstance(published_at, datetime):
        raise TypeError("event published_at must be datetime.")
    return published_at


def _event_affects_region(event: dict[str, object], *, tenant_region: str | None) -> bool:
    if tenant_region is None:
        return False
    affected_oblasts = event["affected_oblasts"]
    if not isinstance(affected_oblasts, list):
        return False
    return tenant_region in affected_oblasts


def is_operational_grid_event_row(event: Mapping[str, object]) -> bool:
    consumption_change = event.get("consumption_change")
    return any(
        (
            bool(event.get("energy_system_status")),
            bool(event.get("shelling_damage")),
            bool(event.get("outage_or_restriction")),
            bool(event.get("solar_shift_advice")),
            bool(event.get("evening_saving_request")),
            consumption_change in {"increased", "decreased", "stable"},
        )
    )


def _window_risk_score(window_events: list[dict[str, object]]) -> float:
    if not window_events:
        return 0.0
    return max(_event_risk_score(event) for event in window_events)


def _event_risk_score(event: dict[str, object]) -> float:
    score = 0.0
    if bool(event["energy_system_status"]):
        score += 0.10
    if bool(event["shelling_damage"]):
        score += 0.45
    if bool(event["outage_or_restriction"]):
        score += 0.30
    if bool(event["evening_saving_request"]):
        score += 0.10
    if bool(event["solar_shift_advice"]):
        score += 0.05
    return min(1.0, score)


def _hours_since(timestamp: datetime, event_timestamp: datetime) -> float:
    return max(0.0, (timestamp - event_timestamp).total_seconds() / 3600.0)


def _empty_grid_event_signal_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "tenant_id": pl.Utf8,
            "timestamp": pl.Datetime,
            **{column_name: pl.Float64 for column_name in GRID_EVENT_FEATURE_COLUMNS},
        }
    )


__all__ = [
    "GRID_EVENT_FEATURE_COLUMNS",
    "GRID_EVENT_FEATURE_DEFAULTS",
    "TENANT_REGION_BY_ID",
    "build_grid_event_signal_frame",
    "is_operational_grid_event_row",
]
