"""Ukrainian context acquisition readiness before V11 candidate generation.

This module does not train a selector and does not create dispatch decisions.
It checks whether the V10 backfill requirements have source-backed,
prior-available Ukrainian context before V11 lower-tail-risk schedules are
allowed to start.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Final

import polars as pl

UA_CONTEXT_ACQUISITION_SOURCE_INVENTORY_CLAIM_SCOPE: Final[str] = (
    "dfl_ua_context_source_inventory_not_full_dfl"
)
UA_DAM_PUBLICATION_BACKFILL_CLAIM_SCOPE: Final[str] = (
    "dfl_ua_dam_publication_backfill_not_full_dfl"
)
UA_WEATHER_LOAD_PV_BACKFILL_CLAIM_SCOPE: Final[str] = (
    "dfl_ua_weather_load_pv_proxy_backfill_not_full_dfl"
)
UA_GRID_EVENT_BACKFILL_CLAIM_SCOPE: Final[str] = (
    "dfl_ua_grid_event_backfill_not_full_dfl"
)
UA_CALENDAR_BLOCK_BACKFILL_CLAIM_SCOPE: Final[str] = (
    "dfl_ua_calendar_block_context_backfill_not_full_dfl"
)
UA_CONTEXT_BACKFILL_COVERAGE_GATE_CLAIM_SCOPE: Final[str] = (
    "dfl_ua_context_backfill_coverage_gate_not_full_dfl"
)

_JOIN_COLUMNS: Final[tuple[str, str, str]] = (
    "tenant_id",
    "source_model_name",
    "anchor_timestamp",
)
_REQUIREMENT_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        *_JOIN_COLUMNS,
        "split_name",
        "context_backfill_decision",
        "dam_publication_timing_needed",
        "weather_load_pv_proxy_needed",
        "grid_outage_event_context_needed",
        "calendar_holiday_block_context_needed",
        "market_execution_enabled",
    }
)
_DST_CALENDAR_GAP_HOURS: Final[frozenset[datetime]] = frozenset(
    {
        datetime(2025, 3, 30, 23),
        datetime(2026, 3, 29, 23),
    }
)


def build_dfl_ua_context_source_inventory_frame(
    ua_context_backfill_requirements_frame: pl.DataFrame,
    *,
    price_context_frame: pl.DataFrame,
    weather_context_frame: pl.DataFrame,
    tenant_load_frame: pl.DataFrame,
    grid_event_signal_frame: pl.DataFrame,
    source_window_start: str,
    source_window_end: str,
) -> pl.DataFrame:
    """Summarize Ukrainian source families used by the acquisition gate."""

    _validate_requirements(ua_context_backfill_requirements_frame)
    required_rows = _required_rows(ua_context_backfill_requirements_frame)
    inventory_rows = [
        _inventory_row(
            source_family="oree_dam_publication",
            source_name="OREE DAM observed price history",
            frame=price_context_frame,
            required_anchor_rows=required_rows,
            source_window_start=source_window_start,
            source_window_end=source_window_end,
            publication_metadata_supported=_has_any_column(
                price_context_frame,
                (
                    "publication_timestamp",
                    "source_publication_timestamp",
                    "published_at",
                    "available_at",
                ),
            ),
        ),
        _inventory_row(
            source_family="open_meteo_archive_weather",
            source_name="Open-Meteo archive historical weather",
            frame=weather_context_frame,
            required_anchor_rows=required_rows,
            source_window_start=source_window_start,
            source_window_end=source_window_end,
            publication_metadata_supported=False,
        ),
        _inventory_row(
            source_family="tenant_load_pv_proxy",
            source_name="Tenant load/PV configured proxy",
            frame=tenant_load_frame,
            required_anchor_rows=required_rows,
            source_window_start=source_window_start,
            source_window_end=source_window_end,
            publication_metadata_supported=False,
        ),
        _inventory_row(
            source_family="ukrenergo_grid_event_history",
            source_name="Ukrenergo grid-event signal history",
            frame=grid_event_signal_frame,
            required_anchor_rows=required_rows,
            source_window_start=source_window_start,
            source_window_end=source_window_end,
            publication_metadata_supported=False,
        ),
        _inventory_row(
            source_family="ua_calendar_block_context",
            source_name="Ukraine calendar and block-time context",
            frame=ua_context_backfill_requirements_frame,
            required_anchor_rows=required_rows,
            source_window_start=source_window_start,
            source_window_end=source_window_end,
            publication_metadata_supported=True,
        ),
    ]
    return pl.DataFrame(inventory_rows, infer_schema_length=None).sort("source_family")


def build_dfl_ua_dam_publication_backfill_frame(
    ua_context_backfill_requirements_frame: pl.DataFrame,
    price_context_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Check explicit OREE DAM publication metadata for every required anchor."""

    _validate_requirements(ua_context_backfill_requirements_frame)
    rows: list[dict[str, Any]] = []
    for requirement in _requirement_rows(ua_context_backfill_requirements_frame):
        anchor = _datetime_value(requirement["anchor_timestamp"])
        price_row = _latest_row_before_anchor(price_context_frame, anchor_timestamp=anchor)
        publication_at = _explicit_publication_timestamp(price_row)
        prior_available = publication_at is not None and publication_at < anchor
        if price_row is None:
            status = "missing_oree_dam_price_history"
        elif publication_at is None:
            status = "missing_publication_time"
        elif not prior_available:
            status = "publication_not_prior_to_anchor"
        else:
            status = "context_ready"
        rows.append(
            {
                **_base_row(requirement),
                "required_for_v11_gate": _is_required(requirement)
                and bool(requirement["dam_publication_timing_needed"]),
                "prior_available": bool(prior_available),
                "dam_publication_backfill_status": status,
                "feature_available_timestamp": publication_at,
                "source_publication_timestamp": publication_at,
                "selector_feature_dam_publication_ready": float(prior_available),
                "selector_feature_hours_since_dam_publication": _hours_between(
                    publication_at,
                    anchor,
                    default=999.0,
                ),
                "context_source": "oree_dam_publication_metadata",
                "claim_scope": UA_DAM_PUBLICATION_BACKFILL_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
    return _sort_context_frame(rows)


def build_dfl_ua_weather_load_pv_proxy_backfill_frame(
    ua_context_backfill_requirements_frame: pl.DataFrame,
    weather_context_frame: pl.DataFrame,
    tenant_load_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Check prior Open-Meteo/weather and tenant load/PV proxy coverage."""

    _validate_requirements(ua_context_backfill_requirements_frame)
    rows: list[dict[str, Any]] = []
    for requirement in _requirement_rows(ua_context_backfill_requirements_frame):
        tenant_id = str(requirement["tenant_id"])
        anchor = _datetime_value(requirement["anchor_timestamp"])
        weather_row = _latest_row_before_anchor(
            weather_context_frame,
            tenant_id=tenant_id,
            anchor_timestamp=anchor,
        )
        load_row = _latest_row_before_anchor(
            tenant_load_frame,
            tenant_id=tenant_id,
            anchor_timestamp=anchor,
        )
        weather_at = _row_timestamp(weather_row)
        load_at = _row_timestamp(load_row)
        weather_source_backed = _source_kind_is_backed(
            weather_row,
            ("weather_source_kind", "source_kind"),
        )
        load_source_backed = load_row is not None
        prior_available = bool(
            weather_row is not None
            and load_row is not None
            and weather_at is not None
            and load_at is not None
            and weather_at < anchor
            and load_at < anchor
            and weather_source_backed
            and load_source_backed
        )
        if weather_row is None:
            status = "missing_prior_weather_history"
        elif not weather_source_backed:
            status = "weather_history_not_source_backed"
        elif load_row is None:
            status = "missing_load_pv_proxy"
        elif not prior_available:
            status = "weather_load_not_prior_to_anchor"
        else:
            status = "context_ready"
        rows.append(
            {
                **_base_row(requirement),
                "required_for_v11_gate": _is_required(requirement)
                and bool(requirement["weather_load_pv_proxy_needed"]),
                "prior_available": prior_available,
                "weather_load_pv_backfill_status": status,
                "feature_available_timestamp": max(
                    [value for value in (weather_at, load_at) if value is not None],
                    default=None,
                ),
                "selector_feature_weather_load_pv_ready": float(prior_available),
                "selector_feature_weather_temperature_c": _row_float(
                    weather_row,
                    ("weather_temperature", "temperature"),
                ),
                "selector_feature_weather_effective_solar": _row_float(
                    weather_row,
                    ("weather_effective_solar", "effective_solar"),
                ),
                "selector_feature_net_load_mw": _row_float(
                    load_row,
                    ("net_load_mw", "load_mw", "consumption_mw"),
                ),
                "selector_feature_pv_estimate_mw": _row_float(
                    load_row,
                    ("pv_estimate_mw", "solar_generation_mw"),
                ),
                "context_source": "open_meteo_archive_plus_tenant_load_proxy",
                "claim_scope": UA_WEATHER_LOAD_PV_BACKFILL_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
    return _sort_context_frame(rows)


def build_dfl_ua_grid_event_backfill_frame(
    ua_context_backfill_requirements_frame: pl.DataFrame,
    grid_event_signal_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Check prior Ukrenergo grid-event coverage or source-backed no-event rows."""

    _validate_requirements(ua_context_backfill_requirements_frame)
    rows: list[dict[str, Any]] = []
    for requirement in _requirement_rows(ua_context_backfill_requirements_frame):
        tenant_id = str(requirement["tenant_id"])
        anchor = _datetime_value(requirement["anchor_timestamp"])
        grid_row = _latest_row_before_anchor(
            grid_event_signal_frame,
            tenant_id=tenant_id,
            anchor_timestamp=anchor,
        )
        grid_at = _row_timestamp(grid_row)
        source_covers_anchor = _grid_source_covers_anchor(grid_row, anchor)
        prior_available = bool(
            grid_row is not None
            and grid_at is not None
            and grid_at < anchor
            and source_covers_anchor
        )
        if grid_row is None:
            status = "missing_grid_event_history"
        elif not source_covers_anchor:
            status = "missing_grid_event_history_source_window"
        elif not prior_available:
            status = "grid_event_history_not_prior_to_anchor"
        else:
            status = "context_ready"
        rows.append(
            {
                **_base_row(requirement),
                "required_for_v11_gate": _is_required(requirement)
                and bool(requirement["grid_outage_event_context_needed"]),
                "prior_available": prior_available,
                "grid_event_backfill_status": status,
                "feature_available_timestamp": grid_at,
                "selector_feature_grid_event_context_ready": float(prior_available),
                "selector_feature_grid_event_count_24h": _row_float(
                    grid_row,
                    ("grid_event_count_24h",),
                ),
                "selector_feature_national_grid_risk_score": _row_float(
                    grid_row,
                    ("national_grid_risk_score",),
                ),
                "selector_feature_event_source_freshness_hours": _row_float(
                    grid_row,
                    ("event_source_freshness_hours",),
                    default=999.0,
                ),
                "source_coverage_start_timestamp": _row_datetime(
                    grid_row,
                    ("source_coverage_start_timestamp",),
                ),
                "source_coverage_end_timestamp": _row_datetime(
                    grid_row,
                    ("source_coverage_end_timestamp",),
                ),
                "context_source": "ukrenergo_grid_event_signal_history",
                "claim_scope": UA_GRID_EVENT_BACKFILL_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
    return _sort_context_frame(rows)


def build_dfl_ua_calendar_block_context_backfill_frame(
    ua_context_backfill_requirements_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Build deterministic calendar/block context while preserving DST exclusions."""

    _validate_requirements(ua_context_backfill_requirements_frame)
    rows: list[dict[str, Any]] = []
    for requirement in _requirement_rows(ua_context_backfill_requirements_frame):
        anchor = _datetime_value(requirement["anchor_timestamp"])
        is_gap = anchor in _DST_CALENDAR_GAP_HOURS
        prior_available = not is_gap
        rows.append(
            {
                **_base_row(requirement),
                "required_for_v11_gate": _is_required(requirement)
                and bool(requirement["calendar_holiday_block_context_needed"]),
                "prior_available": prior_available,
                "calendar_block_backfill_status": (
                    "dst_calendar_gap_excluded" if is_gap else "context_ready"
                ),
                "feature_available_timestamp": anchor,
                "selector_feature_anchor_hour": float(anchor.hour),
                "selector_feature_anchor_day_of_week": float(anchor.weekday()),
                "selector_feature_anchor_is_weekend": float(anchor.weekday() >= 5),
                "selector_feature_morning_block": float(6 <= anchor.hour <= 10),
                "selector_feature_evening_block": float(17 <= anchor.hour <= 22),
                "selector_feature_calendar_block_context_ready": float(
                    prior_available
                ),
                "context_source": "ua_calendar_block_context",
                "claim_scope": UA_CALENDAR_BLOCK_BACKFILL_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
    return _sort_context_frame(rows)


def build_dfl_ua_context_backfill_coverage_gate_frame(
    ua_context_backfill_requirements_frame: pl.DataFrame,
    dam_publication_backfill_frame: pl.DataFrame,
    weather_load_pv_backfill_frame: pl.DataFrame,
    grid_event_backfill_frame: pl.DataFrame,
    calendar_block_context_backfill_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Decide whether V11 candidate generation may start."""

    _validate_requirements(ua_context_backfill_requirements_frame)
    dam_by_key = _context_by_key(dam_publication_backfill_frame)
    weather_by_key = _context_by_key(weather_load_pv_backfill_frame)
    grid_by_key = _context_by_key(grid_event_backfill_frame)
    calendar_by_key = _context_by_key(calendar_block_context_backfill_frame)
    rows: list[dict[str, Any]] = []
    for requirement in _requirement_rows(ua_context_backfill_requirements_frame):
        key = _anchor_key(requirement)
        required = _is_required(requirement)
        blockers: list[str] = []
        if required and bool(requirement["dam_publication_timing_needed"]):
            _append_blocker(
                blockers,
                "dam_publication",
                dam_by_key.get(key),
                "dam_publication_backfill_status",
            )
        if required and bool(requirement["weather_load_pv_proxy_needed"]):
            _append_blocker(
                blockers,
                "weather_load_pv",
                weather_by_key.get(key),
                "weather_load_pv_backfill_status",
            )
        if required and bool(requirement["grid_outage_event_context_needed"]):
            _append_blocker(
                blockers,
                "grid_event",
                grid_by_key.get(key),
                "grid_event_backfill_status",
            )
        if required and bool(requirement["calendar_holiday_block_context_needed"]):
            _append_blocker(
                blockers,
                "calendar_block",
                calendar_by_key.get(key),
                "calendar_block_backfill_status",
            )
        ready = required and not blockers
        rows.append(
            {
                **_base_row(requirement),
                "required_for_v11_gate": required,
                "v11_candidate_generation_ready": ready,
                "context_backfill_gate_decision": (
                    "context_backfill_ready" if ready else "data_acquisition_needed"
                ),
                "blocking_context_families": ",".join(blockers)
                if blockers
                else "none",
                "blocked_context_family_count": len(blockers),
                "dam_publication_ready": _ready(dam_by_key.get(key)),
                "weather_load_pv_ready": _ready(weather_by_key.get(key)),
                "grid_event_ready": _ready(grid_by_key.get(key)),
                "calendar_block_ready": _ready(calendar_by_key.get(key)),
                "dt_lava_ready": False,
                "target_label_space": "v11_precondition_context_coverage",
                "raw_hourly_action_imitation": False,
                "claim_scope": UA_CONTEXT_BACKFILL_COVERAGE_GATE_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "no_eu_rows_as_ukrainian_targets": True,
                "market_execution_enabled": False,
            }
        )
    return _sort_context_frame(rows)


def _inventory_row(
    *,
    source_family: str,
    source_name: str,
    frame: pl.DataFrame,
    required_anchor_rows: int,
    source_window_start: str,
    source_window_end: str,
    publication_metadata_supported: bool,
) -> dict[str, Any]:
    return {
        "source_family": source_family,
        "source_name": source_name,
        "source_backed": bool(frame.height > 0 or source_family == "ua_calendar_block_context"),
        "source_rows": frame.height,
        "required_anchor_rows": required_anchor_rows,
        "source_window_start": source_window_start,
        "source_window_end": source_window_end,
        "publication_metadata_supported": publication_metadata_supported,
        "prior_timestamp_supported": "timestamp" in frame.columns
        or source_family == "ua_calendar_block_context",
        "training_use_scope": "prior_only_context_readiness",
        "claim_scope": UA_CONTEXT_ACQUISITION_SOURCE_INVENTORY_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
        "no_eu_rows_as_ukrainian_targets": True,
        "market_execution_enabled": False,
    }


def _validate_requirements(frame: pl.DataFrame) -> None:
    missing_columns = _REQUIREMENT_COLUMNS.difference(frame.columns)
    if missing_columns:
        raise ValueError(
            "ua_context_backfill_requirements_frame is missing required columns: "
            f"{sorted(missing_columns)}"
        )
    if frame.filter(pl.col("market_execution_enabled")).height:
        raise ValueError("UA context acquisition requires market_execution_enabled=false.")


def _requirement_rows(frame: pl.DataFrame) -> list[dict[str, Any]]:
    return list(frame.iter_rows(named=True))


def _required_rows(frame: pl.DataFrame) -> int:
    return sum(1 for row in _requirement_rows(frame) if _is_required(row))


def _is_required(row: dict[str, Any]) -> bool:
    return bool(row.get("requires_new_ukrainian_context_rows", False)) or str(
        row.get("context_backfill_decision", "")
    ) == "data_acquisition_needed"


def _base_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "tenant_id": str(row["tenant_id"]),
        "source_model_name": str(row["source_model_name"]),
        "anchor_timestamp": _datetime_value(row["anchor_timestamp"]),
        "split_name": str(row["split_name"]),
        "anchor_key": str(
            row.get(
                "anchor_key",
                "|".join(
                    (
                        str(row["tenant_id"]),
                        str(row["source_model_name"]),
                        _datetime_value(row["anchor_timestamp"]).isoformat(),
                    )
                ),
            )
        ),
    }


def _context_by_key(frame: pl.DataFrame) -> dict[tuple[str, str, datetime], dict[str, Any]]:
    if frame.height == 0:
        return {}
    missing_columns = set(_JOIN_COLUMNS).difference(frame.columns)
    if missing_columns:
        raise ValueError(f"context frame is missing columns: {sorted(missing_columns)}")
    return {_anchor_key(row): row for row in frame.iter_rows(named=True)}


def _anchor_key(row: dict[str, Any]) -> tuple[str, str, datetime]:
    return (
        str(row["tenant_id"]),
        str(row["source_model_name"]),
        _datetime_value(row["anchor_timestamp"]),
    )


def _append_blocker(
    blockers: list[str],
    family: str,
    row: dict[str, Any] | None,
    status_column: str,
) -> None:
    if row is None:
        blockers.append(f"{family}:missing_context_row")
        return
    if not _ready(row):
        blockers.append(f"{family}:{row.get(status_column, 'not_ready')}")


def _ready(row: dict[str, Any] | None) -> bool:
    return bool(row is not None and row.get("prior_available", False))


def _latest_row_before_anchor(
    frame: pl.DataFrame,
    *,
    anchor_timestamp: datetime,
    tenant_id: str | None = None,
) -> dict[str, Any] | None:
    if frame.height == 0 or "timestamp" not in frame.columns:
        return None
    filtered = frame
    if tenant_id is not None and "tenant_id" in filtered.columns:
        filtered = filtered.filter(pl.col("tenant_id") == tenant_id)
    rows = [
        row
        for row in filtered.iter_rows(named=True)
        if (row_timestamp := _row_timestamp(row)) is not None
        and row_timestamp < anchor_timestamp
    ]
    if not rows:
        return None
    return max(rows, key=lambda row: _row_timestamp(row) or datetime.min)


def _explicit_publication_timestamp(row: dict[str, Any] | None) -> datetime | None:
    if row is None:
        return None
    for column in (
        "publication_timestamp",
        "source_publication_timestamp",
        "published_at",
        "available_at",
    ):
        value = row.get(column)
        if value is not None:
            return _datetime_value(value)
    return None


def _row_timestamp(row: dict[str, Any] | None) -> datetime | None:
    if row is None:
        return None
    for column in ("timestamp", "feature_available_timestamp", "published_at"):
        value = row.get(column)
        if value is not None:
            return _datetime_value(value)
    return None


def _datetime_value(value: Any) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return value.replace(tzinfo=None)
        return value
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is not None:
            return parsed.replace(tzinfo=None)
        return parsed
    raise TypeError(f"Expected datetime-compatible value, got {type(value).__name__}.")


def _row_float(
    row: dict[str, Any] | None,
    columns: tuple[str, ...],
    *,
    default: float = 0.0,
) -> float:
    if row is None:
        return default
    for column in columns:
        value = row.get(column)
        if value is not None:
            return float(value)
    return default


def _row_datetime(row: dict[str, Any] | None, columns: tuple[str, ...]) -> datetime | None:
    if row is None:
        return None
    for column in columns:
        value = row.get(column)
        if value is not None:
            return _datetime_value(value)
    return None


def _grid_source_covers_anchor(row: dict[str, Any] | None, anchor: datetime) -> bool:
    if row is None:
        return False
    coverage_start = _row_datetime(row, ("source_coverage_start_timestamp",))
    coverage_end = _row_datetime(row, ("source_coverage_end_timestamp",))
    return bool(
        coverage_start is not None
        and coverage_end is not None
        and coverage_start <= anchor <= coverage_end
    )


def _source_kind_is_backed(row: dict[str, Any] | None, columns: tuple[str, ...]) -> bool:
    if row is None:
        return False
    for column in columns:
        value = row.get(column)
        if value is None:
            continue
        source_kind = str(value).casefold()
        return any(
            token in source_kind
            for token in ("observed", "historical", "open_meteo", "oree")
        )
    return False


def _hours_between(start: datetime | None, end: datetime, *, default: float) -> float:
    if start is None:
        return default
    return max(0.0, (end - start).total_seconds() / 3600.0)


def _has_any_column(frame: pl.DataFrame, columns: tuple[str, ...]) -> bool:
    return any(column in frame.columns for column in columns)


def _sort_context_frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows, infer_schema_length=None).sort(list(_JOIN_COLUMNS))


__all__ = [
    "UA_CALENDAR_BLOCK_BACKFILL_CLAIM_SCOPE",
    "UA_CONTEXT_ACQUISITION_SOURCE_INVENTORY_CLAIM_SCOPE",
    "UA_CONTEXT_BACKFILL_COVERAGE_GATE_CLAIM_SCOPE",
    "UA_DAM_PUBLICATION_BACKFILL_CLAIM_SCOPE",
    "UA_GRID_EVENT_BACKFILL_CLAIM_SCOPE",
    "UA_WEATHER_LOAD_PV_BACKFILL_CLAIM_SCOPE",
    "build_dfl_ua_calendar_block_context_backfill_frame",
    "build_dfl_ua_context_backfill_coverage_gate_frame",
    "build_dfl_ua_context_source_inventory_frame",
    "build_dfl_ua_dam_publication_backfill_frame",
    "build_dfl_ua_grid_event_backfill_frame",
    "build_dfl_ua_weather_load_pv_proxy_backfill_frame",
]
