"""Tenant consumption schedule and net-load feature builders."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Final
from zoneinfo import ZoneInfo

import polars as pl

DAY_INDEX_BY_NAME: Final[dict[str, int]] = {
    "mon": 0,
    "tue": 1,
    "wed": 2,
    "thu": 3,
    "fri": 4,
    "sat": 5,
    "sun": 6,
}
ALL_DAYS: Final[tuple[str, ...]] = tuple(DAY_INDEX_BY_NAME)
TENANT_CONSUMPTION_SCHEDULE_COLUMNS: Final[tuple[str, ...]] = (
    "tenant_id",
    "tenant_name",
    "tenant_type",
    "timezone",
    "profile_label",
    "days_csv",
    "start_minute",
    "end_minute",
    "load_multiplier",
    "off_hours_multiplier",
    "battery_support_fraction",
    "base_load_kw",
    "peak_load_kw",
    "solar_capacity_kw",
    "source_kind",
    "reason_code",
)
TENANT_NET_LOAD_HOURLY_COLUMNS: Final[tuple[str, ...]] = (
    "tenant_id",
    "timestamp",
    "forecast_anchor",
    "timezone",
    "profile_label",
    "load_mw",
    "pv_estimate_mw",
    "net_load_mw",
    "btm_battery_power_mw",
    "source_kind",
    "weather_source_kind",
    "reason_code",
)
TENANT_HISTORICAL_NET_LOAD_COLUMNS: Final[tuple[str, ...]] = (
    "tenant_id",
    "timestamp",
    "timezone",
    "profile_label",
    "load_mw",
    "pv_estimate_mw",
    "net_load_mw",
    "btm_battery_power_mw",
    "source_kind",
    "weather_source_kind",
    "reason_code",
    "claim_scope",
    "not_full_dfl",
    "not_market_execution",
)


@dataclass(frozen=True, slots=True)
class TenantScheduleState:
    tenant_id: str
    timestamp: datetime
    timezone: str
    profile_label: str
    load_multiplier: float
    off_hours_multiplier: float
    battery_support_fraction: float
    base_load_kw: float
    peak_load_kw: float
    solar_capacity_kw: float
    reason_code: str


def build_tenant_consumption_schedule_frame(*, location_config_path: str | None = None) -> pl.DataFrame:
    from smart_arbitrage.assets.bronze.market_weather import (
        list_available_weather_tenants,
        resolve_tenant_registry_entry,
    )

    rows: list[dict[str, Any]] = []
    for tenant_summary in list_available_weather_tenants(location_config_path=location_config_path):
        tenant_id = tenant_summary.get("tenant_id")
        if tenant_id is None:
            continue
        tenant_entry = resolve_tenant_registry_entry(
            tenant_id=str(tenant_id),
            location_config_path=location_config_path,
        )
        rows.extend(_schedule_rows_from_tenant_entry(tenant_entry))

    if not rows:
        return pl.DataFrame(schema={column_name: pl.Null for column_name in TENANT_CONSUMPTION_SCHEDULE_COLUMNS})
    return pl.DataFrame(rows).select(TENANT_CONSUMPTION_SCHEDULE_COLUMNS).sort(["tenant_id", "start_minute"])


def resolve_tenant_schedule_state(
    schedule_frame: pl.DataFrame,
    *,
    tenant_id: str,
    timestamp: datetime,
) -> TenantScheduleState:
    if schedule_frame.height == 0:
        raise ValueError("schedule_frame must contain tenant schedule rows.")
    missing_columns = set(TENANT_CONSUMPTION_SCHEDULE_COLUMNS).difference(schedule_frame.columns)
    if missing_columns:
        raise ValueError(f"schedule_frame is missing required columns: {sorted(missing_columns)}")

    tenant_rows = [row for row in schedule_frame.iter_rows(named=True) if str(row["tenant_id"]) == tenant_id]
    if not tenant_rows:
        raise ValueError(f"Missing consumption schedule for tenant_id={tenant_id}.")

    local_timestamp = _to_local_datetime(timestamp, str(tenant_rows[0]["timezone"]))
    local_minute = local_timestamp.hour * 60 + local_timestamp.minute
    day_index = local_timestamp.weekday()
    matched_row = next(
        (
            row
            for row in tenant_rows
            if _day_index_matches(row["days_csv"], day_index)
            and _minute_matches_interval(
                local_minute,
                start_minute=int(row["start_minute"]),
                end_minute=int(row["end_minute"]),
            )
        ),
        None,
    )
    row = matched_row or tenant_rows[0]
    return TenantScheduleState(
        tenant_id=tenant_id,
        timestamp=timestamp,
        timezone=str(row["timezone"]),
        profile_label=str(row["profile_label"]),
        load_multiplier=float(row["load_multiplier"] if matched_row is not None else row["off_hours_multiplier"]),
        off_hours_multiplier=float(row["off_hours_multiplier"]),
        battery_support_fraction=float(row["battery_support_fraction"]),
        base_load_kw=float(row["base_load_kw"]),
        peak_load_kw=float(row["peak_load_kw"]),
        solar_capacity_kw=float(row["solar_capacity_kw"]),
        reason_code=str(row["reason_code"] if matched_row is not None else "off_hours"),
    )


def build_tenant_net_load_hourly_frame(
    schedule_frame: pl.DataFrame,
    *,
    weather_frame: pl.DataFrame | None = None,
    anchor_timestamp: datetime | None = None,
    horizon_hours: int = 24,
) -> pl.DataFrame:
    if horizon_hours <= 0:
        raise ValueError("horizon_hours must be positive.")
    anchor = _truncate_to_hour(anchor_timestamp or datetime.now(tz=UTC))
    rows: list[dict[str, Any]] = []
    tenant_ids = sorted(str(value) for value in schedule_frame.select("tenant_id").unique().to_series().to_list())
    weather_rows = _weather_rows_by_tenant_timestamp(weather_frame)
    for tenant_id in tenant_ids:
        for hour_offset in range(horizon_hours):
            timestamp = anchor + timedelta(hours=hour_offset)
            schedule_state = resolve_tenant_schedule_state(
                schedule_frame,
                tenant_id=tenant_id,
                timestamp=timestamp,
            )
            load_mw = _scheduled_load_mw(schedule_state)
            pv_estimate_mw, weather_source_kind = _pv_estimate_mw(
                schedule_state,
                timestamp=timestamp,
                weather_rows=weather_rows,
            )
            net_load_mw = load_mw - pv_estimate_mw
            rows.append(
                {
                    "tenant_id": tenant_id,
                    "timestamp": timestamp,
                    "forecast_anchor": anchor,
                    "timezone": schedule_state.timezone,
                    "profile_label": schedule_state.profile_label,
                    "load_mw": round(load_mw, 6),
                    "pv_estimate_mw": round(pv_estimate_mw, 6),
                    "net_load_mw": round(net_load_mw, 6),
                    "btm_battery_power_mw": round(
                        net_load_mw * schedule_state.battery_support_fraction,
                        6,
                    ),
                    "source_kind": "configured",
                    "weather_source_kind": weather_source_kind,
                    "reason_code": schedule_state.reason_code,
                }
            )

    if not rows:
        return pl.DataFrame(schema={column_name: pl.Null for column_name in TENANT_NET_LOAD_HOURLY_COLUMNS})
    return pl.DataFrame(rows).select(TENANT_NET_LOAD_HOURLY_COLUMNS).sort(["tenant_id", "timestamp"])


def build_tenant_historical_net_load_frame(
    schedule_frame: pl.DataFrame,
    benchmark_feature_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Build configured tenant-load proxies aligned to benchmark history timestamps."""

    _require_columns(
        schedule_frame,
        required_columns=frozenset(TENANT_CONSUMPTION_SCHEDULE_COLUMNS),
        frame_name="tenant_consumption_schedule_bronze",
    )
    _require_columns(
        benchmark_feature_frame,
        required_columns=frozenset({"tenant_id", "timestamp"}),
        frame_name="real_data_benchmark_silver_feature_frame",
    )
    if benchmark_feature_frame.height == 0:
        return pl.DataFrame(schema={column_name: pl.Null for column_name in TENANT_HISTORICAL_NET_LOAD_COLUMNS})

    benchmark_rows = _benchmark_weather_rows_by_tenant_timestamp(benchmark_feature_frame)
    rows: list[dict[str, Any]] = []
    for tenant_id, timestamp in sorted(key for key in benchmark_rows if key[0] is not None):
        resolved_tenant_id = str(tenant_id)
        schedule_state = resolve_tenant_schedule_state(
            schedule_frame,
            tenant_id=resolved_tenant_id,
            timestamp=timestamp,
        )
        load_mw = _scheduled_load_mw(schedule_state)
        pv_estimate_mw, weather_source_kind = _pv_estimate_mw(
            schedule_state,
            timestamp=timestamp,
            weather_rows=benchmark_rows,
        )
        net_load_mw = load_mw - pv_estimate_mw
        rows.append(
            {
                "tenant_id": resolved_tenant_id,
                "timestamp": timestamp,
                "timezone": schedule_state.timezone,
                "profile_label": schedule_state.profile_label,
                "load_mw": round(load_mw, 6),
                "pv_estimate_mw": round(pv_estimate_mw, 6),
                "net_load_mw": round(net_load_mw, 6),
                "btm_battery_power_mw": round(
                    net_load_mw * schedule_state.battery_support_fraction,
                    6,
                ),
                "source_kind": "configured_proxy",
                "weather_source_kind": weather_source_kind,
                "reason_code": schedule_state.reason_code,
                "claim_scope": "tenant_historical_net_load_configured_proxy",
                "not_full_dfl": True,
                "not_market_execution": True,
            }
        )

    if not rows:
        return pl.DataFrame(schema={column_name: pl.Null for column_name in TENANT_HISTORICAL_NET_LOAD_COLUMNS})
    return pl.DataFrame(rows).select(TENANT_HISTORICAL_NET_LOAD_COLUMNS).sort(["tenant_id", "timestamp"])


def _schedule_rows_from_tenant_entry(tenant_entry: dict[str, Any]) -> list[dict[str, Any]]:
    tenant_id = _required_text(tenant_entry.get("id"), field_name="tenant id")
    energy_system = _mapping_value(tenant_entry.get("energy_system"), field_name="energy_system")
    location = _mapping_value(tenant_entry.get("location"), field_name="location")
    schedule = tenant_entry.get("consumption_schedule")
    if not isinstance(schedule, dict):
        schedule = _default_schedule_for_profile(str(energy_system.get("load_profile", tenant_entry.get("type", ""))))

    timezone = _text_value(schedule.get("timezone"), default=str(location.get("timezone", "Europe/Kyiv")))
    profile_label = _text_value(schedule.get("profile_label"), default=f"{energy_system.get('load_profile', 'tenant')}_schedule")
    off_hours_multiplier = _float_value(schedule.get("off_hours_multiplier"), default=0.35)
    battery_support_fraction = _float_value(schedule.get("battery_support_fraction"), default=0.08)
    intervals = schedule.get("intervals")
    if not isinstance(intervals, list) or not intervals:
        intervals = _default_schedule_for_profile(str(energy_system.get("load_profile", tenant_entry.get("type", ""))))["intervals"]

    return [
        {
            "tenant_id": tenant_id,
            "tenant_name": _text_value(tenant_entry.get("name"), default=tenant_id),
            "tenant_type": _text_value(tenant_entry.get("type"), default="unknown"),
            "timezone": timezone,
            "profile_label": profile_label,
            "days_csv": ",".join(_days_from_interval(interval)),
            "start_minute": _minute_of_day(_text_value(interval.get("start"), default="00:00")),
            "end_minute": _minute_of_day(_text_value(interval.get("end"), default="24:00")),
            "load_multiplier": _float_value(interval.get("load_multiplier"), default=1.0),
            "off_hours_multiplier": off_hours_multiplier,
            "battery_support_fraction": battery_support_fraction,
            "base_load_kw": _float_value(energy_system.get("base_load_kw"), default=0.0),
            "peak_load_kw": _float_value(energy_system.get("peak_load_kw"), default=0.0),
            "solar_capacity_kw": _float_value(energy_system.get("solar_capacity_kw"), default=0.0),
            "source_kind": "configured",
            "reason_code": _text_value(interval.get("reason_code"), default="scheduled_load"),
        }
        for interval in intervals
        if isinstance(interval, dict)
    ]


def _default_schedule_for_profile(profile: str) -> dict[str, Any]:
    normalized_profile = profile.lower()
    if normalized_profile in {"commercial", "mall"}:
        return {
            "profile_label": "mall_open_hours",
            "off_hours_multiplier": 0.45,
            "battery_support_fraction": 0.08,
            "intervals": [_interval(ALL_DAYS, "10:00", "22:00", 1.0, "open_hours")],
        }
    if normalized_profile == "office":
        return {
            "profile_label": "office_workday",
            "off_hours_multiplier": 0.35,
            "battery_support_fraction": 0.06,
            "intervals": [_interval(("mon", "tue", "wed", "thu", "fri"), "08:00", "18:00", 1.0, "office_hours")],
        }
    if normalized_profile == "industrial":
        return {
            "profile_label": "factory_two_shift",
            "off_hours_multiplier": 0.5,
            "battery_support_fraction": 0.1,
            "intervals": [
                _interval(("mon", "tue", "wed", "thu", "fri", "sat"), "06:00", "14:00", 0.95, "first_shift"),
                _interval(("mon", "tue", "wed", "thu", "fri", "sat"), "14:00", "22:00", 1.0, "second_shift"),
            ],
        }
    if normalized_profile in {"hospital", "critical"}:
        return {
            "profile_label": "critical_24_7",
            "off_hours_multiplier": 1.0,
            "battery_support_fraction": 0.12,
            "intervals": [_interval(ALL_DAYS, "00:00", "24:00", 1.0, "critical_24_7")],
        }
    if normalized_profile in {"hotel", "hospitality"}:
        return {
            "profile_label": "hotel_daily_evening_peak",
            "off_hours_multiplier": 0.55,
            "battery_support_fraction": 0.09,
            "intervals": [
                _interval(ALL_DAYS, "06:00", "18:00", 0.85, "guest_day_load"),
                _interval(ALL_DAYS, "18:00", "23:00", 1.0, "evening_peak"),
            ],
        }
    return {
        "profile_label": "generic_business_hours",
        "off_hours_multiplier": 0.4,
        "battery_support_fraction": 0.06,
        "intervals": [_interval(("mon", "tue", "wed", "thu", "fri"), "09:00", "18:00", 1.0, "business_hours")],
    }


def _interval(
    days: tuple[str, ...],
    start: str,
    end: str,
    load_multiplier: float,
    reason_code: str,
) -> dict[str, Any]:
    return {
        "days": list(days),
        "start": start,
        "end": end,
        "load_multiplier": load_multiplier,
        "reason_code": reason_code,
    }


def _days_from_interval(interval: dict[str, Any]) -> tuple[str, ...]:
    raw_days = interval.get("days", ALL_DAYS)
    if not isinstance(raw_days, list):
        return ALL_DAYS
    days = tuple(str(day).lower()[:3] for day in raw_days if str(day).lower()[:3] in DAY_INDEX_BY_NAME)
    return days or ALL_DAYS


def _day_index_matches(days_csv: Any, day_index: int) -> bool:
    days = [item.strip() for item in str(days_csv).split(",") if item.strip()]
    return any(DAY_INDEX_BY_NAME.get(day) == day_index for day in days)


def _minute_matches_interval(local_minute: int, *, start_minute: int, end_minute: int) -> bool:
    if start_minute <= end_minute:
        return start_minute <= local_minute < end_minute
    return local_minute >= start_minute or local_minute < end_minute


def _minute_of_day(value: str) -> int:
    if value == "24:00":
        return 24 * 60
    hour_text, minute_text = value.split(":", maxsplit=1)
    hour = int(hour_text)
    minute = int(minute_text)
    if not 0 <= hour <= 23 or not 0 <= minute <= 59:
        raise ValueError(f"Invalid schedule time: {value}")
    return hour * 60 + minute


def _to_local_datetime(timestamp: datetime, timezone: str) -> datetime:
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=UTC)
    return timestamp.astimezone(ZoneInfo(timezone))


def _scheduled_load_mw(schedule_state: TenantScheduleState) -> float:
    return (
        schedule_state.base_load_kw
        + (schedule_state.peak_load_kw - schedule_state.base_load_kw) * schedule_state.load_multiplier
    ) / 1000.0


def _pv_estimate_mw(
    schedule_state: TenantScheduleState,
    *,
    timestamp: datetime,
    weather_rows: dict[tuple[str | None, datetime], dict[str, Any]],
) -> tuple[float, str]:
    tenant_weather = weather_rows.get((schedule_state.tenant_id, timestamp))
    generic_weather = weather_rows.get((None, timestamp))
    weather_row = tenant_weather or generic_weather
    if weather_row is not None:
        solar_factor = max(0.0, min(1.0, _float_value(weather_row.get("effective_solar"), default=0.0) / 1000.0))
        return schedule_state.solar_capacity_kw * solar_factor / 1000.0, str(weather_row.get("source_kind", "observed"))
    return schedule_state.solar_capacity_kw * _daylight_solar_factor(timestamp, schedule_state.timezone) / 1000.0, "schedule_estimate"


def _weather_rows_by_tenant_timestamp(weather_frame: pl.DataFrame | None) -> dict[tuple[str | None, datetime], dict[str, Any]]:
    if weather_frame is None or weather_frame.height == 0 or "timestamp" not in weather_frame.columns:
        return {}
    rows: dict[tuple[str | None, datetime], dict[str, Any]] = {}
    for row in weather_frame.iter_rows(named=True):
        timestamp = row["timestamp"]
        if not isinstance(timestamp, datetime):
            continue
        tenant_id = None if "tenant_id" not in row or row["tenant_id"] is None else str(row["tenant_id"])
        rows[(tenant_id, _truncate_to_hour(timestamp))] = row
    return rows


def _benchmark_weather_rows_by_tenant_timestamp(
    benchmark_feature_frame: pl.DataFrame,
) -> dict[tuple[str | None, datetime], dict[str, Any]]:
    rows: dict[tuple[str | None, datetime], dict[str, Any]] = {}
    for row in benchmark_feature_frame.iter_rows(named=True):
        timestamp = row["timestamp"]
        if not isinstance(timestamp, datetime):
            continue
        tenant_id = str(row["tenant_id"])
        rows[(tenant_id, _truncate_to_hour(timestamp))] = {
            "tenant_id": tenant_id,
            "timestamp": _truncate_to_hour(timestamp),
            "effective_solar": _float_value(row.get("weather_effective_solar"), default=0.0),
            "source_kind": _text_value(
                row.get("weather_source_kind"),
                default=_text_value(row.get("source_kind"), default="observed"),
            ),
        }
    return rows


def _daylight_solar_factor(timestamp: datetime, timezone: str) -> float:
    local_timestamp = _to_local_datetime(timestamp, timezone)
    hour = local_timestamp.hour + local_timestamp.minute / 60.0
    if hour < 6.0 or hour > 18.0:
        return 0.0
    distance_from_noon = abs(hour - 12.0)
    return max(0.0, 1.0 - distance_from_noon / 6.0)


def _truncate_to_hour(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).replace(minute=0, second=0, microsecond=0)


def _mapping_value(value: Any, *, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} mapping is required.")
    return value


def _required_text(value: Any, *, field_name: str) -> str:
    text_value = _text_value(value, default="")
    if not text_value:
        raise ValueError(f"{field_name} is required.")
    return text_value


def _text_value(value: Any, *, default: str) -> str:
    if value is None:
        return default
    cleaned = str(value).strip()
    return cleaned or default


def _float_value(value: Any, *, default: float) -> float:
    if value is None:
        return default
    if isinstance(value, bool):
        raise ValueError("Boolean values are not valid numeric schedule values.")
    return float(value)


def _missing_column_failures(frame: pl.DataFrame, required_columns: frozenset[str]) -> list[str]:
    missing = sorted(required_columns.difference(frame.columns))
    return [f"missing required columns: {missing}"] if missing else []


def _require_columns(frame: pl.DataFrame, *, required_columns: frozenset[str], frame_name: str) -> None:
    failures = _missing_column_failures(frame, required_columns)
    if failures:
        raise ValueError(f"{frame_name} " + "; ".join(failures))
