from __future__ import annotations

from typing import Any

import dagster as dg
import polars as pl

from smart_arbitrage.assets.bronze.market_weather import (
    list_available_weather_tenants,
    resolve_tenant_registry_entry,
)
from smart_arbitrage.assets.mvp_demo import (
    DEMO_BATTERY_CAPEX_USD_PER_KWH,
    DEMO_BATTERY_CYCLES_PER_DAY,
    DEMO_BATTERY_LIFETIME_YEARS,
    DEMO_USD_TO_UAH_RATE,
)
from smart_arbitrage.gatekeeper.schemas import BatteryPhysicalMetrics
from smart_arbitrage.resources.battery_telemetry_store import (
    BATTERY_TELEMETRY_OBSERVATION_COLUMNS,
    BatteryTelemetryObservation,
    SourceKind,
    build_hourly_battery_state_snapshots,
    get_battery_telemetry_store,
    hourly_snapshots_to_frame,
    telemetry_observations_to_frame,
)


@dg.asset(group_name="bronze", tags={"medallion": "bronze", "domain": "battery_telemetry"})
def battery_telemetry_bronze(context) -> pl.DataFrame:
    """Raw 5-minute battery telemetry observations from MQTT/Postgres."""

    observations = get_battery_telemetry_store().list_battery_telemetry()
    telemetry_frame = telemetry_observations_to_frame(observations)
    _add_metadata(
        context,
        {
            "rows": telemetry_frame.height,
            "tenant_count": telemetry_frame.select("tenant_id").n_unique() if telemetry_frame.height else 0,
            "source": "battery_telemetry_store",
        },
    )
    return telemetry_frame


@dg.asset(group_name="silver", tags={"medallion": "silver", "domain": "battery_telemetry"})
def battery_state_hourly_silver(
    context,
    battery_telemetry_bronze: pl.DataFrame,
) -> pl.DataFrame:
    """Hourly Level 1 battery-state snapshots derived from raw telemetry."""

    observations = _observations_from_frame(battery_telemetry_bronze)
    metrics_by_tenant = _battery_metrics_by_tenant(observations)
    snapshots = build_hourly_battery_state_snapshots(
        observations,
        battery_metrics_by_tenant=metrics_by_tenant,
    )
    get_battery_telemetry_store().upsert_hourly_snapshots(snapshots)
    snapshot_frame = hourly_snapshots_to_frame(snapshots)
    _add_metadata(
        context,
        {
            "rows": snapshot_frame.height,
            "tenant_count": snapshot_frame.select("tenant_id").n_unique() if snapshot_frame.height else 0,
            "fresh_snapshot_rows": (
                snapshot_frame.filter(pl.col("telemetry_freshness") == "fresh").height
                if snapshot_frame.height
                else 0
            ),
        },
    )
    return snapshot_frame


battery_telemetry_hourly_refresh_job = dg.define_asset_job(
    "battery_telemetry_hourly_refresh",
    selection=[battery_telemetry_bronze, battery_state_hourly_silver],
)

battery_telemetry_hourly_refresh_schedule = dg.ScheduleDefinition(
    name="battery_telemetry_hourly_refresh_schedule",
    job=battery_telemetry_hourly_refresh_job,
    cron_schedule="0 * * * *",
    execution_timezone="Europe/Kyiv",
    description="Hourly aggregation of 5-minute battery telemetry into Level 1 battery-state snapshots.",
)

BATTERY_TELEMETRY_ASSETS = [
    battery_telemetry_bronze,
    battery_state_hourly_silver,
]

BATTERY_TELEMETRY_SCHEDULES = [
    battery_telemetry_hourly_refresh_schedule,
]


def _observations_from_frame(frame: pl.DataFrame) -> list[BatteryTelemetryObservation]:
    if frame.height == 0:
        return []
    missing_columns = set(BATTERY_TELEMETRY_OBSERVATION_COLUMNS).difference(frame.columns)
    if missing_columns:
        raise ValueError(f"battery telemetry frame is missing required columns: {sorted(missing_columns)}")

    observations: list[BatteryTelemetryObservation] = []
    for row in frame.iter_rows(named=True):
        observations.append(
            BatteryTelemetryObservation(
                tenant_id=str(row["tenant_id"]),
                observed_at=row["observed_at"],
                current_soc=float(row["current_soc"]),
                soh=float(row["soh"]),
                power_mw=float(row["power_mw"]),
                temperature_c=None if row["temperature_c"] is None else float(row["temperature_c"]),
                source=str(row["source"]),
                source_kind=_source_kind_value(row["source_kind"]),
                raw_payload={},
            )
        )
    return observations


def _battery_metrics_by_tenant(
    observations: list[BatteryTelemetryObservation],
) -> dict[str, BatteryPhysicalMetrics]:
    tenant_ids = {observation.tenant_id for observation in observations}
    if not tenant_ids:
        tenant_ids = {
            str(tenant["tenant_id"])
            for tenant in list_available_weather_tenants()
            if tenant.get("tenant_id") is not None
        }
    return {
        tenant_id: _tenant_battery_metrics(tenant_id)
        for tenant_id in tenant_ids
    }


def _tenant_battery_metrics(tenant_id: str) -> BatteryPhysicalMetrics:
    tenant_entry = resolve_tenant_registry_entry(tenant_id=tenant_id)
    energy_system = tenant_entry.get("energy_system")
    if not isinstance(energy_system, dict):
        raise ValueError(f"Tenant {tenant_id} is missing energy_system.")

    capacity_kwh = _positive_float(energy_system.get("battery_capacity_kwh"), field_name="battery_capacity_kwh")
    max_power_kw = _positive_float(
        energy_system.get("battery_max_power_kw", capacity_kwh * 0.5),
        field_name="battery_max_power_kw",
    )
    round_trip_efficiency = _bounded_float(
        energy_system.get("round_trip_efficiency", 0.92),
        field_name="round_trip_efficiency",
        minimum=0.0,
        maximum=1.0,
    )
    soc_min_fraction = _bounded_float(
        energy_system.get("soc_min_fraction", 0.05),
        field_name="soc_min_fraction",
        minimum=0.0,
        maximum=1.0,
    )
    soc_max_fraction = _bounded_float(
        energy_system.get("soc_max_fraction", 0.95),
        field_name="soc_max_fraction",
        minimum=0.0,
        maximum=1.0,
    )
    return BatteryPhysicalMetrics(
        capacity_mwh=capacity_kwh / 1000.0,
        max_power_mw=max_power_kw / 1000.0,
        round_trip_efficiency=round_trip_efficiency,
        degradation_cost_per_cycle_uah=_degradation_cost_per_cycle_uah(
            energy_system=energy_system,
            capacity_kwh=capacity_kwh,
        ),
        soc_min_fraction=soc_min_fraction,
        soc_max_fraction=soc_max_fraction,
    )


def _degradation_cost_per_cycle_uah(*, energy_system: dict[str, Any], capacity_kwh: float) -> float:
    capex_usd_per_kwh = _positive_float(
        energy_system.get("battery_capex_usd_per_kwh", DEMO_BATTERY_CAPEX_USD_PER_KWH),
        field_name="battery_capex_usd_per_kwh",
    )
    lifetime_years = _positive_float(
        energy_system.get("battery_lifetime_years", DEMO_BATTERY_LIFETIME_YEARS),
        field_name="battery_lifetime_years",
    )
    cycles_per_day = _positive_float(
        energy_system.get("battery_cycles_per_day", DEMO_BATTERY_CYCLES_PER_DAY),
        field_name="battery_cycles_per_day",
    )
    lifetime_cycles = lifetime_years * 365.0 * cycles_per_day
    return capex_usd_per_kwh * capacity_kwh * DEMO_USD_TO_UAH_RATE / lifetime_cycles


def _positive_float(value: Any, *, field_name: str) -> float:
    parsed_value = _float_value(value, field_name=field_name)
    if parsed_value <= 0.0:
        raise ValueError(f"{field_name} must be positive.")
    return parsed_value


def _bounded_float(value: Any, *, field_name: str, minimum: float, maximum: float) -> float:
    parsed_value = _float_value(value, field_name=field_name)
    if not minimum <= parsed_value <= maximum:
        raise ValueError(f"{field_name} must be between {minimum} and {maximum}.")
    return parsed_value


def _float_value(value: Any, *, field_name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be numeric.")
    try:
        return float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{field_name} must be numeric.") from error


def _source_kind_value(value: Any) -> SourceKind:
    if value in {"observed", "synthetic"}:
        return value
    raise ValueError(f"Unsupported source_kind: {value}")


def _add_metadata(context: dg.AssetExecutionContext | None, metadata: dict[str, Any]) -> None:
    if context is not None:
        context.add_output_metadata(metadata)
