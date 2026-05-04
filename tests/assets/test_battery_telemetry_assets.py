from datetime import UTC, datetime, timedelta

import polars as pl

from smart_arbitrage.assets.telemetry.battery import (
    BATTERY_TELEMETRY_ASSETS,
    battery_state_hourly_silver,
    battery_telemetry_bronze,
    battery_telemetry_hourly_refresh_schedule,
)
from smart_arbitrage.defs import defs
from smart_arbitrage.resources.battery_telemetry_store import (
    BatteryTelemetryObservation,
    InMemoryBatteryTelemetryStore,
)


def test_battery_telemetry_assets_aggregate_and_persist_hourly_snapshots(monkeypatch) -> None:
    store = InMemoryBatteryTelemetryStore()
    start = datetime(2026, 5, 4, 10, tzinfo=UTC)
    store.upsert_battery_telemetry(
        [
            BatteryTelemetryObservation(
                tenant_id="client_003_dnipro_factory",
                observed_at=start + timedelta(minutes=5 * index),
                current_soc=0.50 + index * 0.005,
                soh=0.96,
                power_mw=0.1,
                temperature_c=23.0,
                source="simulated_mqtt",
                source_kind="synthetic",
                raw_payload={"sequence": index},
            )
            for index in range(12)
        ]
    )
    monkeypatch.setattr("smart_arbitrage.assets.telemetry.battery.get_battery_telemetry_store", lambda: store)

    telemetry_frame = battery_telemetry_bronze(None)
    snapshot_frame = battery_state_hourly_silver(None, telemetry_frame)

    assert telemetry_frame.height == 12
    assert snapshot_frame.height == 1
    assert snapshot_frame.select("tenant_id").item() == "client_003_dnipro_factory"
    assert snapshot_frame.select("telemetry_freshness").item() == "fresh"
    assert store.get_latest_hourly_snapshot(tenant_id="client_003_dnipro_factory") is not None


def test_battery_telemetry_assets_and_schedule_are_registered() -> None:
    asset_keys = {asset.key.to_user_string() for asset in BATTERY_TELEMETRY_ASSETS}
    registered_asset_keys = {asset.key.to_user_string() for asset in defs.assets or []}
    schedule_names = {schedule.name for schedule in defs.schedules or []}

    assert {"battery_telemetry_bronze", "battery_state_hourly_silver"}.issubset(asset_keys)
    assert asset_keys.issubset(registered_asset_keys)
    assert battery_telemetry_hourly_refresh_schedule.name in schedule_names
