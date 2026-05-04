from datetime import UTC, datetime, timedelta

import pytest

from smart_arbitrage.gatekeeper.schemas import BatteryPhysicalMetrics
from smart_arbitrage.resources.battery_telemetry_store import (
    BatteryTelemetryObservation,
    build_hourly_battery_state_snapshots,
)


def test_build_hourly_battery_state_snapshots_aggregates_five_minute_telemetry() -> None:
    metrics = BatteryPhysicalMetrics(
        capacity_mwh=1.0,
        max_power_mw=0.5,
        round_trip_efficiency=0.92,
        degradation_cost_per_cycle_uah=100.0,
    )
    start = datetime(2026, 5, 4, 10, tzinfo=UTC)
    observations = [
        BatteryTelemetryObservation(
            tenant_id="client_001_kyiv_mall",
            observed_at=start + timedelta(minutes=5 * index),
            current_soc=0.50 + index * 0.01,
            soh=0.97 - index * 0.001,
            power_mw=0.12 if index < 6 else -0.08,
            temperature_c=24.0 + index * 0.1,
            source="simulated_mqtt",
            source_kind="synthetic",
            raw_payload={"sequence": index},
        )
        for index in range(12)
    ]

    snapshots = build_hourly_battery_state_snapshots(
        observations,
        battery_metrics_by_tenant={"client_001_kyiv_mall": metrics},
    )

    assert len(snapshots) == 1
    snapshot = snapshots[0]
    assert snapshot.tenant_id == "client_001_kyiv_mall"
    assert snapshot.snapshot_hour == start
    assert snapshot.observation_count == 12
    assert snapshot.soc_open == pytest.approx(0.50)
    assert snapshot.soc_close == pytest.approx(0.61)
    assert snapshot.soc_mean == pytest.approx(0.555)
    assert snapshot.soh_close == pytest.approx(0.959)
    assert snapshot.power_mw_mean == pytest.approx(0.02)
    assert snapshot.throughput_mwh == pytest.approx((6 * 0.12 + 6 * 0.08) * (5.0 / 60.0))
    assert snapshot.efc_delta == pytest.approx(snapshot.throughput_mwh / 2.0)
    assert snapshot.telemetry_freshness == "fresh"
