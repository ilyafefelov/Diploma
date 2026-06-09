from datetime import UTC, datetime

import polars as pl
import pytest

from smart_arbitrage.tenant_load import (
    build_tenant_consumption_schedule_frame,
    build_tenant_historical_net_load_frame,
    build_tenant_net_load_hourly_frame,
    resolve_tenant_schedule_state,
)


def test_tenant_schedule_parser_returns_profile_specific_hourly_state() -> None:
    schedule_frame = build_tenant_consumption_schedule_frame()

    office_work_hour = resolve_tenant_schedule_state(
        schedule_frame,
        tenant_id="client_002_lviv_office",
        timestamp=datetime(2026, 5, 4, 9, tzinfo=UTC),
    )
    office_night_hour = resolve_tenant_schedule_state(
        schedule_frame,
        tenant_id="client_002_lviv_office",
        timestamp=datetime(2026, 5, 4, 22, tzinfo=UTC),
    )
    factory_first_shift = resolve_tenant_schedule_state(
        schedule_frame,
        tenant_id="client_003_dnipro_factory",
        timestamp=datetime(2026, 5, 4, 7, tzinfo=UTC),
    )
    factory_second_shift = resolve_tenant_schedule_state(
        schedule_frame,
        tenant_id="client_003_dnipro_factory",
        timestamp=datetime(2026, 5, 4, 15, tzinfo=UTC),
    )
    hospital_night_hour = resolve_tenant_schedule_state(
        schedule_frame,
        tenant_id="client_004_kharkiv_hospital",
        timestamp=datetime(2026, 5, 4, 3, tzinfo=UTC),
    )

    assert office_work_hour.load_multiplier == pytest.approx(1.0)
    assert office_work_hour.reason_code == "office_hours"
    assert office_night_hour.load_multiplier == pytest.approx(0.35)
    assert office_night_hour.reason_code == "off_hours"
    assert factory_first_shift.reason_code == "first_shift"
    assert factory_second_shift.reason_code == "second_shift"
    assert hospital_night_hour.load_multiplier == pytest.approx(1.0)
    assert hospital_night_hour.reason_code == "critical_24_7"


def test_net_load_hourly_frame_uses_config_schedule_and_known_weather_without_future_leakage() -> None:
    schedule_frame = build_tenant_consumption_schedule_frame()
    weather_frame = pl.DataFrame(
        {
            "tenant_id": ["client_002_lviv_office", "client_002_lviv_office"],
            "timestamp": [
                datetime(2026, 5, 4, 9, tzinfo=UTC),
                datetime(2026, 5, 4, 10, tzinfo=UTC),
            ],
            "effective_solar": [600.0, 200.0],
            "source_kind": ["observed", "observed"],
        }
    )

    net_load = build_tenant_net_load_hourly_frame(
        schedule_frame,
        weather_frame=weather_frame,
        anchor_timestamp=datetime(2026, 5, 4, 9, tzinfo=UTC),
        horizon_hours=1,
    )

    row = net_load.filter(pl.col("tenant_id") == "client_002_lviv_office").row(0, named=True)
    assert row["load_mw"] == pytest.approx(0.12)
    assert row["pv_estimate_mw"] == pytest.approx(0.048)
    assert row["net_load_mw"] == pytest.approx(0.072)
    assert row["source_kind"] == "configured"
    assert row["weather_source_kind"] == "observed"
    assert row["forecast_anchor"] == datetime(2026, 5, 4, 9, tzinfo=UTC)


def test_historical_net_load_frame_aligns_to_benchmark_timestamps_without_wall_clock() -> None:
    schedule_frame = build_tenant_consumption_schedule_frame()
    benchmark_frame = pl.DataFrame(
        {
            "tenant_id": [
                "client_002_lviv_office",
                "client_002_lviv_office",
                "client_003_dnipro_factory",
            ],
            "timestamp": [
                datetime(2026, 1, 5, 9, tzinfo=UTC),
                datetime(2026, 1, 5, 10, tzinfo=UTC),
                datetime(2026, 1, 5, 7, tzinfo=UTC),
            ],
            "weather_effective_solar": [600.0, 200.0, 0.0],
            "weather_source_kind": ["historical_open_meteo", "historical_open_meteo", "historical_open_meteo"],
        }
    )

    net_load = build_tenant_historical_net_load_frame(schedule_frame, benchmark_frame)

    assert net_load.height == 3
    assert "forecast_anchor" not in net_load.columns
    office_row = net_load.filter(
        (pl.col("tenant_id") == "client_002_lviv_office")
        & (pl.col("timestamp") == datetime(2026, 1, 5, 9, tzinfo=UTC))
    ).row(0, named=True)
    assert office_row["load_mw"] == pytest.approx(0.12)
    assert office_row["pv_estimate_mw"] == pytest.approx(0.048)
    assert office_row["net_load_mw"] == pytest.approx(0.072)
    assert office_row["source_kind"] == "configured_proxy"
    assert office_row["weather_source_kind"] == "historical_open_meteo"
    assert office_row["claim_scope"] == "tenant_historical_net_load_configured_proxy"
    assert office_row["not_full_dfl"] is True
    assert office_row["not_market_execution"] is True
