from datetime import datetime

import pytest

from smart_arbitrage.assets.gold.baseline_solver import (
    BaselineForecastPoint,
    BaselineSolverConfig,
    HourlyDamBaselineSolver,
)
from smart_arbitrage.gatekeeper.schemas import BatteryPhysicalMetrics
import smart_arbitrage.dfl.relaxed_dispatch as relaxed_dispatch
from smart_arbitrage.dfl.relaxed_dispatch import solve_relaxed_dispatch


def test_relaxed_dispatch_layer_returns_feasible_charge_then_discharge() -> None:
    result = solve_relaxed_dispatch(
        prices_uah_mwh=[100.0, 1000.0],
        starting_soc_fraction=0.5,
        capacity_mwh=1.0,
        max_power_mw=0.25,
        soc_min_fraction=0.05,
        soc_max_fraction=0.95,
        degradation_cost_per_mwh=0.0,
    )

    assert result.charge_mw[0] > result.discharge_mw[0]
    assert result.discharge_mw[1] > result.charge_mw[1]
    assert result.solver_status == "cvxpylayer_scaled"
    assert all(0.0 <= value <= 0.25 + 1e-5 for value in result.charge_mw)
    assert all(0.0 <= value <= 0.25 + 1e-5 for value in result.discharge_mw)
    assert all(0.05 - 1e-5 <= value <= 0.95 + 1e-5 for value in result.soc_fraction)


def test_relaxed_dispatch_scales_large_uah_prices() -> None:
    result = solve_relaxed_dispatch(
        prices_uah_mwh=[1000.0, 10000.0, 1000.0, 10000.0],
        starting_soc_fraction=0.5,
        capacity_mwh=1.0,
        max_power_mw=0.25,
        soc_min_fraction=0.25,
        soc_max_fraction=0.75,
        degradation_cost_per_mwh=0.0,
    )

    assert result.solver_status == "cvxpylayer_scaled"
    assert result.charge_mw[0] > result.discharge_mw[0]
    assert result.discharge_mw[1] > result.charge_mw[1]
    assert result.charge_mw[2] > result.discharge_mw[2]
    assert result.discharge_mw[3] > result.charge_mw[3]


def test_relaxed_dispatch_uses_bounded_surrogate_when_solver_fails(monkeypatch) -> None:
    def failing_layer(*args: object, **kwargs: object) -> object:
        raise RuntimeError("synthetic cvxpylayer failure")

    monkeypatch.setattr(relaxed_dispatch, "_relaxed_dispatch_layer", failing_layer)

    result = solve_relaxed_dispatch(
        prices_uah_mwh=[1000.0, 10000.0, 1000.0, 10000.0],
        starting_soc_fraction=0.5,
        capacity_mwh=1.0,
        max_power_mw=0.25,
        soc_min_fraction=0.25,
        soc_max_fraction=0.75,
        degradation_cost_per_mwh=0.0,
    )

    assert result.solver_status.startswith("surrogate_bounded")
    assert len(result.charge_mw) == 4
    assert len(result.discharge_mw) == 4
    assert len(result.soc_fraction) == 5
    assert all(0.0 <= value <= 0.25 + 1e-9 for value in result.charge_mw)
    assert all(0.0 <= value <= 0.25 + 1e-9 for value in result.discharge_mw)
    assert all(0.25 - 1e-9 <= value <= 0.75 + 1e-9 for value in result.soc_fraction)
    assert result.soc_fraction[-1] == pytest.approx(0.5, abs=1e-6)


def test_scaled_relaxed_dispatch_matches_strict_lp_fixture_value() -> None:
    prices = [1000.0, 10000.0, 1000.0, 10000.0]
    relaxed = solve_relaxed_dispatch(
        prices_uah_mwh=prices,
        starting_soc_fraction=0.5,
        capacity_mwh=1.0,
        max_power_mw=0.25,
        soc_min_fraction=0.25,
        soc_max_fraction=0.75,
        round_trip_efficiency=1.0,
        degradation_cost_per_mwh=0.0,
    )
    strict = HourlyDamBaselineSolver(
        BaselineSolverConfig(planning_horizon_hours=4)
    ).solve_dispatch_from_forecast(
        forecast=[
            BaselineForecastPoint(
                forecast_timestamp=datetime(2026, 1, 1, hour),
                source_timestamp=datetime(2025, 12, 31, hour),
                predicted_price_uah_mwh=price,
            )
            for hour, price in enumerate(prices)
        ],
        battery_metrics=BatteryPhysicalMetrics(
            capacity_mwh=1.0,
            max_power_mw=0.25,
            round_trip_efficiency=1.0,
            degradation_cost_per_cycle_uah=1.0,
            soc_min_fraction=0.25,
            soc_max_fraction=0.75,
        ),
        current_soc_fraction=0.5,
    )

    strict_value = sum(point.net_objective_value_uah for point in strict.schedule)
    assert relaxed.objective_value <= strict_value + 1.0
    assert relaxed.objective_value / strict_value >= 0.90
    strict_net_power = [point.net_power_mw for point in strict.schedule]
    relaxed_net_power = [
        discharge_mw - charge_mw
        for charge_mw, discharge_mw in zip(relaxed.charge_mw, relaxed.discharge_mw, strict=True)
    ]
    assert relaxed_net_power[0] < 0.0 and strict_net_power[0] < 0.0
    assert relaxed_net_power[1] > 0.0 and strict_net_power[1] > 0.0
    assert relaxed_net_power[2] < 0.0 and strict_net_power[2] < 0.0
    assert relaxed_net_power[3] > 0.0 and strict_net_power[3] > 0.0


def test_relaxed_dispatch_rejects_invalid_horizon() -> None:
    with pytest.raises(ValueError, match="prices_uah_mwh must contain at least one price"):
        solve_relaxed_dispatch(
            prices_uah_mwh=[],
            starting_soc_fraction=0.5,
            capacity_mwh=1.0,
            max_power_mw=0.25,
            soc_min_fraction=0.05,
            soc_max_fraction=0.95,
        )
