from datetime import UTC, datetime, timedelta

import pytest

from smart_arbitrage.assets.gold.baseline_solver import (
	BaselineForecastPoint,
	BaselineSolverConfig,
	HourlyDamBaselineSolver,
)
from smart_arbitrage.gatekeeper.schemas import BatteryPhysicalMetrics
from smart_arbitrage.optimization.projected_battery_state import ScheduledPowerPoint, simulate_projected_battery_state


def test_battery_metrics_derives_degradation_cost_from_equivalent_full_cycles() -> None:
	metrics = BatteryPhysicalMetrics(
		capacity_mwh=4.0,
		max_power_mw=2.0,
		round_trip_efficiency=0.81,
		degradation_cost_per_cycle_uah=40.0,
		soc_min_fraction=0.25,
		soc_max_fraction=0.75,
	)

	assert metrics.degradation_cost_per_mwh_throughput_uah == pytest.approx(5.0)


def test_projected_battery_state_degradation_penalty_uses_throughput_efc_cost() -> None:
	metrics = BatteryPhysicalMetrics(
		capacity_mwh=4.0,
		max_power_mw=2.0,
		round_trip_efficiency=0.81,
		degradation_cost_per_cycle_uah=40.0,
		soc_min_fraction=0.25,
		soc_max_fraction=0.75,
	)
	anchor = datetime(2026, 5, 1, 6, tzinfo=UTC)

	result = simulate_projected_battery_state(
		schedule=[
			ScheduledPowerPoint(interval_start=anchor, net_power_mw=1.0),
			ScheduledPowerPoint(interval_start=anchor + timedelta(hours=1), net_power_mw=-2.0),
		],
		battery_metrics=metrics,
		starting_soc_fraction=0.5,
	)

	expected_penalty = result.total_throughput_mwh * metrics.degradation_cost_per_mwh_throughput_uah
	expected_efc_cost = (
		result.total_throughput_mwh
		/ (2.0 * metrics.capacity_mwh)
		* metrics.degradation_cost_per_cycle_uah
	)
	assert result.total_degradation_penalty_uah == pytest.approx(expected_penalty)
	assert result.total_degradation_penalty_uah == pytest.approx(expected_efc_cost)


def test_lp_baseline_degradation_penalty_uses_same_throughput_efc_cost() -> None:
	metrics = BatteryPhysicalMetrics(
		capacity_mwh=1.0,
		max_power_mw=0.5,
		round_trip_efficiency=0.9,
		degradation_cost_per_cycle_uah=120.0,
		soc_min_fraction=0.1,
		soc_max_fraction=0.9,
	)
	anchor = datetime(2026, 5, 1, 0, tzinfo=UTC)
	forecast = [
		BaselineForecastPoint(
			forecast_timestamp=anchor + timedelta(hours=index + 1),
			source_timestamp=anchor - timedelta(hours=24 - index),
			predicted_price_uah_mwh=price,
		)
		for index, price in enumerate([1000.0, 6500.0, 900.0, 7000.0])
	]
	solver = HourlyDamBaselineSolver(BaselineSolverConfig(planning_horizon_hours=4))

	result = solver.solve_dispatch_from_forecast(
		forecast=forecast,
		battery_metrics=metrics,
		current_soc_fraction=0.5,
		anchor_timestamp=anchor,
	)

	assert any(point.throughput_mwh > 1e-6 for point in result.schedule)
	for point in result.schedule:
		expected_penalty = point.throughput_mwh * metrics.degradation_cost_per_mwh_throughput_uah
		expected_efc_cost = point.throughput_mwh / (2.0 * metrics.capacity_mwh) * metrics.degradation_cost_per_cycle_uah
		assert point.degradation_penalty_uah == pytest.approx(expected_penalty)
		assert point.degradation_penalty_uah == pytest.approx(expected_efc_cost)
