from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from math import sqrt

from smart_arbitrage.gatekeeper.schemas import BatteryPhysicalMetrics


@dataclass(frozen=True, slots=True)
class ScheduledPowerPoint:
	interval_start: datetime
	net_power_mw: float


@dataclass(frozen=True, slots=True)
class ProjectedBatteryTracePoint:
	step_index: int
	interval_start: datetime
	requested_net_power_mw: float
	feasible_net_power_mw: float
	soc_before_fraction: float
	soc_after_fraction: float
	throughput_mwh: float
	degradation_penalty_uah: float


@dataclass(frozen=True, slots=True)
class ProjectedBatterySimulationResult:
	interval_minutes: int
	starting_soc_fraction: float
	total_throughput_mwh: float
	total_degradation_penalty_uah: float
	trace: list[ProjectedBatteryTracePoint]


def simulate_projected_battery_state(
	*,
	schedule: list[ScheduledPowerPoint],
	battery_metrics: BatteryPhysicalMetrics,
	starting_soc_fraction: float,
	interval_minutes: int = 60,
) -> ProjectedBatterySimulationResult:
	if not 0.0 <= starting_soc_fraction <= 1.0:
		raise ValueError("starting_soc_fraction must be between 0.0 and 1.0.")
	if interval_minutes != 60:
		raise ValueError("Projected battery state simulator supports only hourly intervals.")
	if len(schedule) == 0:
		raise ValueError("schedule must contain at least one hourly recommendation.")

	for index in range(1, len(schedule)):
		expected_timestamp = schedule[index - 1].interval_start + timedelta(minutes=interval_minutes)
		if schedule[index].interval_start != expected_timestamp:
			raise ValueError("schedule must contain contiguous hourly intervals.")

	dt_hours = interval_minutes / 60.0
	charge_efficiency = sqrt(battery_metrics.round_trip_efficiency)
	discharge_efficiency = sqrt(battery_metrics.round_trip_efficiency)
	soc_floor_mwh = battery_metrics.soc_min_fraction * battery_metrics.capacity_mwh
	soc_ceiling_mwh = battery_metrics.soc_max_fraction * battery_metrics.capacity_mwh
	current_soc_mwh = starting_soc_fraction * battery_metrics.capacity_mwh

	trace: list[ProjectedBatteryTracePoint] = []
	total_throughput_mwh = 0.0
	total_degradation_penalty_uah = 0.0

	for step_index, point in enumerate(schedule):
		requested_net_power_mw = point.net_power_mw
		power_limited_mw = max(-battery_metrics.max_power_mw, min(battery_metrics.max_power_mw, requested_net_power_mw))

		feasible_net_power_mw = 0.0
		if power_limited_mw > 0.0:
			available_energy_mwh = max(0.0, current_soc_mwh - soc_floor_mwh)
			max_discharge_mw = available_energy_mwh * discharge_efficiency / dt_hours
			feasible_net_power_mw = min(power_limited_mw, max_discharge_mw)
		elif power_limited_mw < 0.0:
			available_headroom_mwh = max(0.0, soc_ceiling_mwh - current_soc_mwh)
			max_charge_mw = available_headroom_mwh / (charge_efficiency * dt_hours)
			feasible_charge_mw = min(abs(power_limited_mw), max_charge_mw)
			feasible_net_power_mw = -feasible_charge_mw

		soc_before_fraction = current_soc_mwh / battery_metrics.capacity_mwh
		if feasible_net_power_mw >= 0.0:
			current_soc_mwh -= feasible_net_power_mw * dt_hours / discharge_efficiency
		else:
			current_soc_mwh += abs(feasible_net_power_mw) * charge_efficiency * dt_hours

		current_soc_mwh = max(soc_floor_mwh, min(soc_ceiling_mwh, current_soc_mwh))
		throughput_mwh = abs(feasible_net_power_mw) * dt_hours
		degradation_penalty_uah = battery_metrics.degradation_cost_per_mwh_throughput_uah * throughput_mwh
		total_throughput_mwh += throughput_mwh
		total_degradation_penalty_uah += degradation_penalty_uah
		trace.append(
			ProjectedBatteryTracePoint(
				step_index=step_index,
				interval_start=point.interval_start,
				requested_net_power_mw=requested_net_power_mw,
				feasible_net_power_mw=feasible_net_power_mw,
				soc_before_fraction=soc_before_fraction,
				soc_after_fraction=current_soc_mwh / battery_metrics.capacity_mwh,
				throughput_mwh=throughput_mwh,
				degradation_penalty_uah=degradation_penalty_uah,
			)
		)

	return ProjectedBatterySimulationResult(
		interval_minutes=interval_minutes,
		starting_soc_fraction=starting_soc_fraction,
		total_throughput_mwh=total_throughput_mwh,
		total_degradation_penalty_uah=total_degradation_penalty_uah,
		trace=trace,
	)