"""Differentiable relaxed LP primitive for storage dispatch DFL experiments."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from math import sqrt

import cvxpy as cp
from cvxpylayers.torch import CvxpyLayer
import torch


@dataclass(frozen=True, slots=True)
class RelaxedDispatchResult:
    charge_mw: list[float]
    discharge_mw: list[float]
    soc_fraction: list[float]
    objective_value: float
    academic_scope: str = "differentiable_relaxed_lp_training_primitive_not_final_milp"


def solve_relaxed_dispatch(
    *,
    prices_uah_mwh: list[float],
    starting_soc_fraction: float,
    capacity_mwh: float,
    max_power_mw: float,
    soc_min_fraction: float,
    soc_max_fraction: float,
    round_trip_efficiency: float = 1.0,
    degradation_cost_per_mwh: float = 0.0,
) -> RelaxedDispatchResult:
    """Solve a relaxed differentiable LP for one storage horizon.

    This is the training-time relaxation. Final benchmark evaluation must still use
    the strict LP/simulator path with feasibility checks and no silent violations.
    """

    _validate_inputs(
        prices_uah_mwh=prices_uah_mwh,
        starting_soc_fraction=starting_soc_fraction,
        capacity_mwh=capacity_mwh,
        max_power_mw=max_power_mw,
        soc_min_fraction=soc_min_fraction,
        soc_max_fraction=soc_max_fraction,
        round_trip_efficiency=round_trip_efficiency,
        degradation_cost_per_mwh=degradation_cost_per_mwh,
    )
    layer = _relaxed_dispatch_layer(
        len(prices_uah_mwh),
        starting_soc_fraction,
        capacity_mwh,
        max_power_mw,
        soc_min_fraction,
        soc_max_fraction,
        round_trip_efficiency,
        degradation_cost_per_mwh,
    )
    prices = torch.tensor(prices_uah_mwh, dtype=torch.float64)
    charge, discharge, soc = layer(prices, solver_args={"eps": 1e-8, "max_iters": 10000})
    charge_values = _tensor_to_list(charge)
    discharge_values = _tensor_to_list(discharge)
    soc_values = _tensor_to_list(soc)
    objective_value = sum(
        price * (discharge_value - charge_value)
        - degradation_cost_per_mwh * (charge_value + discharge_value)
        for price, charge_value, discharge_value in zip(prices_uah_mwh, charge_values, discharge_values, strict=True)
    )
    return RelaxedDispatchResult(
        charge_mw=charge_values,
        discharge_mw=discharge_values,
        soc_fraction=soc_values,
        objective_value=float(objective_value),
    )


@lru_cache(maxsize=64)
def _relaxed_dispatch_layer(
    horizon_hours: int,
    starting_soc_fraction: float,
    capacity_mwh: float,
    max_power_mw: float,
    soc_min_fraction: float,
    soc_max_fraction: float,
    round_trip_efficiency: float,
    degradation_cost_per_mwh: float,
) -> CvxpyLayer:
    prices = cp.Parameter(horizon_hours)
    charge = cp.Variable(horizon_hours, nonneg=True)
    discharge = cp.Variable(horizon_hours, nonneg=True)
    soc = cp.Variable(horizon_hours + 1)
    one_way_efficiency = sqrt(round_trip_efficiency)
    constraints = [
        soc[0] == starting_soc_fraction,
        soc[horizon_hours] == starting_soc_fraction,
        charge <= max_power_mw,
        discharge <= max_power_mw,
        soc >= soc_min_fraction,
        soc <= soc_max_fraction,
    ]
    for step_index in range(horizon_hours):
        constraints.append(
            soc[step_index + 1]
            == soc[step_index]
            + (charge[step_index] * one_way_efficiency / capacity_mwh)
            - (discharge[step_index] / one_way_efficiency / capacity_mwh)
        )
    cost = cp.sum(cp.multiply(prices, charge - discharge)) + degradation_cost_per_mwh * cp.sum(charge + discharge)
    problem = cp.Problem(cp.Minimize(cost), constraints)
    if not problem.is_dpp():
        raise ValueError("relaxed dispatch LP must satisfy DPP for cvxpylayers.")
    return CvxpyLayer(problem, parameters=[prices], variables=[charge, discharge, soc])


def _validate_inputs(
    *,
    prices_uah_mwh: list[float],
    starting_soc_fraction: float,
    capacity_mwh: float,
    max_power_mw: float,
    soc_min_fraction: float,
    soc_max_fraction: float,
    round_trip_efficiency: float,
    degradation_cost_per_mwh: float,
) -> None:
    if not prices_uah_mwh:
        raise ValueError("prices_uah_mwh must contain at least one price.")
    if capacity_mwh <= 0.0:
        raise ValueError("capacity_mwh must be positive.")
    if max_power_mw <= 0.0:
        raise ValueError("max_power_mw must be positive.")
    if not 0.0 <= soc_min_fraction <= starting_soc_fraction <= soc_max_fraction <= 1.0:
        raise ValueError("SOC bounds must contain starting_soc_fraction and stay within [0, 1].")
    if not 0.0 < round_trip_efficiency <= 1.0:
        raise ValueError("round_trip_efficiency must be in (0, 1].")
    if degradation_cost_per_mwh < 0.0:
        raise ValueError("degradation_cost_per_mwh cannot be negative.")


def _tensor_to_list(value: torch.Tensor) -> list[float]:
    return [float(item) for item in value.detach().cpu().tolist()]
