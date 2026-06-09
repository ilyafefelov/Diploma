"""Differentiable relaxed LP primitive for storage dispatch DFL experiments."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from math import isfinite, sqrt
from typing import Final

import cvxpy as cp
from cvxpylayers.torch import CvxpyLayer
import torch

DEFAULT_RELAXED_PRICE_SCALE_UAH_PER_MWH: Final[float] = 1000.0


@dataclass(frozen=True, slots=True)
class RelaxedDispatchResult:
    charge_mw: list[float]
    discharge_mw: list[float]
    soc_fraction: list[float]
    objective_value: float
    solver_status: str
    academic_scope: str = "differentiable_relaxed_lp_training_primitive_not_final_milp"


@dataclass(frozen=True, slots=True)
class RelaxedDispatchTensorResult:
    charge_mw: torch.Tensor
    discharge_mw: torch.Tensor
    soc_fraction: torch.Tensor
    solver_status: str


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
    price_scale_uah_per_mwh: float = DEFAULT_RELAXED_PRICE_SCALE_UAH_PER_MWH,
    fallback_to_surrogate: bool = True,
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
        price_scale_uah_per_mwh=price_scale_uah_per_mwh,
    )
    result = solve_relaxed_dispatch_tensor(
        prices_uah_mwh=torch.tensor(prices_uah_mwh, dtype=torch.float64),
        starting_soc_fraction=starting_soc_fraction,
        capacity_mwh=capacity_mwh,
        max_power_mw=max_power_mw,
        soc_min_fraction=soc_min_fraction,
        soc_max_fraction=soc_max_fraction,
        round_trip_efficiency=round_trip_efficiency,
        degradation_cost_per_mwh=degradation_cost_per_mwh,
        price_scale_uah_per_mwh=price_scale_uah_per_mwh,
        fallback_to_surrogate=fallback_to_surrogate,
        solver_args={"eps": 1e-8, "max_iters": 10000},
    )
    charge_values = _tensor_to_list(result.charge_mw)
    discharge_values = _tensor_to_list(result.discharge_mw)
    soc_values = _tensor_to_list(result.soc_fraction)
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
        solver_status=result.solver_status,
    )


def solve_relaxed_dispatch_tensor(
    *,
    prices_uah_mwh: torch.Tensor,
    starting_soc_fraction: float,
    capacity_mwh: float,
    max_power_mw: float,
    soc_min_fraction: float,
    soc_max_fraction: float,
    round_trip_efficiency: float = 1.0,
    degradation_cost_per_mwh: float = 0.0,
    price_scale_uah_per_mwh: float = DEFAULT_RELAXED_PRICE_SCALE_UAH_PER_MWH,
    fallback_to_surrogate: bool = True,
    solver_args: dict[str, float | int] | None = None,
) -> RelaxedDispatchTensorResult:
    """Solve a batch of relaxed dispatch problems with scaling and fallback.

    UAH prices are divided by ``price_scale_uah_per_mwh`` only inside the
    optimization layer. Degradation cost is scaled by the same factor, so the LP
    tradeoff is invariant while SCS receives numerically smaller coefficients.
    """

    _validate_tensor_inputs(
        prices_uah_mwh=prices_uah_mwh,
        starting_soc_fraction=starting_soc_fraction,
        capacity_mwh=capacity_mwh,
        max_power_mw=max_power_mw,
        soc_min_fraction=soc_min_fraction,
        soc_max_fraction=soc_max_fraction,
        round_trip_efficiency=round_trip_efficiency,
        degradation_cost_per_mwh=degradation_cost_per_mwh,
        price_scale_uah_per_mwh=price_scale_uah_per_mwh,
    )
    prices = prices_uah_mwh.to(dtype=torch.float64)
    horizon_hours = prices.shape[-1]
    try:
        layer = _relaxed_dispatch_layer(
            horizon_hours,
            starting_soc_fraction,
            capacity_mwh,
            max_power_mw,
            soc_min_fraction,
            soc_max_fraction,
            round_trip_efficiency,
            degradation_cost_per_mwh / price_scale_uah_per_mwh,
        )
        charge, discharge, soc = layer(
            prices / price_scale_uah_per_mwh,
            solver_args=solver_args or {"eps": 1e-6, "max_iters": 5000},
        )
        if not all(torch.isfinite(value).all().item() for value in (charge, discharge, soc)):
            raise RuntimeError("cvxpylayer returned non-finite relaxed dispatch values")
        return RelaxedDispatchTensorResult(
            charge_mw=charge,
            discharge_mw=discharge,
            soc_fraction=soc,
            solver_status="cvxpylayer_scaled",
        )
    except Exception as exc:
        if not fallback_to_surrogate:
            raise
        return _bounded_surrogate_dispatch(
            prices_uah_mwh=prices,
            starting_soc_fraction=starting_soc_fraction,
            capacity_mwh=capacity_mwh,
            max_power_mw=max_power_mw,
            soc_min_fraction=soc_min_fraction,
            soc_max_fraction=soc_max_fraction,
            round_trip_efficiency=round_trip_efficiency,
            reason=f"{exc.__class__.__name__}",
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
    price_scale_uah_per_mwh: float,
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
    if price_scale_uah_per_mwh <= 0.0 or not isfinite(price_scale_uah_per_mwh):
        raise ValueError("price_scale_uah_per_mwh must be a finite positive value.")


def _validate_tensor_inputs(
    *,
    prices_uah_mwh: torch.Tensor,
    starting_soc_fraction: float,
    capacity_mwh: float,
    max_power_mw: float,
    soc_min_fraction: float,
    soc_max_fraction: float,
    round_trip_efficiency: float,
    degradation_cost_per_mwh: float,
    price_scale_uah_per_mwh: float,
) -> None:
    if prices_uah_mwh.ndim not in {1, 2}:
        raise ValueError("prices_uah_mwh tensor must be 1D or 2D.")
    if prices_uah_mwh.shape[-1] <= 0:
        raise ValueError("prices_uah_mwh must contain at least one price.")
    _validate_inputs(
        prices_uah_mwh=[1.0],
        starting_soc_fraction=starting_soc_fraction,
        capacity_mwh=capacity_mwh,
        max_power_mw=max_power_mw,
        soc_min_fraction=soc_min_fraction,
        soc_max_fraction=soc_max_fraction,
        round_trip_efficiency=round_trip_efficiency,
        degradation_cost_per_mwh=degradation_cost_per_mwh,
        price_scale_uah_per_mwh=price_scale_uah_per_mwh,
    )


def _bounded_surrogate_dispatch(
    *,
    prices_uah_mwh: torch.Tensor,
    starting_soc_fraction: float,
    capacity_mwh: float,
    max_power_mw: float,
    soc_min_fraction: float,
    soc_max_fraction: float,
    round_trip_efficiency: float,
    reason: str,
) -> RelaxedDispatchTensorResult:
    original_ndim = prices_uah_mwh.ndim
    prices = prices_uah_mwh if original_ndim == 2 else prices_uah_mwh.unsqueeze(0)
    centered = prices - torch.mean(prices, dim=1, keepdim=True)
    temperature = torch.clamp(
        torch.std(prices.detach(), dim=1, keepdim=True, unbiased=False),
        min=1.0,
    )
    low_weights = torch.softmax(-centered / temperature, dim=1)
    high_weights = torch.softmax(centered / temperature, dim=1)
    one_way_efficiency = sqrt(round_trip_efficiency)
    max_charge_mwh = (soc_max_fraction - starting_soc_fraction) * capacity_mwh / one_way_efficiency
    max_discharge_mwh = (starting_soc_fraction - soc_min_fraction) * capacity_mwh * one_way_efficiency
    total_charge_mwh = max(
        0.0,
        min(max_power_mw, max_charge_mwh, max_discharge_mwh / round_trip_efficiency),
    )
    total_charge = prices.new_tensor(total_charge_mwh)
    charge = low_weights * total_charge
    discharge = high_weights * total_charge * round_trip_efficiency
    soc_deltas = (
        charge * one_way_efficiency / capacity_mwh
        - discharge / one_way_efficiency / capacity_mwh
    )
    starting_soc = prices.new_full((prices.shape[0], 1), starting_soc_fraction)
    soc = torch.cat([starting_soc, starting_soc + torch.cumsum(soc_deltas, dim=1)], dim=1)
    soc = torch.clamp(soc, min=soc_min_fraction, max=soc_max_fraction)
    if original_ndim == 1:
        charge = charge.squeeze(0)
        discharge = discharge.squeeze(0)
        soc = soc.squeeze(0)
    return RelaxedDispatchTensorResult(
        charge_mw=charge,
        discharge_mw=discharge,
        soc_fraction=soc,
        solver_status=f"surrogate_bounded:{reason}",
    )


def _tensor_to_list(value: torch.Tensor) -> list[float]:
    return [float(item) for item in value.detach().cpu().tolist()]
