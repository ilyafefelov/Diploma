"""Decision-focused loss components for tiny DFL correction experiments."""

from __future__ import annotations

from dataclasses import dataclass

import torch


@dataclass(frozen=True, slots=True)
class DecisionLossWeights:
    """Weights for the first bounded DFL loss."""

    relaxed_regret: float = 1.0
    spread_shape: float = 0.05
    rank_shape: float = 100.0
    mae: float = 0.01
    throughput: float = 1.0


@dataclass(frozen=True, slots=True)
class DecisionLossResult:
    """Named tensors for auditability and training diagnostics."""

    total_loss: torch.Tensor
    relaxed_realized_value_uah: torch.Tensor
    relaxed_regret_uah: torch.Tensor
    spread_shape_loss: torch.Tensor
    rank_shape_loss: torch.Tensor
    mae_loss: torch.Tensor
    throughput_regularizer: torch.Tensor


def compute_decision_loss_v1(
    *,
    predicted_prices: torch.Tensor,
    actual_prices: torch.Tensor,
    charge_mw: torch.Tensor,
    discharge_mw: torch.Tensor,
    oracle_value_uah: torch.Tensor,
    degradation_cost_per_mwh: float = 0.0,
    weights: DecisionLossWeights | None = None,
) -> DecisionLossResult:
    """Combine relaxed regret, AFL-style shape terms, MAE, and throughput regularization.

    ``charge_mw`` and ``discharge_mw`` are expected to come from the relaxed storage
    layer for ``predicted_prices``. The strict LP/oracle benchmark remains the final
    evaluator; this loss is only a training-time research primitive.
    """

    resolved_weights = weights or DecisionLossWeights()
    _validate_inputs(
        predicted_prices=predicted_prices,
        actual_prices=actual_prices,
        charge_mw=charge_mw,
        discharge_mw=discharge_mw,
        oracle_value_uah=oracle_value_uah,
        degradation_cost_per_mwh=degradation_cost_per_mwh,
    )
    throughput = charge_mw + discharge_mw
    realized_values = torch.sum(
        actual_prices * (discharge_mw - charge_mw)
        - degradation_cost_per_mwh * throughput,
        dim=1,
    )
    relaxed_regret = torch.clamp(oracle_value_uah - realized_values, min=0.0)
    spread_shape = torch.mean(torch.abs(_spread(predicted_prices) - _spread(actual_prices)))
    rank_shape = torch.mean(1.0 - _centered_cosine_similarity(predicted_prices, actual_prices))
    mae = torch.mean(torch.abs(predicted_prices - actual_prices))
    throughput_regularizer = torch.mean(throughput)
    total = (
        resolved_weights.relaxed_regret * torch.mean(relaxed_regret)
        + resolved_weights.spread_shape * spread_shape
        + resolved_weights.rank_shape * rank_shape
        + resolved_weights.mae * mae
        + resolved_weights.throughput * throughput_regularizer
    )
    return DecisionLossResult(
        total_loss=total,
        relaxed_realized_value_uah=torch.mean(realized_values),
        relaxed_regret_uah=torch.mean(relaxed_regret),
        spread_shape_loss=spread_shape,
        rank_shape_loss=rank_shape,
        mae_loss=mae,
        throughput_regularizer=throughput_regularizer,
    )


def _validate_inputs(
    *,
    predicted_prices: torch.Tensor,
    actual_prices: torch.Tensor,
    charge_mw: torch.Tensor,
    discharge_mw: torch.Tensor,
    oracle_value_uah: torch.Tensor,
    degradation_cost_per_mwh: float,
) -> None:
    if predicted_prices.ndim != 2:
        raise ValueError("predicted_prices, actual_prices, charge_mw, and discharge_mw must share the same 2D shape.")
    expected_shape = predicted_prices.shape
    if (
        actual_prices.shape != expected_shape
        or charge_mw.shape != expected_shape
        or discharge_mw.shape != expected_shape
    ):
        raise ValueError("predicted_prices, actual_prices, charge_mw, and discharge_mw must share the same 2D shape.")
    if expected_shape[0] == 0 or expected_shape[1] < 2:
        raise ValueError("decision loss requires at least one example and two horizon steps.")
    if oracle_value_uah.shape != (expected_shape[0],):
        raise ValueError("oracle_value_uah must be a 1D tensor with one value per example.")
    if degradation_cost_per_mwh < 0.0:
        raise ValueError("degradation_cost_per_mwh cannot be negative.")


def _spread(values: torch.Tensor) -> torch.Tensor:
    return torch.amax(values, dim=1) - torch.amin(values, dim=1)


def _centered_cosine_similarity(left: torch.Tensor, right: torch.Tensor) -> torch.Tensor:
    left_centered = left - torch.mean(left, dim=1, keepdim=True)
    right_centered = right - torch.mean(right, dim=1, keepdim=True)
    numerator = torch.sum(left_centered * right_centered, dim=1)
    denominator = torch.linalg.norm(left_centered, dim=1) * torch.linalg.norm(right_centered, dim=1)
    return torch.where(
        denominator > 1e-12,
        numerator / denominator,
        torch.ones_like(denominator),
    ).clamp(min=-1.0, max=1.0)
