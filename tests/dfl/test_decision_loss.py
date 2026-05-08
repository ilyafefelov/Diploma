import pytest
import torch

from smart_arbitrage.dfl.decision_loss import (
    DecisionLossWeights,
    compute_decision_loss_v1,
)


def test_decision_loss_v1_returns_relaxed_regret_component() -> None:
    predicted_prices = torch.tensor([[10.0, 20.0, 30.0]], dtype=torch.float64, requires_grad=True)
    actual_prices = torch.tensor([[10.0, 20.0, 30.0]], dtype=torch.float64)
    charge = torch.tensor([[1.0, 0.0, 0.0]], dtype=torch.float64, requires_grad=True)
    discharge = torch.tensor([[0.0, 0.0, 1.0]], dtype=torch.float64, requires_grad=True)

    result = compute_decision_loss_v1(
        predicted_prices=predicted_prices,
        actual_prices=actual_prices,
        charge_mw=charge,
        discharge_mw=discharge,
        oracle_value_uah=torch.tensor([25.0], dtype=torch.float64),
        weights=DecisionLossWeights(
            relaxed_regret=1.0,
            spread_shape=0.0,
            rank_shape=0.0,
            mae=0.0,
            throughput=0.0,
        ),
    )

    assert result.relaxed_realized_value_uah.item() == pytest.approx(20.0)
    assert result.relaxed_regret_uah.item() == pytest.approx(5.0)
    assert result.total_loss.item() == pytest.approx(5.0)


def test_decision_loss_v1_penalizes_spread_rank_mae_and_throughput() -> None:
    aligned = compute_decision_loss_v1(
        predicted_prices=torch.tensor([[10.0, 20.0, 30.0]], dtype=torch.float64),
        actual_prices=torch.tensor([[10.0, 20.0, 30.0]], dtype=torch.float64),
        charge_mw=torch.tensor([[0.0, 0.0, 0.0]], dtype=torch.float64),
        discharge_mw=torch.tensor([[0.0, 0.0, 0.0]], dtype=torch.float64),
        oracle_value_uah=torch.tensor([0.0], dtype=torch.float64),
    )
    misaligned = compute_decision_loss_v1(
        predicted_prices=torch.tensor([[30.0, 20.0, 10.0]], dtype=torch.float64),
        actual_prices=torch.tensor([[10.0, 20.0, 30.0]], dtype=torch.float64),
        charge_mw=torch.tensor([[1.0, 0.0, 0.0]], dtype=torch.float64),
        discharge_mw=torch.tensor([[0.0, 0.0, 1.0]], dtype=torch.float64),
        oracle_value_uah=torch.tensor([0.0], dtype=torch.float64),
    )

    assert aligned.spread_shape_loss.item() == pytest.approx(0.0)
    assert aligned.mae_loss.item() == pytest.approx(0.0)
    assert misaligned.total_loss.item() > aligned.total_loss.item()
    assert misaligned.throughput_regularizer.item() > 0.0


def test_decision_loss_v1_backpropagates_to_forecast_and_dispatch_tensors() -> None:
    predicted_prices = torch.tensor([[11.0, 19.0, 31.0]], dtype=torch.float64, requires_grad=True)
    charge = torch.tensor([[1.0, 0.0, 0.0]], dtype=torch.float64, requires_grad=True)
    discharge = torch.tensor([[0.0, 0.0, 1.0]], dtype=torch.float64, requires_grad=True)

    result = compute_decision_loss_v1(
        predicted_prices=predicted_prices,
        actual_prices=torch.tensor([[10.0, 20.0, 30.0]], dtype=torch.float64),
        charge_mw=charge,
        discharge_mw=discharge,
        oracle_value_uah=torch.tensor([25.0], dtype=torch.float64),
    )
    result.total_loss.backward()

    assert predicted_prices.grad is not None
    assert charge.grad is not None
    assert discharge.grad is not None


def test_decision_loss_v1_rejects_mismatched_shapes() -> None:
    with pytest.raises(ValueError, match="same 2D shape"):
        compute_decision_loss_v1(
            predicted_prices=torch.tensor([[10.0, 20.0]], dtype=torch.float64),
            actual_prices=torch.tensor([[10.0, 20.0, 30.0]], dtype=torch.float64),
            charge_mw=torch.tensor([[0.0, 0.0]], dtype=torch.float64),
            discharge_mw=torch.tensor([[0.0, 0.0]], dtype=torch.float64),
            oracle_value_uah=torch.tensor([0.0], dtype=torch.float64),
        )
