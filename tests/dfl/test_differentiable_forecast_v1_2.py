from datetime import datetime, timedelta

import polars as pl
import pytest
import torch

from smart_arbitrage.dfl.differentiable_forecast_v1_2 import (
    build_price_corrector,
    extract_temporal_examples,
    profile_aware_training_objective,
    training_objective,
)


def _rolling_rows() -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    anchor = datetime(2026, 1, 1, 23)
    for window_index in (4, 3, 2, 1):
        for offset in range(2):
            timestamp = anchor + timedelta(days=((4 - window_index) * 2 + offset))
            rows.append(
                {
                    "tenant_id": "client_003_dnipro_factory",
                    "source_model_name": "source_a",
                    "forecast_model_name": "raw_a",
                    "anchor_timestamp": timestamp,
                    "evaluation_window_index": window_index,
                    "selection_role": "raw_reference",
                    "starting_soc_fraction": 0.5,
                    "oracle_value_uah": 100.0,
                    "regret_uah": 20.0,
                    "safety_violation_count": 0,
                    "evaluation_payload": {
                        "horizon": [
                            {
                                "step_index": step,
                                "interval_start": (timestamp + timedelta(hours=step + 1)).isoformat(),
                                "forecast_price_uah_mwh": 1000.0 + step,
                                "actual_price_uah_mwh": 1100.0 + step,
                                "net_power_mw": 0.0,
                                "degradation_penalty_uah": 0.0,
                            }
                            for step in range(4)
                        ]
                    },
                }
            )
    return pl.DataFrame(rows)


def test_extract_temporal_examples_preserves_window_boundaries() -> None:
    examples = extract_temporal_examples(_rolling_rows(), source_model_name="source_a")

    assert len(examples) == 8
    assert {example.window_index for example in examples} == {1, 2, 3, 4}
    train = [example for example in examples if example.window_index in {4, 3, 2}]
    evaluation = [example for example in examples if example.window_index == 1]
    assert {example.anchor_timestamp for example in train}.isdisjoint(
        {example.anchor_timestamp for example in evaluation}
    )


@pytest.mark.parametrize("architecture", ["mlp", "transformer"])
def test_price_correctors_preserve_batch_and_horizon_shape(architecture: str) -> None:
    model = build_price_corrector(
        architecture=architecture,
        horizon_hours=4,
        hidden_dim=16,
    )
    values = torch.tensor(
        [[-1.0, -0.5, 0.5, 1.0], [1.0, 0.5, -0.5, -1.0]],
        dtype=torch.float64,
    )

    corrected = model(values)

    assert corrected.shape == values.shape
    assert torch.isfinite(corrected).all()


def test_decision_objective_backpropagates_through_strictly_convex_layer() -> None:
    predicted = torch.tensor(
        [[1000.0, 4000.0, 1200.0, 6000.0]],
        dtype=torch.float64,
        requires_grad=True,
    )
    actual = torch.tensor(
        [[900.0, 4500.0, 1000.0, 6500.0]],
        dtype=torch.float64,
    )

    result = training_objective(
        objective_kind="decision_focused",
        predicted_prices=predicted,
        actual_prices=actual,
    )
    result.loss.backward()

    assert result.solver_status == "cvxpylayer_scaled_strictly_convex"
    assert predicted.grad is not None
    assert torch.isfinite(predicted.grad).all()
    assert torch.count_nonzero(predicted.grad).item() > 0


def test_profile_aware_objective_uses_tenant_battery_contracts() -> None:
    examples = extract_temporal_examples(_rolling_rows(), source_model_name="source_a")[:2]
    predicted = torch.tensor(
        [example.forecast_prices for example in examples],
        dtype=torch.float64,
        requires_grad=True,
    )
    actual = torch.tensor(
        [example.actual_prices for example in examples],
        dtype=torch.float64,
    )

    result = profile_aware_training_objective(
        objective_kind="decision_focused",
        predicted_prices=predicted,
        actual_prices=actual,
        examples=examples,
    )
    result.loss.backward()

    assert result.solver_status == "cvxpylayer_scaled_strictly_convex"
    assert predicted.grad is not None
    assert torch.isfinite(predicted.grad).all()
