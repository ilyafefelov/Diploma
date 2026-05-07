"""Bounded offline DFL experiment over rolling-origin benchmark rows."""

from __future__ import annotations

from datetime import datetime
import math
from typing import Any, Final

import polars as pl
import torch

from smart_arbitrage.dfl.relaxed_dispatch import _relaxed_dispatch_layer

EXPERIMENT_NAME: Final[str] = "offline_horizon_bias_dfl_v0"
CLAIM_SCOPE: Final[str] = "offline_dfl_experiment_not_full_dfl"
ACADEMIC_SCOPE: Final[str] = (
    "Offline differentiable relaxed-LP experiment over historical rolling-origin rows only. "
    "This is not full DFL, not a Decision Transformer, and not market execution."
)
REQUIRED_EVALUATION_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "forecast_model_name",
        "anchor_timestamp",
        "starting_soc_fraction",
        "evaluation_payload",
    }
)


def build_offline_dfl_experiment_frame(
    evaluation_frame: pl.DataFrame,
    *,
    tenant_id: str = "client_003_dnipro_factory",
    forecast_model_names: tuple[str, ...] = ("tft_silver_v0", "nbeatsx_silver_v0"),
    validation_fraction: float = 0.2,
    max_train_anchors: int = 32,
    max_validation_anchors: int = 18,
    epoch_count: int = 8,
    learning_rate: float = 10.0,
    capacity_mwh: float = 1.0,
    max_power_mw: float = 0.25,
    soc_min_fraction: float = 0.05,
    soc_max_fraction: float = 0.95,
    degradation_cost_per_mwh: float = 0.0,
) -> pl.DataFrame:
    """Train horizon price biases on prior anchors and score held-out anchors.

    The training loop is intentionally narrow: it learns one horizon-specific bias
    vector per tenant/model using the differentiable relaxed LP, then evaluates on
    later anchors. Final benchmark claims must still come from strict LP assets.
    """

    _validate_inputs(
        evaluation_frame=evaluation_frame,
        validation_fraction=validation_fraction,
        max_train_anchors=max_train_anchors,
        max_validation_anchors=max_validation_anchors,
        epoch_count=epoch_count,
        learning_rate=learning_rate,
        capacity_mwh=capacity_mwh,
        max_power_mw=max_power_mw,
        soc_min_fraction=soc_min_fraction,
        soc_max_fraction=soc_max_fraction,
        degradation_cost_per_mwh=degradation_cost_per_mwh,
    )
    rows: list[dict[str, Any]] = []
    for forecast_model_name in forecast_model_names:
        model_frame = (
            evaluation_frame
            .filter(
                (pl.col("tenant_id") == tenant_id)
                & (pl.col("forecast_model_name") == forecast_model_name)
            )
            .sort("anchor_timestamp")
        )
        if model_frame.height < 4:
            continue
        validation_rows = max(1, math.ceil(model_frame.height * validation_fraction))
        train_frame = model_frame.head(model_frame.height - validation_rows).tail(max_train_anchors)
        validation_frame = model_frame.tail(validation_rows).head(max_validation_anchors)
        if train_frame.height == 0 or validation_frame.height == 0:
            continue
        training_examples = _examples_from_frame(train_frame)
        validation_examples = _examples_from_frame(validation_frame)
        if not training_examples or not validation_examples:
            continue
        horizon_hours = len(training_examples[0].forecast_prices)
        if any(len(example.forecast_prices) != horizon_hours for example in training_examples + validation_examples):
            continue

        learned_biases, final_training_loss = _train_horizon_biases(
            training_examples=training_examples,
            horizon_hours=horizon_hours,
            epoch_count=epoch_count,
            learning_rate=learning_rate,
            capacity_mwh=capacity_mwh,
            max_power_mw=max_power_mw,
            soc_min_fraction=soc_min_fraction,
            soc_max_fraction=soc_max_fraction,
            degradation_cost_per_mwh=degradation_cost_per_mwh,
        )
        baseline_validation = _score_examples(
            examples=validation_examples,
            horizon_biases=[0.0] * horizon_hours,
            capacity_mwh=capacity_mwh,
            max_power_mw=max_power_mw,
            soc_min_fraction=soc_min_fraction,
            soc_max_fraction=soc_max_fraction,
            degradation_cost_per_mwh=degradation_cost_per_mwh,
        )
        dfl_validation = _score_examples(
            examples=validation_examples,
            horizon_biases=learned_biases,
            capacity_mwh=capacity_mwh,
            max_power_mw=max_power_mw,
            soc_min_fraction=soc_min_fraction,
            soc_max_fraction=soc_max_fraction,
            degradation_cost_per_mwh=degradation_cost_per_mwh,
        )
        rows.append(
            {
                "experiment_name": EXPERIMENT_NAME,
                "tenant_id": tenant_id,
                "forecast_model_name": forecast_model_name,
                "train_anchor_count": len(training_examples),
                "validation_anchor_count": len(validation_examples),
                "horizon_hours": horizon_hours,
                "epoch_count": epoch_count,
                "learning_rate": learning_rate,
                "last_training_anchor_timestamp": training_examples[-1].anchor_timestamp,
                "first_validation_anchor_timestamp": validation_examples[0].anchor_timestamp,
                "last_validation_anchor_timestamp": validation_examples[-1].anchor_timestamp,
                "learned_horizon_biases_uah_mwh": learned_biases,
                "final_training_loss": final_training_loss,
                "baseline_validation_realized_value_uah": baseline_validation.mean_realized_value_uah,
                "dfl_validation_realized_value_uah": dfl_validation.mean_realized_value_uah,
                "oracle_validation_realized_value_uah": dfl_validation.mean_oracle_value_uah,
                "baseline_validation_relaxed_regret_uah": baseline_validation.mean_regret_uah,
                "dfl_validation_relaxed_regret_uah": dfl_validation.mean_regret_uah,
                "validation_relaxed_regret_delta_uah": (
                    baseline_validation.mean_regret_uah - dfl_validation.mean_regret_uah
                ),
                "improved_over_baseline": dfl_validation.mean_regret_uah <= baseline_validation.mean_regret_uah,
                "data_quality_tier": "thesis_grade",
                "claim_scope": CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "academic_scope": ACADEMIC_SCOPE,
            }
        )
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).sort(["tenant_id", "forecast_model_name"])


class _ExperimentExample:
    def __init__(
        self,
        *,
        anchor_timestamp: datetime,
        starting_soc_fraction: float,
        forecast_prices: list[float],
        actual_prices: list[float],
    ) -> None:
        self.anchor_timestamp = anchor_timestamp
        self.starting_soc_fraction = starting_soc_fraction
        self.forecast_prices = forecast_prices
        self.actual_prices = actual_prices


class _Score:
    def __init__(
        self,
        *,
        mean_realized_value_uah: float,
        mean_oracle_value_uah: float,
        mean_regret_uah: float,
    ) -> None:
        self.mean_realized_value_uah = mean_realized_value_uah
        self.mean_oracle_value_uah = mean_oracle_value_uah
        self.mean_regret_uah = mean_regret_uah


def _validate_inputs(
    *,
    evaluation_frame: pl.DataFrame,
    validation_fraction: float,
    max_train_anchors: int,
    max_validation_anchors: int,
    epoch_count: int,
    learning_rate: float,
    capacity_mwh: float,
    max_power_mw: float,
    soc_min_fraction: float,
    soc_max_fraction: float,
    degradation_cost_per_mwh: float,
) -> None:
    missing_columns = REQUIRED_EVALUATION_COLUMNS.difference(evaluation_frame.columns)
    if missing_columns:
        raise ValueError(f"evaluation_frame is missing required columns: {sorted(missing_columns)}")
    if not 0.0 < validation_fraction < 1.0:
        raise ValueError("validation_fraction must be between 0 and 1.")
    if max_train_anchors <= 0:
        raise ValueError("max_train_anchors must be positive.")
    if max_validation_anchors <= 0:
        raise ValueError("max_validation_anchors must be positive.")
    if epoch_count <= 0:
        raise ValueError("epoch_count must be positive.")
    if learning_rate <= 0.0:
        raise ValueError("learning_rate must be positive.")
    if capacity_mwh <= 0.0:
        raise ValueError("capacity_mwh must be positive.")
    if max_power_mw <= 0.0:
        raise ValueError("max_power_mw must be positive.")
    if not 0.0 <= soc_min_fraction <= soc_max_fraction <= 1.0:
        raise ValueError("SOC bounds must stay within [0, 1].")
    if degradation_cost_per_mwh < 0.0:
        raise ValueError("degradation_cost_per_mwh cannot be negative.")


def _examples_from_frame(frame: pl.DataFrame) -> list[_ExperimentExample]:
    examples: list[_ExperimentExample] = []
    for row in frame.iter_rows(named=True):
        payload = _mapping(row["evaluation_payload"])
        if payload.get("data_quality_tier") != "thesis_grade":
            continue
        horizon_rows = _horizon_rows(payload)
        forecast_prices = _horizon_values(horizon_rows, "forecast_price_uah_mwh")
        actual_prices = _horizon_values(horizon_rows, "actual_price_uah_mwh")
        if len(forecast_prices) < 2 or len(forecast_prices) != len(actual_prices):
            continue
        anchor_timestamp = row["anchor_timestamp"]
        if not isinstance(anchor_timestamp, datetime):
            continue
        examples.append(
            _ExperimentExample(
                anchor_timestamp=anchor_timestamp,
                starting_soc_fraction=float(row["starting_soc_fraction"]),
                forecast_prices=forecast_prices,
                actual_prices=actual_prices,
            )
        )
    return examples


def _train_horizon_biases(
    *,
    training_examples: list[_ExperimentExample],
    horizon_hours: int,
    epoch_count: int,
    learning_rate: float,
    capacity_mwh: float,
    max_power_mw: float,
    soc_min_fraction: float,
    soc_max_fraction: float,
    degradation_cost_per_mwh: float,
) -> tuple[list[float], float]:
    starting_soc_fraction = _single_starting_soc(training_examples)
    layer = _relaxed_dispatch_layer(
        horizon_hours,
        starting_soc_fraction,
        capacity_mwh,
        max_power_mw,
        soc_min_fraction,
        soc_max_fraction,
        1.0,
        degradation_cost_per_mwh,
    )
    forecast_prices = _price_tensor([example.forecast_prices for example in training_examples])
    actual_prices = _price_tensor([example.actual_prices for example in training_examples])
    horizon_biases = torch.zeros(horizon_hours, dtype=torch.float64, requires_grad=True)
    optimizer = torch.optim.Adam([horizon_biases], lr=learning_rate)
    final_loss = 0.0
    for _ in range(epoch_count):
        optimizer.zero_grad()
        corrected_prices = forecast_prices + horizon_biases
        charge, discharge, _ = layer(corrected_prices, solver_args={"eps": 1e-6, "max_iters": 5000})
        realized_values = _realized_values(
            actual_prices=actual_prices,
            charge=charge,
            discharge=discharge,
            degradation_cost_per_mwh=degradation_cost_per_mwh,
        )
        regularization = torch.mean(torch.square(horizon_biases / 1000.0))
        loss = -torch.mean(realized_values) + regularization
        loss.backward()
        optimizer.step()
        with torch.no_grad():
            horizon_biases.clamp_(min=-5000.0, max=5000.0)
        final_loss = float(loss.detach().cpu())
    return [round(float(value), 6) for value in horizon_biases.detach().cpu().tolist()], final_loss


def _score_examples(
    *,
    examples: list[_ExperimentExample],
    horizon_biases: list[float],
    capacity_mwh: float,
    max_power_mw: float,
    soc_min_fraction: float,
    soc_max_fraction: float,
    degradation_cost_per_mwh: float,
) -> _Score:
    starting_soc_fraction = _single_starting_soc(examples)
    horizon_hours = len(horizon_biases)
    layer = _relaxed_dispatch_layer(
        horizon_hours,
        starting_soc_fraction,
        capacity_mwh,
        max_power_mw,
        soc_min_fraction,
        soc_max_fraction,
        1.0,
        degradation_cost_per_mwh,
    )
    forecast_prices = _price_tensor([example.forecast_prices for example in examples])
    actual_prices = _price_tensor([example.actual_prices for example in examples])
    bias_tensor = torch.tensor(horizon_biases, dtype=torch.float64)
    with torch.no_grad():
        charge, discharge, _ = layer(forecast_prices + bias_tensor, solver_args={"eps": 1e-6, "max_iters": 5000})
        oracle_charge, oracle_discharge, _ = layer(actual_prices, solver_args={"eps": 1e-6, "max_iters": 5000})
        realized_values = _realized_values(
            actual_prices=actual_prices,
            charge=charge,
            discharge=discharge,
            degradation_cost_per_mwh=degradation_cost_per_mwh,
        )
        oracle_values = _realized_values(
            actual_prices=actual_prices,
            charge=oracle_charge,
            discharge=oracle_discharge,
            degradation_cost_per_mwh=degradation_cost_per_mwh,
        )
        regrets = torch.clamp(oracle_values - realized_values, min=0.0)
    return _Score(
        mean_realized_value_uah=float(torch.mean(realized_values).detach().cpu()),
        mean_oracle_value_uah=float(torch.mean(oracle_values).detach().cpu()),
        mean_regret_uah=float(torch.mean(regrets).detach().cpu()),
    )


def _single_starting_soc(examples: list[_ExperimentExample]) -> float:
    values = {round(example.starting_soc_fraction, 8) for example in examples}
    if len(values) != 1:
        raise ValueError("offline DFL experiment requires a single starting SOC within the batch.")
    return values.pop()


def _realized_values(
    *,
    actual_prices: torch.Tensor,
    charge: torch.Tensor,
    discharge: torch.Tensor,
    degradation_cost_per_mwh: float,
) -> torch.Tensor:
    return torch.sum(
        actual_prices * (discharge - charge) - degradation_cost_per_mwh * (charge + discharge),
        dim=1,
    )


def _price_tensor(values: list[list[float]]) -> torch.Tensor:
    return torch.tensor(values, dtype=torch.float64)


def _mapping(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return value


def _horizon_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    horizon = payload.get("horizon")
    if not isinstance(horizon, list):
        return []
    return [item for item in horizon if isinstance(item, dict)]


def _horizon_values(horizon_rows: list[dict[str, Any]], column_name: str) -> list[float]:
    return [float(row[column_name]) for row in horizon_rows if column_name in row]
