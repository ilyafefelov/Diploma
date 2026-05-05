"""Small regret-weighted forecast calibration pilot.

This is intentionally not a full differentiable optimizer. It is a thesis-safe
pilot that weights forecast calibration by downstream regret so that the next
DFL iteration has a measured value-oriented starting point.
"""

from __future__ import annotations

import math

import polars as pl

PILOT_NAME = "regret_weighted_bias_correction_v0"


def run_regret_weighted_dfl_pilot(
    training_frame: pl.DataFrame,
    *,
    tenant_id: str,
    forecast_model_name: str,
    validation_fraction: float = 0.2,
) -> pl.DataFrame:
    """Fit a regret-weighted mean price bias on prior rows and score a holdout slice."""

    if not 0.0 < validation_fraction < 1.0:
        raise ValueError("validation_fraction must be between 0 and 1.")
    _validate_training_frame(training_frame)
    model_frame = (
        training_frame
        .filter(
            (pl.col("tenant_id") == tenant_id)
            & (pl.col("forecast_model_name") == forecast_model_name)
        )
        .sort("anchor_timestamp")
    )
    if model_frame.height < 4:
        raise ValueError("Regret-weighted DFL pilot requires at least 4 examples.")
    validation_rows = max(1, math.ceil(model_frame.height * validation_fraction))
    train_frame = model_frame.head(model_frame.height - validation_rows)
    validation_frame = model_frame.tail(validation_rows)
    if train_frame.height == 0:
        raise ValueError("Regret-weighted DFL pilot requires at least one training row.")

    bias = _weighted_mean(
        [
            float(row["mean_actual_price_uah_mwh"]) - float(row["mean_forecast_price_uah_mwh"])
            for row in train_frame.iter_rows(named=True)
        ],
        [float(row["training_weight"]) for row in train_frame.iter_rows(named=True)],
    )
    before_errors: list[float] = []
    after_errors: list[float] = []
    weights: list[float] = []
    regrets: list[float] = []
    for row in validation_frame.iter_rows(named=True):
        actual = float(row["mean_actual_price_uah_mwh"])
        forecast = float(row["mean_forecast_price_uah_mwh"])
        weight = float(row["training_weight"])
        before_errors.append(abs(actual - forecast))
        after_errors.append(abs(actual - (forecast + bias)))
        weights.append(weight)
        regrets.append(float(row["regret_uah"]))

    before_mae = _weighted_mean(before_errors, weights)
    after_mae = _weighted_mean(after_errors, weights)
    return pl.DataFrame(
        [
            {
                "pilot_name": PILOT_NAME,
                "scope": "pilot_not_full_dfl",
                "tenant_id": tenant_id,
                "forecast_model_name": forecast_model_name,
                "train_rows": train_frame.height,
                "validation_rows": validation_frame.height,
                "regret_weighted_bias_uah_mwh": bias,
                "validation_weighted_mae_before": before_mae,
                "validation_weighted_mae_after": after_mae,
                "validation_weighted_mae_delta": before_mae - after_mae,
                "mean_validation_regret_uah": sum(regrets) / len(regrets),
                "expanded_to_all_tenants_ready": after_mae <= before_mae,
                "academic_scope": (
                    "Regret-weighted forecast calibration pilot only. "
                    "This is not a full cvxpylayers DFL training loop."
                ),
            }
        ]
    )


def _validate_training_frame(training_frame: pl.DataFrame) -> None:
    required_columns = {
        "tenant_id",
        "anchor_timestamp",
        "forecast_model_name",
        "mean_forecast_price_uah_mwh",
        "mean_actual_price_uah_mwh",
        "training_weight",
        "regret_uah",
    }
    missing_columns = required_columns.difference(training_frame.columns)
    if missing_columns:
        raise ValueError(f"training_frame is missing required columns: {sorted(missing_columns)}")


def _weighted_mean(values: list[float], weights: list[float]) -> float:
    if len(values) != len(weights) or not values:
        raise ValueError("values and weights must have the same non-zero length.")
    denominator = sum(weights)
    if denominator <= 0.0:
        raise ValueError("weights must sum to a positive value.")
    return sum(value * weight for value, weight in zip(values, weights)) / denominator
