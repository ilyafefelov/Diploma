"""Input contract for the preregistered aligned differentiable DFL experiment.

The experiment compares the same contextual transformer under forecast loss and
hybrid forecast-plus-decision loss. This module intentionally accepts only a
fully source-backed, point-in-time feature panel; it must refuse the older
candidate library, whose outcome fields are useful labels but not sufficient
full-history context.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import numpy as np
import polars as pl
import torch
from torch import nn

from smart_arbitrage.dfl.differentiable_forecast_v1_2 import (
    TemporalPriceExample,
    TrainingObjectiveResult,
    profile_aware_training_objective,
)

ALIGNED_DFL_FEATURE_COLUMNS: Final[tuple[str, ...]] = (
    "forecast_p50_uah_mwh",
    "price_lag_24_uah_mwh",
    "weather_temperature_c",
    "calendar_hour_sin",
    "calendar_hour_cos",
    "forecast_p10_uah_mwh",
    "forecast_p90_uah_mwh",
    "poland_lag24_uah_mwh",
)
_IDENTITY_COLUMNS: Final[frozenset[str]] = frozenset(
    {"tenant_id", "source_model_name", "anchor_timestamp", "starting_soc_fraction"}
)
_LABEL_COLUMNS: Final[frozenset[str]] = frozenset(
    {"actual_price_uah_mwh_vector"}
)


@dataclass(frozen=True, slots=True)
class AlignedDflContextTensor:
    """Prior-safe context tensor with labels deliberately kept separate."""

    features: np.ndarray
    feature_names: tuple[str, ...]


class AlignedDflTransformer(nn.Module):
    """One contextual forecast architecture shared by both aligned losses."""

    def __init__(self, *, feature_count: int, hidden_dim: int) -> None:
        super().__init__()
        if feature_count <= 0 or hidden_dim <= 0 or hidden_dim % 2:
            raise ValueError("feature_count and even hidden_dim must be positive.")
        self.input_projection = nn.Linear(feature_count, hidden_dim)
        layer = nn.TransformerEncoderLayer(
            d_model=hidden_dim,
            nhead=2,
            dim_feedforward=hidden_dim * 2,
            dropout=0.0,
            activation="gelu",
            batch_first=True,
            norm_first=True,
        )
        self.encoder = nn.TransformerEncoder(layer, num_layers=2)
        self.output = nn.Linear(hidden_dim, 1)

    def forward(self, context: torch.Tensor) -> torch.Tensor:
        if context.ndim != 3:
            raise ValueError("aligned DFL context must have shape [batch, horizon, feature].")
        return self.output(self.encoder(self.input_projection(context))).squeeze(-1)


def warm_start_hybrid_transformer(
    *,
    forecast_model: AlignedDflTransformer,
    hybrid_model: AlignedDflTransformer,
) -> None:
    """Copy the forecast checkpoint into the same-architecture hybrid model."""

    hybrid_model.load_state_dict(forecast_model.state_dict())


def hybrid_forecast_decision_loss(
    *,
    predicted_prices: torch.Tensor,
    actual_prices: torch.Tensor,
    examples: list[TemporalPriceExample],
    hybrid_weight: float,
    smoothing_weight: float,
) -> TrainingObjectiveResult:
    """Combine forecast and storage-decision objectives under one contract."""

    if not 0.0 <= hybrid_weight <= 1.0:
        raise ValueError("hybrid_weight must be in [0, 1].")
    if smoothing_weight < 0.0:
        raise ValueError("smoothing_weight must be non-negative.")
    forecast_mse = torch.mean(torch.square(predicted_prices - actual_prices))
    decision = profile_aware_training_objective(
        objective_kind="decision_focused",
        predicted_prices=predicted_prices,
        actual_prices=actual_prices,
        examples=examples,
        enforce_terminal_soc_equality=False,
    )
    smoothness = torch.mean(torch.square(predicted_prices[:, 1:] - predicted_prices[:, :-1]))
    loss = (
        (1.0 - hybrid_weight) * (forecast_mse / 1_000_000.0)
        + hybrid_weight * decision.loss
        + smoothing_weight * (smoothness / 1_000_000.0)
    )
    return TrainingObjectiveResult(
        loss=loss,
        forecast_mse=forecast_mse,
        realized_value_uah=decision.realized_value_uah,
        solver_status=decision.solver_status,
    )


def build_aligned_dfl_context_tensor(frame: pl.DataFrame) -> AlignedDflContextTensor:
    """Validate and convert a complete point-in-time DFL context panel."""

    required = _IDENTITY_COLUMNS | _LABEL_COLUMNS | set(ALIGNED_DFL_FEATURE_COLUMNS)
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(
            "aligned DFL panel is missing required prior-safe columns: "
            f"{sorted(missing)}"
        )
    if frame.is_empty():
        raise ValueError("aligned DFL panel must contain at least one row.")
    vectors: list[np.ndarray] = []
    horizon: int | None = None
    for row in frame.select(ALIGNED_DFL_FEATURE_COLUMNS).iter_rows(named=True):
        columns: list[np.ndarray] = []
        for feature_name in ALIGNED_DFL_FEATURE_COLUMNS:
            value = row[feature_name]
            if not isinstance(value, list) or not value:
                raise ValueError(
                    f"aligned DFL feature {feature_name!r} must be a non-empty list."
                )
            column = np.asarray(value, dtype=np.float64)
            if not np.isfinite(column).all():
                raise ValueError(
                    f"aligned DFL feature {feature_name!r} must contain finite values."
                )
            columns.append(column)
        row_horizon = len(columns[0])
        if any(len(column) != row_horizon for column in columns):
            raise ValueError("aligned DFL feature vectors must share one horizon per row.")
        if horizon is None:
            horizon = row_horizon
        elif horizon != row_horizon:
            raise ValueError("aligned DFL panel must use one shared forecast horizon.")
        vectors.append(np.stack(columns, axis=-1))
    return AlignedDflContextTensor(
        features=np.stack(vectors, axis=0),
        feature_names=ALIGNED_DFL_FEATURE_COLUMNS,
    )


def assess_aligned_dfl_feature_readiness(frame: pl.DataFrame) -> dict[str, object]:
    """Report whether an artifact can enter the aligned DFL training protocol."""

    required = _IDENTITY_COLUMNS | _LABEL_COLUMNS | set(ALIGNED_DFL_FEATURE_COLUMNS)
    missing = sorted(required.difference(frame.columns))
    return {
        "ready": not missing and frame.height > 0,
        "row_count": frame.height,
        "required_feature_columns": list(ALIGNED_DFL_FEATURE_COLUMNS),
        "missing_columns": missing,
        "market_execution_enabled": False,
        "claim_scope": "aligned_dfl_feature_readiness_not_market_execution",
    }
