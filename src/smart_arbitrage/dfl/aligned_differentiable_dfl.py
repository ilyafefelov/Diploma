"""Input contract for the preregistered aligned differentiable DFL experiment.

The experiment compares the same contextual transformer under forecast loss and
hybrid forecast-plus-decision loss. This module intentionally accepts only a
fully source-backed, point-in-time feature panel; it must refuse the older
candidate library, whose outcome fields are useful labels but not sufficient
full-history context.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
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


def build_aligned_dfl_context_frame(
    hourly_context: pl.DataFrame,
    rolling_quantile_rows: pl.DataFrame,
) -> pl.DataFrame:
    """Join source-backed hourly covariates to rolling TFT quantile forecasts.

    The output has one row per tenant/anchor. Forecast quantiles remain model
    inputs; realized prices are retained only as labels. Poland inputs are
    accepted only on the governed experimental-ablation route, never silently
    promoted into official headline training.
    """

    hourly_required = {
        "tenant_id",
        "ds",
        "lag_24_price_uah_mwh",
        "weather_temperature",
        "hour_sin",
        "hour_cos",
        "entsoe_pl_lag24_day_ahead_price_uah_mwh",
        "external_feature_training_status",
    }
    rolling_required = {
        "tenant_id",
        "anchor_timestamp",
        "starting_soc_fraction",
        "oracle_value_uah",
        "regret_uah",
        "forecast_model_name",
        "evaluation_payload",
    }
    missing_hourly = sorted(hourly_required.difference(hourly_context.columns))
    if missing_hourly:
        raise ValueError(f"hourly_context is missing columns: {missing_hourly}")
    missing_rolling = sorted(rolling_required.difference(rolling_quantile_rows.columns))
    if missing_rolling:
        raise ValueError(f"rolling_quantile_rows is missing columns: {missing_rolling}")
    statuses = set(hourly_context["external_feature_training_status"].unique().to_list())
    if statuses != {"experimental_ablation_only"}:
        raise ValueError(
            "aligned DFL Poland context requires experimental_ablation_only source status."
        )

    hourly_by_key: dict[tuple[str, datetime], dict[str, object]] = {}
    for row in hourly_context.select(sorted(hourly_required)).iter_rows(named=True):
        timestamp = row["ds"]
        if not isinstance(timestamp, datetime):
            raise TypeError("hourly_context.ds must contain datetime values.")
        hourly_by_key[(str(row["tenant_id"]), timestamp)] = row

    grouped: dict[tuple[str, datetime, float], dict[str, dict[str, object]]] = {}
    for row in rolling_quantile_rows.iter_rows(named=True):
        anchor = row["anchor_timestamp"]
        if not isinstance(anchor, datetime):
            raise TypeError("rolling_quantile_rows.anchor_timestamp must contain datetimes.")
        role = _tft_quantile_role(str(row["forecast_model_name"]))
        if role is None:
            continue
        key = (str(row["tenant_id"]), anchor, float(row["starting_soc_fraction"]))
        quantiles = grouped.setdefault(key, {})
        if role in quantiles:
            raise ValueError(f"Duplicate {role} row for aligned DFL key: {key}")
        quantiles[role] = row

    rows: list[dict[str, object]] = []
    for key, quantiles in sorted(grouped.items(), key=lambda item: item[0]):
        if set(quantiles) != {"p10", "p50", "p90"}:
            continue
        tenant_id, anchor_timestamp, starting_soc_fraction = key
        vectors = {
            role: _horizon_vectors(quantiles[role]["evaluation_payload"])
            for role in ("p10", "p50", "p90")
        }
        timestamps = vectors["p50"]["timestamps"]
        actual_prices = vectors["p50"]["actual_prices"]
        for role in ("p10", "p90"):
            if vectors[role]["timestamps"] != timestamps:
                raise ValueError(f"TFT quantile horizons disagree for aligned DFL key: {key}")
            if vectors[role]["actual_prices"] != actual_prices:
                raise ValueError(f"TFT quantile labels disagree for aligned DFL key: {key}")
        hourly_rows = [hourly_by_key.get((tenant_id, timestamp)) for timestamp in timestamps]
        if any(row is None for row in hourly_rows):
            raise ValueError(f"Missing hourly source context for aligned DFL key: {key}")
        resolved_hourly = [row for row in hourly_rows if row is not None]
        p50_row = quantiles["p50"]
        rows.append(
            {
                "tenant_id": tenant_id,
                "source_model_name": str(p50_row["forecast_model_name"]),
                "anchor_timestamp": anchor_timestamp,
                "starting_soc_fraction": starting_soc_fraction,
                "forecast_p10_uah_mwh": vectors["p10"]["forecast_prices"],
                "forecast_p50_uah_mwh": vectors["p50"]["forecast_prices"],
                "forecast_p90_uah_mwh": vectors["p90"]["forecast_prices"],
                "price_lag_24_uah_mwh": [
                    float(row["lag_24_price_uah_mwh"]) for row in resolved_hourly
                ],
                "weather_temperature_c": [
                    float(row["weather_temperature"]) for row in resolved_hourly
                ],
                "calendar_hour_sin": [float(row["hour_sin"]) for row in resolved_hourly],
                "calendar_hour_cos": [float(row["hour_cos"]) for row in resolved_hourly],
                "poland_lag24_uah_mwh": [
                    float(row["entsoe_pl_lag24_day_ahead_price_uah_mwh"])
                    for row in resolved_hourly
                ],
                "actual_price_uah_mwh_vector": actual_prices,
                "oracle_value_uah": float(p50_row["oracle_value_uah"]),
                "raw_regret_uah": float(p50_row["regret_uah"]),
                "market_execution_enabled": False,
                "claim_scope": "aligned_dfl_experimental_poland_context_not_market_execution",
            }
        )
    if not rows:
        raise ValueError("No complete p10/p50/p90 aligned DFL examples were found.")
    return pl.DataFrame(rows).sort(["tenant_id", "anchor_timestamp"])


def _tft_quantile_role(model_name: str) -> str | None:
    if not model_name.startswith("tft_"):
        return None
    if "p10" in model_name:
        return "p10"
    if "p90" in model_name:
        return "p90"
    if model_name.endswith("_v1"):
        return "p50"
    return None


def _horizon_vectors(payload: object) -> dict[str, list[object]]:
    if not isinstance(payload, dict):
        raise TypeError("evaluation_payload must be a mapping.")
    horizon = payload.get("horizon")
    if not isinstance(horizon, list) or not horizon:
        raise ValueError("evaluation_payload.horizon must be a non-empty list.")
    timestamps: list[datetime] = []
    forecast_prices: list[float] = []
    actual_prices: list[float] = []
    for point in horizon:
        if not isinstance(point, dict):
            raise TypeError("evaluation_payload.horizon entries must be mappings.")
        interval_start = datetime.fromisoformat(str(point["interval_start"]).replace("Z", "+00:00"))
        timestamps.append(interval_start.replace(tzinfo=None))
        forecast_prices.append(float(point["forecast_price_uah_mwh"]))
        actual_prices.append(float(point["actual_price_uah_mwh"]))
    return {
        "timestamps": timestamps,
        "forecast_prices": forecast_prices,
        "actual_prices": actual_prices,
    }


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
