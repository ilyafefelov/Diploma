from datetime import datetime

import polars as pl
import pytest
import torch

from smart_arbitrage.dfl.aligned_differentiable_dfl import (
    ALIGNED_DFL_FEATURE_COLUMNS,
    AlignedDflTransformer,
    build_aligned_dfl_context_tensor,
    warm_start_hybrid_transformer,
)


def test_aligned_dfl_context_requires_all_preregistered_prior_features() -> None:
    with pytest.raises(ValueError, match="missing required prior-safe columns"):
        build_aligned_dfl_context_tensor(_context_frame().drop("poland_lag24_uah_mwh"))


def test_aligned_dfl_context_excludes_outcomes_from_model_inputs() -> None:
    result = build_aligned_dfl_context_tensor(_context_frame())

    assert result.feature_names == ALIGNED_DFL_FEATURE_COLUMNS
    assert result.features.shape == (1, 2, len(ALIGNED_DFL_FEATURE_COLUMNS))
    assert "actual_price_uah_mwh_vector" not in result.feature_names
    assert "regret_uah" not in result.feature_names


def test_aligned_dfl_uses_same_transformer_and_warm_start_for_hybrid_loss() -> None:
    forecast_model = AlignedDflTransformer(
        feature_count=len(ALIGNED_DFL_FEATURE_COLUMNS),
        hidden_dim=16,
    ).double()
    hybrid_model = AlignedDflTransformer(
        feature_count=len(ALIGNED_DFL_FEATURE_COLUMNS),
        hidden_dim=16,
    ).double()
    context = torch.ones((2, 2, len(ALIGNED_DFL_FEATURE_COLUMNS)), dtype=torch.float64)

    warm_start_hybrid_transformer(
        forecast_model=forecast_model,
        hybrid_model=hybrid_model,
    )

    assert forecast_model(context).shape == (2, 2)
    for forecast_parameter, hybrid_parameter in zip(
        forecast_model.parameters(), hybrid_model.parameters(), strict=True
    ):
        assert torch.equal(forecast_parameter, hybrid_parameter)


def _context_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "tenant_id": ["client_001_kyiv_mall"],
            "source_model_name": ["calibrated"],
            "anchor_timestamp": [datetime(2026, 1, 1, 23)],
            "starting_soc_fraction": [0.5],
            "forecast_p50_uah_mwh": [[100.0, 200.0]],
            "price_lag_24_uah_mwh": [[90.0, 190.0]],
            "weather_temperature_c": [[5.0, 5.5]],
            "calendar_hour_sin": [[0.0, 0.25]],
            "calendar_hour_cos": [[1.0, 0.97]],
            "forecast_p10_uah_mwh": [[80.0, 180.0]],
            "forecast_p90_uah_mwh": [[120.0, 220.0]],
            "poland_lag24_uah_mwh": [[85.0, 185.0]],
            "actual_price_uah_mwh_vector": [[105.0, 205.0]],
            "regret_uah": [10.0],
        }
    )
