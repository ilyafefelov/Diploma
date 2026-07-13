from datetime import datetime

import polars as pl
import pytest
import torch

from smart_arbitrage.dfl.aligned_differentiable_dfl import (
    ALIGNED_DFL_FEATURE_COLUMNS,
    AlignedDflTransformer,
    assess_aligned_dfl_feature_readiness,
    build_aligned_dfl_context_frame,
    build_aligned_dfl_context_tensor,
    hybrid_forecast_decision_loss,
    warm_start_hybrid_transformer,
)
from smart_arbitrage.dfl.differentiable_forecast_v1_2 import TemporalPriceExample


def test_aligned_dfl_context_requires_all_preregistered_prior_features() -> None:
    with pytest.raises(ValueError, match="missing required prior-safe columns"):
        build_aligned_dfl_context_tensor(_context_frame().drop("poland_lag24_uah_mwh"))


def test_aligned_dfl_context_excludes_outcomes_from_model_inputs() -> None:
    result = build_aligned_dfl_context_tensor(_context_frame())

    assert result.feature_names == ALIGNED_DFL_FEATURE_COLUMNS
    assert result.features.shape == (1, 2, len(ALIGNED_DFL_FEATURE_COLUMNS))
    assert "actual_price_uah_mwh_vector" not in result.feature_names
    assert "regret_uah" not in result.feature_names


def test_aligned_dfl_readiness_reports_missing_feature_families() -> None:
    readiness = assess_aligned_dfl_feature_readiness(_context_frame().drop("weather_temperature_c"))

    assert readiness["ready"] is False
    assert readiness["row_count"] == 1
    assert readiness["missing_columns"] == ["weather_temperature_c"]


def test_aligned_dfl_context_builder_keeps_actuals_outside_prior_safe_inputs() -> None:
    frame = build_aligned_dfl_context_frame(
        _hourly_context_frame(),
        _rolling_quantile_rows(),
    )

    assert frame.height == 1
    assert frame["forecast_p10_uah_mwh"].to_list() == [[80.0, 180.0]]
    assert frame["forecast_p50_uah_mwh"].to_list() == [[100.0, 200.0]]
    assert frame["forecast_p90_uah_mwh"].to_list() == [[120.0, 220.0]]
    assert frame["actual_price_uah_mwh_vector"].to_list() == [[105.0, 205.0]]
    assert frame["poland_lag24_uah_mwh"].to_list() == [[85.0, 185.0]]
    assert build_aligned_dfl_context_tensor(frame).features.shape == (1, 2, 8)


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


def test_aligned_dfl_hybrid_loss_uses_unconstrained_terminal_storage_contract() -> None:
    prices = torch.tensor([[100.0, 1000.0]], dtype=torch.float64)
    result = hybrid_forecast_decision_loss(
        predicted_prices=prices,
        actual_prices=prices,
        examples=[
            TemporalPriceExample(
                tenant_id="client_001_kyiv_mall",
                source_model_name="calibrated",
                anchor_timestamp=datetime(2026, 1, 1, 23),
                window_index=1,
                starting_soc_fraction=0.5,
                forecast_prices=(100.0, 1000.0),
                actual_prices=(100.0, 1000.0),
                oracle_value_uah=100.0,
                raw_regret_uah=10.0,
            )
        ],
        hybrid_weight=0.5,
        smoothing_weight=0.01,
    )

    assert torch.isfinite(result.loss)
    assert "cvxpylayer" in result.solver_status


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


def _hourly_context_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "tenant_id": ["client_001_kyiv_mall", "client_001_kyiv_mall"],
            "ds": [datetime(2026, 1, 2), datetime(2026, 1, 2, 1)],
            "lag_24_price_uah_mwh": [90.0, 190.0],
            "weather_temperature": [5.0, 5.5],
            "hour_sin": [0.0, 0.25],
            "hour_cos": [1.0, 0.97],
            "entsoe_pl_lag24_day_ahead_price_uah_mwh": [85.0, 185.0],
            "external_feature_training_status": [
                "experimental_ablation_only",
                "experimental_ablation_only",
            ],
        }
    )


def _rolling_quantile_rows() -> pl.DataFrame:
    horizon = [
        {
            "interval_start": "2026-01-02T00:00:00",
            "actual_price_uah_mwh": 105.0,
        },
        {
            "interval_start": "2026-01-02T01:00:00",
            "actual_price_uah_mwh": 205.0,
        },
    ]
    def payload(predictions: list[float]) -> dict[str, object]:
        return {
            "horizon": [
                {**point, "forecast_price_uah_mwh": prediction}
                for point, prediction in zip(horizon, predictions, strict=True)
            ]
        }

    return pl.DataFrame(
        {
            "tenant_id": ["client_001_kyiv_mall"] * 3,
            "anchor_timestamp": [datetime(2026, 1, 1, 23)] * 3,
            "starting_soc_fraction": [0.5] * 3,
            "oracle_value_uah": [300.0] * 3,
            "regret_uah": [10.0] * 3,
            "forecast_model_name": [
                "tft_example_p10",
                "tft_example_v1",
                "tft_example_p90",
            ],
            "evaluation_payload": [payload([80.0, 180.0]), payload([100.0, 200.0]), payload([120.0, 220.0])],
        }
    )
