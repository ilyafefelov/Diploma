from datetime import datetime

import polars as pl

from smart_arbitrage.assets.bronze.market_weather import build_synthetic_market_price_history
from smart_arbitrage.forecasting.neural_features import (
    DEFAULT_NEURAL_FORECAST_HORIZON_HOURS,
    build_neural_forecast_feature_frame,
)
from smart_arbitrage.forecasting.sota_training import build_sota_forecast_training_frame


def test_sota_training_frame_uses_official_library_schema_without_future_target_leakage() -> None:
    price_history = build_synthetic_market_price_history(
        history_hours=15 * 24,
        forecast_hours=DEFAULT_NEURAL_FORECAST_HORIZON_HOURS,
        now=datetime(2026, 5, 4, 12, 0),
    )
    feature_frame = build_neural_forecast_feature_frame(price_history, future_weather_mode="forecast_only")

    training_frame = build_sota_forecast_training_frame(
        feature_frame,
        tenant_id="client_003_dnipro_factory",
    )

    assert {"unique_id", "ds", "y", "split", "tenant_id", "sota_schema_version"}.issubset(training_frame.columns)
    assert training_frame.select("unique_id").to_series().unique().to_list() == ["client_003_dnipro_factory:DAM"]
    assert training_frame.filter(pl.col("split") == "forecast").select("y").null_count().item() == DEFAULT_NEURAL_FORECAST_HORIZON_HOURS
    assert training_frame.filter(pl.col("split") == "train").select("y").drop_nulls().height > 168
    assert "known_future_feature_columns_csv" in training_frame.columns
    assert "historical_observed_feature_columns_csv" in training_frame.columns


def test_sota_training_frame_rejects_missing_required_silver_columns() -> None:
    bad_frame = pl.DataFrame({"timestamp": [datetime(2026, 5, 1)], "split": ["train"]})

    try:
        build_sota_forecast_training_frame(bad_frame, tenant_id="tenant")
    except ValueError as error:
        assert "missing required columns" in str(error)
    else:
        raise AssertionError("build_sota_forecast_training_frame should reject incomplete frames.")
