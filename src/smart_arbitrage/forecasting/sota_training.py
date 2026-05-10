"""SOTA-ready training-frame contracts for official neural forecasting backends."""

from __future__ import annotations

from typing import Final

import polars as pl

from smart_arbitrage.forecasting.neural_features import NEURAL_FORECAST_FEATURE_COLUMNS

SOTA_SCHEMA_VERSION: Final[str] = "sota_forecast_training_v1"
SOTA_UNIQUE_ID_SUFFIX: Final[str] = "DAM"

KNOWN_FUTURE_FEATURE_COLUMNS: Final[tuple[str, ...]] = (
    "hour_sin",
    "hour_cos",
    "weekday_sin",
    "weekday_cos",
    "is_weekend",
    "weather_temperature",
    "weather_wind_speed",
    "weather_cloudcover",
    "weather_precipitation",
    "weather_effective_solar",
    "weather_known_future_available",
    "market_price_cap_max",
    "market_price_cap_min",
    "market_regime_code",
    "days_since_regime_change",
    "is_price_cap_changed_recently",
)

HISTORICAL_OBSERVED_FEATURE_COLUMNS: Final[tuple[str, ...]] = tuple(
    column_name
    for column_name in NEURAL_FORECAST_FEATURE_COLUMNS
    if column_name not in KNOWN_FUTURE_FEATURE_COLUMNS
)


def build_sota_forecast_training_frame(
    feature_frame: pl.DataFrame,
    *,
    tenant_id: str,
    market_venue: str = SOTA_UNIQUE_ID_SUFFIX,
) -> pl.DataFrame:
    """Convert the leakage-safe Silver feature frame into a backend-neutral schema.

    The output intentionally follows the common long-form shape used by NeuralForecast
    and PyTorch-Forecasting adapters: ``unique_id``, ``ds``, ``y`` plus covariates.
    Forecast-horizon target values are always masked, even if a caller accidentally
    passes realized future prices in ``target_price_uah_mwh``.
    """

    _validate_feature_frame(feature_frame)
    unique_id = f"{tenant_id}:{market_venue}"
    known_future_csv = ",".join(KNOWN_FUTURE_FEATURE_COLUMNS)
    historical_observed_csv = ",".join(HISTORICAL_OBSERVED_FEATURE_COLUMNS)
    feature_columns = [
        column_name
        for column_name in NEURAL_FORECAST_FEATURE_COLUMNS
        if column_name in feature_frame.columns
    ]
    return (
        feature_frame
        .with_columns(
            [
                pl.lit(unique_id).alias("unique_id"),
                pl.lit(tenant_id).alias("tenant_id"),
                pl.lit(market_venue).alias("market_venue"),
                pl.col("timestamp").alias("ds"),
                pl.when(pl.col("split") == "forecast")
                .then(pl.lit(None, dtype=pl.Float64))
                .otherwise(pl.col("target_price_uah_mwh").cast(pl.Float64))
                .alias("y"),
                (pl.col("split") == "train").alias("is_train"),
                (pl.col("split") == "forecast").alias("is_forecast"),
                pl.lit(SOTA_SCHEMA_VERSION).alias("sota_schema_version"),
                pl.lit("neuralforecast_nbeatsx,pytorch_forecasting_tft").alias("supported_backends_csv"),
                pl.lit(known_future_csv).alias("known_future_feature_columns_csv"),
                pl.lit(historical_observed_csv).alias("historical_observed_feature_columns_csv"),
                pl.lit("tenant_id,market_venue").alias("static_feature_columns_csv"),
            ]
        )
        .select(
            [
                "unique_id",
                "ds",
                "y",
                "split",
                "tenant_id",
                "market_venue",
                "is_train",
                "is_forecast",
                "sota_schema_version",
                "supported_backends_csv",
                "known_future_feature_columns_csv",
                "historical_observed_feature_columns_csv",
                "static_feature_columns_csv",
                *feature_columns,
            ]
        )
        .sort("ds")
    )


def _validate_feature_frame(feature_frame: pl.DataFrame) -> None:
    required_columns = {"timestamp", "target_price_uah_mwh", "split", *NEURAL_FORECAST_FEATURE_COLUMNS}
    missing_columns = required_columns.difference(feature_frame.columns)
    if missing_columns:
        raise ValueError(f"feature_frame is missing required columns: {sorted(missing_columns)}")
