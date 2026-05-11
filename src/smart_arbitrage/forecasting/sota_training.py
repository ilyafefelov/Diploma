"""SOTA-ready training-frame contracts for official neural forecasting backends."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Final

import polars as pl

from smart_arbitrage.assets.gold.baseline_solver import DEFAULT_PRICE_COLUMN, DEFAULT_TIMESTAMP_COLUMN
from smart_arbitrage.forecasting.neural_features import (
    DEFAULT_NEURAL_FORECAST_HORIZON_HOURS,
    FutureWeatherMode,
    build_neural_forecast_feature_frame,
)
from smart_arbitrage.forecasting.neural_features import NEURAL_FORECAST_FEATURE_COLUMNS
from smart_arbitrage.forecasting.market_coupling_availability import (
    EXTERNAL_TRAINING_BLOCKERS,
    REQUIRED_MARKET_COUPLING_AVAILABILITY_COLUMNS,
)

SOTA_SCHEMA_VERSION: Final[str] = "sota_forecast_training_v1"
OFFICIAL_GLOBAL_PANEL_SOTA_SCHEMA_VERSION: Final[str] = "official_global_panel_sota_v1"
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


def build_official_global_panel_training_frame(
    silver_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    horizon_hours: int = DEFAULT_NEURAL_FORECAST_HORIZON_HOURS,
    market_venue: str = SOTA_UNIQUE_ID_SUFFIX,
    future_weather_mode: FutureWeatherMode = "forecast_only",
    temporal_scaler_type: str = "robust",
    anchor_timestamp: datetime | None = None,
    market_coupling_availability_frame: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Build one Nixtla/PyTorch-ready panel across tenants.

    This is the official-model training contract for serious evidence. It keeps
    the compact single-tenant SOTA frame available for smoke tests while giving
    Nixtla NBEATSx a global ``unique_id`` panel with explicit point-in-time
    feature/scaler metadata.
    """

    _validate_global_panel_inputs(
        silver_frame,
        tenant_ids=tenant_ids,
        horizon_hours=horizon_hours,
        temporal_scaler_type=temporal_scaler_type,
    )
    market_coupling_metadata = _market_coupling_feature_route_metadata(
        market_coupling_availability_frame
    )
    tenant_frames: list[pl.DataFrame] = []
    for tenant_id in tenant_ids:
        tenant_history = _tenant_history_from_silver(
            silver_frame,
            tenant_id=tenant_id,
            anchor_timestamp=anchor_timestamp,
            horizon_hours=horizon_hours,
        )
        feature_frame = build_neural_forecast_feature_frame(
            tenant_history,
            horizon_hours=horizon_hours,
            future_weather_mode=future_weather_mode,
        )
        tenant_training_frame = build_sota_forecast_training_frame(
            feature_frame,
            tenant_id=tenant_id,
            market_venue=market_venue,
        ).with_columns(
            [
                pl.lit(OFFICIAL_GLOBAL_PANEL_SOTA_SCHEMA_VERSION).alias("sota_schema_version"),
                pl.lit("official_global_panel").alias("training_panel_kind"),
                pl.lit("train_rows_only_per_unique_id").alias("target_scaler_fit_scope"),
                pl.lit("train_rows_only_per_unique_id").alias("feature_scaler_fit_scope"),
                pl.lit(temporal_scaler_type).alias("temporal_scaler_type"),
                pl.lit("not_full_dfl").alias("claim_boundary"),
                pl.lit(True).alias("not_full_dfl"),
                pl.lit(True).alias("not_market_execution"),
                pl.lit(market_coupling_metadata["external_feature_training_status"]).alias(
                    "external_feature_training_status"
                ),
                pl.lit(market_coupling_metadata["allowed_external_feature_columns_csv"]).alias(
                    "allowed_external_feature_columns_csv"
                ),
                pl.lit(market_coupling_metadata["blocked_external_feature_columns_csv"]).alias(
                    "blocked_external_feature_columns_csv"
                ),
                pl.lit(market_coupling_metadata["external_training_blockers_csv"]).alias(
                    "external_training_blockers_csv"
                ),
                pl.lit(market_coupling_metadata["external_feature_governance_scope"]).alias(
                    "external_feature_governance_scope"
                ),
            ]
        )
        tenant_frames.append(tenant_training_frame)
    return pl.concat(tenant_frames, how="diagonal_relaxed").sort(["unique_id", "ds"])


def _validate_feature_frame(feature_frame: pl.DataFrame) -> None:
    required_columns = {"timestamp", "target_price_uah_mwh", "split", *NEURAL_FORECAST_FEATURE_COLUMNS}
    missing_columns = required_columns.difference(feature_frame.columns)
    if missing_columns:
        raise ValueError(f"feature_frame is missing required columns: {sorted(missing_columns)}")


def _validate_global_panel_inputs(
    silver_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    horizon_hours: int,
    temporal_scaler_type: str,
) -> None:
    if not tenant_ids:
        raise ValueError("tenant_ids must contain at least one tenant.")
    if horizon_hours <= 0:
        raise ValueError("horizon_hours must be positive.")
    if not temporal_scaler_type.strip():
        raise ValueError("temporal_scaler_type must be non-empty.")
    required_columns = {"tenant_id", DEFAULT_TIMESTAMP_COLUMN, DEFAULT_PRICE_COLUMN}
    missing_columns = required_columns.difference(silver_frame.columns)
    if missing_columns:
        raise ValueError(f"silver_frame is missing required columns: {sorted(missing_columns)}")


def _tenant_history_from_silver(
    silver_frame: pl.DataFrame,
    *,
    tenant_id: str,
    anchor_timestamp: datetime | None,
    horizon_hours: int,
) -> pl.DataFrame:
    tenant_frame = (
        silver_frame
        .filter(pl.col("tenant_id") == tenant_id)
        .drop("tenant_id")
        .drop_nulls(subset=[DEFAULT_TIMESTAMP_COLUMN, DEFAULT_PRICE_COLUMN])
        .unique(subset=[DEFAULT_TIMESTAMP_COLUMN], keep="last")
        .sort(DEFAULT_TIMESTAMP_COLUMN)
    )
    if anchor_timestamp is not None:
        anchor = anchor_timestamp.replace(tzinfo=None)
        latest_allowed_timestamp = anchor + timedelta(hours=horizon_hours)
        tenant_frame = tenant_frame.filter(
            pl.col(DEFAULT_TIMESTAMP_COLUMN) <= pl.lit(latest_allowed_timestamp)
        )
    if tenant_frame.is_empty():
        raise ValueError(f"Missing Silver benchmark rows for tenant_id={tenant_id}.")
    if "source_kind" in tenant_frame.columns and tenant_frame.filter(pl.col("source_kind") != "observed").height:
        raise ValueError("official global panel training requires observed source rows.")
    return tenant_frame


def _market_coupling_feature_route_metadata(
    availability_frame: pl.DataFrame | None,
) -> dict[str, str]:
    if availability_frame is None:
        return {
            "external_feature_training_status": "not_configured",
            "allowed_external_feature_columns_csv": "",
            "blocked_external_feature_columns_csv": "",
            "external_training_blockers_csv": "",
            "external_feature_governance_scope": "market_coupling_not_attached",
        }

    missing_columns = sorted(
        REQUIRED_MARKET_COUPLING_AVAILABILITY_COLUMNS.difference(availability_frame.columns)
    )
    if missing_columns:
        raise ValueError(f"market_coupling_availability_frame missing columns: {missing_columns}")

    rows = list(availability_frame.iter_rows(named=True))
    unready_training_rows = [
        row
        for row in rows
        if bool(row["training_use_allowed"])
        and (
            str(row["readiness_status"]) != "training_ready"
            or str(row["training_blockers_csv"]) == EXTERNAL_TRAINING_BLOCKERS
            or not _all_market_coupling_statuses_ready(row)
        )
    ]
    if unready_training_rows:
        names = sorted(str(row["feature_name"]) for row in unready_training_rows)
        raise ValueError(
            "external market-coupling features cannot be training_use_allowed "
            f"before governance mapping is complete: {names}"
        )

    allowed = sorted(
        str(row["feature_name"])
        for row in rows
        if bool(row["training_use_allowed"]) and str(row["readiness_status"]) == "training_ready"
    )
    blocked = sorted(str(row["feature_name"]) for row in rows if str(row["feature_name"]).strip())
    status = "training_ready" if allowed else "blocked_by_governance"
    return {
        "external_feature_training_status": status,
        "allowed_external_feature_columns_csv": ",".join(allowed),
        "blocked_external_feature_columns_csv": ",".join(blocked),
        "external_training_blockers_csv": EXTERNAL_TRAINING_BLOCKERS,
        "external_feature_governance_scope": "market_coupling_temporal_availability_frame",
    }


def _all_market_coupling_statuses_ready(row: dict[str, object]) -> bool:
    return all(
        str(row[column_name]) == "ready"
        for column_name in (
            "licensing_status",
            "timezone_status",
            "currency_status",
            "market_rules_status",
            "temporal_availability_status",
            "domain_shift_status",
        )
    )
