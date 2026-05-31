"""Source-backed operator preview forecast materialization utilities."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Final

import polars as pl

from smart_arbitrage.assets.gold.baseline_solver import DEFAULT_PRICE_COLUMN, DEFAULT_TIMESTAMP_COLUMN
from smart_arbitrage.forecasting.neural_features import build_neural_forecast_feature_frame
from smart_arbitrage.forecasting.official_adapters import (
    build_official_nbeatsx_forecast,
    build_official_tft_forecast,
)
from smart_arbitrage.forecasting.sota_training import build_sota_forecast_training_frame
from smart_arbitrage.resources.forecast_store import ForecastStore
from smart_arbitrage.resources.market_data_store import MarketDataStore


OPERATOR_PREVIEW_FORECAST_CLAIM_BOUNDARY: Final[str] = (
    "operator_preview_forecast_rows_not_market_execution"
)
OPERATOR_PREVIEW_FORECAST_MODEL_NAMES_BY_VENUE: Final[dict[str, tuple[str, str]]] = {
    "DAM": ("nbeatsx_official_v0", "tft_official_v0"),
    "IDM": ("nbeatsx_official_idm_v0", "tft_official_idm_v0"),
}
MIN_OPERATOR_PREVIEW_TRAIN_ROWS: Final[int] = 168

_ForecastBuilder = Callable[..., pl.DataFrame]


@dataclass(frozen=True)
class OperatorPreviewForecastBuilders:
    nbeatsx_builder: _ForecastBuilder = build_official_nbeatsx_forecast
    tft_builder: _ForecastBuilder = build_official_tft_forecast


@dataclass(frozen=True)
class OperatorPreviewForecastMaterializationResult:
    market_venue: str
    forecast_start: datetime
    forecast_end: datetime
    horizon_hours: int
    source_history_rows: int
    run_ids: dict[str, str]
    claim_boundary: str = OPERATOR_PREVIEW_FORECAST_CLAIM_BOUNDARY
    market_execution_enabled: bool = False


def operator_preview_forecast_model_names(market_venue: str) -> tuple[str, str]:
    resolved_market_venue = _normalize_market_venue(market_venue)
    return OPERATOR_PREVIEW_FORECAST_MODEL_NAMES_BY_VENUE[resolved_market_venue]


def resolve_next_operator_preview_forecast_start(
    *,
    market_data_store: MarketDataStore,
    market_venue: str,
) -> datetime:
    """Return midnight after the latest complete source-backed OREE delivery day."""

    source_frame = _source_backed_oree_market_frame(
        market_data_store=market_data_store,
        market_venue=market_venue,
    )
    complete_days = (
        source_frame
        .with_columns(pl.col(DEFAULT_TIMESTAMP_COLUMN).dt.date().alias("_delivery_date"))
        .group_by("_delivery_date")
        .agg(pl.len().alias("row_count"))
        .filter(pl.col("row_count") >= 24)
        .sort("_delivery_date")
    )
    if complete_days.is_empty():
        raise ValueError("source-backed observed OREE history has no complete 24-hour delivery day")
    latest_complete_day = complete_days.select("_delivery_date").to_series().item(-1)
    return datetime.combine(latest_complete_day, datetime.min.time()) + timedelta(days=1)


def materialize_operator_preview_forecast_runs(
    *,
    market_data_store: MarketDataStore,
    forecast_store: ForecastStore,
    tenant_id: str,
    market_venue: str,
    forecast_start: datetime,
    horizon_hours: int = 72,
    builders: OperatorPreviewForecastBuilders | None = None,
    nbeatsx_max_steps: int = 1,
    tft_max_epochs: int = 1,
) -> OperatorPreviewForecastMaterializationResult:
    """Train/predict official NBEATSx/TFT adapters and persist complete preview rows."""

    if horizon_hours <= 0:
        raise ValueError("horizon_hours must be positive")
    resolved_market_venue = _normalize_market_venue(market_venue)
    resolved_forecast_start = _naive_hour(forecast_start)
    source_frame = _source_backed_oree_market_frame(
        market_data_store=market_data_store,
        market_venue=resolved_market_venue,
    )
    training_frame = build_operator_preview_training_frame(
        source_frame=source_frame,
        tenant_id=tenant_id,
        market_venue=resolved_market_venue,
        forecast_start=resolved_forecast_start,
        horizon_hours=horizon_hours,
    )
    selected_builders = builders or OperatorPreviewForecastBuilders()
    nbeatsx_model_name, tft_model_name = operator_preview_forecast_model_names(resolved_market_venue)
    raw_frames = {
        nbeatsx_model_name: selected_builders.nbeatsx_builder(
            training_frame,
            horizon_hours=horizon_hours,
            max_steps=nbeatsx_max_steps,
        ),
        tft_model_name: selected_builders.tft_builder(
            training_frame,
            horizon_hours=horizon_hours,
            max_epochs=tft_max_epochs,
        ),
    }
    forecast_frames = {
        model_name: _complete_forecast_frame(
            forecast_frame=_with_model_name(forecast_frame, model_name=model_name),
            model_name=model_name,
            forecast_start=resolved_forecast_start,
            horizon_hours=horizon_hours,
        )
        for model_name, forecast_frame in raw_frames.items()
    }
    run_ids = {
        model_name: forecast_store.upsert_forecast_run(
            model_name=model_name,
            forecast_frame=forecast_frame,
            point_prediction_column=_point_prediction_column(forecast_frame),
        )
        for model_name, forecast_frame in forecast_frames.items()
    }
    return OperatorPreviewForecastMaterializationResult(
        market_venue=resolved_market_venue,
        forecast_start=resolved_forecast_start,
        forecast_end=resolved_forecast_start + timedelta(hours=horizon_hours - 1),
        horizon_hours=horizon_hours,
        source_history_rows=_source_history_frame(
            source_frame=source_frame,
            forecast_start=resolved_forecast_start,
        ).height,
        run_ids=run_ids,
    )


def build_operator_preview_training_frame(
    *,
    source_frame: pl.DataFrame,
    tenant_id: str,
    market_venue: str,
    forecast_start: datetime,
    horizon_hours: int,
) -> pl.DataFrame:
    resolved_market_venue = _normalize_market_venue(market_venue)
    resolved_forecast_start = _naive_hour(forecast_start)
    history = _source_history_frame(
        source_frame=_validate_source_frame(source_frame, market_venue=resolved_market_venue),
        forecast_start=resolved_forecast_start,
    )
    if history.height < MIN_OPERATOR_PREVIEW_TRAIN_ROWS:
        raise ValueError(
            f"source-backed observed OREE {resolved_market_venue} history requires at least "
            f"{MIN_OPERATOR_PREVIEW_TRAIN_ROWS} rows before forecast_start"
        )
    latest_history_timestamp = history.select(DEFAULT_TIMESTAMP_COLUMN).to_series().item(-1)
    if latest_history_timestamp != resolved_forecast_start - timedelta(hours=1):
        raise ValueError("forecast_start must be the hour after the latest source-backed observed OREE row")
    feature_frame = build_neural_forecast_feature_frame(
        pl.concat(
            [
                _feature_history_frame(history),
                _future_feature_placeholder_frame(
                    history=history,
                    forecast_start=resolved_forecast_start,
                    horizon_hours=horizon_hours,
                ),
            ],
            how="vertical_relaxed",
        ),
        horizon_hours=horizon_hours,
        future_weather_mode="forecast_only",
    )
    return build_sota_forecast_training_frame(
        feature_frame,
        tenant_id=tenant_id,
        market_venue=resolved_market_venue,
    )


def _source_backed_oree_market_frame(
    *,
    market_data_store: MarketDataStore,
    market_venue: str,
) -> pl.DataFrame:
    return _validate_source_frame(
        market_data_store.list_market_price_frame(
            market_venue=_normalize_market_venue(market_venue),
            source_kind="observed",
        ),
        market_venue=market_venue,
    )


def _validate_source_frame(source_frame: pl.DataFrame, *, market_venue: str) -> pl.DataFrame:
    resolved_market_venue = _normalize_market_venue(market_venue)
    required_columns = {
        DEFAULT_TIMESTAMP_COLUMN,
        DEFAULT_PRICE_COLUMN,
        "source",
        "source_kind",
        "source_url",
        "market_venue",
    }
    missing_columns = required_columns.difference(source_frame.columns)
    if source_frame.is_empty() or missing_columns:
        raise ValueError(
            f"source-backed observed OREE {resolved_market_venue} rows are required for operator preview forecasts"
        )
    filtered = (
        source_frame
        .with_columns(
            [
                pl.col(DEFAULT_TIMESTAMP_COLUMN).dt.replace_time_zone(None).alias(DEFAULT_TIMESTAMP_COLUMN),
                pl.col("source").cast(pl.Utf8).str.to_lowercase().alias("_source_lower"),
                pl.col("source_kind").cast(pl.Utf8).str.to_lowercase().alias("_source_kind_lower"),
                pl.col("source_url").cast(pl.Utf8).str.to_lowercase().alias("_source_url_lower"),
                pl.col("market_venue").cast(pl.Utf8).str.to_uppercase().alias("_market_venue_upper"),
            ]
        )
        .filter(pl.col("_market_venue_upper") == resolved_market_venue)
        .filter(pl.col("_source_kind_lower") == "observed")
        .filter(pl.col("_source_lower").str.contains("oree") | pl.col("_source_url_lower").str.contains("oree"))
        .filter(~pl.col("_source_lower").str.contains("synthetic|demo"))
        .filter(~pl.col("_source_url_lower").str.contains("synthetic|demo"))
        .drop(["_source_lower", "_source_kind_lower", "_source_url_lower", "_market_venue_upper"])
        .sort(DEFAULT_TIMESTAMP_COLUMN)
        .unique(subset=[DEFAULT_TIMESTAMP_COLUMN], keep="last")
        .sort(DEFAULT_TIMESTAMP_COLUMN)
    )
    if filtered.is_empty():
        raise ValueError(
            f"source-backed observed OREE {resolved_market_venue} rows are required for operator preview forecasts"
        )
    return filtered


def _source_history_frame(*, source_frame: pl.DataFrame, forecast_start: datetime) -> pl.DataFrame:
    return source_frame.filter(pl.col(DEFAULT_TIMESTAMP_COLUMN) < _naive_hour(forecast_start)).sort(DEFAULT_TIMESTAMP_COLUMN)


def _feature_history_frame(history: pl.DataFrame) -> pl.DataFrame:
    return history.select(
        [
            DEFAULT_TIMESTAMP_COLUMN,
            DEFAULT_PRICE_COLUMN,
            *[column for column in ("volume_mwh", "low_volume", "price_spike") if column in history.columns],
        ]
    )


def _future_feature_placeholder_frame(
    *,
    history: pl.DataFrame,
    forecast_start: datetime,
    horizon_hours: int,
) -> pl.DataFrame:
    history_by_timestamp = {
        row[DEFAULT_TIMESTAMP_COLUMN]: row
        for row in history.select(
            [
                DEFAULT_TIMESTAMP_COLUMN,
                DEFAULT_PRICE_COLUMN,
                *[column for column in ("volume_mwh", "low_volume", "price_spike") if column in history.columns],
            ]
        ).iter_rows(named=True)
    }
    latest_row = history.row(-1, named=True)
    rows: list[dict[str, Any]] = []
    for hour_index in range(horizon_hours):
        timestamp = forecast_start + timedelta(hours=hour_index)
        lag_row = history_by_timestamp.get(timestamp - timedelta(hours=24), latest_row)
        rows.append(
            {
                DEFAULT_TIMESTAMP_COLUMN: timestamp,
                DEFAULT_PRICE_COLUMN: float(lag_row[DEFAULT_PRICE_COLUMN]),
                "volume_mwh": float(lag_row.get("volume_mwh", 1000.0)),
                "low_volume": bool(lag_row.get("low_volume", False)),
                "price_spike": bool(lag_row.get("price_spike", False)),
            }
        )
    return pl.DataFrame(rows)


def _complete_forecast_frame(
    *,
    forecast_frame: pl.DataFrame,
    model_name: str,
    forecast_start: datetime,
    horizon_hours: int,
) -> pl.DataFrame:
    if forecast_frame.is_empty():
        raise ValueError(f"{model_name} did not produce forecast rows; install optional SOTA deps and rerun")
    if "forecast_timestamp" not in forecast_frame.columns:
        raise ValueError(f"{model_name} forecast frame is missing forecast_timestamp")
    point_prediction_column = _point_prediction_column(forecast_frame)
    expected_timestamps = [forecast_start + timedelta(hours=hour_index) for hour_index in range(horizon_hours)]
    complete_frame = (
        forecast_frame
        .with_columns(pl.col("forecast_timestamp").dt.replace_time_zone(None).alias("forecast_timestamp"))
        .filter(pl.col("forecast_timestamp").is_in(expected_timestamps))
        .sort("forecast_timestamp")
        .unique(subset=["forecast_timestamp"], keep="last")
        .sort("forecast_timestamp")
    )
    actual_timestamps = complete_frame.select("forecast_timestamp").to_series().to_list()
    if actual_timestamps != expected_timestamps:
        raise ValueError(f"{model_name} did not produce a complete {horizon_hours}-hour operator preview horizon")
    return complete_frame.with_columns(pl.col(point_prediction_column).cast(pl.Float64).alias(point_prediction_column))


def _with_model_name(forecast_frame: pl.DataFrame, *, model_name: str) -> pl.DataFrame:
    if forecast_frame.is_empty():
        return forecast_frame
    return forecast_frame.with_columns(pl.lit(model_name).alias("model_name"))


def _point_prediction_column(forecast_frame: pl.DataFrame) -> str:
    if "predicted_price_p50_uah_mwh" in forecast_frame.columns:
        return "predicted_price_p50_uah_mwh"
    if "predicted_price_uah_mwh" in forecast_frame.columns:
        return "predicted_price_uah_mwh"
    raise ValueError("operator preview forecast frame must include predicted_price_uah_mwh or predicted_price_p50_uah_mwh")


def _normalize_market_venue(market_venue: str) -> str:
    resolved_market_venue = market_venue.strip().upper()
    if resolved_market_venue not in OPERATOR_PREVIEW_FORECAST_MODEL_NAMES_BY_VENUE:
        raise ValueError("market_venue must be DAM or IDM")
    return resolved_market_venue


def _naive_hour(value: datetime) -> datetime:
    return value.replace(tzinfo=None, minute=0, second=0, microsecond=0)

