"""On-demand source refresh and forecast-store warming for operator preview."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from math import ceil
from typing import Literal

import polars as pl

from smart_arbitrage.assets.bronze.market_weather import build_observed_market_price_history
from smart_arbitrage.assets.gold.baseline_solver import DEFAULT_PRICE_COLUMN, DEFAULT_TIMESTAMP_COLUMN
from smart_arbitrage.research.operator_preview_forecast import (
    OPERATOR_PREVIEW_FORECAST_CLAIM_BOUNDARY,
    OperatorPreviewForecastBuilders,
    materialize_operator_preview_forecast_runs,
    operator_preview_forecast_model_names,
    resolve_next_operator_preview_forecast_start,
)
from smart_arbitrage.resources.forecast_store import ForecastStore
from smart_arbitrage.resources.market_data_store import (
    MarketDataStore,
    market_price_observations_from_frame,
)


OPERATOR_PREVIEW_READ_MODEL_BOUNDARY = "operator_preview_no_market_submission"
OPERATOR_PREVIEW_FORECAST_MIN_HORIZON_HOURS = 72
OPERATOR_PREVIEW_FORECAST_MAX_HORIZON_HOURS = 168
MIN_OPERATOR_PREVIEW_SOURCE_HISTORY_DAYS = 7

OperatorPreviewEnsureStatus = Literal[
    "ready",
    "materialized",
    "blocked_source_unavailable",
    "blocked_outside_policy_horizon",
    "failed",
]
SourceHistoryFetcher = Callable[..., pl.DataFrame]
ForecastBuilder = Callable[..., pl.DataFrame]


@dataclass(frozen=True, slots=True)
class OperatorPreviewEnsureResult:
    tenant_id: str
    market_venue: str
    target_delivery_date: date
    status: OperatorPreviewEnsureStatus
    stage: str
    message: str
    latest_observed_timestamp: datetime | None = None
    forecast_start: datetime | None = None
    forecast_horizon_end: datetime | None = None
    horizon_hours: int | None = None
    source_refresh_rows: int = 0
    source_refresh_dates: tuple[str, ...] = ()
    forecast_rows: int = 0
    forecast_run_ids: Mapping[str, str] = field(default_factory=dict)
    claim_boundary: str = OPERATOR_PREVIEW_FORECAST_CLAIM_BOUNDARY
    read_model_boundary: str = OPERATOR_PREVIEW_READ_MODEL_BOUNDARY
    market_execution_enabled: bool = False


def ensure_operator_preview_window(
    *,
    market_data_store: MarketDataStore,
    forecast_store: ForecastStore,
    tenant_id: str,
    market_venue: str,
    target_delivery_date: date,
    source_history_fetcher: SourceHistoryFetcher = build_observed_market_price_history,
    nbeatsx_builder: ForecastBuilder | None = None,
    tft_builder: ForecastBuilder | None = None,
    min_horizon_hours: int = OPERATOR_PREVIEW_FORECAST_MIN_HORIZON_HOURS,
    max_horizon_hours: int = OPERATOR_PREVIEW_FORECAST_MAX_HORIZON_HOURS,
    cache_horizon_hours: int | None = None,
) -> OperatorPreviewEnsureResult:
    """Ensure the selected delivery date has source-backed forecast-store rows.

    Missing observed rows are fetched from OREE only when the current complete
    source-backed history cannot cover the target within the bounded preview
    horizon. Forecast outputs remain read-model rows; no market payload is
    emitted.
    """

    resolved_market_venue = _normalize_market_venue(market_venue)
    resolved_nbeatsx_builder = nbeatsx_builder or build_source_backed_lag_operator_preview_forecast
    resolved_tft_builder = tft_builder or build_source_backed_lag_operator_preview_forecast
    try:
        forecast_start = resolve_next_operator_preview_forecast_start(
            market_data_store=market_data_store,
            market_venue=resolved_market_venue,
        )
    except ValueError:
        forecast_start = None

    source_refresh_rows = 0
    source_refresh_dates: tuple[str, ...] = ()
    source_frame = _observed_oree_market_frame(
        market_data_store=market_data_store,
        market_venue=resolved_market_venue,
    )
    latest_observed_timestamp = _latest_observed_timestamp(source_frame)
    required_hours = _required_horizon_hours(
        forecast_start=forecast_start,
        target_delivery_date=target_delivery_date,
    )
    if forecast_start is None or required_hours is None or required_hours > max_horizon_hours:
        refresh_result = _refresh_source_history(
            market_data_store=market_data_store,
            market_venue=resolved_market_venue,
            target_delivery_date=target_delivery_date,
            current_source_frame=source_frame,
            source_history_fetcher=source_history_fetcher,
            max_horizon_hours=max_horizon_hours,
        )
        if refresh_result.status != "ready":
            return OperatorPreviewEnsureResult(
                tenant_id=tenant_id,
                market_venue=resolved_market_venue,
                target_delivery_date=target_delivery_date,
                status=refresh_result.status,
                stage=refresh_result.stage,
                message=refresh_result.message,
                latest_observed_timestamp=latest_observed_timestamp,
                source_refresh_dates=refresh_result.source_refresh_dates,
            )
        source_refresh_rows = refresh_result.source_refresh_rows
        source_refresh_dates = refresh_result.source_refresh_dates
        source_frame = _observed_oree_market_frame(
            market_data_store=market_data_store,
            market_venue=resolved_market_venue,
        )
        latest_observed_timestamp = _latest_observed_timestamp(source_frame)
        try:
            forecast_start = resolve_next_operator_preview_forecast_start(
                market_data_store=market_data_store,
                market_venue=resolved_market_venue,
            )
        except ValueError as error:
            return OperatorPreviewEnsureResult(
                tenant_id=tenant_id,
                market_venue=resolved_market_venue,
                target_delivery_date=target_delivery_date,
                status="blocked_source_unavailable",
                stage="source_refresh",
                message=f"source-backed rows unavailable after refresh: {error}",
                latest_observed_timestamp=latest_observed_timestamp,
                source_refresh_rows=source_refresh_rows,
                source_refresh_dates=source_refresh_dates,
            )
        required_hours = _required_horizon_hours(
            forecast_start=forecast_start,
            target_delivery_date=target_delivery_date,
        )

    if required_hours is None or required_hours > max_horizon_hours:
        return OperatorPreviewEnsureResult(
            tenant_id=tenant_id,
            market_venue=resolved_market_venue,
            target_delivery_date=target_delivery_date,
            status="blocked_outside_policy_horizon",
            stage="forecast_horizon",
            message=(
                f"target_delivery_date={target_delivery_date.isoformat()} is outside the "
                f"{max_horizon_hours}-hour source-backed operator preview horizon"
            ),
            latest_observed_timestamp=latest_observed_timestamp,
            forecast_start=forecast_start,
            source_refresh_rows=source_refresh_rows,
            source_refresh_dates=source_refresh_dates,
        )

    if _forecast_store_covers_target(
        forecast_store=forecast_store,
        market_venue=resolved_market_venue,
        target_delivery_date=target_delivery_date,
    ):
        return OperatorPreviewEnsureResult(
            tenant_id=tenant_id,
            market_venue=resolved_market_venue,
            target_delivery_date=target_delivery_date,
            status="ready",
            stage="forecast_store",
            message="source-backed operator preview forecast rows already cover the target delivery date",
            latest_observed_timestamp=latest_observed_timestamp,
            forecast_start=forecast_start,
            source_refresh_rows=source_refresh_rows,
            source_refresh_dates=source_refresh_dates,
        )

    horizon_hours = _materialization_horizon_hours(
        required_hours=required_hours,
        min_horizon_hours=min_horizon_hours,
        max_horizon_hours=max_horizon_hours,
        cache_horizon_hours=cache_horizon_hours,
    )
    try:
        materialization = materialize_operator_preview_forecast_runs(
            market_data_store=market_data_store,
            forecast_store=forecast_store,
            tenant_id=tenant_id,
            market_venue=resolved_market_venue,
            forecast_start=forecast_start,
            horizon_hours=horizon_hours,
            builders=OperatorPreviewForecastBuilders(
                nbeatsx_builder=resolved_nbeatsx_builder,
                tft_builder=resolved_tft_builder,
            ),
            nbeatsx_max_steps=1,
            tft_max_epochs=1,
        )
    except (ImportError, RuntimeError, ValueError) as error:
        return OperatorPreviewEnsureResult(
            tenant_id=tenant_id,
            market_venue=resolved_market_venue,
            target_delivery_date=target_delivery_date,
            status="failed",
            stage="forecast_materialization",
            message=str(error),
            latest_observed_timestamp=latest_observed_timestamp,
            forecast_start=forecast_start,
            horizon_hours=horizon_hours,
            source_refresh_rows=source_refresh_rows,
            source_refresh_dates=source_refresh_dates,
        )

    forecast_rows = materialization.horizon_hours * len(materialization.run_ids)
    return OperatorPreviewEnsureResult(
        tenant_id=tenant_id,
        market_venue=resolved_market_venue,
        target_delivery_date=target_delivery_date,
        status="materialized",
        stage="forecast_materialization",
        message=(
            "source-backed forecast-store rows materialized for operator preview; "
            "market execution remains disabled"
        ),
        latest_observed_timestamp=latest_observed_timestamp,
        forecast_start=materialization.forecast_start,
        forecast_horizon_end=materialization.forecast_end,
        horizon_hours=materialization.horizon_hours,
        source_refresh_rows=source_refresh_rows,
        source_refresh_dates=source_refresh_dates,
        forecast_rows=forecast_rows,
        forecast_run_ids=dict(materialization.run_ids),
    )


def inspect_operator_preview_window(
    *,
    market_data_store: MarketDataStore,
    forecast_store: ForecastStore,
    tenant_id: str,
    market_venue: str,
    target_delivery_date: date,
) -> OperatorPreviewEnsureResult:
    resolved_market_venue = _normalize_market_venue(market_venue)
    source_frame = _observed_oree_market_frame(
        market_data_store=market_data_store,
        market_venue=resolved_market_venue,
    )
    latest_observed_timestamp = _latest_observed_timestamp(source_frame)
    try:
        forecast_start = resolve_next_operator_preview_forecast_start(
            market_data_store=market_data_store,
            market_venue=resolved_market_venue,
        )
    except ValueError as error:
        return OperatorPreviewEnsureResult(
            tenant_id=tenant_id,
            market_venue=resolved_market_venue,
            target_delivery_date=target_delivery_date,
            status="blocked_source_unavailable",
            stage="source_history",
            message=f"source-backed rows unavailable: {error}",
            latest_observed_timestamp=latest_observed_timestamp,
        )
    required_hours = _required_horizon_hours(
        forecast_start=forecast_start,
        target_delivery_date=target_delivery_date,
    )
    if required_hours is None or required_hours > OPERATOR_PREVIEW_FORECAST_MAX_HORIZON_HOURS:
        status: OperatorPreviewEnsureStatus = "blocked_outside_policy_horizon"
        message = (
            f"target_delivery_date={target_delivery_date.isoformat()} is outside the "
            f"{OPERATOR_PREVIEW_FORECAST_MAX_HORIZON_HOURS}-hour source-backed operator preview horizon"
        )
    elif _forecast_store_covers_target(
        forecast_store=forecast_store,
        market_venue=resolved_market_venue,
        target_delivery_date=target_delivery_date,
    ):
        status = "ready"
        message = "source-backed operator preview forecast rows already cover the target delivery date"
    else:
        status = "failed"
        message = "forecast-store rows are not materialized yet"
    return OperatorPreviewEnsureResult(
        tenant_id=tenant_id,
        market_venue=resolved_market_venue,
        target_delivery_date=target_delivery_date,
        status=status,
        stage="readiness",
        message=message,
        latest_observed_timestamp=latest_observed_timestamp,
        forecast_start=forecast_start,
        horizon_hours=required_hours,
    )


def build_source_backed_lag_operator_preview_forecast(
    training_frame: pl.DataFrame,
    *,
    horizon_hours: int = OPERATOR_PREVIEW_FORECAST_MIN_HORIZON_HOURS,
    **_: object,
) -> pl.DataFrame:
    if training_frame.is_empty() or "ds" not in training_frame.columns or "y" not in training_frame.columns:
        return pl.DataFrame()
    latest_price_by_hour: dict[int, float] = {}
    last_observed_price: float | None = None
    for row in (
        training_frame
        .filter(pl.col("is_train") & pl.col("y").is_not_null())
        .sort("ds")
        .iter_rows(named=True)
    ):
        timestamp = _datetime_value(row["ds"])
        observed_price = float(row["y"])
        latest_price_by_hour[timestamp.hour] = observed_price
        last_observed_price = observed_price
    future_timestamps = [
        _datetime_value(row["ds"])
        for row in (
            training_frame
            .filter(pl.col("is_forecast"))
            .sort("ds")
            .head(horizon_hours)
            .select("ds")
            .iter_rows(named=True)
        )
    ]
    if not latest_price_by_hour or not future_timestamps:
        return pl.DataFrame()
    assert last_observed_price is not None
    return pl.DataFrame(
        {
            "forecast_timestamp": future_timestamps,
            "predicted_price_uah_mwh": [
                latest_price_by_hour.get(timestamp.hour, last_observed_price)
                for timestamp in future_timestamps
            ],
            "adapter_scope": [
                "source_backed_lag_operator_preview_not_market_execution"
                for _ in future_timestamps
            ],
        }
    )


@dataclass(frozen=True, slots=True)
class _SourceRefreshResult:
    status: Literal["ready", "blocked_source_unavailable", "blocked_outside_policy_horizon"]
    stage: str
    message: str
    source_refresh_rows: int = 0
    source_refresh_dates: tuple[str, ...] = ()


def _refresh_source_history(
    *,
    market_data_store: MarketDataStore,
    market_venue: str,
    target_delivery_date: date,
    current_source_frame: pl.DataFrame,
    source_history_fetcher: SourceHistoryFetcher,
    max_horizon_hours: int,
) -> _SourceRefreshResult:
    missing_dates = _missing_source_dates_for_target(
        current_source_frame,
        target_delivery_date=target_delivery_date,
    )
    if not missing_dates:
        return _SourceRefreshResult(
            status="ready",
            stage="source_refresh",
            message="source-backed observed rows already cover the refresh window",
        )

    max_refresh_days = _max_source_refresh_days(max_horizon_hours)
    if len(missing_dates) > max_refresh_days:
        return _SourceRefreshResult(
            status="blocked_outside_policy_horizon",
            stage="forecast_horizon",
            message=(
                f"target_delivery_date={target_delivery_date.isoformat()} is outside the "
                f"{max_horizon_hours}-hour source-backed operator preview horizon; "
                f"source refresh would require {len(missing_dates)} observed delivery days, "
                f"above the bounded {max_refresh_days}-day refresh window"
            ),
        )

    start_date = min(missing_dates)
    end_date = max(missing_dates)
    try:
        refreshed_frame = source_history_fetcher(
            start_date=start_date,
            end_date=end_date,
            market_venue=market_venue,
        )
    except ValueError as error:
        return _SourceRefreshResult(
            status="blocked_source_unavailable",
            stage="source_refresh",
            message=f"source-backed rows unavailable from OREE for {start_date.isoformat()}..{end_date.isoformat()}: {error}",
            source_refresh_dates=tuple(day.isoformat() for day in missing_dates),
        )
    if refreshed_frame.is_empty():
        return _SourceRefreshResult(
            status="blocked_source_unavailable",
            stage="source_refresh",
            message=f"source-backed rows unavailable from OREE for {start_date.isoformat()}..{end_date.isoformat()}",
            source_refresh_dates=tuple(day.isoformat() for day in missing_dates),
        )
    market_data_store.upsert_market_prices(market_price_observations_from_frame(refreshed_frame))
    return _SourceRefreshResult(
        status="ready",
        stage="source_refresh",
        message="source-backed observed OREE rows refreshed",
        source_refresh_rows=refreshed_frame.height,
        source_refresh_dates=tuple(day.isoformat() for day in missing_dates),
    )


def _missing_source_dates_for_target(source_frame: pl.DataFrame, *, target_delivery_date: date) -> list[date]:
    last_required_date = target_delivery_date - timedelta(days=1)
    first_required_date = target_delivery_date - timedelta(days=MIN_OPERATOR_PREVIEW_SOURCE_HISTORY_DAYS)
    complete_dates = _complete_delivery_dates(source_frame)
    if complete_dates:
        first_required_date = min(first_required_date, max(complete_dates) + timedelta(days=1))
    if last_required_date < first_required_date:
        return []
    return [
        first_required_date + timedelta(days=day_index)
        for day_index in range((last_required_date - first_required_date).days + 1)
        if first_required_date + timedelta(days=day_index) not in complete_dates
    ]


def _complete_delivery_dates(source_frame: pl.DataFrame) -> set[date]:
    if source_frame.is_empty() or DEFAULT_TIMESTAMP_COLUMN not in source_frame.columns:
        return set()
    return set(
        source_frame
        .with_columns(pl.col(DEFAULT_TIMESTAMP_COLUMN).dt.date().alias("_delivery_date"))
        .group_by("_delivery_date")
        .agg(pl.len().alias("row_count"))
        .filter(pl.col("row_count") >= 24)
        .select("_delivery_date")
        .to_series()
        .to_list()
    )


def _max_source_refresh_days(max_horizon_hours: int) -> int:
    return max(MIN_OPERATOR_PREVIEW_SOURCE_HISTORY_DAYS, int(ceil(max_horizon_hours / 24.0))) + 1


def _forecast_store_covers_target(
    *,
    forecast_store: ForecastStore,
    market_venue: str,
    target_delivery_date: date,
) -> bool:
    model_names = operator_preview_forecast_model_names(market_venue)
    window_start = datetime.combine(target_delivery_date, datetime.min.time())
    window_end = datetime.combine(target_delivery_date + timedelta(days=1), datetime.min.time())
    forecast_frame = forecast_store.forecast_observation_frame_for_window(
        model_names=model_names,
        window_start=window_start,
        window_end=window_end,
        limit_per_model=24,
    )
    if forecast_frame.is_empty() or "model_name" not in forecast_frame.columns:
        return False
    counts = {
        str(row["model_name"]): int(row["row_count"])
        for row in (
            forecast_frame
            .group_by("model_name")
            .agg(pl.len().alias("row_count"))
            .iter_rows(named=True)
        )
    }
    return any(counts.get(model_name, 0) >= 24 for model_name in model_names)


def _observed_oree_market_frame(
    *,
    market_data_store: MarketDataStore,
    market_venue: str,
) -> pl.DataFrame:
    source_frame = market_data_store.list_market_price_frame(
        market_venue=market_venue,
        source_kind="observed",
    )
    required_columns = {
        DEFAULT_TIMESTAMP_COLUMN,
        DEFAULT_PRICE_COLUMN,
        "source",
        "source_kind",
        "source_url",
        "market_venue",
    }
    if source_frame.is_empty() or not required_columns.issubset(source_frame.columns):
        return pl.DataFrame()
    return (
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
        .filter(pl.col("_market_venue_upper") == market_venue)
        .filter(pl.col("_source_kind_lower") == "observed")
        .filter(pl.col("_source_lower").str.contains("oree") | pl.col("_source_url_lower").str.contains("oree"))
        .filter(~pl.col("_source_lower").str.contains("synthetic|demo"))
        .filter(~pl.col("_source_url_lower").str.contains("synthetic|demo"))
        .drop(["_source_lower", "_source_kind_lower", "_source_url_lower", "_market_venue_upper"])
        .sort(DEFAULT_TIMESTAMP_COLUMN)
        .unique(subset=[DEFAULT_TIMESTAMP_COLUMN], keep="last")
        .sort(DEFAULT_TIMESTAMP_COLUMN)
    )


def _latest_observed_timestamp(source_frame: pl.DataFrame) -> datetime | None:
    if source_frame.is_empty() or DEFAULT_TIMESTAMP_COLUMN not in source_frame.columns:
        return None
    return _datetime_value(source_frame.select(DEFAULT_TIMESTAMP_COLUMN).max().item())


def _required_horizon_hours(
    *,
    forecast_start: datetime | None,
    target_delivery_date: date,
) -> int | None:
    if forecast_start is None:
        return None
    target_delivery_end = datetime.combine(
        target_delivery_date + timedelta(days=1),
        datetime.min.time(),
    )
    if target_delivery_end <= forecast_start:
        return None
    return int(ceil((target_delivery_end - forecast_start).total_seconds() / 3600.0))


def _materialization_horizon_hours(
    *,
    required_hours: int,
    min_horizon_hours: int,
    max_horizon_hours: int,
    cache_horizon_hours: int | None,
) -> int:
    requested_horizon = max(required_hours, min_horizon_hours, cache_horizon_hours or 0)
    return min(max_horizon_hours, requested_horizon)


def _normalize_market_venue(market_venue: str) -> str:
    resolved_market_venue = market_venue.strip().upper()
    if resolved_market_venue not in {"DAM", "IDM"}:
        raise ValueError("market_venue must be DAM or IDM")
    return resolved_market_venue


def _datetime_value(value: object) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    raise TypeError("expected datetime value")
