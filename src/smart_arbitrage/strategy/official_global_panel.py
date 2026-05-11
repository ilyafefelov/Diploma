"""Strict LP/oracle scoring for official global-panel forecast candidates."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, Final

import polars as pl

from smart_arbitrage.assets.gold.baseline_solver import (
    DEFAULT_PRICE_COLUMN,
    DEFAULT_TIMESTAMP_COLUMN,
    HourlyDamBaselineSolver,
)
from smart_arbitrage.strategy.forecast_strategy_evaluation import (
    ForecastCandidate,
    evaluate_forecast_candidates_against_oracle,
    tenant_battery_defaults_from_registry,
)

OFFICIAL_GLOBAL_PANEL_NBEATSX_STRATEGY_KIND: Final[str] = (
    "official_global_panel_nbeatsx_strict_lp_benchmark"
)
OFFICIAL_GLOBAL_PANEL_NBEATSX_CLAIM_SCOPE: Final[str] = (
    "official_global_panel_nbeatsx_strict_lp_not_full_dfl"
)
OFFICIAL_GLOBAL_PANEL_NBEATSX_MODEL_NAME: Final[str] = "nbeatsx_official_global_panel_v1"


def build_official_global_panel_nbeatsx_strict_lp_benchmark_frame(
    real_data_benchmark_silver_feature_frame: pl.DataFrame,
    nbeatsx_official_global_panel_price_forecast: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Strict-score global-panel NBEATSx forecasts beside frozen strict control."""

    if not tenant_ids:
        raise ValueError("tenant_ids must contain at least one tenant.")
    if nbeatsx_official_global_panel_price_forecast.is_empty():
        raise ValueError("Missing global-panel NBEATSx forecast rows.")
    resolved_generated_at = generated_at or datetime.now(UTC)
    rows: list[pl.DataFrame] = []
    for tenant_id in tenant_ids:
        tenant_forecast = _tenant_forecast(
            nbeatsx_official_global_panel_price_forecast,
            tenant_id=tenant_id,
        )
        anchor_timestamp = _anchor_from_forecast(tenant_forecast)
        price_history = _tenant_price_history(
            real_data_benchmark_silver_feature_frame,
            tenant_id=tenant_id,
        )
        defaults = tenant_battery_defaults_from_registry(tenant_id)
        strict_forecast = _strict_forecast_frame(price_history, anchor_timestamp=anchor_timestamp)
        evaluation = evaluate_forecast_candidates_against_oracle(
            price_history=price_history,
            tenant_id=tenant_id,
            battery_metrics=defaults.metrics,
            starting_soc_fraction=defaults.initial_soc_fraction,
            starting_soc_source="tenant_default",
            anchor_timestamp=anchor_timestamp,
            candidates=[
                ForecastCandidate(
                    model_name="strict_similar_day",
                    forecast_frame=strict_forecast,
                    point_prediction_column="predicted_price_uah_mwh",
                ),
                ForecastCandidate(
                    model_name=OFFICIAL_GLOBAL_PANEL_NBEATSX_MODEL_NAME,
                    forecast_frame=tenant_forecast,
                    point_prediction_column="predicted_price_uah_mwh",
                ),
            ],
            evaluation_id=f"{tenant_id}:official-global-panel-nbeatsx:{anchor_timestamp:%Y%m%dT%H%M}",
            generated_at=resolved_generated_at,
        )
        rows.append(
            _with_global_panel_metadata(
                evaluation,
                tenant_id=tenant_id,
                anchor_timestamp=anchor_timestamp,
                price_history=price_history,
            )
        )
    return pl.concat(rows, how="diagonal_relaxed").sort(
        ["tenant_id", "anchor_timestamp", "rank_by_regret", "forecast_model_name"]
    )


def _tenant_forecast(forecast_frame: pl.DataFrame, *, tenant_id: str) -> pl.DataFrame:
    required_columns = {"unique_id", "forecast_timestamp", "predicted_price_uah_mwh"}
    missing_columns = required_columns.difference(forecast_frame.columns)
    if missing_columns:
        raise ValueError(f"global-panel NBEATSx forecast is missing columns: {sorted(missing_columns)}")
    unique_id = f"{tenant_id}:DAM"
    tenant_forecast = (
        forecast_frame
        .filter(pl.col("unique_id") == unique_id)
        .select(["forecast_timestamp", "predicted_price_uah_mwh"])
        .sort("forecast_timestamp")
    )
    if tenant_forecast.is_empty():
        raise ValueError(f"Missing global-panel NBEATSx forecast rows for tenant_id={tenant_id}.")
    return tenant_forecast


def _tenant_price_history(silver_frame: pl.DataFrame, *, tenant_id: str) -> pl.DataFrame:
    required_columns = {"tenant_id", DEFAULT_TIMESTAMP_COLUMN, DEFAULT_PRICE_COLUMN}
    missing_columns = required_columns.difference(silver_frame.columns)
    if missing_columns:
        raise ValueError(f"real_data_benchmark_silver_feature_frame is missing columns: {sorted(missing_columns)}")
    tenant_frame = (
        silver_frame
        .filter(pl.col("tenant_id") == tenant_id)
        .drop("tenant_id")
        .drop_nulls(subset=[DEFAULT_TIMESTAMP_COLUMN, DEFAULT_PRICE_COLUMN])
        .unique(subset=[DEFAULT_TIMESTAMP_COLUMN], keep="last")
        .sort(DEFAULT_TIMESTAMP_COLUMN)
    )
    if tenant_frame.is_empty():
        raise ValueError(f"Missing Silver benchmark rows for tenant_id={tenant_id}.")
    if "source_kind" in tenant_frame.columns and tenant_frame.filter(pl.col("source_kind") != "observed").height:
        raise ValueError("official global-panel strict benchmark requires observed source rows.")
    return tenant_frame


def _anchor_from_forecast(forecast_frame: pl.DataFrame) -> datetime:
    first_timestamp = forecast_frame.select("forecast_timestamp").to_series().item(0)
    if not isinstance(first_timestamp, datetime):
        raise TypeError("forecast_timestamp column must contain datetime values.")
    return first_timestamp - timedelta(hours=1)


def _strict_forecast_frame(price_history: pl.DataFrame, *, anchor_timestamp: datetime) -> pl.DataFrame:
    historical_prices = price_history.filter(pl.col(DEFAULT_TIMESTAMP_COLUMN) <= anchor_timestamp)
    forecast = HourlyDamBaselineSolver().build_forecast(
        historical_prices,
        anchor_timestamp=anchor_timestamp,
    )
    return pl.DataFrame(
        {
            "forecast_timestamp": [point.forecast_timestamp for point in forecast],
            "source_timestamp": [point.source_timestamp for point in forecast],
            "predicted_price_uah_mwh": [point.predicted_price_uah_mwh for point in forecast],
        }
    )


def _with_global_panel_metadata(
    evaluation: pl.DataFrame,
    *,
    tenant_id: str,
    anchor_timestamp: datetime,
    price_history: pl.DataFrame,
) -> pl.DataFrame:
    payloads: list[dict[str, Any]] = []
    data_quality_tier = _data_quality_tier(price_history)
    observed_coverage_ratio = _observed_coverage_ratio(price_history)
    for row in evaluation.iter_rows(named=True):
        payload = dict(row["evaluation_payload"])
        payload.update(
            {
                "claim_scope": OFFICIAL_GLOBAL_PANEL_NBEATSX_CLAIM_SCOPE,
                "benchmark_kind": OFFICIAL_GLOBAL_PANEL_NBEATSX_STRATEGY_KIND,
                "data_quality_tier": data_quality_tier,
                "observed_coverage_ratio": observed_coverage_ratio,
                "tenant_id": tenant_id,
                "anchor_timestamp": anchor_timestamp.isoformat(),
                "not_full_dfl": True,
                "not_market_execution": True,
            }
        )
        payloads.append(payload)
    return evaluation.with_columns(
        [
            pl.lit(OFFICIAL_GLOBAL_PANEL_NBEATSX_STRATEGY_KIND).alias("strategy_kind"),
            pl.Series("evaluation_payload", payloads),
        ]
    )


def _data_quality_tier(price_history: pl.DataFrame) -> str:
    if "source_kind" not in price_history.columns:
        return "demo_grade"
    source_kinds = {str(value) for value in price_history["source_kind"].to_list()}
    return "thesis_grade" if source_kinds == {"observed"} else "demo_grade"


def _observed_coverage_ratio(price_history: pl.DataFrame) -> float:
    if price_history.is_empty() or "source_kind" not in price_history.columns:
        return 0.0
    return price_history.filter(pl.col("source_kind") == "observed").height / price_history.height
