from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import dagster as dg
import polars as pl

from smart_arbitrage.assets.bronze.market_weather import list_available_weather_tenants
from smart_arbitrage.resources.strategy_evaluation_store import (
    get_strategy_evaluation_store,
)
from smart_arbitrage.strategy.forecast_strategy_evaluation import (
    ForecastCandidate,
    evaluate_forecast_candidates_against_oracle,
    tenant_battery_defaults_from_registry,
)


class ForecastStrategyComparisonAssetConfig(dg.Config):
    """Tenant cap for Gold forecast-strategy comparison."""

    tenant_ids_csv: str = ""


@dataclass(frozen=True, slots=True)
class _StartingSoc:
    fraction: float
    source: str


@dg.asset(group_name="gold")
def forecast_strategy_comparison_frame(
    context,
    config: ForecastStrategyComparisonAssetConfig,
    dam_price_history: pl.DataFrame,
    strict_similar_day_forecast: pl.DataFrame,
    nbeatsx_price_forecast: pl.DataFrame,
    tft_price_forecast: pl.DataFrame,
    battery_state_hourly_silver=None,
) -> pl.DataFrame:
    """Gold comparison of Silver forecasts routed through the LP and oracle benchmark."""

    rows: list[pl.DataFrame] = []
    anchor_timestamp = _anchor_from_forecast(strict_similar_day_forecast)
    for tenant_id in _tenant_ids_from_csv(config.tenant_ids_csv):
        defaults = tenant_battery_defaults_from_registry(tenant_id)
        starting_soc = _starting_soc_for_tenant(
            tenant_id=tenant_id,
            default_soc_fraction=defaults.initial_soc_fraction,
            battery_state_hourly_silver=battery_state_hourly_silver,
        )
        rows.append(
            evaluate_forecast_candidates_against_oracle(
                price_history=dam_price_history,
                tenant_id=tenant_id,
                battery_metrics=defaults.metrics,
                starting_soc_fraction=starting_soc.fraction,
                starting_soc_source=starting_soc.source,
                anchor_timestamp=anchor_timestamp,
                candidates=[
                    ForecastCandidate(
                        model_name="strict_similar_day",
                        forecast_frame=strict_similar_day_forecast,
                        point_prediction_column="predicted_price_uah_mwh",
                    ),
                    ForecastCandidate(
                        model_name="nbeatsx_silver_v0",
                        forecast_frame=nbeatsx_price_forecast,
                        point_prediction_column="predicted_price_uah_mwh",
                    ),
                    ForecastCandidate(
                        model_name="tft_silver_v0",
                        forecast_frame=tft_price_forecast,
                        point_prediction_column="predicted_price_p50_uah_mwh",
                    ),
                ],
            )
        )
    frame = pl.concat(rows) if rows else pl.DataFrame()
    get_strategy_evaluation_store().upsert_evaluation_frame(frame)
    _add_metadata(
        context,
        {
            "rows": frame.height,
            "tenant_count": frame.select("tenant_id").n_unique() if frame.height else 0,
            "forecast_candidate_count": frame.select("forecast_model_name").n_unique()
            if frame.height
            else 0,
            "market_venue": "DAM",
            "strategy_kind": "forecast_driven_lp",
        },
    )
    return frame


FORECAST_STRATEGY_GOLD_ASSETS = [forecast_strategy_comparison_frame]


def _tenant_ids_from_csv(value: str) -> list[str]:
    tenant_ids = [item.strip() for item in value.split(",") if item.strip()]
    if tenant_ids:
        return tenant_ids
    return [
        str(tenant["tenant_id"])
        for tenant in list_available_weather_tenants()
        if tenant.get("tenant_id") is not None
    ]


def _anchor_from_forecast(forecast_frame: pl.DataFrame) -> datetime:
    if "forecast_timestamp" not in forecast_frame.columns:
        raise ValueError("forecast frame is missing forecast_timestamp.")
    first_timestamp = (
        forecast_frame.sort("forecast_timestamp")
        .select("forecast_timestamp")
        .to_series()
        .item(0)
    )
    if not isinstance(first_timestamp, datetime):
        raise TypeError("forecast_timestamp column must contain datetime values.")
    return first_timestamp - timedelta(hours=1)


def _starting_soc_for_tenant(
    *,
    tenant_id: str,
    default_soc_fraction: float,
    battery_state_hourly_silver: Any,
) -> _StartingSoc:
    if (
        isinstance(battery_state_hourly_silver, pl.DataFrame)
        and battery_state_hourly_silver.height
    ):
        required_columns = {
            "tenant_id",
            "snapshot_hour",
            "soc_close",
            "telemetry_freshness",
        }
        if required_columns.issubset(battery_state_hourly_silver.columns):
            tenant_snapshots = battery_state_hourly_silver.filter(
                (pl.col("tenant_id") == tenant_id)
                & (pl.col("telemetry_freshness") == "fresh")
            ).sort("snapshot_hour")
            if tenant_snapshots.height:
                return _StartingSoc(
                    fraction=float(
                        tenant_snapshots.select("soc_close").to_series().item(-1)
                    ),
                    source="telemetry_hourly",
                )
    return _StartingSoc(fraction=default_soc_fraction, source="tenant_default")


def _add_metadata(
    context: dg.AssetExecutionContext | None, metadata: dict[str, Any]
) -> None:
    if context is not None:
        context.add_output_metadata(metadata)
