"""Silver feature assets for real-data rolling-origin benchmark inputs."""

from __future__ import annotations

from typing import Any

import dagster as dg
import polars as pl

from smart_arbitrage.assets import taxonomy
from smart_arbitrage.assets.bronze.market_weather import list_available_weather_tenants


@dg.asset(
    group_name=taxonomy.SILVER_REAL_DATA_BENCHMARK,
    tags=taxonomy.asset_tags(
        medallion="silver",
        domain="real_data_benchmark",
        elt_stage="transform",
        ml_stage="feature_engineering",
        evidence_scope="thesis_grade",
        market_venue="DAM",
    ),
)
def real_data_benchmark_silver_feature_frame(
    context,
    observed_market_price_history_bronze: pl.DataFrame,
    tenant_historical_weather_bronze: pl.DataFrame,
) -> pl.DataFrame:
    """Tenant-expanded Silver price/weather features for the Gold rolling benchmark."""

    frame = build_real_data_benchmark_silver_feature_frame(
        observed_market_price_history_bronze,
        tenant_historical_weather_bronze,
    )
    _add_metadata(
        context,
        {
            "rows": frame.height,
            "tenant_count": frame.select("tenant_id").n_unique() if frame.height else 0,
            "scope": "real_data_benchmark_silver_price_weather_features",
        },
    )
    return frame


def build_real_data_benchmark_silver_feature_frame(
    observed_market_price_history_bronze: pl.DataFrame,
    tenant_historical_weather_bronze: pl.DataFrame,
) -> pl.DataFrame:
    """Join market-wide observed DAM prices with tenant/location-specific weather."""

    if observed_market_price_history_bronze.height == 0:
        raise ValueError("observed_market_price_history_bronze must contain rows.")
    rows: list[pl.DataFrame] = []
    for tenant_id in _tenant_ids(tenant_historical_weather_bronze):
        rows.append(
            _join_tenant_weather_features(
                observed_market_price_history_bronze,
                tenant_historical_weather_bronze,
                tenant_id=tenant_id,
            ).with_columns(pl.lit(tenant_id).alias("tenant_id"))
        )
    return pl.concat(rows, how="diagonal_relaxed") if rows else pl.DataFrame()


def _tenant_ids(weather_history: pl.DataFrame) -> list[str]:
    if weather_history.height and "tenant_id" in weather_history.columns:
        tenant_ids = sorted(str(value) for value in weather_history.select("tenant_id").drop_nulls().to_series().unique().to_list())
        if tenant_ids:
            return tenant_ids
    return [
        str(tenant["tenant_id"])
        for tenant in list_available_weather_tenants()
        if tenant.get("tenant_id") is not None
    ]


def _join_tenant_weather_features(
    price_history: pl.DataFrame,
    weather_history: pl.DataFrame,
    *,
    tenant_id: str,
) -> pl.DataFrame:
    if weather_history.height == 0:
        return price_history
    required_columns = {"tenant_id", "timestamp"}
    if not required_columns.issubset(weather_history.columns):
        return price_history
    tenant_weather = (
        weather_history
        .filter(pl.col("tenant_id") == tenant_id)
        .select(
            [
                "timestamp",
                *[
                    column_name
                    for column_name in [
                        "temperature",
                        "wind_speed",
                        "cloudcover",
                        "precipitation",
                        "effective_solar",
                        "source_kind",
                    ]
                    if column_name in weather_history.columns
                ],
            ]
        )
        .rename(
            {
                "temperature": "weather_temperature",
                "wind_speed": "weather_wind_speed",
                "cloudcover": "weather_cloudcover",
                "precipitation": "weather_precipitation",
                "effective_solar": "weather_effective_solar",
                "source_kind": "weather_source_kind",
            }
        )
    )
    if tenant_weather.height == 0:
        return price_history
    return price_history.join(tenant_weather, on="timestamp", how="left")


def _add_metadata(context: dg.AssetExecutionContext | None, metadata: dict[str, Any]) -> None:
    if context is not None:
        context.add_output_metadata(metadata)


REAL_DATA_BENCHMARK_SILVER_ASSETS = [real_data_benchmark_silver_feature_frame]
