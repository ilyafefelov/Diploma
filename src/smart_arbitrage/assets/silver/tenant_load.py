"""Silver tenant net-load feature assets."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import dagster as dg
import polars as pl

from smart_arbitrage.assets import taxonomy
from smart_arbitrage.tenant_load import (
    build_tenant_historical_net_load_frame,
    build_tenant_net_load_hourly_frame,
)


@dg.asset(
    group_name=taxonomy.SILVER_TENANT_LOAD,
    tags=taxonomy.asset_tags(
        medallion="silver",
        domain="tenant_load",
        elt_stage="transform",
        ml_stage="feature_engineering",
        evidence_scope="research_only",
    ),
)
def tenant_net_load_hourly_silver(
    context,
    tenant_consumption_schedule_bronze: pl.DataFrame,
    weather_forecast_bronze: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Hourly tenant load/PV/net-load features from configured schedules and weather forecasts."""

    net_load_frame = build_tenant_net_load_hourly_frame(
        tenant_consumption_schedule_bronze,
        weather_frame=weather_forecast_bronze,
        anchor_timestamp=datetime.now(tz=UTC),
        horizon_hours=24,
    )
    _add_metadata(
        context,
        {
            "rows": net_load_frame.height,
            "tenant_count": net_load_frame.select("tenant_id").n_unique() if net_load_frame.height else 0,
            "source_kind": "configured",
            "weather_mode": "forecast_or_schedule_estimate",
        },
    )
    return net_load_frame


@dg.asset(
    group_name=taxonomy.SILVER_TENANT_LOAD,
    tags=taxonomy.asset_tags(
        medallion="silver",
        domain="tenant_load",
        elt_stage="transform",
        ml_stage="feature_engineering",
        evidence_scope="research_only",
    ),
)
def tenant_historical_net_load_silver(
    context,
    tenant_consumption_schedule_bronze: pl.DataFrame,
    real_data_benchmark_silver_feature_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Configured historical tenant net-load proxy aligned to real-data benchmark timestamps."""

    net_load_frame = build_tenant_historical_net_load_frame(
        tenant_consumption_schedule_bronze,
        real_data_benchmark_silver_feature_frame,
    )
    _add_metadata(
        context,
        {
            "rows": net_load_frame.height,
            "tenant_count": net_load_frame.select("tenant_id").n_unique()
            if net_load_frame.height
            else 0,
            "source_kind": "configured_proxy",
            "weather_mode": "historical_open_meteo_or_schedule_estimate",
            "scope": "research_only_not_measured_telemetry",
        },
    )
    return net_load_frame


TENANT_LOAD_SILVER_ASSETS = [tenant_net_load_hourly_silver, tenant_historical_net_load_silver]


def _add_metadata(context: dg.AssetExecutionContext | None, metadata: dict[str, Any]) -> None:
    if context is not None:
        context.add_output_metadata(metadata)
